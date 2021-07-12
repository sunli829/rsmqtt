use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::packet::SUBACK;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, ProtocolLevel};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SubAckProperties {
    pub reason_string: Option<ByteString>,
    #[serde(default)]
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl SubAckProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_data_len!(self.reason_string);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();

        Ok(len)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = &self.reason_string {
            data.put_u8(property::REASON_STRING);
            data.write_string(value)?;
        }

        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }

        Ok(())
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = SubAckProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::REASON_STRING => properties.reason_string = Some(data.read_string()?),
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                _ => return Err(DecodeError::InvalidConnAckProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum SubscribeReasonCode {
    QoS0 = 0,
    QoS1 = 1,
    QoS2 = 2,
    Unspecified = 128,
    ImplementationSpecific = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PkidInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

impl SubscribeReasonCode {
    #[inline]
    pub fn is_success(&self) -> bool {
        Into::<u8>::into(*self) < 0x80
    }
}

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum SubscribeReasonCodeV4 {
    QoS0 = 0,
    QoS1 = 1,
    QoS2 = 2,
    Failure = 128,
}

impl From<SubscribeReasonCodeV4> for SubscribeReasonCode {
    fn from(reason_code: SubscribeReasonCodeV4) -> Self {
        match reason_code {
            SubscribeReasonCodeV4::QoS0 => SubscribeReasonCode::QoS0,
            SubscribeReasonCodeV4::QoS1 => SubscribeReasonCode::QoS1,
            SubscribeReasonCodeV4::QoS2 => SubscribeReasonCode::QoS2,
            SubscribeReasonCodeV4::Failure => SubscribeReasonCode::Unspecified,
        }
    }
}

impl From<SubscribeReasonCode> for SubscribeReasonCodeV4 {
    fn from(reason_code: SubscribeReasonCode) -> Self {
        match reason_code {
            SubscribeReasonCode::QoS0 => SubscribeReasonCodeV4::QoS0,
            SubscribeReasonCode::QoS1 => SubscribeReasonCodeV4::QoS1,
            SubscribeReasonCode::QoS2 => SubscribeReasonCodeV4::QoS2,
            _ => SubscribeReasonCodeV4::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SubAck {
    pub packet_id: NonZeroU16,
    pub reason_codes: Vec<SubscribeReasonCode>,
    #[serde(default)]
    pub properties: SubAckProperties,
}

impl SubAck {
    #[inline]
    fn variable_header_length(&self, level: ProtocolLevel) -> Result<usize, EncodeError> {
        let mut len = 2;
        if level == ProtocolLevel::V5 {
            let properties_len = self.properties.bytes_length()?;
            len += bytes_remaining_length(properties_len)? + self.properties.bytes_length()?;
        }
        Ok(len)
    }

    #[inline]
    fn payload_length(&self, _level: ProtocolLevel) -> Result<usize, EncodeError> {
        Ok(self.reason_codes.len())
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: ProtocolLevel,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        data.put_u8(SUBACK << 4);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        data.put_u16(self.packet_id.get());
        data.write_remaining_length(self.properties.bytes_length()?)?;
        self.properties.encode(data)?;
        for code in self.reason_codes.iter().copied() {
            data.put_u8(code.into());
        }
        Ok(())
    }

    pub(crate) fn decode(mut data: Bytes, level: ProtocolLevel) -> Result<Self, DecodeError> {
        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;

        let mut properties = SubAckProperties::default();
        if level == ProtocolLevel::V5 {
            let properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= properties_len,
                DecodeError::MalformedPacket
            );
            properties = SubAckProperties::decode(data.split_to(properties_len))?;
        }

        let mut reason_codes = Vec::new();
        while data.has_remaining() {
            let n_reason_code = data.read_u8()?;

            match level {
                ProtocolLevel::V4 => {
                    reason_codes.push(
                        TryInto::<SubscribeReasonCodeV4>::try_into(n_reason_code)
                            .map_err(|_| DecodeError::InvalidSubAckReasonCode(n_reason_code))?
                            .into(),
                    );
                }
                ProtocolLevel::V5 => {
                    reason_codes.push(
                        n_reason_code
                            .try_into()
                            .map_err(|_| DecodeError::InvalidSubAckReasonCode(n_reason_code))?,
                    );
                }
            }
        }

        Ok(Self {
            packet_id,
            reason_codes,
            properties,
        })
    }
}
