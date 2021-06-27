use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::packet::SUBACK;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError};

#[derive(Debug, Default)]
pub struct SubAckProperties {
    pub reason_string: Option<ByteString>,
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

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
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

#[derive(Debug)]
pub struct SubAck {
    pub packet_id: NonZeroU16,
    pub reason_codes: Vec<SubscribeReasonCode>,
    pub properties: SubAckProperties,
}

impl SubAck {
    #[inline]
    fn variable_header_length(&self) -> Result<usize, EncodeError> {
        let properties_len = self.properties.bytes_length()?;
        Ok(2 + bytes_remaining_length(properties_len)? + self.properties.bytes_length()?)
    }

    #[inline]
    fn payload_length(&self) -> Result<usize, EncodeError> {
        Ok(self.reason_codes.len())
    }

    pub(crate) fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        data.put_u8(SUBACK << 4);
        data.write_remaining_length(self.variable_header_length()? + self.payload_length()?)?;
        data.put_u16(self.packet_id.get());
        data.write_remaining_length(self.properties.bytes_length()?)?;
        self.properties.encode(data)?;
        for code in self.reason_codes.iter().copied() {
            data.put_u8(code.into());
        }
        Ok(())
    }

    pub(crate) fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;

        let properties_len = data.read_remaining_length()?;
        ensure!(
            data.remaining() >= properties_len,
            DecodeError::MalformedPacket
        );
        let properties = SubAckProperties::decode(data.split_to(properties_len))?;

        let mut reason_codes = Vec::new();
        while data.has_remaining() {
            let n_reason_code = data.read_u8()?;
            reason_codes.push(
                n_reason_code
                    .try_into()
                    .map_err(|_| DecodeError::InvalidSubAckReasonCode(n_reason_code))?,
            );
        }

        Ok(Self {
            packet_id,
            reason_codes,
            properties,
        })
    }
}
