use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::packet::PUBREC;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, Level};

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum PubRecReasonCode {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[derive(Debug, Default)]
pub struct PubRecProperties {
    pub reason_string: Option<ByteString>,
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl PubRecProperties {
    #[inline]
    fn is_empty(&self) -> bool {
        self.reason_string.is_none() && self.user_properties.is_empty()
    }

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
        let mut properties = PubRecProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::REASON_STRING => {
                    properties.reason_string = Some(data.read_string()?);
                }
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                _ => return Err(DecodeError::InvalidPubRecProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(Debug)]
pub struct PubRec {
    pub packet_id: NonZeroU16,
    pub reason_code: PubRecReasonCode,
    pub properties: PubRecProperties,
}

impl PubRec {
    #[inline]
    fn variable_header_length(&self, level: Level) -> Result<usize, EncodeError> {
        match level {
            Level::V4 => Ok(2),
            Level::V5 => {
                if !self.properties.is_empty() {
                    let properties_len = self.properties.bytes_length()?;
                    return Ok(2
                        + 1
                        + bytes_remaining_length(properties_len)?
                        + self.properties.bytes_length()?);
                }

                if self.reason_code == PubRecReasonCode::Success {
                    return Ok(2);
                }

                Ok(3)
            }
        }
    }

    #[inline]
    fn payload_length(&self, _level: Level) -> Result<usize, EncodeError> {
        Ok(0)
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: Level,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        data.put_u8(PUBREC << 4);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        data.put_u16(self.packet_id.get());

        if level == Level::V5 {
            if self.reason_code != PubRecReasonCode::Success {
                data.put_u8(self.reason_code.into());
            }

            if !self.properties.is_empty() {
                data.write_remaining_length(self.properties.bytes_length()?)?;
                self.properties.encode(data)?;
            }
        }

        Ok(())
    }

    pub(crate) fn decode(mut data: Bytes, level: Level) -> Result<Self, DecodeError> {
        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;
        let mut reason_code = PubRecReasonCode::Success;
        let mut properties = PubRecProperties::default();

        if level == Level::V5 {
            if data.has_remaining() {
                let n_reason_code = data.read_u8()?;
                reason_code = n_reason_code
                    .try_into()
                    .map_err(|_| DecodeError::InvalidPubRecReasonCode(n_reason_code))?;
            }

            if data.has_remaining() {
                let properties_len = data.read_remaining_length()?;
                ensure!(
                    data.remaining() >= properties_len,
                    DecodeError::MalformedPacket
                );
                properties = PubRecProperties::decode(data)?;
            }
        }

        Ok(Self {
            packet_id,
            reason_code,
            properties,
        })
    }
}
