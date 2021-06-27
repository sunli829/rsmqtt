use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::packet::PUBCOMP;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError};

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum PubCompReasonCode {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[derive(Debug, Default)]
pub struct PubCompProperties {
    pub reason_string: Option<ByteString>,
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl PubCompProperties {
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
        let mut properties = PubCompProperties::default();

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
                _ => return Err(DecodeError::InvalidPubCompProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(Debug)]
pub struct PubComp {
    pub packet_id: NonZeroU16,
    pub reason_code: PubCompReasonCode,
    pub properties: PubCompProperties,
}

impl PubComp {
    #[inline]
    fn variable_header_length(&self) -> Result<usize, EncodeError> {
        if !self.properties.is_empty() {
            let properties_len = self.properties.bytes_length()?;
            return Ok(2
                + 1
                + bytes_remaining_length(properties_len)?
                + self.properties.bytes_length()?);
        }

        if self.reason_code == PubCompReasonCode::Success {
            return Ok(2);
        }

        Ok(3)
    }

    #[inline]
    fn payload_length(&self) -> Result<usize, EncodeError> {
        Ok(0)
    }

    pub(crate) fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        data.put_u8(PUBCOMP << 4);
        data.write_remaining_length(self.variable_header_length()? + self.payload_length()?)?;

        data.put_u16(self.packet_id.get());

        if self.reason_code != PubCompReasonCode::Success {
            data.put_u8(self.reason_code.into());
        }

        if !self.properties.is_empty() {
            data.write_remaining_length(self.properties.bytes_length()?)?;
            self.properties.encode(data)?;
        }

        Ok(())
    }

    pub(crate) fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;

        let reason_code = if data.has_remaining() {
            let n_reason_code = data.read_u8()?;
            n_reason_code
                .try_into()
                .map_err(|_| DecodeError::InvalidPubCompReasonCode(n_reason_code))?
        } else {
            PubCompReasonCode::Success
        };

        let properties = if data.has_remaining() {
            let properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= properties_len,
                DecodeError::MalformedPacket
            );
            PubCompProperties::decode(data)?
        } else {
            PubCompProperties::default()
        };

        Ok(Self {
            packet_id,
            reason_code,
            properties,
        })
    }
}
