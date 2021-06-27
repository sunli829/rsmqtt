use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::packet::UNSUBACK;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError};

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum UnsubAckReasonCode {
    Success = 0x00,
    NoSubscriptionExisted = 0x11,
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicFilterInvalid = 0x8F,
    PacketIdentifierInUse = 0x91,
}

#[derive(Debug, Default)]
pub struct UnsubAckProperties {
    pub reason_string: Option<ByteString>,
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl UnsubAckProperties {
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
        let mut properties = UnsubAckProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::REASON_STRING => properties.reason_string = Some(data.read_string()?),
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                _ => return Err(DecodeError::InvalidUnsubAckProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(Debug)]
pub struct UnsubAck {
    pub packet_id: NonZeroU16,
    pub reason_codes: Vec<UnsubAckReasonCode>,
    pub properties: UnsubAckProperties,
}

impl UnsubAck {
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
        data.put_u8(UNSUBACK << 4);
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
        let properties = UnsubAckProperties::decode(data.split_to(properties_len))?;

        let mut reason_codes = Vec::new();
        while data.has_remaining() {
            let n_reason_code = data.read_u8()?;
            reason_codes.push(
                n_reason_code
                    .try_into()
                    .map_err(|_| DecodeError::InvalidUnsubAckReasonCode(n_reason_code))?,
            );
        }

        Ok(Self {
            packet_id,
            reason_codes,
            properties,
        })
    }
}
