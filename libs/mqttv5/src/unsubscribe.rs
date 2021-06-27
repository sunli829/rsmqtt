use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::packet::UNSUBSCRIBE;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError};

#[derive(Debug, Default)]
pub struct UnsubscribeProperties {
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl UnsubscribeProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();

        Ok(len)
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = UnsubscribeProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            if flag == property::USER_PROPERTY {
                let key = data.read_string()?;
                let value = data.read_string()?;
                properties.user_properties.push((key, value));
            } else {
                return Err(DecodeError::InvalidUnsubscribeProperty(flag));
            }
        }

        Ok(properties)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Unsubscribe {
    pub packet_id: NonZeroU16,
    pub filters: Vec<ByteString>,
    pub properties: UnsubscribeProperties,
}

impl Unsubscribe {
    #[inline]
    fn variable_header_length(&self) -> Result<usize, EncodeError> {
        let properties_len = self.properties.bytes_length()?;
        Ok(2 + bytes_remaining_length(properties_len)? + self.properties.bytes_length()?)
    }

    #[inline]
    fn payload_length(&self) -> Result<usize, EncodeError> {
        Ok(self
            .filters
            .iter()
            .map(|filter| 2 + filter.len())
            .sum::<usize>())
    }

    pub(crate) fn decode(mut data: Bytes, flags: u8) -> Result<Self, DecodeError> {
        if flags & 0x0f != 0b0010 {
            return Err(DecodeError::MalformedPacket);
        }

        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;

        let properties_len = data.read_remaining_length()?;
        ensure!(
            data.remaining() >= properties_len,
            DecodeError::MalformedPacket
        );
        let properties = UnsubscribeProperties::decode(data.split_to(properties_len))?;

        let mut filters = Vec::new();
        while data.has_remaining() {
            let path = data.read_string()?;
            filters.push(path);
        }

        Ok(Self {
            packet_id,
            filters,
            properties,
        })
    }

    pub(crate) fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        data.put_u8((UNSUBSCRIBE << 4) | 0b0010);
        data.write_remaining_length(self.variable_header_length()? + self.payload_length()?)?;

        data.put_u16(self.packet_id.get());
        data.write_remaining_length(self.properties.bytes_length()?)?;
        self.properties.encode(data)?;

        for filter in &self.filters {
            data.write_string(filter)?;
        }
        Ok(())
    }
}
