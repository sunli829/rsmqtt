use std::convert::TryInto;
use std::num::NonZeroU16;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;

use crate::packet::PUBLISH;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, Level, Qos};

#[derive(Debug, Default, Clone)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<bool>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<NonZeroU16>,
    pub response_topic: Option<ByteString>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(ByteString, ByteString)>,
    pub subscription_identifiers: Vec<usize>,
    pub content_type: Option<ByteString>,
}

impl PublishProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_len!(self.payload_format_indicator, 1);
        len += prop_len!(self.message_expiry_interval, 4);
        len += prop_len!(self.topic_alias, 2);
        len += prop_data_len!(self.response_topic);
        len += prop_data_len!(self.correlation_data);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();
        len += self
            .subscription_identifiers
            .iter()
            .try_fold(0, |acc, &id| {
                bytes_remaining_length(id).map(move |sz| acc + sz)
            })?;
        len += prop_data_len!(self.content_type);

        Ok(len)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = self.payload_format_indicator {
            data.put_u8(property::PAYLOAD_FORMAT_INDICATOR);
            data.write_bool(value)?;
        }

        if let Some(value) = self.message_expiry_interval {
            data.put_u8(property::MESSAGE_EXPIRY_INTERVAL);
            data.put_u32(value);
        }

        if let Some(value) = self.topic_alias {
            data.put_u8(property::TOPIC_ALIAS);
            data.put_u16(value.get());
        }

        if let Some(value) = &self.response_topic {
            data.put_u8(property::RESPONSE_TOPIC);
            data.write_string(value)?;
        }

        if let Some(value) = &self.correlation_data {
            data.put_u8(property::CORRELATION_DATA);
            data.write_binary(value)?;
        }

        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }

        for id in &self.subscription_identifiers {
            data.put_u8(property::SUBSCRIPTION_IDENTIFIER);
            data.write_remaining_length(*id)?;
        }

        if let Some(value) = &self.content_type {
            data.put_u8(property::CONTENT_TYPE);
            data.write_string(value)?;
        }

        Ok(())
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = PublishProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::PAYLOAD_FORMAT_INDICATOR => {
                    properties.payload_format_indicator = Some(data.read_bool()?)
                }
                property::MESSAGE_EXPIRY_INTERVAL => {
                    properties.message_expiry_interval = Some(data.read_u32()?)
                }
                property::TOPIC_ALIAS => {
                    properties.topic_alias = Some(
                        data.read_u16()?
                            .try_into()
                            .map_err(|_| DecodeError::InvalidTopicAlias)?,
                    )
                }
                property::RESPONSE_TOPIC => properties.response_topic = Some(data.read_string()?),
                property::CORRELATION_DATA => {
                    properties.correlation_data = Some(data.read_binary()?)
                }
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                property::SUBSCRIPTION_IDENTIFIER => {
                    properties
                        .subscription_identifiers
                        .push(data.read_remaining_length()?);
                }
                property::CONTENT_TYPE => properties.content_type = Some(data.read_string()?),
                _ => return Err(DecodeError::InvalidPublishProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(Debug, Clone)]
pub struct Publish {
    pub dup: bool,
    pub qos: Qos,
    pub retain: bool,
    pub topic: ByteString,
    pub packet_id: Option<NonZeroU16>,
    pub properties: PublishProperties,
    pub payload: Bytes,
}

impl Publish {
    pub(crate) fn decode(mut data: Bytes, level: Level, flags: u8) -> Result<Self, DecodeError> {
        let dup = flags & 0b1000 > 0;
        let qos: Qos = {
            let n_qos = (flags & 0b110) >> 1;
            n_qos
                .try_into()
                .map_err(|_| DecodeError::InvalidQOS(n_qos))?
        };
        let retain = flags & 0b1 > 0;
        let topic = data.read_string()?;
        let packet_id = if qos != Qos::AtMostOnce {
            Some(
                data.read_u16()?
                    .try_into()
                    .map_err(|_| DecodeError::InvalidPacketId)?,
            )
        } else {
            None
        };

        let mut properties = PublishProperties::default();
        if level == Level::V5 {
            let properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= properties_len,
                DecodeError::MalformedPacket
            );
            properties = PublishProperties::decode(data.split_to(properties_len))?;
        }

        Ok(Self {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            properties,
            payload: data,
        })
    }

    #[inline]
    fn variable_header_length(&self, level: Level) -> Result<usize, EncodeError> {
        let mut len = 2 + self.topic.len() + if self.qos != Qos::AtMostOnce { 2 } else { 0 };
        let properties_len = self.properties.bytes_length()?;
        if level == Level::V5 {
            len += bytes_remaining_length(properties_len)? + properties_len;
        }
        Ok(len)
    }

    #[inline]
    fn payload_length(&self, _level: Level) -> Result<usize, EncodeError> {
        Ok(self.payload.len())
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: Level,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        ensure!(
            self.qos == Qos::AtMostOnce || self.packet_id.is_some(),
            EncodeError::RequirePacketId
        );

        let flag = {
            let mut flag = 0;
            if self.dup {
                flag |= 0b1000;
            }
            let n: u8 = self.qos.into();
            flag |= n << 1;
            if self.retain {
                flag |= 0b1;
            }
            flag
        };

        data.put_u8((PUBLISH << 4) | flag);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        data.write_string(&self.topic)?;

        if let Some(packet_id) = self.packet_id {
            data.put_u16(packet_id.get());
        }

        if level == Level::V5 {
            data.write_remaining_length(self.properties.bytes_length()?)?;
            self.properties.encode(data)?;
        }

        data.put_slice(&self.payload);
        Ok(())
    }
}
