use std::convert::TryInto;
use std::num::{NonZeroU16, NonZeroUsize};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::packet::SUBSCRIBE;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, ProtocolLevel, Qos};

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SubscribeProperties {
    pub id: Option<NonZeroUsize>,
    #[serde(default)]
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl SubscribeProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_remaining_length_len!(self.id);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();

        Ok(len)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = self.id {
            data.put_u8(property::SUBSCRIPTION_IDENTIFIER);
            data.write_remaining_length(value.get())?;
        }

        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }

        Ok(())
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = SubscribeProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::SUBSCRIPTION_IDENTIFIER => {
                    properties.id = Some(
                        data.read_remaining_length()?
                            .try_into()
                            .map_err(|_| DecodeError::MalformedPacket)?,
                    )
                }
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                _ => return Err(DecodeError::InvalidSubscribeProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, IntoPrimitive, TryFromPrimitive, Deserialize, Serialize,
)]
#[repr(u8)]
pub enum RetainHandling {
    /// Send retained messages at the time of the subscribe
    OnEverySubscribe = 0,

    /// Send retained messages at subscribe only if the subscription does not currently exist
    OnNewSubscribe = 1,

    /// Do not send retained messages at the time of the subscribe
    Never = 2,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SubscribeFilter {
    pub path: ByteString,
    pub qos: Qos,
    #[serde(default)]
    pub no_local: bool,
    #[serde(default)]
    pub retain_as_published: bool,
    #[serde(default = "default_retain_handling")]
    pub retain_handling: RetainHandling,
}

fn default_retain_handling() -> RetainHandling {
    RetainHandling::OnEverySubscribe
}

impl SubscribeFilter {
    fn decode(data: &mut Bytes, level: ProtocolLevel) -> Result<Self, DecodeError> {
        let path = data.read_string()?;

        match level {
            ProtocolLevel::V4 => {
                let options = data.read_u8()?;
                if options & 0b11111100 > 0 {
                    return Err(DecodeError::MalformedPacket);
                }
                let qos: Qos = {
                    let n_qos = options & 0b11;
                    n_qos
                        .try_into()
                        .map_err(|_| DecodeError::InvalidQOS(n_qos))?
                };
                Ok(Self {
                    path,
                    qos,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::OnEverySubscribe,
                })
            }
            ProtocolLevel::V5 => {
                let options = data.read_u8()?;
                let qos: Qos = {
                    let n_qos = options & 0b11;
                    n_qos
                        .try_into()
                        .map_err(|_| DecodeError::InvalidQOS(n_qos))?
                };
                let no_local = options & 0b100 > 0;
                let retain_as_published = options & 0b1000 > 0;
                let retain_handling = {
                    let n_retain_handling = (options & 0b110000) >> 4;
                    n_retain_handling
                        .try_into()
                        .map_err(|_| DecodeError::InvalidRetainHandling(n_retain_handling))?
                };

                Ok(Self {
                    path,
                    qos,
                    no_local,
                    retain_as_published,
                    retain_handling,
                })
            }
        }
    }

    fn encode(&self, data: &mut BytesMut, level: ProtocolLevel) -> Result<(), EncodeError> {
        data.write_string(&self.path)?;

        let mut flag = 0;
        flag |= Into::<u8>::into(self.qos);

        if level == ProtocolLevel::V5 {
            if self.no_local {
                flag |= 0b100;
            }
            if self.retain_as_published {
                flag |= 0b1000;
            }
            flag |= Into::<u8>::into(self.retain_handling) << 4;
        }

        data.put_u8(flag);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Subscribe {
    pub packet_id: NonZeroU16,
    #[serde(default)]
    pub properties: SubscribeProperties,
    pub filters: Vec<SubscribeFilter>,
}

impl Subscribe {
    #[inline]
    fn variable_header_length(&self, level: ProtocolLevel) -> Result<usize, EncodeError> {
        let mut len = 2;

        if level == ProtocolLevel::V5 {
            let properties_len = self.properties.bytes_length()?;
            len += bytes_remaining_length(properties_len)? + properties_len;
        }

        Ok(len)
    }

    #[inline]
    fn payload_length(&self, _level: ProtocolLevel) -> Result<usize, EncodeError> {
        let mut len = 0;
        for filter in &self.filters {
            len += 2 + filter.path.len() + 1;
        }
        Ok(len)
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: ProtocolLevel,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        data.put_u8((SUBSCRIBE << 4) | 0b0010);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        data.put_u16(self.packet_id.get());

        if level == ProtocolLevel::V5 {
            data.write_remaining_length(self.properties.bytes_length()?)?;
            self.properties.encode(data)?;
        }

        for filter in &self.filters {
            filter.encode(data, level)?;
        }
        Ok(())
    }

    pub(crate) fn decode(
        mut data: Bytes,
        level: ProtocolLevel,
        flags: u8,
    ) -> Result<Self, DecodeError> {
        // Bits 3,2,1 and 0 of the Fixed Header of the SUBSCRIBE packet are reserved and MUST be
        // set to 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed
        // and close the Network Connection [MQTT-3.8.1-1].
        ensure!((flags & 0x0f) == 0b0010, DecodeError::MalformedPacket);

        let packet_id = data
            .read_u16()?
            .try_into()
            .map_err(|_| DecodeError::InvalidPacketId)?;

        let mut properties = SubscribeProperties::default();
        if level == ProtocolLevel::V5 {
            let properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= properties_len,
                DecodeError::MalformedPacket
            );
            properties = SubscribeProperties::decode(data.split_to(properties_len))?;
        }

        // parse payload
        let mut filters = Vec::new();
        while data.has_remaining() {
            filters.push(SubscribeFilter::decode(&mut data, level)?);
        }

        Ok(Self {
            packet_id,
            properties,
            filters,
        })
    }
}
