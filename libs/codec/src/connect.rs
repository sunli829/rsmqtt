use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use serde::{Deserialize, Serialize};

use crate::packet::CONNECT;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, Login, ProtocolLevel, Qos};

const CF_USERNAME: u8 = 0b10000000;
const CF_PASSWORD: u8 = 0b01000000;
const CF_WILL_RETAIN: u8 = 0b00100000;
const CF_WILL_QOS: u8 = 0b00011000;
const CF_WILL: u8 = 0b00000100;
const CF_CLEAN_START: u8 = 0b00000010;

const QOS_SHIFT: u8 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LastWill {
    pub topic: ByteString,
    #[serde(default)]
    pub payload: Bytes,
    pub qos: Qos,
    #[serde(default)]
    pub retain: bool,
    #[serde(default)]
    pub properties: WillProperties,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct WillProperties {
    pub delay_interval: Option<u32>,
    pub payload_format_indicator: Option<bool>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<ByteString>,
    pub response_topic: Option<ByteString>,
    pub correlation_data: Option<Bytes>,
    #[serde(default)]
    pub user_properties: Vec<(ByteString, ByteString)>,
}

impl WillProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_len!(self.delay_interval, 4);
        len += prop_len!(self.payload_format_indicator, 1);
        len += prop_len!(self.message_expiry_interval, 4);
        len += prop_data_len!(self.content_type);
        len += prop_data_len!(self.response_topic);
        len += prop_data_len!(self.correlation_data);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();

        Ok(len)
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = WillProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::WILL_DELAY_INTERVAL => properties.delay_interval = Some(data.read_u32()?),
                property::PAYLOAD_FORMAT_INDICATOR => {
                    properties.payload_format_indicator = Some(data.read_bool()?)
                }
                property::MESSAGE_EXPIRY_INTERVAL => {
                    properties.message_expiry_interval = Some(data.read_u32()?)
                }
                property::CONTENT_TYPE => properties.content_type = Some(data.read_string()?),
                property::RESPONSE_TOPIC => properties.response_topic = Some(data.read_string()?),
                property::CORRELATION_DATA => {
                    properties.correlation_data = Some(data.read_binary()?)
                }
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                _ => return Err(DecodeError::InvalidWillProperty(flag)),
            }
        }

        Ok(properties)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = self.delay_interval {
            data.put_u8(property::WILL_DELAY_INTERVAL);
            data.put_u32(value);
        }

        if let Some(value) = self.payload_format_indicator {
            data.put_u8(property::PAYLOAD_FORMAT_INDICATOR);
            data.write_bool(value)?;
        }

        if let Some(value) = self.message_expiry_interval {
            data.put_u8(property::MESSAGE_EXPIRY_INTERVAL);
            data.put_u32(value);
        }

        if let Some(value) = &self.content_type {
            data.put_u8(property::CONTENT_TYPE);
            data.write_string(value)?;
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

        Ok(())
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<bool>,
    pub request_problem_info: Option<bool>,
    #[serde(default)]
    pub user_properties: Vec<(ByteString, ByteString)>,
    pub authentication_method: Option<ByteString>,
    pub authentication_data: Option<Bytes>,
}

impl ConnectProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_len!(self.session_expiry_interval, 4);
        len += prop_len!(self.receive_max, 2);
        len += prop_len!(self.max_packet_size, 4);
        len += prop_len!(self.topic_alias_max, 2);
        len += prop_len!(self.request_response_info, 1);
        len += prop_len!(self.request_problem_info, 1);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();
        len += prop_data_len!(self.authentication_method);
        len += prop_data_len!(self.authentication_data);

        Ok(len)
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = ConnectProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::SESSION_EXPIRY_INTERVAL => {
                    properties.session_expiry_interval = Some(data.read_u32()?)
                }
                property::RECEIVE_MAXIMUM => properties.receive_max = Some(data.read_u16()?),
                property::MAXIMUM_PACKET_SIZE => {
                    properties.max_packet_size = Some(data.read_u32()?)
                }
                property::TOPIC_ALIAS_MAXIMUM => {
                    properties.topic_alias_max = Some(data.read_u16()?)
                }
                property::REQUEST_RESPONSE_INFORMATION => {
                    properties.request_response_info = Some(data.read_bool()?)
                }
                property::REQUEST_PROBLEM_INFORMATION => {
                    properties.request_problem_info = Some(data.read_bool()?)
                }
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                property::AUTHENTICATION_METHOD => {
                    properties.authentication_method = Some(data.read_string()?)
                }
                property::AUTHENTICATION_DATA => {
                    properties.authentication_data = Some(data.read_binary()?)
                }
                _ => return Err(DecodeError::InvalidConnectProperty(flag)),
            }
        }

        Ok(properties)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = self.session_expiry_interval {
            data.put_u8(property::SESSION_EXPIRY_INTERVAL);
            data.put_u32(value);
        }

        if let Some(value) = self.receive_max {
            data.put_u8(property::RECEIVE_MAXIMUM);
            data.put_u16(value);
        }

        if let Some(value) = self.max_packet_size {
            data.put_u8(property::MAXIMUM_PACKET_SIZE);
            data.put_u32(value);
        }

        if let Some(value) = self.topic_alias_max {
            data.put_u8(property::TOPIC_ALIAS_MAXIMUM);
            data.put_u16(value);
        }

        if let Some(value) = self.request_response_info {
            data.put_u8(property::REQUEST_RESPONSE_INFORMATION);
            data.write_bool(value)?;
        }

        if let Some(value) = self.request_problem_info {
            data.put_u8(property::REQUEST_PROBLEM_INFORMATION);
            data.write_bool(value)?;
        }

        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }

        if let Some(value) = &self.authentication_method {
            data.put_u8(property::AUTHENTICATION_METHOD);
            data.write_string(value)?;
        }

        if let Some(value) = &self.authentication_data {
            data.put_u8(property::AUTHENTICATION_DATA);
            data.write_binary(value)?;
        }

        Ok(())
    }
}

/// Connection Request
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Connect {
    pub level: ProtocolLevel,
    #[serde(default = "default_keep_alive")]
    pub keep_alive: u16,
    #[serde(default)]
    pub clean_start: bool,
    #[serde(default)]
    pub client_id: ByteString,
    pub last_will: Option<LastWill>,
    pub login: Option<Login>,
    #[serde(default)]
    pub properties: ConnectProperties,
}

fn default_keep_alive() -> u16 {
    60
}

impl Connect {
    #[inline]
    fn variable_header_length(&self, level: ProtocolLevel) -> Result<usize, EncodeError> {
        let mut len =
            // protocol
            2 + 4 +
            // level
            1 +
            // flags
            1 +
            // keep alive
            2;
        if level == ProtocolLevel::V5 {
            let properties_len = self.properties.bytes_length()?;
            len += bytes_remaining_length(properties_len)? + self.properties.bytes_length()?;
        }
        Ok(len)
    }

    #[inline]
    fn payload_length(&self, level: ProtocolLevel) -> Result<usize, EncodeError> {
        let mut len =
            // client id
            2 + self.client_id.len();

        if let Some(last_will) = &self.last_will {
            if level == ProtocolLevel::V5 {
                // will properties
                let properties_len = self.properties.bytes_length()?;
                len += bytes_remaining_length(properties_len)?
                    + last_will.properties.bytes_length()?;
            }

            // will topic
            len += 2 + last_will.topic.len();

            // will payload
            len += 2 + last_will.payload.len();
        }

        if let Some(login) = &self.login {
            if !login.username.is_empty() {
                len += 2 + login.username.len();
            }
            if !login.password.is_empty() {
                len += 2 + login.password.len();
            }
        }

        Ok(len)
    }

    pub(crate) fn decode(mut data: Bytes, _level: ProtocolLevel) -> Result<Self, DecodeError> {
        // parse header
        let protocol = data.read_string()?;
        ensure!(protocol == "MQTT", DecodeError::InvalidProtocol(protocol));

        let n_level = data.read_u8()?;
        let level = n_level
            .try_into()
            .map_err(|_| DecodeError::UnsupportedProtocolLevel(n_level))?;

        let connect_flags = data.read_u8()?;

        if connect_flags & CF_WILL == 0 {
            // If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00) [MQTT-3.1.2-11].
            ensure!(
                connect_flags & CF_WILL_QOS == 0,
                DecodeError::InvalidConnectFlags
            );

            // If the Will Flag is set to 0, then Will Retain MUST be set to 0 [MQTT-3.1.2-13].
            ensure!(
                connect_flags & CF_WILL_RETAIN == 0,
                DecodeError::InvalidConnectFlags
            );
        }

        let will_retain = connect_flags & CF_WILL_RETAIN > 0;
        let will_qos: Qos = {
            let n_qos = (connect_flags & CF_WILL_QOS) >> QOS_SHIFT;
            n_qos
                .try_into()
                .map_err(|_| DecodeError::InvalidQOS(n_qos))?
        };
        let keep_alive = data.read_u16()?;
        let mut properties = ConnectProperties::default();

        if level == ProtocolLevel::V5 {
            // parse properties
            let properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= properties_len,
                DecodeError::MalformedPacket
            );
            properties = ConnectProperties::decode(data.split_to(properties_len))?;
        };

        // parse payload
        let client_id = data.read_string()?;

        let last_will = if connect_flags & CF_WILL > 0 {
            let will_properties_len = data.read_remaining_length()?;
            ensure!(
                data.remaining() >= will_properties_len,
                DecodeError::MalformedPacket
            );

            let mut properties = WillProperties::default();
            if level == ProtocolLevel::V5 {
                properties = WillProperties::decode(data.split_to(will_properties_len))?;
            }

            let topic = data.read_string()?;
            let payload = data.read_binary()?;
            Some(LastWill {
                topic,
                payload,
                qos: will_qos,
                retain: will_retain,
                properties,
            })
        } else {
            None
        };

        let login = {
            let username = if connect_flags & CF_USERNAME > 0 {
                Some(data.read_string()?)
            } else {
                None
            };
            let password = if connect_flags & CF_PASSWORD > 0 {
                Some(data.read_string()?)
            } else {
                None
            };

            username.map(|username| Login {
                username,
                password: password.unwrap_or_default(),
            })
        };

        Ok(Self {
            level,
            keep_alive,
            clean_start: connect_flags & CF_CLEAN_START > 0,
            client_id,
            last_will,
            login,
            properties,
        })
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: ProtocolLevel,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        data.put_u8(CONNECT << 4);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        // write variable header
        data.write_string("MQTT")?;
        data.put_u8(level.into());

        let mut flag = 0;
        if self.clean_start {
            flag |= CF_CLEAN_START;
        }
        if let Some(last_will) = &self.last_will {
            flag |= CF_WILL;
            flag |= Into::<u8>::into(last_will.qos) << QOS_SHIFT;
            if last_will.retain {
                flag |= CF_WILL_RETAIN;
            }
        }
        if let Some(login) = &self.login {
            if !login.username.is_empty() {
                flag |= CF_USERNAME;
            }
            if !login.password.is_empty() {
                flag |= CF_PASSWORD;
            }
        }

        data.put_u8(flag);

        // write payload
        data.put_u16(self.keep_alive);

        if level == ProtocolLevel::V5 {
            let properties_len = self.properties.bytes_length()?;
            data.write_remaining_length(properties_len)?;
            self.properties.encode(data)?;
        }

        data.write_string(&self.client_id)?;

        if let Some(last_will) = &self.last_will {
            if level == ProtocolLevel::V5 {
                let properties_len = last_will.properties.bytes_length()?;
                data.write_remaining_length(properties_len)?;
                last_will.properties.encode(data)?;
            }

            data.write_string(&last_will.topic)?;
            data.write_binary(&last_will.payload)?;
        }

        if let Some(login) = &self.login {
            if !login.username.is_empty() {
                data.write_string(&login.username)?;
            }
            if !login.password.is_empty() {
                data.write_string(&login.password)?;
            }
        }

        Ok(())
    }
}
