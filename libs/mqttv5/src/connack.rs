use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::packet::CONNACK;
use crate::reader::PacketReader;
use crate::writer::{bytes_remaining_length, PacketWriter};
use crate::{property, DecodeError, EncodeError, Qos};

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum ConnectReasonCode {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNamePassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QoSNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeded = 159,
}

#[derive(Debug, Default)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub maximum_qos: Option<Qos>,
    pub retain_available: Option<bool>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<ByteString>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<ByteString>,
    pub user_properties: Vec<(ByteString, ByteString)>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<ByteString>,
    pub server_reference: Option<ByteString>,
    pub authentication_method: Option<ByteString>,
    pub authentication_data: Option<Bytes>,
}

impl ConnAckProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_len!(self.session_expiry_interval, 4);
        len += prop_len!(self.receive_max, 2);
        len += prop_len!(self.maximum_qos, 1);
        len += prop_len!(self.retain_available, 1);
        len += prop_len!(self.max_packet_size, 4);
        len += prop_data_len!(self.assigned_client_identifier);
        len += prop_len!(self.topic_alias_max, 2);
        len += prop_data_len!(self.reason_string);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();
        len += prop_len!(self.wildcard_subscription_available, 1);
        len += prop_len!(self.subscription_identifiers_available, 1);
        len += prop_len!(self.shared_subscription_available, 1);
        len += prop_len!(self.server_keep_alive, 2);
        len += prop_data_len!(self.response_information);
        len += prop_data_len!(self.server_reference);
        len += prop_data_len!(self.authentication_method);
        len += prop_data_len!(self.authentication_data);

        Ok(len)
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

        if let Some(value) = self.maximum_qos {
            data.put_u8(property::MAXIMUM_QOS);
            data.put_u8(value.into());
        }

        if let Some(value) = self.retain_available {
            data.put_u8(property::RETAIN_AVAILABLE);
            data.write_bool(value)?;
        }

        if let Some(value) = self.max_packet_size {
            data.put_u8(property::MAXIMUM_PACKET_SIZE);
            data.put_u32(value);
        }

        if let Some(value) = &self.assigned_client_identifier {
            data.put_u8(property::ASSIGNED_CLIENT_IDENTIFIER);
            data.write_string(value)?;
        }

        if let Some(value) = self.topic_alias_max {
            data.put_u8(property::TOPIC_ALIAS_MAXIMUM);
            data.put_u16(value);
        }

        if let Some(value) = &self.reason_string {
            data.put_u8(property::REASON_STRING);
            data.write_string(value)?;
        }

        for (key, value) in &self.user_properties {
            data.put_u8(property::USER_PROPERTY);
            data.write_string(key)?;
            data.write_string(value)?;
        }

        if let Some(value) = self.wildcard_subscription_available {
            data.put_u8(property::WILDCARD_SUBSCRIPTION_AVAILABLE);
            data.write_bool(value)?;
        }

        if let Some(value) = self.subscription_identifiers_available {
            data.put_u8(property::SUBSCRIPTION_IDENTIFIER_AVAILABLE);
            data.write_bool(value)?;
        }

        if let Some(value) = self.shared_subscription_available {
            data.put_u8(property::SHARED_SUBSCRIPTION_AVAILABLE);
            data.write_bool(value)?;
        }

        if let Some(value) = self.server_keep_alive {
            data.put_u8(property::SERVER_KEEP_ALIVE);
            data.put_u16(value);
        }

        if let Some(value) = &self.response_information {
            data.put_u8(property::RESPONSE_INFORMATION);
            data.write_string(value)?;
        }

        if let Some(value) = &self.server_reference {
            data.put_u8(property::SERVER_REFERENCE);
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

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = ConnAckProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::SESSION_EXPIRY_INTERVAL => {
                    properties.session_expiry_interval = Some(data.read_u32()?)
                }
                property::RECEIVE_MAXIMUM => properties.receive_max = Some(data.read_u16()?),
                property::MAXIMUM_QOS => {
                    let n_qos = data.read_u8()?;
                    properties.maximum_qos = Some(
                        n_qos
                            .try_into()
                            .map_err(|_| DecodeError::InvalidQOS(n_qos))?,
                    );
                }
                property::RETAIN_AVAILABLE => properties.retain_available = Some(data.read_bool()?),
                property::MAXIMUM_PACKET_SIZE => {
                    properties.max_packet_size = Some(data.read_u32()?)
                }
                property::ASSIGNED_CLIENT_IDENTIFIER => {
                    properties.assigned_client_identifier = Some(data.read_string()?)
                }
                property::TOPIC_ALIAS_MAXIMUM => {
                    properties.topic_alias_max = Some(data.read_u16()?)
                }
                property::REASON_STRING => properties.reason_string = Some(data.read_string()?),
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                property::WILDCARD_SUBSCRIPTION_AVAILABLE => {
                    properties.wildcard_subscription_available = Some(data.read_bool()?)
                }
                property::SUBSCRIPTION_IDENTIFIER_AVAILABLE => {
                    properties.subscription_identifiers_available = Some(data.read_bool()?)
                }
                property::SHARED_SUBSCRIPTION_AVAILABLE => {
                    properties.shared_subscription_available = Some(data.read_bool()?)
                }
                property::SERVER_KEEP_ALIVE => {
                    properties.server_keep_alive = Some(data.read_u16()?)
                }
                property::RESPONSE_INFORMATION => {
                    properties.response_information = Some(data.read_string()?)
                }
                property::SERVER_REFERENCE => {
                    properties.server_reference = Some(data.read_string()?)
                }
                property::AUTHENTICATION_METHOD => {
                    properties.authentication_method = Some(data.read_string()?)
                }
                property::AUTHENTICATION_DATA => {
                    properties.authentication_data = Some(data.read_binary()?)
                }
                _ => return Err(DecodeError::InvalidConnAckProperty(flag)),
            }
        }

        Ok(properties)
    }
}

#[derive(Debug)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason_code: ConnectReasonCode,
    pub properties: ConnAckProperties,
}

impl ConnAck {
    #[inline]
    fn variable_header_length(&self) -> Result<usize, EncodeError> {
        let properties_len = self.properties.bytes_length()?;
        Ok(1 + 1 + bytes_remaining_length(properties_len)? + self.properties.bytes_length()?)
    }

    #[inline]
    fn payload_length(&self) -> Result<usize, EncodeError> {
        Ok(0)
    }

    pub(crate) fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        data.put_u8(CONNACK << 4);
        data.write_remaining_length(self.variable_header_length()? + self.payload_length()?)?;
        data.put_u8({
            let mut flags = 0;
            if self.session_present {
                flags |= 0x1;
            }
            flags
        });
        data.put_u8(self.reason_code.into());

        data.write_remaining_length(self.properties.bytes_length()?)?;
        self.properties.encode(data)?;

        Ok(())
    }

    pub(crate) fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let flag = data.read_u8()?;
        if flag & 0b11111110 > 0 {
            return Err(DecodeError::MalformedPacket);
        }
        let session_present = flag & 0x1 > 0;
        let n_reason_code = data.read_u8()?;
        let reason_code = n_reason_code
            .try_into()
            .map_err(|_| DecodeError::InvalidConnAckReasonCode(n_reason_code))?;

        let properties_len = data.read_remaining_length()?;
        ensure!(
            data.remaining() >= properties_len,
            DecodeError::MalformedPacket
        );
        let properties = ConnAckProperties::decode(data.split_to(properties_len))?;

        Ok(Self {
            session_present,
            reason_code,
            properties,
        })
    }
}
