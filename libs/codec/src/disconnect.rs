use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::packet::DISCONNECT;
use crate::reader::PacketReader;
use crate::writer::bytes_remaining_length;
use crate::writer::PacketWriter;
use crate::{property, DecodeError, EncodeError, Level};

#[derive(
    Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum DisconnectReasonCode {
    NormalDisconnection = 0x00,
    DisconnectWithWillMessage = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    ServerBusy = 0x89,
    ServerShuttingDown = 0x8B,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

/// DISCONNECT Properties
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DisconnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<ByteString>,
    #[serde(default)]
    pub user_properties: Vec<(ByteString, ByteString)>,
    pub server_reference: Option<ByteString>,
}

impl DisconnectProperties {
    fn bytes_length(&self) -> Result<usize, EncodeError> {
        let mut len = 0;

        len += prop_len!(self.session_expiry_interval, 4);
        len += prop_data_len!(self.reason_string);
        len += self
            .user_properties
            .iter()
            .map(|(key, value)| prop_kv_len!(key, value))
            .sum::<usize>();
        len += prop_data_len!(self.server_reference);

        Ok(len)
    }

    fn encode(&self, data: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(value) = self.session_expiry_interval {
            data.put_u8(property::SESSION_EXPIRY_INTERVAL);
            data.put_u32(value);
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

        if let Some(value) = &self.server_reference {
            data.put_u8(property::SERVER_REFERENCE);
            data.write_string(value)?;
        }

        Ok(())
    }

    fn decode(mut data: Bytes) -> Result<Self, DecodeError> {
        let mut properties = DisconnectProperties::default();

        while data.has_remaining() {
            let flag = data.read_u8()?;

            match flag {
                property::SESSION_EXPIRY_INTERVAL => {
                    properties.session_expiry_interval = Some(data.read_u32()?)
                }
                property::REASON_STRING => properties.reason_string = Some(data.read_string()?),
                property::USER_PROPERTY => {
                    let key = data.read_string()?;
                    let value = data.read_string()?;
                    properties.user_properties.push((key, value));
                }
                property::SERVER_REFERENCE => {
                    properties.server_reference = Some(data.read_string()?)
                }
                _ => return Err(DecodeError::InvalidDisconnectProperty(flag)),
            }
        }

        Ok(properties)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.session_expiry_interval.is_none()
            && self.reason_string.is_none()
            && self.user_properties.is_empty()
            && self.server_reference.is_none()
    }
}

/// Disconnect notification
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Disconnect {
    /// Disconnect Reason Code
    pub reason_code: DisconnectReasonCode,

    /// Disconnect Properties
    #[serde(default)]
    pub properties: DisconnectProperties,
}

impl Disconnect {
    #[inline]
    fn variable_header_length(&self, level: Level) -> Result<usize, EncodeError> {
        match level {
            Level::V4 => Ok(0),
            Level::V5 => {
                if !self.properties.is_empty() {
                    let properties_len = self.properties.bytes_length()?;
                    return Ok(1
                        + bytes_remaining_length(properties_len)?
                        + self.properties.bytes_length()?);
                }

                if self.reason_code == DisconnectReasonCode::NormalDisconnection {
                    return Ok(0);
                }

                Ok(1)
            }
        }
    }

    #[inline]
    fn payload_length(&self, _level: Level) -> Result<usize, EncodeError> {
        Ok(0)
    }

    pub(crate) fn decode(mut data: Bytes, level: Level) -> Result<Self, DecodeError> {
        match level {
            Level::V4 => {
                if !data.is_empty() {
                    return Err(DecodeError::MalformedPacket);
                }
                Ok(Self {
                    reason_code: DisconnectReasonCode::NormalDisconnection,
                    properties: DisconnectProperties::default(),
                })
            }
            Level::V5 => {
                if !data.has_remaining() {
                    return Ok(Self {
                        reason_code: DisconnectReasonCode::NormalDisconnection,
                        properties: DisconnectProperties::default(),
                    });
                }

                let reason_code = {
                    let code = data.read_u8()?;
                    code.try_into()
                        .map_err(|_| DecodeError::InvalidDisconnectReasonCode(code))?
                };

                let properties = if data.has_remaining() {
                    let properties_len = data.read_remaining_length()?;
                    ensure!(
                        data.remaining() >= properties_len,
                        DecodeError::MalformedPacket
                    );
                    DisconnectProperties::decode(data.split_to(properties_len))?
                } else {
                    DisconnectProperties::default()
                };

                Ok(Self {
                    reason_code,
                    properties,
                })
            }
        }
    }

    pub(crate) fn encode(
        &self,
        data: &mut BytesMut,
        level: Level,
        max_size: usize,
    ) -> Result<(), EncodeError> {
        data.put_u8(DISCONNECT << 4);

        let size = self.variable_header_length(level)? + self.payload_length(level)?;
        ensure!(size < max_size, EncodeError::PacketTooLarge);
        data.write_remaining_length(size)?;

        if level == Level::V5 {
            if self.reason_code != DisconnectReasonCode::NormalDisconnection {
                data.put_u8(self.reason_code.into());
            }

            if !self.properties.is_empty() {
                data.write_remaining_length(self.properties.bytes_length()?)?;
                self.properties.encode(data)?;
            }
        }

        Ok(())
    }
}
