use mqttv5::{DisconnectProperties, DisconnectReasonCode, EncodeError};
use std::fmt::{self, Display, Formatter};
use thiserror::Error;

#[derive(Debug)]
pub struct MqttError {
    pub reason_code: DisconnectReasonCode,
    pub properties: DisconnectProperties,
}

impl MqttError {
    pub fn new(reason_code: DisconnectReasonCode) -> Self {
        Self {
            reason_code,
            properties: DisconnectProperties::default(),
        }
    }

    pub fn with_properties(self, properties: DisconnectProperties) -> Self {
        Self { properties, ..self }
    }
}

impl Display for MqttError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let code = Into::<u8>::into(self.reason_code);
        write!(f, "[{}]", code)
    }
}

impl std::error::Error for MqttError {}

#[derive(Debug, Error)]
pub enum Error {
    #[error("take over")]
    SessionTakeOver,

    #[error("mqtt: {0}")]
    Mqtt(#[from] MqttError),

    #[error("mqtt encode: {0}")]
    MqttEncode(#[from] EncodeError),

    #[error("client disconnect: {0}")]
    ClientDisconnect(MqttError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
