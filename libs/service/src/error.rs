use codec::{DisconnectProperties, DisconnectReasonCode, EncodeError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("take over")]
    SessionTakenOver,

    #[error("server disconnect: {reason_code:?}")]
    ServerDisconnect {
        reason_code: DisconnectReasonCode,
        properties: DisconnectProperties,
    },

    #[error("encode packet: {0}")]
    EncodePacket(#[from] EncodeError),

    #[error("client disconnect: {reason_code:?}")]
    ClientDisconnect {
        reason_code: DisconnectReasonCode,
        properties: DisconnectProperties,
    },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    #[inline]
    pub fn server_disconnect(reason_code: DisconnectReasonCode) -> Self {
        Self::ServerDisconnect {
            reason_code,
            properties: DisconnectProperties::default(),
        }
    }

    #[inline]
    pub fn server_disconnect_with_properties(
        reason_code: DisconnectReasonCode,
        properties: DisconnectProperties,
    ) -> Self {
        Self::ServerDisconnect {
            reason_code,
            properties,
        }
    }

    #[inline]
    pub fn client_disconnect(
        reason_code: DisconnectReasonCode,
        properties: DisconnectProperties,
    ) -> Self {
        Self::ClientDisconnect {
            reason_code,
            properties,
        }
    }
}
