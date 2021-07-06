use std::fmt::Display;

use codec::{Disconnect, DisconnectReasonCode, EncodeError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("take over")]
    SessionTakenOver,

    #[error("internal error: {0}")]
    InternalError(String),

    #[error("server disconnect")]
    ServerDisconnect(Option<Disconnect>),

    #[error("encode packet: {0}")]
    EncodePacket(#[from] EncodeError),

    #[error("client disconnect")]
    ClientDisconnect(Disconnect),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    #[inline]
    pub fn internal_error(err: impl Display) -> Self {
        Self::internal_error(err.to_string())
    }

    #[inline]
    pub fn server_disconnect(reason_code: DisconnectReasonCode) -> Self {
        Self::ServerDisconnect(Some(Disconnect::new(reason_code)))
    }
}
