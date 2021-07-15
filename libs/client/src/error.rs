use codec::{
    ConnectReasonCode, DecodeError, DisconnectReasonCode, EncodeError, PubAckReasonCode,
    PubRecReasonCode,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Connection(String),

    #[error("encode: {0}")]
    Encode(#[from] EncodeError),

    #[error("decode: {0}")]
    Decode(#[from] DecodeError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error")]
    ProtocolError,

    #[error("disconnect by server")]
    DisconnectByServer(Option<DisconnectReasonCode>),

    #[error("handshake error: {0:?}")]
    Handshake(ConnectReasonCode),

    #[error("client closed")]
    ClientClosed,

    #[error("closed")]
    Closed,

    #[error("puback: {0:?}")]
    PubAck(PubAckReasonCode),

    #[error("pubrec: {0:?}")]
    PubRec(PubRecReasonCode),
}

pub type Result<T, E = Error> = ::std::result::Result<T, E>;
