use codec::PubAckReasonCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("NoMatchingSubscribers")]
    NoMatchingSubscribers,

    #[error("NoMatchingSubscribers")]
    UnspecifiedError,

    #[error("NoMatchingSubscribers")]
    ImplementationSpecificError,

    #[error("NoMatchingSubscribers")]
    NotAuthorized,

    #[error("NoMatchingSubscribers")]
    TopicNameInvalid,

    #[error("NoMatchingSubscribers")]
    PacketIdentifierInUse,

    #[error("NoMatchingSubscribers")]
    QuotaExceeded,

    #[error("NoMatchingSubscribers")]
    PayloadFormatInvalid,

    #[error("connection closed")]
    ConnectionClosed,
}

#[derive(Debug, Error)]
pub enum AckError {
    #[error("connection closed")]
    ConnectionClosed,
}
