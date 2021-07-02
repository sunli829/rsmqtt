use bytestring::ByteString;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("malformed packet")]
    MalformedPacket,

    #[error("unknown packet type")]
    UnknownPacketType(u8),

    #[error("packet too large")]
    PacketTooLarge,

    #[error("reserved packet type")]
    ReservedPacketType,

    #[error("invalid protocol: {0}")]
    InvalidProtocol(ByteString),

    #[error("unsupported protocol level: {0}")]
    UnsupportedProtocolLevel(u8),

    #[error("invalid connect flags")]
    InvalidConnectFlags,

    #[error("invalid QOS: {0}")]
    InvalidQOS(u8),

    #[error("invalid retain handling: {0}")]
    InvalidRetainHandling(u8),

    #[error("invalid connect property: {0}")]
    InvalidConnectProperty(u8),

    #[error("invalid conn ack property: {0}")]
    InvalidConnAckProperty(u8),

    #[error("invalid will property: {0}")]
    InvalidWillProperty(u8),

    #[error("invalid disconnect property: {0}")]
    InvalidDisconnectProperty(u8),

    #[error("invalid publish property: {0}")]
    InvalidPublishProperty(u8),

    #[error("invalid subscribe property: {0}")]
    InvalidSubscribeProperty(u8),

    #[error("invalid unsubscribe property: {0}")]
    InvalidUnsubscribeProperty(u8),

    #[error("invalid unsub ack property: {0}")]
    InvalidUnsubAckProperty(u8),

    #[error("invalid pub ack property: {0}")]
    InvalidPubAckProperty(u8),

    #[error("invalid pub rec property: {0}")]
    InvalidPubRecProperty(u8),

    #[error("invalid pub rel property: {0}")]
    InvalidPubRelProperty(u8),

    #[error("invalid pub comp property: {0}")]
    InvalidPubCompProperty(u8),

    #[error("invalid conn ack reason code: {0}")]
    InvalidConnAckReasonCode(u8),

    #[error("invalid disconnect reason code: {0}")]
    InvalidDisconnectReasonCode(u8),

    #[error("invalid pub ack reason code: {0}")]
    InvalidPubAckReasonCode(u8),

    #[error("invalid pub rec reason code: {0}")]
    InvalidPubRecReasonCode(u8),

    #[error("invalid pub rel reason code: {0}")]
    InvalidPubRelReasonCode(u8),

    #[error("invalid pub comp reason code: {0}")]
    InvalidPubCompReasonCode(u8),

    #[error("invalid sub ack reason code: {0}")]
    InvalidSubAckReasonCode(u8),

    #[error("invalid unsub ack reason code: {0}")]
    InvalidUnsubAckReasonCode(u8),

    #[error("invalid packet id: 0")]
    InvalidPacketId,

    #[error("invalid topic alias: 0")]
    InvalidTopicAlias,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("payload too large")]
    PayloadTooLarge,

    #[error("packet too large")]
    PacketTooLarge,

    #[error("require packet id")]
    RequirePacketId,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
