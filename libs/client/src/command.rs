use std::num::NonZeroU16;

use bytes::Bytes;
use bytestring::ByteString;
use codec::{Qos, SubscribeFilter};
use tokio::sync::oneshot;

use crate::Result;

pub struct SubscribeCommand {
    pub filters: Vec<SubscribeFilter>,
}

pub struct UnsubscribeCommand {
    pub filters: Vec<ByteString>,
}

pub struct PublishCommand {
    pub topic: ByteString,
    pub retain: bool,
    pub qos: Qos,
    pub payload: Bytes,
    pub reply: Option<oneshot::Sender<Result<()>>>,
}

pub struct AckCommand {
    pub packet_id: NonZeroU16,
    pub qos: Qos,
}

pub enum Command {
    Subscribe(SubscribeCommand),
    Unsubscribe(UnsubscribeCommand),
    Publish(PublishCommand),
    Ack(AckCommand),
}
