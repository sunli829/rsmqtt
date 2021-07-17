use std::num::NonZeroU16;

use bytestring::ByteString;
use codec::{Publish, Qos, SubscribeFilter};
use tokio::sync::oneshot;

use crate::error::PublishError;
use crate::{AckError, Message};

pub struct SubscribeCommand {
    pub filters: Vec<SubscribeFilter>,
}

pub struct UnsubscribeCommand {
    pub filters: Vec<ByteString>,
}

pub struct PublishCommand {
    pub publish: Publish,
    pub reply: oneshot::Sender<Result<()>, PublishError>,
}

pub struct RequestCommand {
    pub publish: Publish,
    pub reply: oneshot::Sender<Result<Message>>,
}

pub struct AckCommand {
    pub packet_id: NonZeroU16,
    pub qos: Qos,
    pub reply: oneshot::Sender<Result<(), AckError>>,
}

pub enum Command {
    Subscribe(SubscribeCommand),
    Unsubscribe(UnsubscribeCommand),
    Publish(PublishCommand),
    Request(RequestCommand),
    Ack(AckCommand),
}
