use std::num::NonZeroU16;

use bytes::Bytes;
use bytestring::ByteString;
use codec::{Publish, PublishProperties, Qos};
use tokio::sync::{mpsc, oneshot};

use crate::command::{AckCommand, Command};
use crate::AckError;

pub struct Message {
    tx_command: Option<mpsc::Sender<Command>>,
    packet_id: Option<NonZeroU16>,
    topic: ByteString,
    qos: Qos,
    payload: Bytes,
    retain: bool,
    properties: PublishProperties,
}

impl Message {
    pub(crate) fn new(tx_command: Option<mpsc::Sender<Command>>, publish: Publish) -> Self {
        Self {
            tx_command,
            packet_id: publish.packet_id,
            topic: publish.topic,
            qos: publish.qos,
            payload: publish.payload,
            retain: publish.retain,
            properties: publish.properties,
        }
    }

    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub fn qos(&self) -> Qos {
        self.qos
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        self.retain
    }

    #[inline]
    pub fn content_type(&self) -> Option<&str> {
        self.properties.content_type.as_deref()
    }
}

impl Message {
    pub async fn ack(self) -> Result<(), AckError> {
        match self.qos {
            Qos::AtMostOnce => Ok(()),
            Qos::AtLeastOnce | Qos::ExactlyOnce => {
                let (tx_reply, rx_reply) = oneshot::channel();
                self.tx_command
                    .unwrap()
                    .send(Command::Ack(AckCommand {
                        packet_id: self.packet_id.unwrap(),
                        qos: Qos::AtMostOnce,
                        reply: tx_reply,
                    }))
                    .await
                    .map_err(|_| InternalError::Closed)?;
                Ok(())
            }
        }
    }
}
