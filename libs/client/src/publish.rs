use bytes::Bytes;
use bytestring::ByteString;
use codec::{Publish, PublishProperties, Qos};
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, PublishCommand, RequestCommand};
use crate::Message;

pub struct PublishBuilder {
    tx_command: mpsc::Sender<Command>,
    publish: Publish,
}

impl PublishBuilder {
    pub(crate) fn new(tx_command: mpsc::Sender<Command>, topic: ByteString) -> Self {
        Self {
            tx_command,
            publish: Publish {
                dup: false,
                qos: Qos::AtMostOnce,
                retain: false,
                topic,
                packet_id: None,
                properties: PublishProperties::default(),
                payload: Bytes::default(),
            },
        }
    }

    #[inline]
    pub fn retain(mut self) -> Self {
        self.publish.retain = true;
        self
    }

    #[inline]
    pub fn qos(mut self, qos: Qos) -> Self {
        self.publish.qos = qos;
        self
    }

    #[inline]
    pub fn payload(mut self, payload: impl Into<Bytes>) -> Self {
        self.publish.payload = payload.into();
        self
    }

    #[inline]
    pub fn content_type(mut self, ty: impl Into<ByteString>) -> Self {
        self.publish.properties.content_type = Some(ty.into());
        self
    }

    #[inline]
    pub fn expiry_interval(mut self, seconds: u32) -> Self {
        self.publish.properties.message_expiry_interval = Some(seconds);
        self
    }

    #[inline]
    pub fn user_property(
        mut self,
        name: impl Into<ByteString>,
        value: impl Into<ByteString>,
    ) -> Self {
        self.publish
            .properties
            .user_properties
            .push((name.into(), value.into()));
        self
    }

    pub async fn send(self) -> Result<()> {
        match self.publish.qos {
            Qos::AtMostOnce => {
                self.tx_command
                    .send(Command::Publish(PublishCommand {
                        publish: self.publish,
                        reply: None,
                    }))
                    .await
                    .map_err(|_| Error::Closed)?;
                Ok(())
            }
            Qos::AtLeastOnce | Qos::ExactlyOnce => {
                let (tx_reply, rx_reply) = oneshot::channel();
                self.tx_command
                    .send(Command::Publish(PublishCommand {
                        publish: self.publish,
                        reply: Some(tx_reply),
                    }))
                    .await
                    .map_err(|_| Error::Closed)?;
                rx_reply.await.map_err(|_| Error::Closed)?
            }
        }
    }

    pub async fn request(self) -> Result<Message> {
        let (tx_reply, rx_reply) = oneshot::channel();
        self.tx_command
            .send(Command::Request(RequestCommand {
                publish: self.publish,
                reply: Some(tx_reply),
            }))
            .await
            .map_err(|_| Error::Closed)?;
        rx_reply.await.map_err(|_| Error::Closed)?
    }
}
