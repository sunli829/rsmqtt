use bytes::Bytes;
use bytestring::ByteString;
use codec::{PublishProperties, Qos};
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, PublishCommand};
use crate::{Error, Message, Result};

pub struct PublishBuilder {
    tx_command: mpsc::Sender<Command>,
    topic: ByteString,
    retain: bool,
    qos: Qos,
    payload: Bytes,
    properties: PublishProperties,
}

impl PublishBuilder {
    pub(crate) fn new(tx_command: mpsc::Sender<Command>, topic: ByteString) -> Self {
        Self {
            tx_command,
            topic,
            retain: false,
            qos: Qos::AtMostOnce,
            payload: Bytes::new(),
            properties: PublishProperties::default(),
        }
    }

    #[inline]
    pub fn retain(self) -> Self {
        Self {
            retain: true,
            ..self
        }
    }

    #[inline]
    pub fn qos(self, qos: Qos) -> Self {
        Self { qos, ..self }
    }

    #[inline]
    pub fn payload(self, payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            ..self
        }
    }

    #[inline]
    pub fn content_type(mut self, ty: impl Into<ByteString>) -> Self {
        self.properties.content_type = Some(ty.into());
        self
    }

    #[inline]
    pub fn expiry_interval(mut self, seconds: u32) -> Self {
        self.properties.message_expiry_interval = Some(seconds);
        self
    }

    #[inline]
    pub fn user_property(
        mut self,
        name: impl Into<ByteString>,
        value: impl Into<ByteString>,
    ) -> Self {
        self.properties
            .user_properties
            .push((name.into(), value.into()));
        self
    }

    pub async fn send(self) -> Result<()> {
        match self.qos {
            Qos::AtMostOnce => {
                self.tx_command
                    .send(Command::Publish(PublishCommand {
                        topic: self.topic,
                        retain: self.retain,
                        qos: self.qos,
                        payload: self.payload,
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
                        topic: self.topic,
                        retain: self.retain,
                        qos: self.qos,
                        payload: self.payload,
                        reply: Some(tx_reply),
                    }))
                    .await
                    .map_err(|_| Error::Closed)?;
                rx_reply.await.map_err(|_| Error::Closed)?
            }
        }
    }

    pub async fn request(self) -> Result<Message> {
        todo!()
    }
}
