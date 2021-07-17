use bytestring::ByteString;
use codec::{Connect, ConnectProperties, Login, ProtocolLevel};
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc;
use tokio_stream::Stream;

use crate::command::Command;
use crate::core::Core;
use crate::{Message, PublishBuilder, SubscribeBuilder, UnsubscribeBuilder};

pub struct ClientBuilder<A> {
    addrs: A,
    connect: Connect,
}

impl<A: ToSocketAddrs> ClientBuilder<A> {
    fn new(addrs: A) -> Self {
        Self {
            addrs,
            connect: Connect {
                level: ProtocolLevel::V5,
                keep_alive: 30,
                clean_start: false,
                client_id: ByteString::new(),
                last_will: None,
                login: None,
                properties: ConnectProperties::default(),
            },
        }
    }

    #[inline]
    pub fn keep_alive(mut self, seconds: u16) -> Self {
        self.connect.keep_alive = seconds;
        self
    }

    #[inline]
    pub fn clean_start(mut self) -> Self {
        self.connect.clean_start = true;
        self
    }

    #[inline]
    pub fn client_id(mut self, client_id: impl Into<ByteString>) -> Self {
        self.connect.client_id = client_id.into();
        self
    }

    #[inline]
    pub fn login(mut self, user: impl Into<ByteString>, password: impl Into<ByteString>) -> Self {
        self.connect.login = Some(Login {
            username: user.into(),
            password: password.into(),
        });
        self
    }

    #[inline]
    pub fn session_expiry_interval(mut self, value: u32) -> Self {
        self.connect.properties.session_expiry_interval = Some(value);
        self
    }

    #[inline]
    pub fn receive_max(mut self, value: u16) -> Self {
        self.connect.properties.receive_max = Some(value);
        self
    }

    #[inline]
    pub fn max_packet_size(mut self, value: u32) -> Self {
        self.connect.properties.max_packet_size = Some(value);
        self
    }

    #[inline]
    pub fn topic_alias_max(mut self, value: u16) -> Self {
        self.connect.properties.topic_alias_max = Some(value);
        self
    }

    #[inline]
    pub fn user_property(
        mut self,
        name: impl Into<ByteString>,
        value: impl Into<ByteString>,
    ) -> Self {
        self.connect
            .properties
            .user_properties
            .push((name.into(), value.into()));
        self
    }

    pub async fn build(self) -> Result<(Client, impl Stream<Item = Message> + Send + 'static)> {
        let addrs = tokio::net::lookup_host(self.addrs).await?.collect();
        let (tx_command, rx_msg) = Core::run(addrs, self.connect);
        Ok((
            Client { tx_command },
            tokio_stream::wrappers::ReceiverStream::new(rx_msg),
        ))
    }
}

#[derive(Clone)]
pub struct Client {
    tx_command: mpsc::Sender<Command>,
}

impl Client {
    pub fn new<A: ToSocketAddrs>(addrs: A) -> ClientBuilder<A> {
        ClientBuilder::new(addrs)
    }

    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder::new(self.tx_command.clone())
    }

    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder::new(self.tx_command.clone())
    }

    pub fn publish(&self, topic: impl Into<ByteString>) -> PublishBuilder {
        PublishBuilder::new(self.tx_command.clone(), topic.into())
    }
}
