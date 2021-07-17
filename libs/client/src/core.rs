use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::pin::Pin;

use bytestring::ByteString;
use codec::{
    Connect, Disconnect, Packet, PacketIdAllocator, PubAck, PubAckProperties, PubAckReasonCode,
    PubComp, PubCompProperties, PubCompReasonCode, PubRec, PubRecProperties, PubRecReasonCode,
    PubRel, PubRelProperties, PubRelReasonCode, Publish, Qos, SubAck, Subscribe, SubscribeFilter,
    SubscribeProperties, UnsubAck, Unsubscribe,
};
use fnv::FnvHashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant, Sleep};

use crate::command::{
    AckCommand, Command, PublishCommand, RequestCommand, SubscribeCommand, UnsubscribeCommand,
};
use crate::Message;

type Codec = codec::Codec<Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>>;

enum InternalError {
    ClientClosed,
    ProtocolError,
}

enum Request {
    Subscribe {
        subscribe: Subscribe,
    },
    Publish {
        publish: Publish,
        reply: oneshot::Sender<Result<()>>,
    },
    Request {
        publish: Publish,
        reply: oneshot::Sender<Result<Message>>,
    },
}

struct ConnectedState {
    codec: Codec,
    packet_id_allocator: PacketIdAllocator,
    keep_alive_delay: Pin<Box<Sleep>>,
    inflight_packets: FnvHashMap<NonZeroU16, InflightPacket>,
    uncompleted_messages: FnvHashMap<NonZeroU16, Message>,
}

enum State {
    Connecting,
    Connected(ConnectedState),
}

pub struct Core {
    addrs: Vec<SocketAddr>,
    connect: Connect,
    keep_alive: u16,
    tx_command: mpsc::Sender<Command>,
    rx_command: mpsc::Receiver<Command>,
    subscriptions: HashMap<ByteString, SubscribeFilter>,
    tx_msg: mpsc::Sender<Message>,
    req_id: u64,
}

impl Core {
    pub fn run(
        addrs: Vec<SocketAddr>,
        connect: Connect,
    ) -> (mpsc::Sender<Command>, mpsc::Receiver<Message>) {
        let (tx_command, rx_command) = mpsc::channel(16);
        let (tx_msg, rx_msg) = mpsc::channel(16);
        let core = Self {
            addrs,
            keep_alive: connect.keep_alive,
            connect,
            tx_command: tx_command.clone(),
            rx_command,
            subscriptions: HashMap::new(),
            tx_msg,
            req_id: 1,
        };
        tokio::spawn(core.client_loop());
        (tx_command, rx_msg)
    }

    async fn client_loop(mut self) {
        let mut state = State::Connecting;

        loop {
            match &mut state {
                State::Connecting => match self.do_connect().await {
                    Ok(connected_state) => {
                        state = State::Connected(connected_state);
                    }
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            "failed to connect to broker",
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
                State::Connected(connected_state) => {
                    if let Err(err) = self.do_connected(connected_state).await {
                        tracing::error!(
                            error = %err,
                            "connection error",
                        );

                        for (_, InflightPacket { reply, .. }) in
                            std::mem::take(&mut connected_state.inflight_packets)
                        {
                            if let Some(reply) = reply {
                                //reply.send(Err(err.clone())).ok();
                            }
                        }

                        state = State::Connecting;
                    }
                }
            }
        }
    }

    async fn do_connect(&mut self) -> Result<ConnectedState> {
        let stream = TcpStream::connect(&*self.addrs).await?;
        let (reader, writer) = stream.into_split();
        let mut connected_state = ConnectedState {
            codec: Codec::new(Box::new(reader), Box::new(writer)),
            packet_id_allocator: PacketIdAllocator::default(),
            keep_alive_delay: Box::pin(tokio::time::sleep(Duration::from_secs(
                self.keep_alive as u64,
            ))),
            inflight_packets: FnvHashMap::default(),
            uncompleted_messages: FnvHashMap::default(),
        };

        // connect
        connected_state
            .codec
            .encode(&Packet::Connect(self.connect.clone()))
            .await?;

        let packet = receive_packet(&mut connected_state.codec)
            .await?
            .ok_or(Error::DisconnectByServer(None))?;
        let conn_ack = match packet {
            Packet::ConnAck(conn_ack) => conn_ack,
            _ => anyhow::bail!("protocol error"),
        };

        if !conn_ack.reason_code.is_success() {
            return Err(Error::Handshake(conn_ack.reason_code));
        }

        if let Some(server_keep_alive) = conn_ack.properties.server_keep_alive {
            self.keep_alive = server_keep_alive;
        }

        // re-subscribe
        if !conn_ack.session_present && !self.subscriptions.is_empty() {
            let packet_id = connected_state.packet_id_allocator.take();
            let filters = self.subscriptions.values().cloned().collect();

            let packet = Packet::Subscribe(Subscribe {
                packet_id,
                properties: SubscribeProperties::default(),
                filters,
            });

            send_packet(&mut connected_state.codec, &packet).await?;
            connected_state.inflight_packets.insert(
                packet_id,
                InflightPacket {
                    packet,
                    reply: None,
                },
            );
        }

        Ok(connected_state)
    }

    async fn do_connected(&mut self, connected_state: &mut ConnectedState) -> Result<()> {
        tokio::select! {
            res = self.rx_command.recv() => {
                match res {
                    Some(command) => self.handle_command(connected_state, command).await,
                    None => Err(InternalError::ClientClosed),
                }
            }
            _ = &mut connected_state.keep_alive_delay => {
                send_packet(&mut connected_state.codec, &Packet::PingReq).await?;
                Ok(())
            },
            res = receive_packet(&mut connected_state.codec) => {
                match res {
                    Ok(Some(packet)) => {
                        connected_state.keep_alive_delay
                            .as_mut()
                            .reset(Instant::now() + Duration::from_secs(self.keep_alive as u64));
                        self.handle_packet(connected_state, packet).await
                    }
                    Ok(None) => Err(Error::DisconnectByServer(None)),
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            "connection error",
                        );
                        Err(err)
                    },
                }
            }
        }
    }

    async fn handle_command(
        &mut self,
        connected_state: &mut ConnectedState,
        command: Command,
    ) -> Result<()> {
        match command {
            Command::Subscribe(subscribe) => {
                self.handle_subscribe_command(connected_state, subscribe)
                    .await
            }
            Command::Unsubscribe(unsubscribe) => {
                self.handle_unsubscribe_command(connected_state, unsubscribe)
                    .await
            }
            Command::Publish(publish) => {
                self.handle_publish_command(connected_state, publish).await
            }
            Command::Request(request) => {
                self.handle_request_command(connected_state, request).await
            }
            Command::Ack(ack) => self.handle_ack_command(connected_state, ack).await,
        }
    }

    async fn handle_subscribe_command(
        &mut self,
        connected_state: &mut ConnectedState,
        subscribe: SubscribeCommand,
    ) -> Result<()> {
        let packet_id = connected_state.packet_id_allocator.take();
        for filter in subscribe.filters.iter().cloned() {
            self.subscriptions.insert(filter.path.clone(), filter);
        }
        let packet = Packet::Subscribe(Subscribe {
            packet_id,
            properties: SubscribeProperties::default(),
            filters: subscribe.filters,
        });
        send_packet(&mut connected_state.codec, &packet).await?;
        connected_state.inflight_packets.insert(
            packet_id,
            InflightPacket {
                packet,
                reply: None,
            },
        );
        Ok(())
    }

    async fn handle_unsubscribe_command(
        &mut self,
        connected_state: &mut ConnectedState,
        unsubscribe: UnsubscribeCommand,
    ) -> Result<()> {
        let packet_id = connected_state.packet_id_allocator.take();
        for path in &unsubscribe.filters {
            self.subscriptions.remove(path);
        }
        let packet = Packet::Unsubscribe(Unsubscribe {
            packet_id,
            filters: unsubscribe.filters,
            properties: Default::default(),
        });
        connected_state.codec.encode(&packet).await?;
        connected_state.inflight_packets.insert(
            packet_id,
            InflightPacket {
                packet,
                reply: None,
            },
        );
        Ok(())
    }

    async fn handle_publish_command(
        &mut self,
        connected_state: &mut ConnectedState,
        publish: PublishCommand,
    ) -> Result<()> {
        match publish.publish.qos {
            Qos::AtMostOnce => {
                connected_state
                    .codec
                    .encode(&Packet::Publish(publish.publish))
                    .await?;
                Ok(())
            }
            Qos::AtLeastOnce | Qos::ExactlyOnce => {
                let packet_id = connected_state.packet_id_allocator.take();
                let packet = Packet::Publish(publish.publish);
                send_packet(&mut connected_state.codec, &packet).await?;
                connected_state.inflight_packets.insert(
                    packet_id,
                    InflightPacket {
                        packet,
                        reply: publish.reply,
                    },
                );
                Ok(())
            }
        }
    }

    async fn handle_request_command(
        &mut self,
        connected_state: &mut ConnectedState,
        mut request: RequestCommand,
    ) -> Result<()> {
        request.publish.properties.correlation_data = {
            let req_id = self.req_id;
            self.req_id += 1;
            let correlation_data = req_id.to_le_bytes();
            Some(correlation_data.to_vec().into())
        };

        match request.publish.qos {
            Qos::AtMostOnce => {
                connected_state
                    .codec
                    .encode(&Packet::Publish(request.publish))
                    .await?;
                Ok(())
            }
            Qos::AtLeastOnce | Qos::ExactlyOnce => {
                let packet_id = connected_state.packet_id_allocator.take();
                let packet = Packet::Publish(request.publish);
                send_packet(&mut connected_state.codec, &packet).await?;
                connected_state.inflight_packets.insert(
                    packet_id,
                    InflightPacket {
                        packet,
                        reply: None,
                    },
                );
                Ok(())
            }
        }
    }

    async fn handle_ack_command(
        &mut self,
        connected_state: &mut ConnectedState,
        ack: AckCommand,
    ) -> Result<()> {
        match ack.qos {
            Qos::AtMostOnce => unreachable!(),
            Qos::AtLeastOnce => {
                send_packet(
                    &mut connected_state.codec,
                    &Packet::PubAck(PubAck {
                        packet_id: ack.packet_id,
                        reason_code: PubAckReasonCode::Success,
                        properties: PubAckProperties::default(),
                    }),
                )
                .await?;
                Ok(())
            }
            Qos::ExactlyOnce => {
                send_packet(
                    &mut connected_state.codec,
                    &Packet::PubComp(PubComp {
                        packet_id: ack.packet_id,
                        reason_code: PubCompReasonCode::Success,
                        properties: PubCompProperties::default(),
                    }),
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn handle_packet(
        &mut self,
        connected_state: &mut ConnectedState,
        packet: Packet,
    ) -> Result<(), InternalError> {
        match packet {
            Packet::PingResp => Ok(()),
            Packet::Publish(publish) => self.handle_publish(connected_state, publish).await,
            Packet::PubAck(pub_ack) => self.handle_pub_ack(connected_state, pub_ack).await,
            Packet::PubRec(pub_rec) => self.handle_pub_rec(connected_state, pub_rec).await,
            Packet::PubComp(pub_comp) => self.handle_pub_comp(connected_state, pub_comp).await,
            Packet::PubRel(pub_rel) => self.handle_pub_rel(connected_state, pub_rel).await,
            Packet::SubAck(sub_ack) => self.handle_sub_ack(connected_state, sub_ack).await,
            Packet::UnsubAck(ubsub_ack) => self.handle_unsub_ack(connected_state, ubsub_ack).await,
            Packet::Disconnect(disconnect) => self.handle_disconnect(disconnect).await,
            _ => Err(InternalError::ProtocolError),
        }
    }

    async fn handle_publish(
        &mut self,
        connected_state: &mut ConnectedState,
        publish: Publish,
    ) -> Result<(), InternalError> {
        match publish.qos {
            Qos::AtMostOnce => {
                let msg = Message::new(None, publish);
                self.tx_msg
                    .send(msg)
                    .await
                    .map_err(|_| InternalError::ClientClosed)?;
                Ok(())
            }
            Qos::AtLeastOnce => {
                let packet_id = publish
                    .packet_id
                    .ok_or_else(|| InternalError::protocolError)?;
                let msg = Message::new(Some(self.tx_command.clone()), publish);
                self.tx_msg
                    .send(msg)
                    .await
                    .map_err(|_| InternalError::ClientClosed)?;
                send_packet(
                    &mut connected_state.codec,
                    &Packet::PubAck(PubAck {
                        packet_id,
                        reason_code: PubAckReasonCode::Success,
                        properties: PubAckProperties::default(),
                    }),
                )
                .await?;
                Ok(())
            }
            Qos::ExactlyOnce => {
                let packet_id = publish
                    .packet_id
                    .ok_or_else(|| InternalError::ProtocolError)?;
                let msg = Message::new(Some(self.tx_command.clone()), publish);

                if connected_state
                    .uncompleted_messages
                    .contains_key(&packet_id)
                {
                    send_packet(
                        &mut connected_state.codec,
                        &Packet::PubRec(PubRec {
                            packet_id,
                            reason_code: PubRecReasonCode::PacketIdentifierInUse,
                            properties: PubRecProperties::default(),
                        }),
                    )
                    .await?;
                } else {
                    connected_state.uncompleted_messages.insert(packet_id, msg);
                    send_packet(
                        &mut connected_state.codec,
                        &Packet::PubRec(PubRec {
                            packet_id,
                            reason_code: PubRecReasonCode::Success,
                            properties: PubRecProperties::default(),
                        }),
                    )
                    .await?;
                }

                Ok(())
            }
        }
    }

    async fn handle_pub_ack(
        &mut self,
        connected_state: &mut ConnectedState,
        pub_ack: PubAck,
    ) -> Result<()> {
        if let Some(InflightPacket {
            packet: Packet::Publish(Publish { .. }),
            reply,
        }) = connected_state.inflight_packets.remove(&pub_ack.packet_id)
        {
            if pub_ack.reason_code.is_success() {
                reply.unwrap().send(Ok(())).ok();
            } else {
                reply
                    .unwrap()
                    .send(Err(Error::PubAck(pub_ack.reason_code)))
                    .ok();
            }
            Ok(())
        } else {
            Err(InternalError::ProtocolError)
        }
    }

    async fn handle_pub_rec(
        &mut self,
        connected_state: &mut ConnectedState,
        pub_rec: PubRec,
    ) -> Result<()> {
        if let Some(InflightPacket {
            packet: Packet::Publish(Publish { .. }),
            ..
        }) = connected_state.inflight_packets.get(&pub_rec.packet_id)
        {
            if pub_rec.reason_code.is_success() {
                send_packet(
                    &mut connected_state.codec,
                    &Packet::PubRel(PubRel {
                        packet_id: pub_rec.packet_id,
                        reason_code: PubRelReasonCode::Success,
                        properties: PubRelProperties::default(),
                    }),
                )
                .await?;
            } else {
                let InflightPacket { reply, .. } = connected_state
                    .inflight_packets
                    .remove(&pub_rec.packet_id)
                    .unwrap();
                reply
                    .unwrap()
                    .send(Err(Error::PubRec(pub_rec.reason_code)))
                    .ok();
            }
        } else {
            send_packet(
                &mut connected_state.codec,
                &Packet::PubRel(PubRel {
                    packet_id: pub_rec.packet_id,
                    reason_code: PubRelReasonCode::PacketIdentifierNotFound,
                    properties: PubRelProperties::default(),
                }),
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_pub_comp(
        &mut self,
        connected_state: &mut ConnectedState,
        pub_comp: PubComp,
    ) -> Result<(), InternalError> {
        if let Some(InflightPacket {
            packet: Packet::Publish(Publish { .. }),
            reply,
        }) = connected_state.inflight_packets.remove(&pub_comp.packet_id)
        {
            if pub_comp.reason_code.is_success() {
                reply.unwrap().send(Ok(())).ok();
            } else {
                reply.unwrap().send(Err(InternalError::ProtocolError)).ok();
            }
            Ok(())
        } else {
            Err(InternalError::ProtocolError)
        }
    }

    async fn handle_pub_rel(
        &mut self,
        connected_state: &mut ConnectedState,
        pub_rel: PubRel,
    ) -> Result<(), InternalError> {
        if let Some(msg) = connected_state
            .uncompleted_messages
            .remove(&pub_rel.packet_id)
        {
            self.tx_msg
                .send(msg)
                .await
                .map_err(|_| InternalError::Closed)?;
            Ok(())
        } else {
            Err(InternalError::ProtocolError)
        }
    }

    async fn handle_sub_ack(
        &mut self,
        connected_state: &mut ConnectedState,
        sub_ack: SubAck,
    ) -> Result<()> {
        if let Some(InflightPacket {
            packet: Packet::Subscribe(subscribe),
            ..
        }) = connected_state.inflight_packets.remove(&sub_ack.packet_id)
        {
            if sub_ack.reason_codes.len() != subscribe.filters.len() {
                return Err(InternalError::ProtocolError);
            }
            for (reason_code, filter) in sub_ack.reason_codes.into_iter().zip(subscribe.filters) {
                if reason_code.is_success() {
                    tracing::debug!(
                        path = %filter.path,
                        qos = ?reason_code.qos(),
                        "subscribe success"
                    );
                } else {
                    self.subscriptions.remove(&*filter.path);
                    tracing::debug!(
                        path = %filter.path,
                        reason_code = ?reason_code,
                        "subscribe failed"
                    );
                }
            }
            Ok(())
        } else {
            Err(InternalError::ProtocolError)
        }
    }

    async fn handle_unsub_ack(
        &mut self,
        connected_state: &mut ConnectedState,
        unsub_ack: UnsubAck,
    ) -> Result<()> {
        if let Some(InflightPacket {
            packet: Packet::Unsubscribe(unsubscribe),
            ..
        }) = connected_state
            .inflight_packets
            .remove(&unsub_ack.packet_id)
        {
            if unsub_ack.reason_codes.len() != unsubscribe.filters.len() {
                return Err(InternalError::ProtocolError);
            }
            for (reason_code, path) in unsub_ack.reason_codes.into_iter().zip(unsubscribe.filters) {
                if reason_code.is_success() {
                    tracing::debug!(
                        path = %path,
                        "unsubscribe success"
                    );
                } else {
                    self.subscriptions.remove(&path);
                    tracing::debug!(
                        path = %path,
                        "unsubscribe failed"
                    );
                }
            }
            Ok(())
        } else {
            Err(InternalError::ProtocolError)
        }
    }

    async fn handle_disconnect(&mut self, disconnect: Disconnect) -> Result<()> {
        Err(Error::DisconnectByServer(Some(disconnect.reason_code)))
    }
}

async fn send_packet(codec: &mut Codec, packet: &Packet) -> Result<()> {
    tracing::debug!(packet = ?packet, "send packet");
    codec.encode(packet).await?;
    Ok(())
}

async fn receive_packet(codec: &mut Codec) -> Result<Option<Packet>> {
    match codec.decode().await? {
        Some((packet, _)) => {
            tracing::debug!(packet = ?packet, "received packet");
            Ok(Some(packet))
        }
        None => Ok(None),
    }
}
