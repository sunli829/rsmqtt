use std::convert::TryInto;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::BytesMut;
use bytestring::ByteString;
use fnv::FnvHashMap;
use mqttv5::{
    ConnAck, ConnAckProperties, Connect, ConnectReasonCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, EncodeError, LastWill, Packet, PacketEncoder, PubAck, PubAckReasonCode,
    PubComp, PubCompProperties, PubCompReasonCode, PubRec, PubRecReasonCode, PubRel,
    PubRelReasonCode, Publish, Qos, SubAck, SubAckProperties, Subscribe, SubscribeReasonCode,
    UnsubAck, UnsubAckReasonCode, Unsubscribe,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, Notify};

use crate::defaults;
use crate::error::{Error, MqttError};
use crate::filter::{self, TopicFilter};
use crate::message::Message;
use crate::server::{Control, ServerState};

pub struct Connection<W> {
    state: Arc<ServerState>,
    remote_addr: String,
    client_id: Option<ByteString>,
    control_sender: Option<mpsc::UnboundedSender<Control>>,
    notify: Arc<Notify>,
    encoder: PacketEncoder<W>,
    session_expiry_interval: u32,
    receive_in_max: usize,
    receive_out_max: usize,
    receive_in_quota: usize,
    receive_out_quota: usize,
    topic_alias_max: usize,
    max_packet_size_in: Option<u32>,
    topic_alias: FnvHashMap<NonZeroU16, ByteString>,
    keep_alive: u16,
    last_active: Instant,
    last_will: Option<LastWill>,
    last_will_expiry_interval: u32,
    next_packet_id: u16,
}

impl<W: AsyncWrite + Unpin> Connection<W> {
    fn take_packet_id(&mut self) -> NonZeroU16 {
        let id = self.next_packet_id;
        if self.next_packet_id == u16::MAX {
            self.next_packet_id = 1;
        } else {
            self.next_packet_id += 1;
        }
        id.try_into().unwrap()
    }

    async fn send_packet(&mut self, packet: &Packet) -> Result<(), Error> {
        tracing::debug!(
            remote_addr = %self.remote_addr,
            packet = ?packet,
            "send packet",
        );
        match self.encoder.encode(packet).await {
            Ok(packet_size) => {
                self.state.metrics.inc_msgs_sent(1);
                self.state.metrics.inc_bytes_sent(packet_size);
                if let Packet::Publish(publish) = packet {
                    self.state.metrics.inc_pub_bytes_sent(publish.payload.len());
                }
                Ok(())
            }
            Err(EncodeError::PayloadTooLarge) => {
                Err(MqttError::new(DisconnectReasonCode::PacketTooLarge).into())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn send_disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        properties: Option<DisconnectProperties>,
    ) -> Result<(), Error> {
        self.send_packet(&Packet::Disconnect(Disconnect {
            reason_code,
            properties: properties.unwrap_or_default(),
        }))
        .await
    }

    async fn handle_packet(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Connect(connect) => self.handle_connect(connect).await,
            Packet::Publish(publish) => self.handle_publish(publish).await,
            Packet::PubAck(pub_ack) => self.handle_pub_ack(pub_ack).await,
            Packet::PubRec(pub_rec) => self.handle_pub_rec(pub_rec).await,
            Packet::PubRel(pub_rel) => self.handle_pub_rel(pub_rel).await,
            Packet::PubComp(pub_comp) => self.handle_pub_comp(pub_comp).await,
            Packet::Subscribe(subscribe) => self.handle_subscribe(subscribe).await,
            Packet::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe).await,
            Packet::PingReq => self.handle_ping_req().await,
            Packet::Disconnect(disconnect) => self.handle_disconnect(disconnect).await,
            Packet::SubAck(_) | Packet::ConnAck(_) | Packet::UnsubAck(_) | Packet::PingResp => {
                Err(MqttError::new(DisconnectReasonCode::ProtocolError).into())
            }
        }
    }

    async fn handle_connect(&mut self, mut connect: Connect) -> Result<(), Error> {
        let mut conn_ack_properties = ConnAckProperties::default();

        if self.client_id.is_some() {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        let session_expiry_interval = match (
            connect.properties.session_expiry_interval,
            self.state.config.server.session_expiry_interval,
        ) {
            (Some(client), Some(config)) if client > config => {
                conn_ack_properties.session_expiry_interval = Some(config);
                config
            }
            (Some(client), Some(_) | None) => client,
            (None, Some(config)) => config,
            (None, None) => defaults::SESSION_EXPIRY_INTERVAL,
        };

        let keep_alive = connect.keep_alive.min(defaults::KEEP_ALIVE);
        if keep_alive != connect.keep_alive {
            conn_ack_properties.server_keep_alive = Some(keep_alive);
        }

        let receive_in_max = self
            .state
            .config
            .server
            .receive_max
            .unwrap_or(defaults::RECEIVE_IN_MAXIMUM) as usize;

        let receive_out_max = connect
            .properties
            .receive_max
            .map(|x| x as usize)
            .unwrap_or(usize::MAX);

        if self.state.config.server.maximum_qos != Some(Qos::ExactlyOnce) {
            conn_ack_properties.maximum_qos = self.state.config.server.maximum_qos;
        }

        let max_packet_size_out = connect.properties.max_packet_size;
        let max_packet_size_in = self.state.config.server.max_packet_size;
        if let Some(max_packet_size_in) = max_packet_size_in {
            conn_ack_properties.max_packet_size = Some(max_packet_size_in);
        }

        let topic_alias_max = match (
            connect.properties.topic_alias_max,
            self.state.config.server.topic_alias_max,
        ) {
            (Some(client), Some(config)) if client > config => {
                conn_ack_properties.topic_alias_max = Some(config);
                config
            }
            (Some(client), Some(_) | None) => client,
            (None, Some(config)) => config,
            (None, None) => defaults::TOPIC_ALIAS_MAX,
        };

        if let Some(last_will) = &connect.last_will {
            if Some(last_will.qos) > conn_ack_properties.maximum_qos {
                self.send_packet(&Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: ConnectReasonCode::QoSNotSupported,
                    properties: conn_ack_properties,
                }))
                .await?;
                return Ok(());
            }

            if last_will.retain && self.state.config.server.retain_available.unwrap_or(true) {
                self.send_packet(&Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: ConnectReasonCode::RetainNotSupported,
                    properties: conn_ack_properties,
                }))
                .await?;
                return Ok(());
            }
        }

        // create session
        if connect.client_id.is_empty() {
            connect.client_id = format!("auto-{}", uuid::Uuid::new_v4()).into();
            conn_ack_properties.assigned_client_identifier = Some(connect.client_id.clone());
        }

        let last_will_expiry_interval = connect
            .last_will
            .as_ref()
            .map(|last_will| last_will.properties.delay_interval)
            .flatten()
            .unwrap_or_default();

        let (session_present, notify) = match self
            .state
            .storage
            .create_session(
                connect.client_id.clone(),
                connect.clean_start,
                connect.last_will.clone(),
                session_expiry_interval,
                last_will_expiry_interval,
            )
            .await
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to create session"
                );
                return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
            }
        };

        if session_present {
            if let Some(join_handle) = self
                .state
                .session_timeouts
                .lock()
                .await
                .remove(&connect.client_id)
            {
                join_handle.abort();
            }
        }

        self.notify = notify;
        self.client_id = Some(connect.client_id.clone());
        self.keep_alive = keep_alive;
        self.receive_in_max = receive_in_max;
        self.receive_out_max = receive_out_max;
        self.receive_in_quota = receive_in_max;
        self.receive_out_quota = receive_out_max;
        self.max_packet_size_in = max_packet_size_in;
        self.topic_alias_max = topic_alias_max as usize;
        self.session_expiry_interval = session_expiry_interval;
        self.last_will_expiry_interval = last_will_expiry_interval;
        self.last_will = connect.last_will.clone();

        self.encoder
            .set_max_size(max_packet_size_out.unwrap_or(u32::MAX));

        loop {
            let mut connections = self.state.connections.write().await;
            if let Some(control_sender) = connections.get(&connect.client_id).cloned() {
                drop(connections);
                let (tx_reply, rx_reply) = oneshot::channel();
                if control_sender
                    .send(Control::SessionTakenOver(tx_reply))
                    .is_err()
                {
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
                if rx_reply.await.is_err() {
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
            } else {
                connections.insert(
                    connect.client_id.clone(),
                    self.control_sender.take().unwrap(),
                );
                break;
            }
        }

        self.send_packet(&Packet::ConnAck(ConnAck {
            session_present,
            reason_code: ConnectReasonCode::Success,
            properties: conn_ack_properties,
        }))
        .await?;
        self.state.metrics.inc_connection_count(1);

        // retry send inflight packets
        match self
            .state
            .storage
            .get_all_inflight_pub_packets(&connect.client_id)
            .await
        {
            Ok(packets) => {
                for mut publish in packets {
                    publish.dup = true;
                    self.send_packet(&Packet::Publish(publish)).await?;
                }
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to take all inflight packets"
                );
                return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
            }
        }

        Ok(())
    }

    async fn handle_publish(&mut self, mut publish: Publish) -> Result<(), Error> {
        let client_id = match self.client_id.clone() {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        self.state
            .metrics
            .inc_pub_bytes_received(publish.payload.len());
        self.state.metrics.inc_pub_msgs_received(1);

        if matches!((
            self.state.config.server.topic_alias_max,
            publish.properties.topic_alias,
        ), (Some(config), Some(client)) if client.get() > config)
        {
            // A Topic Alias value of 0 or greater than the Maximum Topic Alias is a Protocol Error, the
            // receiver uses DISCONNECT with Reason Code of 0x94 (Topic Alias invalid) as described in section 4.13.
            return Err(MqttError::new(DisconnectReasonCode::TopicAliasInvalid).into());
        }

        if publish.topic.is_empty() && publish.properties.topic_alias.is_none() {
            // It is a Protocol Error if the Topic Name is zero length and there is no Topic Alias.
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        if publish.topic.starts_with('$') {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        if publish.qos > Qos::AtMostOnce && publish.packet_id.is_none() {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        if !publish.properties.subscription_identifiers.is_empty() {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        if !filter::valid_topic(&publish.topic) {
            return Err(MqttError::new(DisconnectReasonCode::TopicNameInvalid).into());
        }

        if publish.retain && !self.state.config.server.retain_available.unwrap_or(true) {
            // If the Server included Retain Available in its CONNACK response to a Client
            // with its value set to 0 and it receives a PUBLISH packet with the RETAIN flag is
            // set to 1, then it uses the DISCONNECT Reason Code of 0x9A (Retain not supported) as
            // described in section 4.13.
            return Err(MqttError::new(DisconnectReasonCode::RetainNotSupported).into());
        }

        publish.topic = match publish.properties.topic_alias {
            Some(topic_alias) if !publish.topic.is_empty() => {
                self.topic_alias.insert(topic_alias, publish.topic.clone());
                publish.topic
            }
            Some(topic_alias) => {
                if let Some(topic) = self.topic_alias.get(&topic_alias) {
                    topic.clone()
                } else {
                    return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
                }
            }
            None if !publish.topic.is_empty() => publish.topic.clone(),
            None => {
                return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
            }
        };

        let retain = publish.retain;
        let packet_id = publish.packet_id;

        // create message
        let msg = Message::from_publish(Some(client_id.clone()), publish)
            .with_publisher(client_id.clone());

        if retain {
            // update retained message
            if let Err(err) = self
                .state
                .storage
                .update_retained_message(msg.topic().clone(), msg.clone())
                .await
            {
                tracing::warn!(
                    error = %err,
                    "failed to update retained message",
                );
            }
        }

        match msg.qos() {
            Qos::AtMostOnce => {
                if let Err(err) = self.state.storage.publish(vec![msg]).await {
                    tracing::error!(
                        error = %err,
                        "failed to publish message",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
            }
            Qos::AtLeastOnce => {
                if let Err(err) = self.state.storage.publish(vec![msg]).await {
                    tracing::error!(
                        error = %err,
                        "failed to publish message",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
                self.send_packet(&Packet::PubAck(PubAck {
                    packet_id: packet_id.unwrap(),
                    reason_code: PubAckReasonCode::Success,
                    properties: Default::default(),
                }))
                .await?;
            }
            Qos::ExactlyOnce => {
                if self.receive_in_quota == 0 {
                    self.state.metrics.inc_msg_dropped(1);
                    self.send_packet(&Packet::PubRec(PubRec {
                        packet_id: packet_id.unwrap(),
                        reason_code: PubRecReasonCode::QuotaExceeded,
                        properties: Default::default(),
                    }))
                    .await?;
                }

                match self
                    .state
                    .storage
                    .add_uncompleted_message(&client_id, packet_id.unwrap(), msg.clone())
                    .await
                {
                    Ok(()) => {
                        self.send_packet(&Packet::PubRec(PubRec {
                            packet_id: packet_id.unwrap(),
                            reason_code: PubRecReasonCode::Success,
                            properties: Default::default(),
                        }))
                        .await?;
                    }
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            "failed to save qos2 message",
                        );
                        return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_pub_ack(&mut self, pub_ack: PubAck) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        tracing::debug!(
            remote_addr = %self.remote_addr,
            client_id = %client_id,
            packet_id = pub_ack.packet_id,
            "remove inflight packet",
        );

        match self
            .state
            .storage
            .get_inflight_pub_packets(client_id, pub_ack.packet_id, true)
            .await
        {
            Ok(Some(_)) => {
                self.receive_out_quota += 1;
                Ok(())
            }
            Ok(None) => Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
            Err(err) => {
                tracing::error!(error = %err, "failed to get inflight packet");
                Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into())
            }
        }
    }

    async fn handle_pub_rec(&mut self, pub_rec: PubRec) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        if pub_rec.reason_code != PubRecReasonCode::Success {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        match self
            .state
            .storage
            .get_inflight_pub_packets(client_id, pub_rec.packet_id, false)
            .await
        {
            Ok(Some(_)) => {
                self.send_packet(&Packet::PubRel(PubRel {
                    packet_id: pub_rec.packet_id,
                    reason_code: PubRelReasonCode::Success,
                    properties: Default::default(),
                }))
                .await?;
                Ok(())
            }
            Ok(None) => Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
            Err(err) => {
                tracing::error!(error = %err, "failed to get inflight packet");
                Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into())
            }
        }
    }

    async fn handle_pub_rel(&mut self, pub_rel: PubRel) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        if pub_rel.reason_code != PubRelReasonCode::Success {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        match self
            .state
            .storage
            .get_uncompleted_message(client_id, pub_rel.packet_id, false)
            .await
        {
            Ok(Some(msg)) => {
                if let Err(err) = self.state.storage.publish(vec![msg]).await {
                    tracing::error!(
                        error = %err,
                        "failed to publish message",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }

                self.send_packet(&Packet::PubComp(PubComp {
                    packet_id: pub_rel.packet_id,
                    reason_code: PubCompReasonCode::Success,
                    properties: PubCompProperties::default(),
                }))
                .await?;

                self.receive_in_quota += 1;
            }
            Ok(None) => {
                self.send_packet(&Packet::PubComp(PubComp {
                    packet_id: pub_rel.packet_id,
                    reason_code: PubCompReasonCode::PacketIdentifierNotFound,
                    properties: PubCompProperties::default(),
                }))
                .await?;
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to get uncompleted message",
                );
                return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
            }
        }

        Ok(())
    }

    async fn handle_pub_comp(&mut self, pub_comp: PubComp) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        if pub_comp.reason_code != PubCompReasonCode::Success {
            return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
        }

        tracing::debug!(
            remote_addr = %self.remote_addr,
            client_id = %client_id,
            packet_id = pub_comp.packet_id,
            "remove inflight packet",
        );

        match self
            .state
            .storage
            .get_inflight_pub_packets(client_id, pub_comp.packet_id, true)
            .await
        {
            Ok(Some(_)) => {
                self.receive_out_quota += 1;
                self.handle_notified().await?;
            }
            Ok(None) => {
                tracing::debug!(
                    remote_addr = %self.remote_addr,
                    client_id = %client_id,
                    packet_id = pub_comp.packet_id,
                    "inflight packet not found",
                );
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to get inflight packet",
                );
                return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
            }
        }

        Ok(())
    }

    async fn handle_subscribe(&mut self, subscribe: Subscribe) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };

        let mut reason_codes = Vec::with_capacity(subscribe.filters.len());

        for filter in subscribe.filters {
            let topic_filter = match TopicFilter::try_new(&filter.path) {
                Some(filter) => filter,
                None => {
                    reason_codes.push(SubscribeReasonCode::TopicFilterInvalid);
                    continue;
                }
            };

            if !self
                .state
                .config
                .server
                .wildcard_subscription_available
                .unwrap_or(true)
                && topic_filter.has_wildcards()
            {
                reason_codes.push(SubscribeReasonCode::WildcardSubscriptionsNotSupported);
                continue;
            }

            let qos = if let Some(config) = self.state.config.server.maximum_qos {
                config.min(filter.qos)
            } else {
                filter.qos
            };

            reason_codes.push(match qos {
                Qos::AtMostOnce => SubscribeReasonCode::QoS0,
                Qos::AtLeastOnce => SubscribeReasonCode::QoS1,
                Qos::ExactlyOnce => SubscribeReasonCode::QoS2,
            });

            if let Err(err) = self
                .state
                .storage
                .subscribe(client_id, filter, topic_filter, subscribe.properties.id)
                .await
            {
                tracing::error!(
                    error = %err,
                    "failed to subscribe topic",
                );
                reason_codes.push(SubscribeReasonCode::Unspecified);
                continue;
            };
        }

        self.send_packet(&Packet::SubAck(SubAck {
            packet_id: subscribe.packet_id,
            reason_codes,
            properties: SubAckProperties::default(),
        }))
        .await?;

        Ok(())
    }

    async fn handle_unsubscribe(&mut self, unsubscribe: Unsubscribe) -> Result<(), Error> {
        let client_id = match &self.client_id {
            Some(client_id) => client_id,
            None => return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into()),
        };
        let mut reason_codes = Vec::new();

        for filter in unsubscribe.filters {
            let topic_filter = match TopicFilter::try_new(&filter) {
                Some(topic_filter) => topic_filter,
                None => {
                    reason_codes.push(UnsubAckReasonCode::TopicFilterInvalid);
                    continue;
                }
            };

            match self
                .state
                .storage
                .unsubscribe(client_id, &filter, topic_filter)
                .await
            {
                Ok(true) => reason_codes.push(UnsubAckReasonCode::Success),
                Ok(false) => reason_codes.push(UnsubAckReasonCode::NoSubscriptionExisted),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "failed to unsubscribe",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
            }
        }

        self.send_packet(&Packet::UnsubAck(UnsubAck {
            packet_id: unsubscribe.packet_id,
            reason_codes,
            properties: Default::default(),
        }))
        .await?;
        Ok(())
    }

    async fn handle_ping_req(&mut self) -> Result<(), Error> {
        self.send_packet(&Packet::PingResp).await
    }

    async fn handle_disconnect(&mut self, disconnect: Disconnect) -> Result<(), Error> {
        tracing::debug!(
            remote_addr = %self.remote_addr,
            reason_code = ?disconnect.reason_code,
            "client disconnect"
        );
        if disconnect.reason_code == DisconnectReasonCode::NormalDisconnection {
            self.last_will = None;
        }
        Err(Error::ClientDisconnect(
            MqttError::new(disconnect.reason_code).with_properties(disconnect.properties),
        ))
    }

    async fn handle_control(&mut self, control: Control) -> Result<(), Error> {
        match control {
            Control::SessionTakenOver(reply) => {
                if let Some(client_id) = self.client_id.take() {
                    self.state.connections.write().await.remove(&client_id);
                    self.state.metrics.dec_connection_count(1);
                }
                reply.send(()).ok();
                Err(Error::SessionTakeOver)
            }
        }
    }

    async fn handle_notified(&mut self) -> Result<(), Error> {
        if let Some(client_id) = self.client_id.clone() {
            if self.receive_out_quota == 0 {
                return Ok(());
            }

            let msgs = match self
                .state
                .storage
                .next_messages(&client_id, Some(self.receive_out_quota))
                .await
            {
                Ok(msgs) => msgs,
                Err(err) => {
                    tracing::warn!(
                        client_id = %client_id,
                        error = %err,
                        "failed to pull next messages",
                    );
                    Vec::new()
                }
            };
            assert!(msgs.len() <= self.receive_out_quota);

            let mut publish_err = None;
            let mut consume_count = 0;
            for msg in msgs {
                let qos = msg.qos();

                if let Err(err) = self.publish_to_client(msg).await {
                    publish_err = Some(err);
                    break;
                }
                consume_count += 1;

                if qos > Qos::AtMostOnce {
                    self.receive_out_quota -= 1;
                }
            }

            if let Some(err) = publish_err {
                tracing::debug!(
                    client_id = %client_id,
                    remote_addr = %self.remote_addr,
                    error = %err,
                    "failed to publish message to client",
                );
                return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
            }

            if consume_count > 0 {
                if let Err(err) = self
                    .state
                    .storage
                    .consume_messages(&client_id, consume_count)
                    .await
                {
                    tracing::error!(
                        error = %err,
                        "failed to consume messages",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::UnspecifiedError).into());
                }
            }
        }

        Ok(())
    }

    async fn publish_to_client(&mut self, msg: Message) -> Result<(), Error> {
        let client_id = match self.client_id.clone() {
            Some(client_id) => client_id,
            None => return Ok(()),
        };

        let mut publish = match msg.to_publish_and_update_expiry_interval() {
            Some(publish) => publish,
            None => return Ok(()),
        };

        self.state.metrics.inc_pub_msgs_sent(1);

        match publish.qos {
            Qos::AtMostOnce => self.send_packet(&Packet::Publish(publish)).await,
            Qos::AtLeastOnce | Qos::ExactlyOnce => {
                let packet_id = self.take_packet_id();
                publish.packet_id = Some(packet_id);

                tracing::debug!(
                    remote_addr = %self.remote_addr,
                    client_id = %client_id,
                    packet_id = packet_id,
                    "add inflight packet",
                );
                if let Err(err) = self
                    .state
                    .storage
                    .add_inflight_pub_packet(&client_id, publish.clone())
                    .await
                {
                    tracing::error!(
                        error = %err,
                        "failed to add inflight packet",
                    );
                    return Err(MqttError::new(DisconnectReasonCode::ProtocolError).into());
                }
                self.send_packet(&Packet::Publish(publish)).await?;
                Ok(())
            }
        }
    }
}

pub async fn run(
    mut reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin,
    remote_addr: String,
    state: Arc<ServerState>,
) {
    state.metrics.inc_socket_connections(1);

    let (control_sender, mut control_receiver) = mpsc::unbounded_channel();
    let mut connection = Connection {
        state: state.clone(),
        remote_addr,
        client_id: None,
        control_sender: Some(control_sender),
        notify: Arc::new(Notify::new()),
        encoder: PacketEncoder::new(writer),
        session_expiry_interval: 0,
        receive_in_max: 0,
        receive_out_max: 0,
        receive_in_quota: 0,
        receive_out_quota: 0,
        topic_alias_max: 0,
        max_packet_size_in: Some(defaults::MAX_PACKET_SIZE),
        topic_alias: FnvHashMap::default(),
        keep_alive: defaults::KEEP_ALIVE,
        last_active: Instant::now(),
        last_will: None,
        last_will_expiry_interval: 0,
        next_packet_id: 1,
    };
    let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(10));
    let mut data = BytesMut::new();

    loop {
        tokio::select! {
            _ = keep_alive_interval.tick() => {
                if connection.last_active.elapsed().as_secs() > connection.keep_alive as u64 {
                    tracing::debug!(
                        remote_addr = %connection.remote_addr,
                        "keep alive timeout",
                    );
                    connection.send_disconnect(DisconnectReasonCode::KeepAliveTimeout, None).await.ok();
                    break;
                }
            }
            res = Packet::decode(&mut reader, &mut data, connection.max_packet_size_in) => {
                match res {
                    Ok(Some((packet, packet_size))) => {
                        connection.state.metrics.inc_bytes_received(packet_size);
                        connection.state.metrics.inc_msgs_received(1);
                        connection.last_active = Instant::now();
                        tracing::debug!(
                            remote_addr = %connection.remote_addr,
                            packet = ?packet,
                            "receive packet",
                        );
                        match connection.handle_packet(packet).await {
                            Ok(_) => {}
                            Err(Error::Mqtt(err)) => {
                                tracing::debug!(
                                    remote_addr = %connection.remote_addr,
                                    error = %err,
                                    "mqtt error",
                                );
                                connection.send_disconnect(
                                    err.reason_code,
                                    Some(err.properties),
                                ).await.ok();
                                break;
                            }
                            Err(Error::ClientDisconnect(_)) => break,
                            Err(err) => {
                                tracing::debug!(
                                    remote_addr = %connection.remote_addr,
                                    error = %err,
                                    "error",
                                );
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::debug!(
                            remote_addr = %connection.remote_addr,
                            error = %err,
                            "decode packet",
                        );
                        break;
                    }
                }
            }
            item = control_receiver.recv() => {
                if let Some(control) = item {
                    match connection.handle_control(control).await {
                        Ok(()) => {}
                        Err(Error::SessionTakeOver) => {
                            connection.send_disconnect(
                                DisconnectReasonCode::SessionTakenOver,
                                None,
                            ).await.ok();
                            break;
                        },
                        Err(err) => {
                            tracing::debug!(
                                remote_addr = %connection.remote_addr,
                                error = %err,
                                "error",
                            );
                            break;
                        }
                    }
                }
            }
            _ = connection.notify.notified() => {
                if let Err(err) = connection.handle_notified().await {
                    tracing::debug!(
                        remote_addr = %connection.remote_addr,
                        error = %err,
                        "error",
                    );
                    break;
                }
            }
        }
    }

    if let Some(client_id) = connection.client_id {
        connection
            .state
            .connections
            .write()
            .await
            .remove(&client_id);
        connection.state.metrics.dec_connection_count(1);

        crate::server::add_session_timeout_handle(
            state.clone(),
            client_id,
            connection.last_will,
            connection.session_expiry_interval,
            connection.last_will_expiry_interval,
        )
        .await;
    }

    state.metrics.dec_socket_connections(1);
}
