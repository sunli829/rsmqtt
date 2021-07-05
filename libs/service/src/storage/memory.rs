use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU16;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use bytestring::ByteString;
use codec::{LastWill, Publish, Qos, RetainHandling, SubscribeFilter};
use fnv::FnvHashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Notify;

use crate::filter::TopicFilter;
use crate::message::Message;
use crate::storage::{SessionInfo, Storage, StorageMetrics};

macro_rules! session_not_found {
    ($client_id:expr) => {
        anyhow::bail!("session '{}' not found", $client_id)
    };
}

#[derive(Clone)]
struct Filter {
    subscribe_filter: SubscribeFilter,
    topic_filter: TopicFilter,
    id: Option<usize>,
}

impl Deref for Filter {
    type Target = SubscribeFilter;

    fn deref(&self) -> &Self::Target {
        &self.subscribe_filter
    }
}

struct Session {
    queue: VecDeque<Message>,
    notify: Arc<Notify>,
    subscription_filters: HashMap<ByteString, Filter>,
    last_will: Option<LastWill>,
    session_expiry_interval: u32,
    last_will_expiry_interval: u32,
    inflight_pub_packets: VecDeque<Publish>,
    uncompleted_messages: FnvHashMap<NonZeroU16, Message>,
}

#[derive(Default)]
struct StorageMemoryInner {
    retain_messages: HashMap<ByteString, Message>,

    sessions: HashMap<ByteString, RwLock<Session>>,

    /// All of the share subscriptions
    ///
    /// share name -> client id -> path -> filter
    share_subscriptions: HashMap<String, HashMap<String, HashMap<ByteString, Filter>>>,
}

impl StorageMemoryInner {
    fn add_share_subscription(&mut self, share_name: &str, client_id: &str, filter: Filter) {
        self.share_subscriptions
            .entry(share_name.to_string())
            .or_default()
            .entry(client_id.to_string())
            .or_default()
            .insert(filter.path.clone(), filter);
    }

    fn remove_share_subscription(&mut self, share_name: &str, client_id: &str, path: &str) -> bool {
        let mut res = false;
        if let Some(clients) = self.share_subscriptions.get_mut(share_name) {
            if let Some(filters) = clients.get_mut(client_id) {
                res = filters.remove(path).is_some();
                if filters.is_empty() {
                    clients.remove(client_id);
                }
            }
            if clients.is_empty() {
                self.share_subscriptions.remove(share_name);
            }
        }
        res
    }
}

#[derive(Default)]
pub struct StorageMemory {
    inner: RwLock<StorageMemoryInner>,
}

#[async_trait::async_trait]
impl Storage for StorageMemory {
    async fn update_retained_message(&self, topic: ByteString, msg: Message) -> Result<()> {
        let mut inner = self.inner.write();
        if msg.is_empty() {
            inner.retain_messages.remove(&topic);
        } else {
            inner.retain_messages.insert(topic, msg);
        }
        Ok(())
    }

    async fn create_session(
        &self,
        client_id: ByteString,
        clean_start: bool,
        last_will: Option<LastWill>,
        session_expiry_interval: u32,
        last_will_expiry_interval: u32,
    ) -> Result<(bool, Arc<Notify>)> {
        let mut inner = self.inner.write();
        let mut session_present = false;

        if !clean_start {
            if let Some(session) = inner.sessions.get_mut(&client_id) {
                let mut session = session.write();
                session.last_will = last_will.clone();
                session.session_expiry_interval = session_expiry_interval;
                session.last_will_expiry_interval = last_will_expiry_interval;
                session_present = true;
            }
        } else {
            inner.sessions.remove(&client_id);
        }

        if !session_present {
            let session = RwLock::new(Session {
                queue: VecDeque::new(),
                notify: Arc::new(Notify::new()),
                subscription_filters: HashMap::new(),
                last_will,
                session_expiry_interval,
                last_will_expiry_interval,
                inflight_pub_packets: VecDeque::default(),
                uncompleted_messages: FnvHashMap::default(),
            });
            inner.sessions.insert(client_id.clone(), session);
        }

        let notify = inner
            .sessions
            .get(&client_id)
            .unwrap()
            .read()
            .notify
            .clone();
        Ok((session_present, notify))
    }

    async fn remove_session(&self, client_id: &str) -> Result<bool> {
        let mut inner = self.inner.write();
        let mut found = false;
        if inner.sessions.remove(client_id).is_some() {
            found = true;
        }
        for clients in inner.share_subscriptions.values_mut() {
            clients.remove(client_id);
        }
        Ok(found)
    }

    async fn get_sessions(&self) -> Result<Vec<SessionInfo>> {
        let inner = self.inner.read();
        Ok(inner
            .sessions
            .iter()
            .map(|(client_id, session)| {
                let session = session.read();
                SessionInfo {
                    client_id: client_id.clone(),
                    last_will: session.last_will.clone(),
                    session_expiry_interval: session.session_expiry_interval,
                    last_will_expiry_interval: session.last_will_expiry_interval,
                }
            })
            .collect())
    }

    async fn subscribe(
        &self,
        client_id: &str,
        subscribe_filter: SubscribeFilter,
        topic_filter: TopicFilter,
        id: Option<usize>,
    ) -> Result<()> {
        let filter = Filter {
            subscribe_filter,
            topic_filter,
            id,
        };

        if let Some(share_name) = filter.topic_filter.share_name().map(ToString::to_string) {
            let mut inner = self.inner.write();
            if !inner.sessions.contains_key(client_id) {
                session_not_found!(client_id)
            }
            inner.add_share_subscription(&share_name, client_id, filter);
            Ok(())
        } else {
            let inner = self.inner.read();

            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();

                let is_new_subscribe = session
                    .subscription_filters
                    .insert(filter.path.clone(), filter.clone())
                    .is_none();

                println!("***** is_new_subscribe: {}", is_new_subscribe);

                let publish_retain = matches!(
                    (filter.retain_handling, is_new_subscribe),
                    (RetainHandling::OnEverySubscribe, _) | (RetainHandling::OnNewSubscribe, true)
                );

                if publish_retain {
                    let mut has_retain = false;

                    for msg in inner.retain_messages.values() {
                        if let Some(msg) =
                            filter_message(client_id, msg, std::slice::from_ref(&filter))
                        {
                            session.queue.push_back(msg);
                            has_retain = true;
                        }
                    }

                    if has_retain {
                        session.notify.notify_one();
                    }
                }

                return Ok(());
            }

            session_not_found!(client_id)
        }
    }

    async fn unsubscribe(
        &self,
        client_id: &str,
        path: &str,
        topic_filter: TopicFilter,
    ) -> Result<bool> {
        if let Some(share_name) = topic_filter.share_name() {
            let mut inner = self.inner.write();
            if !inner.sessions.contains_key(client_id) {
                session_not_found!(client_id)
            }
            Ok(inner.remove_share_subscription(share_name, client_id, &path))
        } else {
            let inner = self.inner.read();
            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();
                return Ok(session.subscription_filters.remove(path).is_some());
            }
            session_not_found!(client_id)
        }
    }

    async fn next_messages(&self, client_id: &str, limit: Option<usize>) -> Result<Vec<Message>> {
        let inner = self.inner.read();

        if let Some(session) = inner.sessions.get(client_id) {
            let session = session.write();
            let mut limit = limit.unwrap_or(usize::MAX);
            let mut res = Vec::new();
            let mut offset = 0;

            if limit == 0 {
                return Ok(Vec::new());
            }

            while let Some(msg) = session.queue.get(offset) {
                offset += 1;

                if msg.is_expired() {
                    continue;
                }

                res.push(msg.clone());
                limit -= 1;
                if limit == 0 {
                    break;
                }
            }

            return Ok(res);
        }

        session_not_found!(client_id)
    }

    async fn consume_messages(&self, client_id: &str, mut count: usize) -> Result<()> {
        let inner = self.inner.read();

        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            while !session.queue.is_empty() && count > 0 {
                session.queue.pop_front();
                count -= 1;
            }
            return Ok(());
        }

        session_not_found!(client_id)
    }

    async fn publish(&self, msgs: Vec<Message>) -> Result<()> {
        if !msgs.is_empty() {
            let inner = self.inner.read();
            let mut matched_clients = Vec::new();

            for msg in msgs {
                for (client_id, session) in inner.sessions.iter() {
                    let session = session.upgradable_read();
                    if let Some(msg) =
                        filter_message(client_id, &msg, session.subscription_filters.values())
                    {
                        let mut session = RwLockUpgradableReadGuard::upgrade(session);
                        session.queue.push_back(msg);
                        session.notify.notify_one();
                    }
                }

                matched_clients.clear();
                for clients in inner.share_subscriptions.values() {
                    for (client_id, filters) in clients {
                        if let Some(msg) = filter_message(client_id, &msg, filters.values()) {
                            matched_clients.push((client_id, msg));
                        }
                    }

                    if !matched_clients.is_empty() {
                        let (client_id, msg) =
                            matched_clients.swap_remove(fastrand::usize(0..matched_clients.len()));
                        if let Some(session) = inner.sessions.get(client_id.as_str()) {
                            let mut session = session.write();
                            session.queue.push_back(msg);
                            session.notify.notify_one();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn add_inflight_pub_packet(&self, client_id: &str, publish: Publish) -> Result<()> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            session.inflight_pub_packets.push_back(publish);
            return Ok(());
        }
        session_not_found!(client_id)
    }

    async fn get_inflight_pub_packets(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Result<Option<Publish>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            return if remove {
                let mut session = session.write();
                if session
                    .inflight_pub_packets
                    .front()
                    .map(|publish| publish.packet_id == Some(packet_id))
                    .unwrap_or_default()
                {
                    Ok(session.inflight_pub_packets.pop_front())
                } else {
                    Ok(None)
                }
            } else {
                let session = session.read();
                Ok(session
                    .inflight_pub_packets
                    .iter()
                    .find(|publish| publish.packet_id == Some(packet_id))
                    .cloned())
            };
        }
        session_not_found!(client_id)
    }

    async fn get_all_inflight_pub_packets(&self, client_id: &str) -> Result<Vec<Publish>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let session = session.read();
            return Ok(session.inflight_pub_packets.iter().cloned().collect());
        }
        session_not_found!(client_id)
    }

    async fn add_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        msg: Message,
    ) -> Result<bool> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            if session.uncompleted_messages.contains_key(&packet_id) {
                return Ok(false);
            }
            session.uncompleted_messages.insert(packet_id, msg);
            return Ok(true);
        }
        session_not_found!(client_id)
    }

    async fn get_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Result<Option<Message>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            return if remove {
                let mut session = session.write();
                Ok(session.uncompleted_messages.remove(&packet_id))
            } else {
                let session = session.read();
                Ok(session.uncompleted_messages.get(&packet_id).cloned())
            };
        }
        session_not_found!(client_id)
    }

    async fn metrics(&self) -> Result<StorageMetrics> {
        let inner = self.inner.read();
        Ok(StorageMetrics {
            session_count: inner.sessions.len(),
            inflight_messages_count: inner
                .sessions
                .values()
                .map(|session| session.read().inflight_pub_packets.len())
                .sum::<usize>(),
            retained_messages_count: inner.retain_messages.len(),
            messages_count: inner.retain_messages.len()
                + inner
                    .sessions
                    .values()
                    .map(|session| session.read().queue.len())
                    .sum::<usize>(),
            messages_bytes: inner
                .retain_messages
                .values()
                .map(|msg| msg.payload().len())
                .sum::<usize>()
                + inner
                    .sessions
                    .values()
                    .map(|session| {
                        session
                            .read()
                            .queue
                            .iter()
                            .map(|msg| msg.payload().len())
                            .sum::<usize>()
                    })
                    .sum::<usize>(),
            subscriptions_count: inner
                .share_subscriptions
                .values()
                .map(|clients| clients.values().map(|subscriptions| subscriptions.len()))
                .flatten()
                .sum::<usize>()
                + inner
                    .sessions
                    .values()
                    .map(|session| session.read().subscription_filters.len())
                    .sum::<usize>(),
        })
    }
}

fn filter_message<'a>(
    client_id: &str,
    msg: &Message,
    filters: impl IntoIterator<Item = &'a Filter>,
) -> Option<Message> {
    let mut matched = false;
    let mut max_qos = Qos::AtMostOnce;
    let mut retain = msg.is_retain();
    let mut ids = Vec::new();

    if msg.is_expired() {
        return None;
    }

    for filter in filters {
        if filter.no_local && msg.publisher().map(|s| &**s) == Some(client_id) {
            // If no local is true, Application Messages MUST NOT be forwarded to a connection with
            // a ClientID equal to the ClientID of the publishing connection [MQTT-3.8.3-3]
            continue;
        }

        if !filter.topic_filter.matches(msg.topic()) {
            continue;
        }

        if let Some(id) = filter.id {
            // If the Client specified a Subscription Identifier for any of the overlapping
            // subscriptions the Server MUST send those Subscription Identifiers in the message
            // which is published as the result of the subscriptions [MQTT-3.3.4-3].
            //
            // If the Server sends a single copy of the message it MUST include in the PUBLISH packet
            // the Subscription Identifiers for all matching subscriptions which have a Subscription Identifiers,
            // their order is not significant [MQTT-3.3.4-4].
            ids.push(id);
        }

        // When Clients make subscriptions with Topic Filters that include wildcards, it is possible
        // for a Client’s subscriptions to overlap so that a published message might match multiple filters.
        // In this case the Server MUST deliver the message to the Client respecting the maximum QoS of all
        // the matching subscriptions [MQTT-3.3.4-2].
        max_qos = max_qos.max(filter.qos);

        if !filter.retain_as_published {
            retain = false;
        }

        matched = true;
    }

    if matched {
        let mut properties = msg.properties().clone();
        properties.subscription_identifiers = ids;
        let msg = Message::new(
            msg.topic().clone(),
            msg.qos().min(max_qos),
            msg.payload().clone(),
        )
        .with_retain(retain);
        Some(msg)
    } else {
        None
    }
}
