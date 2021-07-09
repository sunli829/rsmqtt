use std::collections::{HashMap, VecDeque};
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;

use codec::{LastWill, Publish, Qos, RetainHandling};
use fnv::FnvHashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Notify;

use crate::filter::TopicFilter;
use crate::message::Message;

#[derive(Debug)]
pub struct StorageMetrics {
    pub session_count: usize,
    pub inflight_messages_count: usize,
    pub retained_messages_count: usize,
    pub messages_count: usize,
    pub messages_bytes: usize,
    pub subscriptions_count: usize,
}

#[derive(Debug)]
pub struct FilterItem {
    pub topic_filter: TopicFilter,
    pub qos: Qos,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
    pub id: Option<NonZeroUsize>,
}

#[derive(Debug, Default)]
pub struct Filters(HashMap<String, FilterItem>);

impl Filters {
    #[inline]
    pub fn insert(&mut self, item: FilterItem) -> Option<FilterItem> {
        self.0.insert(item.topic_filter.path().to_string(), item)
    }

    #[inline]
    pub fn remove(&mut self, path: &str) -> Option<FilterItem> {
        self.0.remove(path)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn filter_message(&self, client_id: &str, msg: &Message) -> Option<Message> {
        let mut matched = false;
        let mut max_qos = Qos::AtMostOnce;
        let mut retain = msg.is_retain();
        let mut ids = Vec::new();

        if msg.is_expired() {
            return None;
        }

        for filter in self.0.values() {
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
            .with_retain(retain)
            .with_properties(properties);
            Some(msg)
        } else {
            None
        }
    }
}

struct Session {
    queue: VecDeque<Message>,
    notify: Arc<Notify>,
    subscription_filters: Filters,
    last_will: Option<LastWill>,
    session_expiry_interval: u32,
    last_will_expiry_interval: u32,
    inflight_pub_packets: VecDeque<Publish>,
    uncompleted_messages: FnvHashMap<NonZeroU16, Message>,
}

#[derive(Default)]
struct StorageInner {
    retain_messages: HashMap<String, Message>,
    sessions: HashMap<String, RwLock<Session>>,

    /// All of the share subscriptions
    ///
    /// share name -> client id -> filters
    share_subscriptions: HashMap<String, HashMap<String, Filters>>,
}

#[derive(Default)]
pub struct Storage {
    inner: RwLock<StorageInner>,
}

impl Storage {
    pub fn update_retained_message(&self, topic: &str, msg: Message) {
        let mut inner = self.inner.write();
        if msg.is_empty() {
            inner.retain_messages.remove(topic);
        } else {
            inner.retain_messages.insert(topic.to_string(), msg);
        }
    }

    pub fn create_session(
        &self,
        client_id: &str,
        clean_start: bool,
        last_will: Option<LastWill>,
        session_expiry_interval: u32,
        last_will_expiry_interval: u32,
    ) -> (bool, Arc<Notify>) {
        let mut inner = self.inner.write();
        let mut session_present = false;

        if !clean_start {
            if let Some(session) = inner.sessions.get_mut(client_id) {
                let mut session = session.write();
                session.last_will = last_will.clone();
                session.session_expiry_interval = session_expiry_interval;
                session.last_will_expiry_interval = last_will_expiry_interval;
                session_present = true;
            }
        } else {
            inner.sessions.remove(client_id);
            for clients in inner.share_subscriptions.values_mut() {
                clients.remove(client_id);
            }
        }

        if !session_present {
            let session = RwLock::new(Session {
                queue: VecDeque::new(),
                notify: Arc::new(Notify::new()),
                subscription_filters: Filters::default(),
                last_will,
                session_expiry_interval,
                last_will_expiry_interval,
                inflight_pub_packets: VecDeque::default(),
                uncompleted_messages: FnvHashMap::default(),
            });
            inner.sessions.insert(client_id.to_string(), session);
        }

        let notify = inner.sessions.get(client_id).unwrap().read().notify.clone();
        (session_present, notify)
    }

    pub fn remove_session(&self, client_id: &str) -> bool {
        let mut inner = self.inner.write();
        let mut found = false;
        if inner.sessions.remove(client_id).is_some() {
            found = true;
        }
        for clients in inner.share_subscriptions.values_mut() {
            clients.remove(client_id);
        }
        found
    }

    pub fn subscribe(&self, client_id: &str, filter: FilterItem) {
        if let Some(share_name) = filter.topic_filter.share_name().map(ToString::to_string) {
            let mut inner = self.inner.write();
            inner
                .share_subscriptions
                .entry(share_name)
                .or_default()
                .entry(client_id.to_string())
                .or_default()
                .insert(filter);
        } else {
            let inner = self.inner.read();
            let mut session = inner.sessions.get(client_id).unwrap().write();

            let retain_handling = filter.retain_handling;
            let is_new_subscribe = session.subscription_filters.insert(filter).is_none();

            let publish_retain = matches!(
                (retain_handling, is_new_subscribe),
                (RetainHandling::OnEverySubscribe, _) | (RetainHandling::OnNewSubscribe, true)
            );

            if publish_retain {
                let mut has_retain = false;

                for msg in inner.retain_messages.values() {
                    if let Some(msg) = session.subscription_filters.filter_message(client_id, msg) {
                        session.queue.push_back(msg);
                        has_retain = true;
                    }
                }

                if has_retain {
                    session.notify.notify_one();
                }
            }
        }
    }

    pub fn unsubscribe(&self, client_id: &str, filter: TopicFilter) -> bool {
        if let Some(share_name) = filter.share_name() {
            let mut inner = self.inner.write();
            let mut found = false;
            if let Some(clients) = inner.share_subscriptions.get_mut(share_name) {
                if let Some(filters) = clients.get_mut(client_id) {
                    found = filters.remove(filter.path()).is_some();
                    if filters.is_empty() {
                        clients.remove(client_id);
                    }
                }
                if clients.is_empty() {
                    inner.share_subscriptions.remove(share_name);
                }
            }
            found
        } else {
            let inner = self.inner.read();
            let mut session = inner.sessions.get(client_id).unwrap().write();
            session.subscription_filters.remove(filter.path()).is_some()
        }
    }

    pub fn next_messages(&self, client_id: &str, limit: Option<usize>) -> Vec<Message> {
        let inner = self.inner.read();
        let session = inner.sessions.get(client_id).unwrap().read();
        let mut limit = limit.unwrap_or(usize::MAX);
        let mut res = Vec::new();
        let mut offset = 0;

        if limit > 0 {
            while let Some(msg) = session.queue.get(offset) {
                offset += 1;
                res.push(msg.clone());
                limit -= 1;
                if limit == 0 {
                    break;
                }
            }
        }

        res
    }

    pub fn consume_messages(&self, client_id: &str, mut count: usize) {
        let inner = self.inner.read();
        let mut session = inner.sessions.get(client_id).unwrap().write();
        while !session.queue.is_empty() && count > 0 {
            session.queue.pop_front();
            count -= 1;
        }
    }

    pub fn publish(&self, msgs: impl IntoIterator<Item = Message>) {
        let inner = self.inner.read();
        let mut matched_clients = Vec::new();

        for msg in msgs {
            for (client_id, session) in inner.sessions.iter() {
                let session = session.upgradable_read();
                if let Some(msg) = session.subscription_filters.filter_message(client_id, &msg) {
                    let mut session = RwLockUpgradableReadGuard::upgrade(session);
                    session.queue.push_back(msg);
                    session.notify.notify_one();
                }
            }

            matched_clients.clear();
            for clients in inner.share_subscriptions.values() {
                for (client_id, filters) in clients {
                    if let Some(msg) = filters.filter_message(client_id, &msg) {
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

    pub fn add_inflight_pub_packet(&self, client_id: &str, publish: Publish) {
        let inner = self.inner.read();
        let mut session = inner.sessions.get(client_id).unwrap().write();
        session.inflight_pub_packets.push_back(publish);
    }

    pub fn get_inflight_pub_packets(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Option<Publish> {
        let inner = self.inner.read();
        if remove {
            let mut session = inner.sessions.get(client_id).unwrap().write();
            if session
                .inflight_pub_packets
                .front()
                .map(|publish| publish.packet_id == Some(packet_id))
                .unwrap_or_default()
            {
                session.inflight_pub_packets.pop_front()
            } else {
                None
            }
        } else {
            let session = inner.sessions.get(client_id).unwrap().read();
            session
                .inflight_pub_packets
                .front()
                .filter(|publish| publish.packet_id == Some(packet_id))
                .cloned()
        }
    }

    pub fn get_all_inflight_pub_packets(&self, client_id: &str) -> Vec<Publish> {
        let inner = self.inner.read();
        let session = inner.sessions.get(client_id).unwrap().read();
        session.inflight_pub_packets.iter().cloned().collect()
    }

    pub fn add_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        msg: Message,
    ) -> bool {
        let inner = self.inner.read();
        let mut session = inner.sessions.get(client_id).unwrap().write();
        if session.uncompleted_messages.contains_key(&packet_id) {
            return false;
        }
        session.uncompleted_messages.insert(packet_id, msg);
        true
    }

    pub fn remove_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
    ) -> Option<Message> {
        let inner = self.inner.read();
        let mut session = inner.sessions.get(client_id).unwrap().write();
        session.uncompleted_messages.remove(&packet_id)
    }

    pub fn metrics(&self) -> StorageMetrics {
        let inner = self.inner.read();
        StorageMetrics {
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
                .map(|clients| clients.values().map(|filters| filters.len()))
                .flatten()
                .sum::<usize>()
                + inner
                    .sessions
                    .values()
                    .map(|session| session.read().subscription_filters.len())
                    .sum::<usize>(),
        }
    }
}
