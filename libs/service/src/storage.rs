use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use codec::{LastWill, Publish, Qos, RetainHandling};
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::filter_util::Filter;
use crate::message::Message;
use crate::trie::Trie;

#[derive(Debug)]
pub struct StorageMetrics {
    pub session_count: usize,
    pub inflight_messages_count: usize,
    pub retained_messages_count: usize,
    pub messages_count: usize,
    pub messages_bytes: usize,
    pub subscriptions_count: usize,
    pub clients_expired: usize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FilterItem {
    pub qos: Qos,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
    pub id: Option<NonZeroUsize>,
}

struct Session {
    queue: VecDeque<Message>,
    notify: Arc<Notify>,
    last_will: Option<LastWill>,
    inflight_pub_packets: VecDeque<Publish>,
    last_will_timeout_key: Option<TimeoutKey>,
    remove_timeout_key: Option<TimeoutKey>,
}

impl Session {
    #[inline]
    fn add_message<'a>(
        &mut self,
        msg: &Message,
        filter_items: impl IntoIterator<Item = &'a FilterItem>,
    ) {
        let mut filter_items = filter_items.into_iter();
        let first_item = match filter_items.next() {
            Some(first_item) => first_item,
            None => return,
        };
        let mut qos = first_item.qos;
        let mut retain_as_published = first_item.retain_as_published;
        let mut ids = first_item.id.into_iter().collect::<Vec<_>>();

        for item in filter_items {
            // When Clients make subscriptions with Topic Filters that include wildcards, it is possible
            // for a Client’s subscriptions to overlap so that a published message might match multiple filters.
            // In this case the Server MUST deliver the message to the Client respecting the maximum QoS of all
            // the matching subscriptions [MQTT-3.3.4-2].
            qos = qos.max(item.qos);

            retain_as_published &= item.retain_as_published;

            // If the Client specified a Subscription Identifier for any of the overlapping
            // subscriptions the Server MUST send those Subscription Identifiers in the message
            // which is published as the result of the subscriptions [MQTT-3.3.4-3].
            //
            // If the Server sends a single copy of the message it MUST include in the PUBLISH packet
            // the Subscription Identifiers for all matching subscriptions which have a Subscription Identifiers,
            // their order is not significant [MQTT-3.3.4-4].
            ids.extend(item.id.into_iter());
        }

        let mut new_msg = Message::new(
            msg.topic().clone(),
            msg.qos().min(qos),
            msg.payload().clone(),
        )
        .with_properties({
            let mut properties = msg.properties().clone();
            properties.subscription_identifiers = ids;
            properties
        });

        if retain_as_published {
            new_msg = new_msg.with_retain(msg.is_retain());
        }

        self.queue.push_back(new_msg);
        self.notify.notify_one();
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct TimeoutKey {
    client_id: String,
    timeout: Instant,
}

impl PartialOrd for TimeoutKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.timeout.cmp(&other.timeout) {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => self.client_id.partial_cmp(&other.client_id),
        }
    }
}

impl Ord for TimeoutKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Default)]
struct StorageInner {
    sessions: HashMap<String, RwLock<Session>>,
    filter_tree: Trie,
    send_last_will_timeout: BTreeSet<TimeoutKey>,
    remove_timeout: BTreeSet<TimeoutKey>,
    clients_expired: usize,
}

impl StorageInner {
    pub fn deliver(&self, msgs: impl IntoIterator<Item = Message>) {
        for msg in msgs {
            if msg.is_expired() {
                continue;
            }

            for (client_id, filter_items) in self.filter_tree.matches(msg.topic()) {
                let filter_items = filter_items.into_iter().filter(|filter_item| {
                    // If no local is true, Application Messages MUST NOT be forwarded to a connection with
                    // a ClientID equal to the ClientID of the publishing connection [MQTT-3.8.3-3]
                    !filter_item.no_local || msg.from_client_id().map(|s| &**s) != Some(client_id)
                });

                if let Some(session) = self.sessions.get(client_id) {
                    let mut session = session.write();
                    session.add_message(&msg, filter_items);
                }
            }

            for (client_id, filter_items) in self.filter_tree.matches_shared(msg.topic()) {
                if let Some(session) = self.sessions.get(client_id) {
                    let mut session = session.write();
                    session.add_message(&msg, filter_items);
                }
            }
        }
    }

    fn remove_session(&mut self, client_id: &str) {
        if let Some(session) = self.sessions.remove(client_id) {
            let session = session.into_inner();
            if let Some(key) = &session.last_will_timeout_key {
                self.send_last_will_timeout.remove(key);
            }
            if let Some(key) = &session.remove_timeout_key {
                self.remove_timeout.remove(key);
            }
        }
        self.filter_tree.unsubscribe_all(client_id);
    }
}

#[derive(Default)]
pub struct Storage {
    inner: RwLock<StorageInner>,
}

#[allow(clippy::too_many_arguments)]
impl Storage {
    pub fn update_retained_message(&self, msg: Message) {
        let mut inner = self.inner.write();
        let topic = msg.topic().clone();
        if !msg.is_empty() {
            inner.filter_tree.set_retained_message(topic, Some(msg));
        } else {
            inner.filter_tree.set_retained_message(topic, None);
        }
    }

    pub fn create_session(
        &self,
        client_id: &str,
        clean_start: bool,
        last_will: Option<LastWill>,
    ) -> (bool, Arc<Notify>) {
        let mut inner = self.inner.write();
        let mut session_present = false;

        if !clean_start {
            let (last_will_timeout_key, remove_timeout_key) =
                if let Some(session) = inner.sessions.get_mut(client_id) {
                    let mut session = session.write();
                    session.last_will = last_will.clone();
                    session_present = true;

                    (
                        session.last_will_timeout_key.take(),
                        session.remove_timeout_key.take(),
                    )
                } else {
                    (None, None)
                };

            if let Some(key) = last_will_timeout_key {
                inner.send_last_will_timeout.remove(&key);
            }
            if let Some(key) = remove_timeout_key {
                inner.remove_timeout.remove(&key);
            }
        } else {
            inner.remove_session(client_id);
        }

        if !session_present {
            let session = RwLock::new(Session {
                queue: VecDeque::new(),
                notify: Arc::new(Notify::new()),
                last_will,
                inflight_pub_packets: VecDeque::default(),
                last_will_timeout_key: None,
                remove_timeout_key: None,
            });
            inner.sessions.insert(client_id.to_string(), session);
        }

        let notify = inner.sessions.get(client_id).unwrap().read().notify.clone();
        (session_present, notify)
    }

    pub fn disconnect_session(&self, client_id: &str, session_expiry_interval: u32) {
        let mut inner = self.inner.write();
        let mut send_last_will_timeout = None;
        let mut remove_timeout = None;

        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            let now = Instant::now();

            if let Some(interval) = session.last_will.as_ref().map(|last_will| {
                last_will
                    .properties
                    .delay_interval
                    .unwrap_or_default()
                    .min(session_expiry_interval)
            }) {
                let key = TimeoutKey {
                    client_id: client_id.to_string(),
                    timeout: now + Duration::from_secs(interval as u64),
                };
                send_last_will_timeout = Some(key.clone());
                session.last_will_timeout_key = Some(key);
            }

            let key = TimeoutKey {
                client_id: client_id.to_string(),
                timeout: now + Duration::from_secs(session_expiry_interval as u64),
            };
            remove_timeout = Some(key.clone());
            session.remove_timeout_key = Some(key);
        }

        if let Some(send_last_will_timeout) = send_last_will_timeout {
            inner.send_last_will_timeout.insert(send_last_will_timeout);
        }

        if let Some(remove_timeout) = remove_timeout {
            inner.remove_timeout.insert(remove_timeout);
        }
    }

    pub fn update_sessions(&self) {
        let mut inner = self.inner.write();
        let now = Instant::now();
        let mut last_wills = Vec::new();

        loop {
            match inner.send_last_will_timeout.iter().next().cloned() {
                Some(key) if key.timeout < now => {
                    inner.send_last_will_timeout.remove(&key);
                    if let Some(session) = inner.sessions.get(&key.client_id) {
                        let mut session = session.write();
                        if let Some(last_will) = session.last_will.take() {
                            last_wills.push((key.client_id, last_will));
                        }
                    }
                }
                _ => break,
            }
        }

        loop {
            match inner.remove_timeout.iter().next().cloned() {
                Some(key) if key.timeout < now => {
                    tracing::debug!(
                        client_id = %key.client_id,
                        "session timeout",
                    );

                    inner.remove_session(&key.client_id);
                    inner.remove_timeout.remove(&key);
                    inner.clients_expired += 1;
                }
                _ => break,
            }
        }

        for (client_id, last_will) in last_wills {
            tracing::debug!(
                publisher = %client_id,
                topic = %last_will.topic,
                "send last will message",
            );

            inner.deliver(std::iter::once(Message::from_last_will(last_will)));
        }
    }

    pub fn subscribe(
        &self,
        client_id: &str,
        filter: Filter<'_>,
        qos: Qos,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: RetainHandling,
        id: Option<NonZeroUsize>,
    ) {
        let mut inner = self.inner.write();
        let filter_item = FilterItem {
            qos,
            no_local,
            retain_as_published,
            retain_handling,
            id,
        };

        let is_new_subscribe = inner
            .filter_tree
            .subscribe(filter, client_id.to_string(), filter_item)
            .is_none();

        if filter.share_name.is_none() {
            // send retained messages
            let publish_retain = matches!(
                (retain_handling, is_new_subscribe),
                (RetainHandling::OnEverySubscribe, _) | (RetainHandling::OnNewSubscribe, true)
            );

            if publish_retain {
                for msg in inner.filter_tree.matches_retained_messages(filter.path) {
                    if msg.is_expired() {
                        continue;
                    }

                    if filter_item.no_local && msg.from_client_id().map(|s| &**s) == Some(client_id)
                    {
                        // If no local is true, Application Messages MUST NOT be forwarded to a connection with
                        // a ClientID equal to the ClientID of the publishing connection [MQTT-3.8.3-3]
                        continue;
                    }

                    if let Some(session) = inner.sessions.get(client_id) {
                        let mut session = session.write();
                        session.add_message(msg, std::iter::once(&filter_item));
                    }
                }
            }
        }
    }

    pub fn unsubscribe(&self, client_id: &str, filter: Filter<'_>) -> bool {
        let mut inner = self.inner.write();
        inner.filter_tree.unsubscribe(filter, client_id).is_some()
    }

    pub fn next_messages(&self, client_id: &str, limit: Option<usize>) -> Vec<Message> {
        let inner = self.inner.read();
        let mut session = inner.sessions.get(client_id).unwrap().write();
        let mut limit = limit.unwrap_or(usize::MAX);
        let mut res = Vec::new();

        if limit > 0 {
            while let Some(msg) = session.queue.pop_front() {
                res.push(msg);
                limit -= 1;
                if limit == 0 {
                    break;
                }
            }
        }

        res
    }

    #[inline]
    pub fn deliver(&self, msgs: impl IntoIterator<Item = Message>) {
        self.inner.read().deliver(msgs);
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

    pub fn metrics(&self) -> StorageMetrics {
        let inner = self.inner.read();
        StorageMetrics {
            session_count: inner.sessions.len(),
            inflight_messages_count: inner
                .sessions
                .values()
                .map(|session| session.read().inflight_pub_packets.len())
                .sum::<usize>(),
            retained_messages_count: inner.filter_tree.retained_messages_count(),
            messages_count: inner.filter_tree.retained_messages_count()
                + inner
                    .sessions
                    .values()
                    .map(|session| session.read().queue.len())
                    .sum::<usize>(),
            messages_bytes: inner.filter_tree.retained_messages_bytes()
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
            subscriptions_count: inner.filter_tree.subscriber_count(),
            clients_expired: inner.clients_expired,
        }
    }
}
