use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU16;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use bytestring::ByteString;
use fnv::FnvHashMap;
use mqttv5::{LastWill, Qos, RetainHandling, SubscribeFilter};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Notify;

use crate::filter::TopicFilter;
use crate::message::Message;
use crate::storage::Storage;

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
    subscribe_filters: HashMap<ByteString, Filter>,
    last_will: Option<LastWill>,
    session_expiry_interval: u32,
    last_will_expiry_interval: u32,
    inflight_messages: VecDeque<(NonZeroU16, Message)>,
    uncompleted_messages: FnvHashMap<NonZeroU16, Message>,
}

#[derive(Default)]
struct StorageMemoryInner {
    retain_messages: HashMap<ByteString, Message>,

    sessions: HashMap<ByteString, RwLock<Session>>,

    /// All of the share subscribes
    ///
    /// share name -> client id -> path -> filter
    share_subscribes: HashMap<String, HashMap<String, HashMap<ByteString, Filter>>>,
}

impl StorageMemoryInner {
    fn add_share_subscribe(&mut self, share_name: &str, client_id: &str, filter: Filter) {
        self.share_subscribes
            .entry(share_name.to_string())
            .or_default()
            .entry(client_id.to_string())
            .or_default()
            .insert(filter.path.clone(), filter);
    }

    fn remove_share_subscribe(&mut self, share_name: &str, client_id: &str, path: &str) -> bool {
        let mut res = false;
        if let Some(clients) = self.share_subscribes.get_mut(share_name) {
            if let Some(filters) = clients.get_mut(client_id) {
                res = filters.remove(path).is_some();
                if filters.is_empty() {
                    clients.remove(client_id);
                }
            }
            if clients.is_empty() {
                self.share_subscribes.remove(share_name);
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
        if msg.payload.is_empty() {
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
                subscribe_filters: HashMap::new(),
                last_will,
                session_expiry_interval,
                last_will_expiry_interval,
                inflight_messages: VecDeque::default(),
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
        Ok((!session_present, notify))
    }

    async fn remove_session(&self, client_id: &str) -> Result<bool> {
        let mut inner = self.inner.write();
        let mut found = false;
        if inner.sessions.remove(client_id).is_some() {
            found = true;
        }
        for clients in inner.share_subscribes.values_mut() {
            clients.remove(client_id);
        }
        Ok(found)
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
            inner.add_share_subscribe(&share_name, client_id, filter);
            Ok(())
        } else {
            let inner = self.inner.read();

            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();

                let is_new_subscribe = session
                    .subscribe_filters
                    .insert(filter.path.clone(), filter.clone())
                    .is_some();

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
                        session.notify.notify_waiters();
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
            Ok(inner.remove_share_subscribe(share_name, client_id, &path))
        } else {
            let inner = self.inner.read();
            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();
                return Ok(session.subscribe_filters.remove(path).is_some());
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
                        filter_message(client_id, &msg, session.subscribe_filters.values())
                    {
                        let mut session = RwLockUpgradableReadGuard::upgrade(session);
                        session.queue.push_back(msg);
                        session.notify.notify_waiters();
                    }
                }

                matched_clients.clear();
                for clients in inner.share_subscribes.values() {
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
                            session.notify.notify_waiters();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn add_inflight_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        msg: Message,
    ) -> Result<()> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            session.inflight_messages.push_back((packet_id, msg));
            return Ok(());
        }
        session_not_found!(client_id)
    }

    async fn get_inflight_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Result<Option<Message>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            return if remove {
                let mut session = session.write();
                if session.inflight_messages.front().map(|(id, _)| *id) == Some(packet_id) {
                    Ok(session.inflight_messages.pop_front().map(|(_, msg)| msg))
                } else {
                    Ok(None)
                }
            } else {
                let session = session.read();
                Ok(session
                    .inflight_messages
                    .iter()
                    .find(|(id, _)| *id == packet_id)
                    .map(|(_, msg)| msg.clone()))
            };
        }
        session_not_found!(client_id)
    }

    async fn get_all_inflight_messages(
        &self,
        client_id: &str,
    ) -> Result<Vec<(NonZeroU16, Message)>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let session = session.read();
            return Ok(session.inflight_messages.iter().cloned().collect());
        }
        session_not_found!(client_id)
    }

    async fn add_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        msg: Message,
    ) -> Result<()> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            session.uncompleted_messages.insert(packet_id, msg);
            return Ok(());
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
}

fn filter_message<'a>(
    client_id: &str,
    msg: &Message,
    filters: impl IntoIterator<Item = &'a Filter>,
) -> Option<Message> {
    let mut matched = false;
    let mut max_qos = Qos::AtMostOnce;
    let mut retain = msg.retain;
    let mut ids = Vec::new();

    for filter in filters {
        if filter.no_local && msg.publisher.as_deref() == Some(client_id) {
            continue;
        }

        if !filter.topic_filter.matches(&msg.topic) {
            continue;
        }

        if let Some(id) = filter.id {
            ids.push(id);
        }

        max_qos = max_qos.max(filter.qos);

        if !filter.retain_as_published {
            retain = false;
        }

        matched = true;
    }

    if matched {
        let mut msg = msg.clone();
        msg.qos = msg.qos.min(max_qos);
        msg.retain = retain;
        msg.properties.subscription_identifiers = ids;
        Some(msg)
    } else {
        None
    }
}
