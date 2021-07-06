use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU16;
use std::sync::Arc;

use anyhow::Result;
use codec::{LastWill, Publish, RetainHandling};
use fnv::FnvHashMap;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::Notify;

use crate::filter::{FilterItem, Filters, TopicFilter};
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

macro_rules! session_not_found {
    ($client_id:expr) => {
        anyhow::bail!("session '{}' not found", $client_id)
    };
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
    pub fn update_retained_message(&self, topic: &str, msg: Message) -> Result<()> {
        let mut inner = self.inner.write();
        if msg.is_empty() {
            inner.retain_messages.remove(topic);
        } else {
            inner.retain_messages.insert(topic.to_string(), msg);
        }
        Ok(())
    }

    pub fn create_session(
        &self,
        client_id: &str,
        clean_start: bool,
        last_will: Option<LastWill>,
        session_expiry_interval: u32,
        last_will_expiry_interval: u32,
    ) -> Result<(bool, Arc<Notify>)> {
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
        Ok((session_present, notify))
    }

    pub fn remove_session(&self, client_id: &str) -> Result<bool> {
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

    pub fn subscribe(&self, client_id: &str, filter: FilterItem) -> Result<()> {
        if let Some(share_name) = filter.topic_filter.share_name().map(ToString::to_string) {
            let mut inner = self.inner.write();
            if !inner.sessions.contains_key(client_id) {
                session_not_found!(client_id)
            }
            inner
                .share_subscriptions
                .entry(share_name)
                .or_default()
                .entry(client_id.to_string())
                .or_default()
                .insert(filter);
            Ok(())
        } else {
            let inner = self.inner.read();

            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();

                let retain_handling = filter.retain_handling;
                let is_new_subscribe = session.subscription_filters.insert(filter).is_none();

                let publish_retain = matches!(
                    (retain_handling, is_new_subscribe),
                    (RetainHandling::OnEverySubscribe, _) | (RetainHandling::OnNewSubscribe, true)
                );

                if publish_retain {
                    let mut has_retain = false;

                    for msg in inner.retain_messages.values() {
                        if let Some(msg) =
                            session.subscription_filters.filter_message(client_id, msg)
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

    pub fn unsubscribe(&self, client_id: &str, filter: TopicFilter) -> Result<bool> {
        if let Some(share_name) = filter.share_name() {
            let mut inner = self.inner.write();
            if !inner.sessions.contains_key(client_id) {
                session_not_found!(client_id)
            }

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
            Ok(found)
        } else {
            let inner = self.inner.read();
            if let Some(session) = inner.sessions.get(client_id) {
                let mut session = session.write();
                return Ok(session.subscription_filters.remove(filter.path()).is_some());
            }
            session_not_found!(client_id)
        }
    }

    pub fn next_messages(&self, client_id: &str, limit: Option<usize>) -> Result<Vec<Message>> {
        let inner = self.inner.read();

        if let Some(session) = inner.sessions.get(client_id) {
            let session = session.read();
            let mut limit = limit.unwrap_or(usize::MAX);
            let mut res = Vec::new();
            let mut offset = 0;

            if limit == 0 {
                return Ok(Vec::new());
            }

            while let Some(msg) = session.queue.get(offset) {
                offset += 1;
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

    pub fn consume_messages(&self, client_id: &str, mut count: usize) -> Result<()> {
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

    pub fn publish(&self, msgs: Vec<Message>) -> Result<()> {
        if msgs.is_empty() {
            return Ok(());
        }

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

        Ok(())
    }

    pub fn add_inflight_pub_packet(&self, client_id: &str, publish: Publish) -> Result<()> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            session.inflight_pub_packets.push_back(publish);
            return Ok(());
        }
        session_not_found!(client_id)
    }

    pub fn get_inflight_pub_packets(
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
                    .front()
                    .filter(|publish| publish.packet_id == Some(packet_id))
                    .cloned())
            };
        }
        session_not_found!(client_id)
    }

    pub fn get_all_inflight_pub_packets(&self, client_id: &str) -> Result<Vec<Publish>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let session = session.read();
            return Ok(session.inflight_pub_packets.iter().cloned().collect());
        }
        session_not_found!(client_id)
    }

    pub fn add_uncompleted_message(
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

    pub fn remove_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
    ) -> Result<Option<Message>> {
        let inner = self.inner.read();
        if let Some(session) = inner.sessions.get(client_id) {
            let mut session = session.write();
            return Ok(session.uncompleted_messages.remove(&packet_id));
        }
        session_not_found!(client_id)
    }

    pub fn metrics(&self) -> Result<StorageMetrics> {
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
                .map(|clients| clients.values().map(|filters| filters.len()))
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
