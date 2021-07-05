use std::num::NonZeroU16;
use std::sync::Arc;

use anyhow::Result;
use bytestring::ByteString;
use codec::{LastWill, Publish, SubscribeFilter};
use tokio::sync::Notify;

use crate::filter::TopicFilter;
use crate::message::Message;

#[derive(Debug)]
pub struct SessionInfo {
    pub client_id: ByteString,
    pub last_will: Option<LastWill>,
    pub session_expiry_interval: u32,
    pub last_will_expiry_interval: u32,
}

#[derive(Debug)]
pub struct StorageMetrics {
    pub session_count: usize,
    pub inflight_messages_count: usize,
    pub retained_messages_count: usize,
    pub messages_count: usize,
    pub messages_bytes: usize,
    pub subscriptions_count: usize,
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn update_retained_message(&self, topic: ByteString, msg: Message) -> Result<()>;

    async fn create_session(
        &self,
        client_id: ByteString,
        clean_start: bool,
        last_will: Option<LastWill>,
        session_expiry_interval: u32,
        last_will_expiry_interval: u32,
    ) -> Result<(bool, Arc<Notify>)>;

    async fn remove_session(&self, client_id: &str) -> Result<bool>;

    async fn get_sessions(&self) -> Result<Vec<SessionInfo>>;

    async fn subscribe(
        &self,
        client_id: &str,
        subscribe_filter: SubscribeFilter,
        topic_filter: TopicFilter,
        id: Option<usize>,
    ) -> Result<()>;

    async fn unsubscribe(
        &self,
        client_id: &str,
        path: &str,
        topic_filter: TopicFilter,
    ) -> Result<bool>;

    async fn next_messages(&self, client_id: &str, limit: Option<usize>) -> Result<Vec<Message>>;

    async fn consume_messages(&self, client_id: &str, count: usize) -> Result<()>;

    async fn publish(&self, msgs: Vec<Message>) -> Result<()>;

    async fn add_inflight_pub_packet(&self, client_id: &str, publish: Publish) -> Result<()>;

    async fn get_inflight_pub_packets(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Result<Option<Publish>>;

    async fn get_all_inflight_pub_packets(&self, client_id: &str) -> Result<Vec<Publish>>;

    async fn add_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        msg: Message,
    ) -> Result<bool>;

    async fn get_uncompleted_message(
        &self,
        client_id: &str,
        packet_id: NonZeroU16,
        remove: bool,
    ) -> Result<Option<Message>>;

    async fn metrics(&self) -> Result<StorageMetrics>;
}
