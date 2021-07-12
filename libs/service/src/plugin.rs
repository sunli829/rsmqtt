use std::sync::Arc;

use codec::{ProtocolLevel, Qos};
use serde_yaml::Value;

use crate::RemoteAddr;
use bytes::Bytes;

pub type PluginResult<T> = anyhow::Result<T>;

#[async_trait::async_trait]
pub trait PluginFactory: 'static {
    fn name(&self) -> &'static str;

    async fn create(&self, config: Value) -> PluginResult<Arc<dyn Plugin>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Action {
    Publish,
    Subscribe,
}

/// Represents a rsmqtt plugin
#[allow(unused_variables, clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn auth(&self, user: &str, password: &str) -> PluginResult<Option<String>> {
        Ok(None)
    }

    async fn check_acl(
        &self,
        remote_addr: &RemoteAddr,
        uid: Option<&str>,
        action: Action,
        topic: &str,
    ) -> PluginResult<bool> {
        Ok(true)
    }

    async fn on_client_connected(
        &self,
        remote_addr: &RemoteAddr,
        client_id: &str,
        uid: Option<&str>,
        keep_alive: u16,
        level: ProtocolLevel,
    ) {
    }

    async fn on_client_disconnected(&self, client_id: &str, uid: Option<&str>) {}

    async fn on_session_subscribed(
        &self,
        client_id: &str,
        uid: Option<&str>,
        topic: &str,
        qos: Qos,
    ) {
    }

    async fn on_session_unsubscribed(&self, client_id: &str, uid: Option<&str>, topic: &str) {}

    async fn on_message_publish(
        &self,
        client_id: &str,
        uid: Option<&str>,
        topic: &str,
        qos: Qos,
        retain: bool,
        payload: Bytes,
    ) {
    }

    async fn on_message_delivered(
        &self,
        client_id: &str,
        uid: Option<&str>,
        from_client_id: Option<&str>,
        from_uid: Option<&str>,
        topic: &str,
        qos: Qos,
        retain: bool,
        payload: Bytes,
    ) {
    }
}
