use std::sync::Arc;

use serde_yaml::Value;

use crate::RemoteAddr;

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
#[allow(unused_variables)]
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
}
