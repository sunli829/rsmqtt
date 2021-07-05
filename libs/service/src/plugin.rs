use anyhow::Result;
use serde_yaml::Value;

use crate::RemoteAddr;

#[derive(Debug)]
pub struct ConnectionInfo<'a> {
    pub remote_addr: &'a RemoteAddr,
    pub uid: Option<&'a str>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Action {
    Publish,
    Subscribe,
}

#[async_trait::async_trait]
pub trait PluginFactory: 'static {
    fn name(&self) -> &'static str;

    async fn create(&self, config: Value) -> Result<Box<dyn Plugin>>;
}

#[allow(unused_variables)]
#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn auth(&self, user: &str, password: &str) -> Result<Option<String>> {
        Ok(None)
    }

    async fn check_acl(
        &self,
        connection_info: ConnectionInfo<'_>,
        action: Action,
        topic: &str,
    ) -> Result<bool> {
        Ok(false)
    }
}
