#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_yaml::Value;

use service::plugin::{Plugin, PluginFactory, PluginResult};

#[derive(Debug, Deserialize)]
struct Config {
    users: HashMap<String, String>,
}

pub struct BasicAuth;

#[async_trait::async_trait]
impl PluginFactory for BasicAuth {
    fn name(&self) -> &'static str {
        "basic-auth"
    }

    async fn create(&self, config: Value) -> PluginResult<Arc<dyn Plugin>> {
        let config: Config = serde_yaml::from_value(config)?;
        Ok(Arc::new(BasicAuthImpl {
            users: config.users,
        }))
    }
}

struct BasicAuthImpl {
    users: HashMap<String, String>,
}

#[async_trait::async_trait]
impl Plugin for BasicAuthImpl {
    async fn auth(&self, user: &str, password: &str) -> PluginResult<Option<String>> {
        match self.users.get(user) {
            Some(phc) if passwd_util::verify_password(&phc, &password) => {
                Ok(Some(user.to_string()))
            }
            _ => Ok(None),
        }
    }
}
