#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;
use serde_yaml::Value;

use service::{Plugin, PluginFactory};

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

    async fn create(&self, config: Value) -> Result<Box<dyn Plugin>> {
        let config: Config = serde_yaml::from_value(config)?;
        Ok(Box::new(BasicAuthImpl {
            users: config.users,
        }))
    }
}

struct BasicAuthImpl {
    users: HashMap<String, String>,
}

#[async_trait::async_trait]
impl Plugin for BasicAuthImpl {
    async fn auth(&self, user: &str, password: &str) -> Result<Option<String>> {
        match self.users.get(user) {
            Some(phc) if passwd_util::verify_password(&phc, password) => Ok(Some(user.to_string())),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_auth() {
        let config = r#"
        users:
            sunli: $pbkdf2-sha512$i=10000,l=32$V9dNu168tQCjFG1uOyIeeQ$wWhxjmLwaVoeUzreotGPOrE34eakNn5lpk8Glr8S4mw
        "#;
        let auth = BasicAuth
            .create(serde_yaml::from_str(config).unwrap())
            .await
            .unwrap();
        assert_eq!(
            auth.auth("sunli", "abcdef").await.unwrap(),
            Some("sunli".to_string())
        );
        assert_eq!(auth.auth("sunli", "abcdef1").await.unwrap(), None);
    }
}
