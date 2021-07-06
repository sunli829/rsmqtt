#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod types;

use std::sync::Arc;

use oso::{Oso, PolarClass};
use serde::Deserialize;
use serde_yaml::Value;
use service::plugin::{Action, Plugin, PluginFactory, PluginResult};
use service::RemoteAddr;

#[derive(Debug, Deserialize)]
struct Config {
    rules: String,
}

pub struct OsoAcl;

#[async_trait::async_trait]
impl PluginFactory for OsoAcl {
    fn name(&self) -> &'static str {
        "oso-acl"
    }

    async fn create(&self, config: Value) -> PluginResult<Arc<dyn Plugin>> {
        let config: Config = serde_yaml::from_value(config)?;
        let mut oso = Oso::new();

        oso.register_class(
            types::Connection::get_polar_class_builder()
                .add_attribute_getter("protocol", |conn| conn.addr.protocol.to_string())
                .add_attribute_getter("addr", |conn| {
                    conn.addr
                        .addr
                        .as_ref()
                        .map(|addr| addr.to_string())
                        .unwrap_or_default()
                })
                .add_attribute_getter("uid", |conn| {
                    conn.uid
                        .as_ref()
                        .map(|uid| uid.to_string())
                        .unwrap_or_default()
                })
                .build(),
        )?;

        oso.register_class(
            types::Filter::get_polar_class_builder()
                .set_constructor(types::Filter::new)
                .add_method("test", types::Filter::test)
                .build(),
        )?;

        oso.load_str(&config.rules)?;
        Ok(Arc::new(OsoAclImpl { oso }))
    }
}

struct OsoAclImpl {
    oso: Oso,
}

#[async_trait::async_trait]
impl Plugin for OsoAclImpl {
    async fn check_acl(
        &self,
        remote_addr: &RemoteAddr,
        uid: Option<&str>,
        action: Action,
        topic: &str,
    ) -> PluginResult<bool> {
        let connection_info = types::Connection {
            addr: remote_addr.clone(),
            uid: uid.map(ToString::to_string),
        };

        Ok(self.oso.is_allowed(
            connection_info,
            match action {
                Action::Publish => "pub",
                Action::Subscribe => "sub",
            },
            topic,
        )?)
    }
}
