#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod types;

use anyhow::Result;
use oso::{Oso, PolarClass};
use serde::Deserialize;
use serde_yaml::Value;
use service::{Action, ConnectionInfo, Plugin, PluginFactory};

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

    async fn create(&self, config: Value) -> Result<Box<dyn Plugin>> {
        let config: Config = serde_yaml::from_value(config)?;
        let mut oso = Oso::new();

        oso.register_class(
            types::Connection::get_polar_class_builder()
                .add_attribute_getter("protocol", |conn| conn.addr.protocol)
                .add_attribute_getter("addr", |conn| conn.addr.addr.clone().unwrap_or_default())
                .add_attribute_getter("uid", |conn| conn.uid.clone().unwrap_or_default())
                .build(),
        )?;

        oso.register_class(
            types::Filter::get_polar_class_builder()
                .set_constructor(types::Filter::new)
                .add_method("test", types::Filter::test)
                .build(),
        )?;

        oso.load_str(&config.rules)?;
        Ok(Box::new(OsoAclImpl { oso }))
    }
}

struct OsoAclImpl {
    oso: Oso,
}

#[async_trait::async_trait]
impl Plugin for OsoAclImpl {
    async fn check_acl(
        &self,
        connection_info: ConnectionInfo<'_>,
        action: Action,
        topic: &str,
    ) -> Result<bool> {
        let connection_info = types::Connection {
            addr: connection_info.remote_addr.clone(),
            uid: connection_info.uid.map(ToString::to_string),
        };
        let action = match action {
            Action::Publish => "pub",
            Action::Subscribe => "sub",
        };

        match self.oso.is_allowed(connection_info, action, topic) {
            Ok(res) => Ok(res),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to call oso::is_allowed"
                );
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use service::RemoteAddr;

    #[tokio::test]
    async fn test_oso_acl() {
        let config = r#"
        rules: |
          allow(conn: Connection, "pub", "test") if conn.uid = "sunli";
          
          allow(conn: Connection, action: String, "test") if conn.addr == "1.1.1.1";
          
          allow(conn: Connection, action: String, topic: String) if new Filter("a/+/c").test(topic);
        "#;
        let acl = OsoAcl
            .create(serde_yaml::from_str(config).unwrap())
            .await
            .unwrap();

        assert!(acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("127.0.0.1".into()),
                    },
                    uid: Some("sunli"),
                },
                Action::Publish,
                "test"
            )
            .await
            .unwrap());

        assert!(!acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("127.0.0.1".into()),
                    },
                    uid: Some("sunli2"),
                },
                Action::Publish,
                "test"
            )
            .await
            .unwrap());

        assert!(!acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("127.0.0.1".into()),
                    },
                    uid: Some("sunli"),
                },
                Action::Subscribe,
                "test"
            )
            .await
            .unwrap());

        assert!(acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("1.1.1.1".into()),
                    },
                    uid: None,
                },
                Action::Subscribe,
                "test"
            )
            .await
            .unwrap());

        assert!(acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("1.1.1.1".into()),
                    },
                    uid: None,
                },
                Action::Subscribe,
                "test"
            )
            .await
            .unwrap());

        assert!(acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("127.0.0.1".into()),
                    },
                    uid: None,
                },
                Action::Subscribe,
                "a/b/c"
            )
            .await
            .unwrap());

        assert!(!acl
            .check_acl(
                ConnectionInfo {
                    remote_addr: &RemoteAddr {
                        protocol: "tcp",
                        addr: Some("127.0.0.1".into()),
                    },
                    uid: None,
                },
                Action::Subscribe,
                "a/b/e"
            )
            .await
            .unwrap());
    }
}
