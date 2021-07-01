use mqttv5::Qos;
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TcpConfig {
    #[serde(default = "default_host")]
    pub host: String,
    pub port: Option<u16>,
    pub tls: Option<TlsConfig>,
}

impl TcpConfig {
    pub fn port(&self) -> u16 {
        self.port
            .unwrap_or_else(|| if self.tls.is_some() { 8883 } else { 1883 })
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    #[serde(default = "default_host")]
    pub host: String,
    pub port: Option<u16>,
    pub tls: Option<TlsConfig>,
    pub websocket: bool,
    pub api: bool,
    pub graphql_api: bool,
}

impl HttpConfig {
    pub fn port(&self) -> u16 {
        self.port
            .unwrap_or_else(|| if self.tls.is_some() { 8443 } else { 8080 })
    }
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub tcp: Option<TcpConfig>,
    pub http: Option<HttpConfig>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            tcp: Some(TcpConfig {
                host: default_host(),
                port: None,
                tls: None,
            }),
            http: Some(HttpConfig {
                host: default_host(),
                port: None,
                tls: None,
                websocket: true,
                api: true,
                graphql_api: true,
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_sys_update_interval")]
    pub sys_update_interval: u64,
    pub keep_alive: Option<u16>,
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub maximum_qos: Option<Qos>,
    pub retain_available: Option<bool>,
    pub wildcard_subscription_available: Option<bool>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            sys_update_interval: 5,
            keep_alive: None,
            session_expiry_interval: None,
            receive_max: None,
            max_packet_size: None,
            topic_alias_max: None,
            maximum_qos: None,
            retain_available: None,
            wildcard_subscription_available: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_type")]
    pub r#type: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            r#type: default_storage_type(),
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_storage_type() -> String {
    "memory".to_string()
}

fn default_sys_update_interval() -> u64 {
    5
}
