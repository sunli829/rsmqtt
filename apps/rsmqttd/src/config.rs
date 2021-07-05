use serde::Deserialize;
use serde_yaml::Value;
use service::ServiceConfig;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub service: ServiceConfig,

    #[serde(default)]
    pub storage: Value,

    #[serde(default)]
    pub plugins: Vec<Value>,
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

fn default_host() -> String {
    "127.0.0.1".to_string()
}
