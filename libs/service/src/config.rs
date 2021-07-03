use codec::Qos;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServiceConfig {
    #[serde(default = "default_sys_update_interval")]
    pub sys_update_interval: u64,
    #[serde(default = "default_max_keep_alive")]
    pub max_keep_alive: u16,
    #[serde(default = "default_max_session_expiry_interval")]
    pub max_session_expiry_interval: u32,
    #[serde(default = "default_receive_max")]
    pub receive_max: u16,
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: u32,
    #[serde(default = "default_max_topic_alias")]
    pub max_topic_alias: u16,
    #[serde(default = "default_max_qos")]
    pub maximum_qos: Qos,
    #[serde(default = "default_retain_available")]
    pub retain_available: bool,
    #[serde(default = "default_wildcard_subscription_available")]
    pub wildcard_subscription_available: bool,
}

fn default_max_keep_alive() -> u16 {
    30
}

fn default_max_session_expiry_interval() -> u32 {
    60
}

fn default_receive_max() -> u16 {
    32
}

fn default_max_packet_size() -> u32 {
    u32::MAX
}

fn default_max_topic_alias() -> u16 {
    32
}

fn default_max_qos() -> Qos {
    Qos::ExactlyOnce
}

fn default_retain_available() -> bool {
    true
}

fn default_wildcard_subscription_available() -> bool {
    true
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            sys_update_interval: 5,
            max_keep_alive: default_max_keep_alive(),
            max_session_expiry_interval: default_max_session_expiry_interval(),
            receive_max: default_receive_max(),
            max_packet_size: default_max_packet_size(),
            max_topic_alias: default_max_topic_alias(),
            maximum_qos: default_max_qos(),
            retain_available: default_retain_available(),
            wildcard_subscription_available: default_wildcard_subscription_available(),
        }
    }
}

fn default_sys_update_interval() -> u64 {
    5
}
