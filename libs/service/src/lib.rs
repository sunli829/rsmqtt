#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod client_loop;
mod config;
mod error;
mod filter;
mod message;
mod metrics;
mod plugin;
mod state;
mod storage;
mod sys_topics;

pub use client_loop::{client_loop, RemoteAddr};
pub use config::ServiceConfig;
pub use error::Error;
pub use filter::TopicFilter;
pub use message::Message;
pub use plugin::{Action, ConnectionInfo, Plugin, PluginFactory};
pub use state::ServiceState;
pub use storage::{SessionInfo, Storage, StorageMetrics};
pub use sys_topics::sys_topics_update_loop;
