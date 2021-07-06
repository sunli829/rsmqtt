#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod client_loop;
mod config;
mod error;
mod filter;
mod message;
mod metrics;
mod state;
mod storage;
mod sys_topics;

pub mod plugin;

pub use client_loop::{client_loop, RemoteAddr};
pub use codec;
pub use config::ServiceConfig;
pub use error::Error;
pub use filter::TopicFilter;
pub use message::Message;
pub use state::ServiceState;
pub use storage::StorageMetrics;
pub use sys_topics::sys_topics_update_loop;
