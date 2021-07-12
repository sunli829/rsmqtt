#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod client_loop;
mod config;
mod error;
mod filter_tree;
mod filter_util;
mod message;
mod metrics;
mod rewrite;
mod state;
mod storage;
mod sys_topics;

pub mod plugin;

pub use client_loop::{client_loop, RemoteAddr};
pub use codec;
pub use config::ServiceConfig;
pub use error::Error;
pub use message::Message;
pub use metrics::Metrics;
pub use state::ServiceState;
