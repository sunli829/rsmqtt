#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod api;
mod config;
mod server;
mod ws_transport;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use service::ServiceState;
use structopt::StructOpt;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use config::Config;
use rsmqttd::create_plugins;

const DEFAULT_CONFIG_FILENAME: &str = ".rsmqttd";

#[derive(StructOpt)]
struct Options {
    /// Path of the config file
    pub config: Option<String>,
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer().compact().with_target(false))
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .init();
}

async fn run() -> Result<()> {
    let options: Options = Options::from_args();

    let config_filename = match options.config {
        Some(config_filename) => Some(PathBuf::from(config_filename)),
        None => dirs::home_dir()
            .map(|home_dir| home_dir.join(DEFAULT_CONFIG_FILENAME))
            .filter(|path| path.exists()),
    };

    let config = if let Some(config_filename) = config_filename {
        tracing::info!(filename = %config_filename.display(), "load config file");

        serde_yaml::from_str::<Config>(
            &std::fs::read_to_string(&config_filename)
                .with_context(|| format!("load config file '{}'.", config_filename.display()))?,
        )
        .with_context(|| format!("parse config file '{}'.", config_filename.display()))?
    } else {
        tracing::info!("use the default config");
        Config::default()
    };

    let plugins = create_plugins(config.plugins).await?;
    let state = ServiceState::new(config.service, plugins)?;

    tokio::spawn({
        let state = state.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(state.config.metrics_update_interval)).await;
                state.update_metrics().await;
                state.update_sys_topics();
            }
        }
    });
    server::run(state, config.network).await
}

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(err) = run().await {
        tracing::error!(
            error = %err,
            "failed to start server",
        );
    }
}
