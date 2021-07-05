#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod api;
mod config;
mod server;
mod ws_transport;

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_yaml::Value;
use service::{Plugin, PluginFactory, ServiceState, Storage};
use storage_memory::MemoryStorage;
use structopt::StructOpt;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use config::Config;

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

fn create_storage(config: Value) -> Result<Box<dyn Storage>> {
    anyhow::ensure!(
        config.is_mapping(),
        "invalid storage config, expect mapping"
    );

    let storage_type = match config.get("type") {
        Some(Value::String(ty)) => ty.as_str(),
        Some(_) => anyhow::bail!("invalid storage type, expect string"),
        None => "memory",
    };

    tracing::info!(r#type = storage_type, "create storage");

    match storage_type {
        "memory" => Ok(Box::new(MemoryStorage::default())),
        _ => anyhow::bail!("unsupported storage type: {}", storage_type),
    }
}

macro_rules! register_plugin {
    ($feature:literal, $registry:expr, $ty:expr) => {
        #[cfg(feature = $feature)]
        {
            let factory = $ty;
            $registry.insert(factory.name(), Box::new(factory) as Box<dyn PluginFactory>);
        }
    };
}

async fn create_plugins(configs: Vec<Value>) -> Result<Vec<(&'static str, Box<dyn Plugin>)>> {
    let mut registry: HashMap<&'static str, Box<dyn PluginFactory>> = HashMap::new();
    let mut plugins = Vec::new();

    register_plugin!(
        "plugin-basic-auth",
        registry,
        rsmqtt_plugin_basic_auth::BasicAuth
    );
    register_plugin!("plugin-oso-acl", registry, rsmqtt_plugin_oso_acl::OsoAcl);

    for config in configs {
        let value = config.get("type").cloned().unwrap_or_default();
        let plugin_type = match config.get("type") {
            Some(Value::String(ty)) => ty.as_str(),
            Some(_) => anyhow::bail!("invalid plugin type, expect string"),
            None => anyhow::bail!("require plugin type"),
        };
        let (name, factory) = registry
            .get_key_value(plugin_type)
            .ok_or_else(|| anyhow::anyhow!("plugin not registered: {}", plugin_type))?;
        plugins.push((*name, factory.create(value).await?));
    }

    Ok(plugins)
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

    let storage = create_storage(config.storage)?;
    let plugins = create_plugins(config.plugins).await?;
    let state = ServiceState::try_new(config.service, storage, plugins).await?;

    tokio::spawn(service::sys_topics_update_loop(state.clone()));
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
