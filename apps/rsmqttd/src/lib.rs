use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde_yaml::Value;
use service::plugin::{Plugin, PluginFactory};

macro_rules! register_plugin {
    ($feature:literal, $registry:expr, $ty:expr) => {
        #[cfg(feature = $feature)]
        {
            let factory = $ty;
            $registry.insert(factory.name(), Box::new(factory) as Box<dyn PluginFactory>);
        }
    };
}

pub async fn create_plugins(configs: Vec<Value>) -> Result<Vec<(&'static str, Arc<dyn Plugin>)>> {
    let mut registry: HashMap<&'static str, Box<dyn PluginFactory>> = HashMap::new();
    let mut plugins = Vec::new();

    register_plugin!(
        "plugin-basic-auth",
        registry,
        rsmqtt_plugin_basic_auth::BasicAuth
    );
    register_plugin!("plugin-oso-acl", registry, rsmqtt_plugin_oso_acl::OsoAcl);

    for config in configs {
        let plugin_type = match config.get("type") {
            Some(Value::String(ty)) => ty.as_str(),
            Some(_) => anyhow::bail!("invalid plugin type, expect string"),
            None => anyhow::bail!("require plugin type"),
        };
        let factory = registry
            .get(plugin_type)
            .ok_or_else(|| anyhow::anyhow!("plugin not registered: {}", plugin_type))?;
        plugins.push((factory.name(), factory.create(config).await?));
    }

    Ok(plugins)
}
