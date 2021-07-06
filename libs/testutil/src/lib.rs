#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod runner;
mod suite;

pub use runner::run;
pub use suite::Suite;

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use serde_yaml::Value;

use service::plugin::Plugin;

pub async fn run_yaml_file<T, F>(path: &Path, create_plugins: T)
where
    T: FnOnce(Vec<Value>) -> F,
    F: Future<Output = Vec<(&'static str, Arc<dyn Plugin>)>>,
{
    let suite: Suite = serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    if suite.disable {
        return;
    }
    run(suite, create_plugins).await;
}
