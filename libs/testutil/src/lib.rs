#![forbid(unsafe_code)]
#![warn(clippy::default_trait_access)]

mod runner;
mod suite;

pub use runner::run;
pub use suite::Suite;

use std::path::Path;

pub async fn run_yaml_file(path: &Path) {
    let suite: Suite = serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    if suite.disable {
        return;
    }
    run(suite).await;
}
