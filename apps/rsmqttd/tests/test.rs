use std::path::Path;

use rsmqttd::create_plugins;

fn do_test(path: &Path) -> datatest_stable::Result<()> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(testutil::run_yaml_file(path, |values| async move {
            create_plugins(values).await.unwrap()
        }));
    Ok(())
}

datatest_stable::harness!(do_test, "tests", r"^.*/*.yaml");
