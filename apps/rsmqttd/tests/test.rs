use std::path::Path;

fn do_test(path: &Path) -> datatest_stable::Result<()> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(testutil::run_yaml_file(path));
    Ok(())
}

datatest_stable::harness!(do_test, "tests", r"^.*/*.yaml");
