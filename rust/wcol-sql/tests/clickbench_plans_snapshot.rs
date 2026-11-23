#![cfg(feature = "native")]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn clickbench_plans_doc_matches_checked_in_snapshot() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let schema = root.join("data/hits_subset_500k.parquet");
    let expected_path = root.join("docs/CLICKBENCH_PLANS.md");
    if !schema.is_file() || !expected_path.is_file() {
        return;
    }
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/clickbench_plans_snapshot.md");
    let status = Command::new(env!("CARGO_BIN_EXE_dump-clickbench-plans"))
        .args(["--out", out.to_str().unwrap()])
        .status()
        .expect("run dump-clickbench-plans");
    assert!(status.success(), "dump-clickbench-plans failed");
    let expected = std::fs::read_to_string(&expected_path).expect("read expected doc");
    let actual = std::fs::read_to_string(&out).expect("read generated doc");
    assert_eq!(
        expected, actual,
        "regenerate with cargo run -p wcol-sql --features native --bin dump-clickbench-plans -- --out docs/CLICKBENCH_PLANS.md from rust/"
    );
}
