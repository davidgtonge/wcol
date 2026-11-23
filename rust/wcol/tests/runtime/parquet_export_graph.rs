//! Execution graph shape for hash shard export and range-sort parquet.

use std::path::PathBuf;
use wcol::formats::parquet::{open_plan, plan_footer};
use wcol::plan::execution_graph::{
    plan_range_exchange, ExecutionPlan, RootOutputKind, WHOLE_STREAM,
};
use wcol::plan::job::{JobRequest, NamedObject, ObjectLocation};
use wcol::plan::job_plan::{plan_job, JobFooter, JobPlan};
use wcol::plan::PhysicalPlannerConfig;
use wcol::program::stream_program::{OP_HASH_REDUCER, OP_SHARD_EXPORT};

fn duckdb_footer() -> JobFooter {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/duckdb_nulls.parquet");
    let file = std::fs::read(&path).unwrap();
    let n = file.len();
    let len = u32::from_le_bytes(file[n - 8..n - 4].try_into().unwrap()) as usize;
    let footer = file[n - 8 - len..n - 8].to_vec();
    JobFooter {
        file_size: n as u64,
        plan: plan_footer(&footer).unwrap(),
        footer,
    }
}

#[test]
fn hash_parquet_without_order_by_emits_export_shards() {
    let footer = duckdb_footer();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(*) AS c FROM t GROUP BY d","output":{"format":"parquet","bucket":"output","prefix":"out/","targetRowGroupRows":4096,"targetFileBytes":999999999},"hashReducers":2}"#;
    let request = JobRequest::Parquet {
        source: NamedObject {
            table: "t".into(),
            locations: vec![ObjectLocation::Url("http://x/t.parquet".into())],
        },
        sql: Some("SELECT d, COUNT(*) AS c FROM t GROUP BY d".into()),
        bucket: "output".into(),
        prefix: "out/".into(),
        partition_by: vec![],
        target_file_bytes: 999_999_999,
        target_row_group_rows: 4096,
        idempotency_key: None,
    };
    let planned = plan_job(&request, &[footer], 2, 1024, None, Some(2), body).unwrap();
    let JobPlan::Parquet { exec, .. } = planned else {
        panic!("expected parquet job plan");
    };
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_output,
        ..
    } = exec
    else {
        panic!("expected graph");
    };
    assert_eq!(root_output, RootOutputKind::Parquet);
    // Each hash reducer feeds its own shard export task, which writes that reducer's file
    // (the host runs it beside the reducer). Nothing concatenates them.
    let opcode = |id: u32| {
        tasks
            .iter()
            .find(|task| task.id == id)
            .map(|task| task.program[5])
    };
    let reducers: Vec<u32> = tasks
        .iter()
        .filter(|task| task.program[5] == OP_HASH_REDUCER)
        .map(|task| task.id)
        .collect();
    assert_eq!(reducers.len(), 2, "two hash reducers");
    for reducer in &reducers {
        let exports: Vec<u32> = edges
            .iter()
            .filter(|edge| edge.from == *reducer && edge.partition == WHOLE_STREAM)
            .map(|edge| edge.to)
            .collect();
        assert_eq!(exports.len(), 1, "reducer {reducer} feeds one shard export");
        assert_eq!(opcode(exports[0]), Some(OP_SHARD_EXPORT));
    }
}

#[test]
fn ordered_parquet_scan_plans_range_exchange() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/duckdb_nulls.parquet");
    let (_, footer) = open_plan(&path).unwrap();
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        parquet_row_group_rows: Some(4096),
        ..PhysicalPlannerConfig::for_coordinator(4)
    };
    let sql = "SELECT d, p FROM t ORDER BY d, p";
    let (_dispatch, plan) =
        wcol::plan::plan_query_dispatch(sql, "t.parquet", &footer, 4, 1024, &config).unwrap();
    assert!(
        plan_range_exchange(&plan).is_some(),
        "multi-node ordered parquet should plan range exchange"
    );
}
