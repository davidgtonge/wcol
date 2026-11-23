use super::*;
use crate::formats::parquet::{open_plan, Plan};
use crate::plan::stream_program::PLAN_V2;
use crate::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
use std::path::PathBuf;

fn flat_footer() -> Plan {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
    if !path.exists() {
        panic!("flat.parquet missing");
    }
    let (_, plan) = open_plan(&path).unwrap();
    plan
}

#[test]
fn aggregate_graph_ends_in_a_finalizer_on_coordinator_slot() {
    let footer = flat_footer();
    let (dispatch, plan) = plan_query_dispatch(
        "SELECT SUM(a) FROM flat",
        "flat",
        &footer,
        2,
        8,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let QueryDispatch::Nodes(_) = dispatch else {
        panic!("expected nodes dispatch");
    };
    let exec =
        execution_plan_from_dispatch(&dispatch, &plan, &PhysicalPlannerConfig::default()).unwrap();
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        root_output,
        ..
    } = exec
    else {
        panic!("expected graph");
    };
    assert_eq!(root_output, RootOutputKind::Rows);
    assert_eq!(edges.len(), tasks.len() - 1);
    assert_eq!(root_task_id, tasks.len() as u32 - 1);
    assert_eq!(tasks[root_task_id as usize].slot, COORDINATOR_SLOT);
    let root = &tasks[root_task_id as usize].program;
    assert_eq!(root.first(), Some(&PLAN_V2));
    assert_eq!(
        root.get(5),
        Some(&crate::program::stream_program::OP_FINALIZE_FRAGMENT)
    );
    for edge in &edges {
        assert_eq!(edge.to, root_task_id);
    }
    // `flat.parquet` is smaller than two bytes, so the byte floor keeps one colocated node.
    assert!(edges
        .iter()
        .all(|edge| edge.transport == EdgeTransport::Colocated));
    assert!(tasks.iter().all(|task| task.slot == COORDINATOR_SLOT));
}

#[test]
fn several_nodes_use_do_rpc() {
    let footer = flat_footer();
    let (_, plan) = plan_query_dispatch(
        "SELECT SUM(a) FROM flat",
        "flat",
        &footer,
        2,
        8,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let programs = vec![
        crate::plan::query_dispatch::NodeProgram {
            node: crate::plan::physical_plan::NodeId(0),
            program: vec![PLAN_V2],
            request_count: 1,
            fetched_bytes: 80_000,
            work: None,
        },
        crate::plan::query_dispatch::NodeProgram {
            node: crate::plan::physical_plan::NodeId(1),
            program: vec![PLAN_V2],
            request_count: 1,
            fetched_bytes: 80_000,
            work: None,
        },
    ];
    let exec = execution_plan_from_dispatch(
        &QueryDispatch::Nodes(programs),
        &plan,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let ExecutionPlan::Graph { tasks, edges, .. } = exec else {
        panic!("expected graph");
    };
    assert_eq!(edges.len(), 2);
    assert!(edges
        .iter()
        .all(|edge| edge.transport == EdgeTransport::DoRpcWcs));
    assert_eq!(tasks[0].slot.0, 0);
    assert_eq!(tasks[1].slot.0, 1);
}

#[test]
fn one_node_aggregate_is_colocated_on_the_coordinator() {
    let footer = flat_footer();
    let config = PhysicalPlannerConfig::for_coordinator(8);
    let (dispatch, plan) = plan_query_dispatch(
        "SELECT SUM(a) FROM flat",
        "flat",
        &footer,
        config.max_nodes,
        8,
        &config,
    )
    .unwrap();
    let exec = execution_plan_from_dispatch(&dispatch, &plan, &config).unwrap();
    let ExecutionPlan::Graph { tasks, edges, .. } = exec else {
        panic!("expected a scan graph");
    };
    assert!(tasks.len() >= 2);
    assert!(tasks.iter().all(|task| task.slot == COORDINATOR_SLOT));
    assert!(edges
        .iter()
        .all(|edge| edge.transport == EdgeTransport::Colocated));
}

#[test]
fn ordered_parquet_range_graph_has_one_writer_per_partition() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/duckdb_nulls.parquet");
    let (_, footer) = open_plan(&path).unwrap();
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        parquet_row_group_rows: Some(4096),
        ..PhysicalPlannerConfig::for_coordinator(4)
    };
    let (dispatch, plan, columns) = crate::plan::plan_query_job(
        "SELECT d, p FROM t ORDER BY d, p",
        &[("t", &footer)],
        4,
        1024,
        &config,
    )
    .unwrap();
    let (_, partitions) = plan_range_exchange(&plan).expect("range exchange");
    let finalize =
        crate::plan::query_dispatch::nodes_parquet_finalize_steps(&plan, &columns, &footer, 4096)
            .unwrap();
    let exec = execution_plan_for(&dispatch, &plan, &config, Some(finalize)).unwrap();
    let ExecutionPlan::Graph { tasks, edges, .. } = exec else {
        panic!("expected graph");
    };
    let shards = parquet_export_shard_task_ids(&tasks, &edges);
    assert_eq!(
        shards.len(),
        partitions as usize,
        "expected one parquet shard writer per range partition"
    );
}

#[test]
fn host_stream_cap_is_128() {
    assert!(check_stream_pool_layout(128, 128, "test").is_ok());
    assert!(check_stream_pool_layout(129, 1, "test").is_err());
    assert!(check_stream_pool_layout(1, 129, "test").is_err());
}

#[test]
fn same_slot_uses_in_process_transport() {
    let from = SlotId(1);
    let to = SlotId(1);
    assert_eq!(
        edge_transport(from, to, &PhysicalPlannerConfig::default()),
        EdgeTransport::InProcess
    );
}
