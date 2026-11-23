//! Aggregate jobs end in a finalizer task: node partials stream in, the finalizer merges
//! them, sorts and limits the result rows in the engine, and emits row frames. The
//! coordinator only prints those frames as JSON. Answers are DuckDB's on the same file.

use std::path::PathBuf;

use wcol::formats::parquet::open_plan;
use wcol::open_wcol_plan;
use wcol::plan::late_rows::{plan_late_pick, plan_late_read, LateRead, LateRowsConfig};
use wcol::plan::result::{i64_column_values, partial_json, rows_by_id_json, rows_json};
use wcol::plan::{
    execution_plan_from_dispatch, plan_query_job, ExecutionPlan, PhysicalPlannerConfig,
    QueryDispatch, RootOutputKind,
};
use wcol::stream::core::Action;

const RING: usize = 256 * 1024;

fn fixture() -> PathBuf {
    fixture_named("duckdb_nulls.parquet")
}

fn fixture_named(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .join(name)
}

/// Run one task to completion. `wires` are producer WCS streams, one per input port.
fn run_task(file: &[u8], program: &[u8], wires: &[Vec<u8>]) -> Vec<u8> {
    let mut pipe = open_wcol_plan(program, RING).unwrap();
    let mut out = Vec::new();
    let mut at = vec![0usize; wires.len()];
    let mut eof = vec![false; wires.len()];
    let mut ended = false;
    let mut rr = 0usize;
    for _ in 0..1_000_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let slice = &file[offset as usize..offset as usize + len as usize];
                for part in slice.chunks(RING) {
                    pipe.input()[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len()).unwrap();
                    pipe.complete_read(ticket).unwrap();
                }
            }
            Action::NeedInput => {
                if wires.is_empty() {
                    if !ended {
                        pipe.end_input().unwrap();
                        ended = true;
                    }
                } else {
                    let nports = wires.len();
                    let mut fed = false;
                    for step in 0..nports {
                        let port = (rr + step) % nports;
                        if at[port] < wires[port].len() {
                            let dst = pipe.input();
                            if dst.is_empty() {
                                break;
                            }
                            let n = dst.len().min(wires[port].len() - at[port]);
                            dst[..n].copy_from_slice(&wires[port][at[port]..at[port] + n]);
                            pipe.commit_input_port(port as u32, n).unwrap();
                            at[port] += n;
                            rr = port + 1;
                            fed = true;
                            break;
                        }
                    }
                    if !fed {
                        if let Some(port) = eof.iter().position(|done| !done) {
                            pipe.end_input_port(port as u32).unwrap();
                            eof[port] = true;
                        }
                    }
                }
            }
            Action::Yield => {}
            Action::HaveOutput => {
                out.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => return out,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", pipe.error().unwrap_or("task failed")),
        }
    }
    panic!("task exceeded poll budget");
}

/// Plan, run the graph, return the client JSON.
fn job_json(sql: &str, nodes: u32) -> String {
    let path = fixture();
    let file = std::fs::read(&path).unwrap();
    let (_, footer) = open_plan(&path).unwrap();
    // The fixture is small: drop the per-node byte floor so `nodes > 1` really fans out.
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        ..PhysicalPlannerConfig::for_coordinator(nodes)
    };
    let (dispatch, plan, columns) =
        plan_query_job(sql, &[("t", &footer)], nodes, 1024, &config).unwrap();
    if let QueryDispatch::Partial(partial) = &dispatch {
        return partial_json(&columns, partial).unwrap();
    }
    let exec = execution_plan_from_dispatch(&dispatch, &plan, &config).unwrap();
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        root_output,
        ..
    } = exec
    else {
        panic!("expected a graph");
    };
    assert_eq!(root_output, RootOutputKind::Rows, "{sql}");
    let producers = edges.iter().filter(|edge| edge.to == root_task_id).count();
    assert!(
        nodes == 1 || producers > 1,
        "{sql}: {producers} producer(s) on {nodes} nodes"
    );
    // A fanned-out graph's finalizer runs on its own node slot, never in the coordinator.
    let root_slot = tasks
        .iter()
        .find(|task| task.id == root_task_id)
        .unwrap()
        .slot;
    if producers > 1 {
        assert_ne!(root_slot, wcol::plan::COORDINATOR_SLOT, "{sql}");
        assert!(tasks
            .iter()
            .filter(|t| t.id != root_task_id)
            .all(|t| t.slot != root_slot));
    }
    rows_json(
        &columns,
        &run_graph(&file, &(tasks, edges, root_task_id)),
        0,
    )
    .unwrap()
}

type Parts = (
    Vec<wcol::plan::ExecutionTask>,
    Vec<wcol::plan::ExecutionEdge>,
    u32,
);

/// Run a two-level graph: every producer of the root, then the root over their frames.
fn run_graph(file: &[u8], (tasks, edges, root_task_id): &Parts) -> Vec<u8> {
    let mut wires = Vec::new();
    for edge in edges.iter().filter(|edge| edge.to == *root_task_id) {
        let task = tasks.iter().find(|task| task.id == edge.from).unwrap();
        wires.push(run_task(file, &task.program, &[]));
    }
    let root = tasks.iter().find(|task| task.id == *root_task_id).unwrap();
    run_task(file, &root.program, &wires)
}

fn graph_parts(exec: ExecutionPlan) -> Parts {
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        ..
    } = exec
    else {
        panic!("expected a graph");
    };
    (tasks, edges, root_task_id)
}

/// `sql` read late: the pick graph, then one read-back scan per column, joined by row id.
fn late_json(name: &str, sql: &str, nodes: u32) -> String {
    let path = fixture_named(name);
    let file = std::fs::read(&path).unwrap();
    let (_, footer) = open_plan(&path).unwrap();
    let objects = [("t", &footer)];
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        late_rows: LateRowsConfig {
            wide_bytes: 0,
            group_bytes: 1,
        },
        ..PhysicalPlannerConfig::for_coordinator(nodes)
    };
    let (dispatch, plan, pick_columns) = plan_late_pick(sql, &objects, 1024, &config)
        .unwrap()
        .expect("a late plan");
    let pick_exec = execution_plan_from_dispatch(&dispatch, &plan, &config).unwrap();
    let picked = run_graph(&file, &graph_parts(pick_exec));
    let row_id = pick_columns[0].column;
    let order = i64_column_values(&picked, row_id).unwrap();
    match plan_late_read(sql, &objects, &picked, 1024, &config).unwrap() {
        LateRead::Nothing(columns) => rows_by_id_json(&columns, row_id, &order, &[], 0).unwrap(),
        LateRead::Graph(exec, columns) => {
            let parts = graph_parts(exec);
            let scans = parts.1.len();
            assert!(scans >= columns.len(), "{sql}: {scans} scans");
            let rows = run_graph(&file, &parts);
            rows_by_id_json(&columns, row_id, &order, &rows, 0).unwrap()
        }
    }
}

/// One-pass rows of `sql` on fixture `name`.
fn one_pass_json(name: &str, sql: &str, nodes: u32) -> String {
    let path = fixture_named(name);
    let file = std::fs::read(&path).unwrap();
    let (_, footer) = open_plan(&path).unwrap();
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        ..PhysicalPlannerConfig::for_coordinator(nodes)
    };
    let (dispatch, plan, columns) =
        plan_query_job(sql, &[("t", &footer)], nodes, 1024, &config).unwrap();
    assert!(
        plan_late_pick(sql, &[("t", &footer)], 1024, &config)
            .unwrap()
            .is_none(),
        "{sql}: small rows are read in one pass"
    );
    let exec = execution_plan_from_dispatch(&dispatch, &plan, &config).unwrap();
    rows_json(&columns, &run_graph(&file, &graph_parts(exec)), 0).unwrap()
}

#[test]
fn late_rows_match_one_pass() {
    for (name, sql) in [
        (
            "duckdb_nulls.parquet",
            "SELECT * FROM t WHERE p > 1000000 ORDER BY p DESC LIMIT 7 OFFSET 2",
        ),
        (
            "duckdb_nulls.parquet",
            "SELECT p, d FROM t ORDER BY p LIMIT 5",
        ),
        ("flat.parquet", "SELECT * FROM t ORDER BY a DESC LIMIT 3"),
        (
            "flat.parquet",
            "SELECT b, big, c FROM t WHERE e > 0 ORDER BY e LIMIT 10",
        ),
        (
            "flat.parquet",
            "SELECT b FROM t WHERE a > 1000000000 ORDER BY a LIMIT 3",
        ),
    ] {
        let expected = one_pass_json(name, sql, 1);
        for nodes in [1, 3] {
            assert_eq!(
                late_json(name, sql, nodes),
                expected,
                "{sql} on {nodes} nodes"
            );
        }
    }
}

fn check(sql: &str, expected: &str) {
    for nodes in [1, 4] {
        assert_eq!(job_json(sql, nodes), expected, "{sql} on {nodes} nodes");
    }
}

#[test]
fn order_by_count_then_key_limit() {
    check(
        "SELECT d, COUNT(*) AS c FROM t GROUP BY d ORDER BY c DESC, d LIMIT 5",
        r#"{"columns":["d","c"],"rows":[[null,589],[13,189],[14,189],[15,189],[16,189]]}"#,
    );
}

#[test]
fn select_order_and_key_sort_desc_with_offset() {
    check(
        "SELECT SUM(p), d FROM t GROUP BY d ORDER BY d DESC LIMIT 3 OFFSET 1",
        r#"{"columns":["sum(p)","d"],"rows":[[385710681504,48],[395769046912,47],[384221979345,46]]}"#,
    );
}

#[test]
fn null_key_sorts_last_ascending() {
    check(
        "SELECT d, COUNT(*) FROM t GROUP BY d ORDER BY d LIMIT 2 OFFSET 49",
        r#"{"columns":["d","count_star()"],"rows":[[49,189],[null,589]]}"#,
    );
}

#[test]
fn ungrouped_aggregates() {
    check(
        "SELECT MIN(p), MAX(p), COUNT(d), SUM(d) FROM t",
        r#"{"columns":["min(p)","max(p)","count(d)","sum(d)"],"rows":[[436797,4294646785,9411,230628]]}"#,
    );
    check(
        "SELECT AVG(d) FROM t",
        r#"{"columns":["avg(d)"],"rows":[[24.50621613006057]]}"#,
    );
}

#[test]
fn count_distinct_and_footer_count() {
    check(
        "SELECT COUNT(DISTINCT d) AS n FROM t",
        r#"{"columns":["n"],"rows":[[50]]}"#,
    );
    check(
        "SELECT COUNT(*) FROM t",
        r#"{"columns":["count_star()"],"rows":[[10000]]}"#,
    );
}

#[test]
fn finalizer_fails_when_a_producer_never_finishes() {
    let path = fixture();
    let file = std::fs::read(&path).unwrap();
    let (_, footer) = open_plan(&path).unwrap();
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        ..PhysicalPlannerConfig::for_coordinator(4)
    };
    let sql = "SELECT d, COUNT(*) FROM t GROUP BY d";
    let (dispatch, plan, _) = plan_query_job(sql, &[("t", &footer)], 4, 1024, &config).unwrap();
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_task_id,
        ..
    } = execution_plan_from_dispatch(&dispatch, &plan, &config).unwrap()
    else {
        panic!("expected a graph");
    };
    let first = edges.iter().find(|edge| edge.to == root_task_id).unwrap();
    let producer = tasks.iter().find(|task| task.id == first.from).unwrap();
    let only_one = run_task(&file, &producer.program, &[]);
    let root = tasks.iter().find(|task| task.id == root_task_id).unwrap();
    let mut pipe = open_wcol_plan(&root.program, RING).unwrap();
    let mut at = 0usize;
    let mut ended0 = false;
    let message = 'drive: loop {
        match pipe.poll() {
            Action::NeedInput => {
                if at < only_one.len() {
                    let dst = pipe.input();
                    let n = dst.len().min(only_one.len() - at);
                    dst[..n].copy_from_slice(&only_one[at..at + n]);
                    pipe.commit_input_port(0, n).unwrap();
                    at += n;
                } else if !ended0 {
                    pipe.end_input_port(0).unwrap();
                    ended0 = true;
                } else {
                    let err = pipe
                        .end_input_port(1)
                        .expect_err("unfinished producer must fail");
                    break 'drive err.to_string();
                }
            }
            Action::HaveOutput => {
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Yield => {}
            Action::Finished => panic!("finalizer finished with a producer missing"),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => break pipe.error().unwrap_or("failed").to_string(),
            Action::ReadAt => panic!("finalizer requested a host read"),
        }
    };
    assert!(
        message.contains("transport EOF before FRAME_END") || message.contains("unfinished"),
        "{message}"
    );
}
