//! `POST /jobs` plans run natively: json, parquet export (per-node parts, a root-written file,
//! an aggregate), and concat. Exported files are read back through the engine and compared
//! with the same query run directly.

use std::path::PathBuf;

use wcol::formats::parquet::plan_footer;
use wcol::open_wcol_plan;
use wcol::plan::job_plan::{job_sources, plan_job, JobFooter, JobPlan};
use wcol::plan::result::rows_json;
use wcol::plan::{ExecutionPlan, RootOutputKind};
use wcol::program::coordinator::{decode_coordinator_steps, CoordinatorStep};
use wcol::program::stream_program::{
    OP_COORDINATOR_FRAGMENT, OP_FINALIZE_FRAGMENT, OP_HASH_REDUCER, PLAN_V2,
};
use wcol::stream::core::Action;

const RING: usize = 256 * 1024;

fn fixture() -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/duckdb_nulls.parquet"),
    )
    .unwrap()
}

/// The footer bytes of a whole Parquet file (`... footer len PAR1`).
fn footer_bytes(file: &[u8]) -> &[u8] {
    let n = file.len();
    let len = u32::from_le_bytes(file[n - 8..n - 4].try_into().unwrap()) as usize;
    &file[n - 8 - len..n - 8]
}

fn footer(file: &[u8]) -> JobFooter {
    JobFooter {
        file_size: file.len() as u64,
        plan: plan_footer(footer_bytes(file)).unwrap(),
        footer: footer_bytes(file).to_vec(),
    }
}

/// Run one task. `sources[i]` answers ReadAt source `i`; `input` is its edge bytes.
fn run_task(sources: &[&[u8]], program: &[u8], wires: &[Vec<u8>]) -> Vec<u8> {
    let mut pipe = open_wcol_plan(program, RING).unwrap();
    let mut out = Vec::new();
    let mut at = vec![0usize; wires.len()];
    let mut eof = vec![false; wires.len()];
    let mut ended = false;
    let mut rr = 0usize;
    loop {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, source, offset, len) = pipe.read_at();
                let file = sources[source as usize];
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
}

fn job(body: &str, files: &[&[u8]], nodes: u32) -> JobPlan {
    let (request, sources) = job_sources(body).unwrap();
    assert_eq!(sources.len(), files.len());
    let footers: Vec<JobFooter> = files.iter().map(|file| footer(file)).collect();
    plan_job(&request, &footers, nodes, 1024, None, None, "{}").unwrap()
}

/// Run a graph: (root output, or each task's output when there are no edges).
fn run_graph(file: &[u8], exec: &ExecutionPlan) -> (RootOutputKind, Vec<Vec<u8>>) {
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
    if edges.is_empty() {
        return (
            *root_output,
            tasks
                .iter()
                .map(|t| run_task(&[file], &t.program, &[]))
                .collect(),
        );
    }
    let mut wires = Vec::new();
    for edge in edges.iter().filter(|e| e.to == *root_task_id) {
        let task = tasks.iter().find(|t| t.id == edge.from).unwrap();
        wires.push(run_task(&[file], &task.program, &[]));
    }
    let root = tasks.iter().find(|t| t.id == *root_task_id).unwrap();
    (*root_output, vec![run_task(&[file], &root.program, &wires)])
}

/// JSON for `sql` over `file` (table `t`).
fn query(file: &[u8], sql: &str, nodes: u32) -> String {
    let body = format!(
        r#"{{"sources":{{"t":{{"key":"http://x/t.parquet"}}}},"sql":{sql:?},"output":{{"format":"json"}}}}"#
    );
    match job(&body, &[file], nodes) {
        JobPlan::Json {
            exec: ExecutionPlan::MetadataPartial(partial),
            columns,
            ..
        } => wcol::plan::result::partial_json(&columns, &partial).unwrap(),
        JobPlan::Json {
            exec,
            columns,
            row_cap,
            ..
        } => {
            let (_, out) = run_graph(file, &exec);
            rows_json(&columns, &out[0], row_cap).unwrap()
        }
        _ => panic!("expected a json plan"),
    }
}

fn export(file: &[u8], sql: Option<&str>, nodes: u32) -> (RootOutputKind, Vec<Vec<u8>>) {
    export_sized(file, sql, nodes, 512 << 20)
}

fn export_sized(
    file: &[u8],
    sql: Option<&str>,
    nodes: u32,
    target_file_bytes: u64,
) -> (RootOutputKind, Vec<Vec<u8>>) {
    let sql = sql
        .map(|sql| format!(r#""sql":{sql:?},"#))
        .unwrap_or_default();
    let body = format!(
        r#"{{"sources":{{"t":{{"bucket":"output","key":"t.parquet"}}}},{sql}"output":{{"format":"parquet","bucket":"output","prefix":"out/","targetRowGroupRows":4096,"targetFileBytes":{target_file_bytes}}}}}"#
    );
    let JobPlan::Parquet { exec, prefix, .. } = job(&body, &[file], nodes) else {
        panic!("expected a parquet plan");
    };
    assert_eq!(prefix, "out/");
    run_graph(file, &exec)
}

fn root_limit_fetch(exec: &ExecutionPlan) -> u64 {
    let ExecutionPlan::Graph {
        tasks,
        root_task_id,
        ..
    } = exec
    else {
        panic!("expected a graph");
    };
    let root = tasks.iter().find(|task| task.id == *root_task_id).unwrap();
    assert_eq!(root.program.first(), Some(&PLAN_V2));
    let opcode = root.program[5];
    assert!(opcode == OP_FINALIZE_FRAGMENT || opcode == OP_COORDINATOR_FRAGMENT);
    let body_len = u32::from_le_bytes(root.program[10..14].try_into().unwrap()) as usize;
    let steps = decode_coordinator_steps(&root.program[14..14 + body_len]).unwrap();
    steps
        .into_iter()
        .find_map(|step| match step {
            CoordinatorStep::Limit { fetch, .. } => fetch,
            _ => None,
        })
        .expect("root has a limit")
}

#[test]
fn json_row_cap_keeps_one_extra_row_and_respects_sql_limit() {
    let file = fixture();
    let capped = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(*) AS c FROM t GROUP BY d ORDER BY d","output":{"format":"json","rowCap":2}}"#;
    let JobPlan::Json {
        exec,
        columns,
        row_cap,
        ..
    } = job(capped, &[&file], 4)
    else {
        panic!("expected json");
    };
    assert_eq!(row_cap, 2);
    assert_eq!(root_limit_fetch(&exec), 3, "cap 2 asks the root for 3 rows");
    let (_, out) = run_graph(&file, &exec);
    assert_eq!(
        rows_json(&columns, &out[0], row_cap).unwrap(),
        r#"{"columns":["d","c"],"rows":[[0,188],[1,188]],"truncated":true}"#
    );

    let limited = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d FROM t ORDER BY d LIMIT 1","output":{"format":"json","rowCap":1000}}"#;
    let JobPlan::Json { exec, .. } = job(limited, &[&file], 1) else {
        panic!("expected json");
    };
    assert_eq!(
        root_limit_fetch(&exec),
        1,
        "SQL LIMIT 1 wins over rowCap 1000"
    );
}

#[test]
fn json_job_caps_rows() {
    let file = fixture();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(*) AS c FROM t GROUP BY d ORDER BY d","output":{"format":"json","rowCap":2}}"#;
    let JobPlan::Json {
        exec,
        columns,
        row_cap,
        ..
    } = job(body, &[&file], 4)
    else {
        panic!("expected json");
    };
    assert_eq!(row_cap, 2);
    let (_, out) = run_graph(&file, &exec);
    assert_eq!(
        rows_json(&columns, &out[0], row_cap).unwrap(),
        r#"{"columns":["d","c"],"rows":[[0,188],[1,188]],"truncated":true}"#
    );
}

#[test]
fn identity_export_keeps_every_row() {
    let file = fixture();
    let (output, parts) = export(&file, None, 4);
    assert_eq!(output, RootOutputKind::Parquet);
    let rows: i64 = parts
        .iter()
        .map(|part| plan_footer(footer_bytes(part)).unwrap().rows)
        .sum();
    assert_eq!(rows, 10_000);
    let check = "SELECT COUNT(*), COUNT(d), SUM(p), MIN(d), MAX(p) FROM t";
    assert_eq!(query(&parts[0], check, 1), query(&file, check, 1));
}

/// A scan export fans out: each node writes its own part, and the parts union to the query.
#[test]
fn scan_export_writes_one_part_per_node() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    let Ok(file) = std::fs::read(&path) else {
        eprintln!("skip: hits subset missing");
        return;
    };
    let file = file.as_slice();
    let body = r#"{"sources":{"hits":{"bucket":"output","key":"hits.parquet"}},"sql":"SELECT CounterID, UserID, RegionID FROM hits WHERE RegionID = 229","output":{"format":"parquet","bucket":"output","prefix":"out/","targetFileBytes":1}}"#;
    let JobPlan::Parquet { exec, .. } = job(body, &[file], 4) else {
        panic!("expected parquet");
    };
    let (output, parts) = run_graph(file, &exec);
    assert_eq!(output, RootOutputKind::Parquet);
    assert!(parts.len() > 1, "{} part(s)", parts.len());
    let sum = |bytes: &[u8], sql: &str| -> (i64, String) {
        let body = format!(
            r#"{{"sources":{{"hits":{{"key":"http://x/h.parquet"}}}},"sql":{sql:?},"output":{{"format":"json"}}}}"#
        );
        let JobPlan::Json { exec, columns, .. } = job(&body, &[bytes], 1) else {
            panic!("json")
        };
        let json = match exec {
            ExecutionPlan::MetadataPartial(partial) => {
                wcol::plan::result::partial_json(&columns, &partial).unwrap()
            }
            exec => rows_json(&columns, &run_graph(bytes, &exec).1[0], 0).unwrap(),
        };
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        (
            v["rows"][0][0].as_i64().unwrap(),
            v["rows"][0][1].to_string(),
        )
    };
    let (rows, _) = parts
        .iter()
        .map(|part| sum(part, "SELECT COUNT(*), SUM(CounterID) FROM hits"))
        .fold((0, String::new()), |(n, _), (m, s)| (n + m, s));
    let (expected, _) = sum(
        file,
        "SELECT COUNT(*), SUM(CounterID) FROM hits WHERE RegionID = 229",
    );
    assert_eq!(rows, expected);
}

#[test]
fn ordered_export_is_one_root_file() {
    let file = fixture();
    let sql = "SELECT d, p FROM t WHERE d = 7 ORDER BY p DESC LIMIT 5";
    let (output, parts) = export(&file, Some(sql), 4);
    assert_eq!((output, parts.len()), (RootOutputKind::Parquet, 1));
    assert_eq!(
        query(&parts[0], "SELECT d, p FROM t", 1),
        query(&file, sql, 4),
        "exported rows differ from the query"
    );
}

#[test]
fn aggregate_export_is_written_by_the_finalizer() {
    let file = fixture();
    let sql = "SELECT COUNT(*) AS n, d, SUM(p) AS s, AVG(p) AS a FROM t GROUP BY d ORDER BY n DESC, d LIMIT 7";
    let (output, parts) = export(&file, Some(sql), 4);
    assert_eq!((output, parts.len()), (RootOutputKind::Parquet, 1));
    let names: Vec<String> = plan_footer(footer_bytes(&parts[0]))
        .unwrap()
        .leaves
        .into_iter()
        .map(|leaf| leaf.name)
        .collect();
    assert_eq!(names, ["n", "d", "s", "a"]);
    assert_eq!(
        query(&parts[0], "SELECT n, d, s, a FROM t", 1),
        query(&file, sql, 4)
    );
}

#[test]
fn concat_copies_every_input_into_one_file() {
    let file = fixture();
    // Page copy needs plain pages: export the plain BIGINT column first.
    let (_, parts) = export(&file, Some("SELECT p FROM t"), 1);
    let input = parts[0].as_slice();
    let body = r#"{"output":{"format":"concat","bucket":"output","key":"merged.parquet"},"inputs":[{"bucket":"output","key":"a.parquet"},{"bucket":"output","key":"b.parquet"}]}"#;
    let JobPlan::Concat { key, programs, .. } = job(body, &[input, input], 4) else {
        panic!("expected concat");
    };
    assert_eq!((key.as_str(), programs.len()), ("merged.parquet", 1));
    let merged = run_task(&[input, input], &programs[0], &[]);
    let read = |bytes: &[u8]| -> serde_json::Value {
        serde_json::from_str(&query(bytes, "SELECT COUNT(*), SUM(p), COUNT(p) FROM t", 1)).unwrap()
    };
    let (one, two) = (read(input), read(&merged));
    for column in 0..3 {
        assert_eq!(
            two["rows"][0][column].as_i64().unwrap(),
            2 * one["rows"][0][column].as_i64().unwrap()
        );
    }
}

#[test]
fn distinct_group_job_plans_hash_reducers() {
    let file = fixture();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(DISTINCT p) AS c FROM t GROUP BY d","output":{"format":"json"},"hashReducers":2,"groupSpill":true}"#;
    let (request, _) = job_sources(body).unwrap();
    let footers = vec![footer(&file)];
    let JobPlan::Json { exec, .. } =
        plan_job(&request, &footers, 2, 1024, Some(1), Some(2), body).unwrap()
    else {
        panic!("expected json");
    };
    let ExecutionPlan::Graph { tasks, .. } = exec else {
        panic!("expected graph");
    };
    assert!(
        tasks
            .iter()
            .any(|task| task.program.get(5) == Some(&OP_HASH_REDUCER)),
        "distinct job planned no hash reducers"
    );
}

#[test]
fn discard_spill_sink_encodes_on_hash_reducers() {
    let file = fixture();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(*) AS c FROM t GROUP BY d","output":{"format":"json"},"hashReducers":2,"spillSink":"discard","targetGroupBytes":4096}"#;
    let (request, _) = job_sources(body).unwrap();
    let footers = vec![footer(&file)];
    let JobPlan::Json { exec, .. } =
        plan_job(&request, &footers, 4, 1024, None, Some(2), body).unwrap()
    else {
        panic!("expected json");
    };
    let ExecutionPlan::Graph { tasks, .. } = exec else {
        panic!("expected graph");
    };
    let reducers: Vec<_> = tasks
        .iter()
        .filter(|task| task.program.get(5) == Some(&OP_HASH_REDUCER))
        .collect();
    assert!(!reducers.is_empty(), "planned no hash reducers");
    for task in reducers {
        assert_eq!(
            *task.program.last().unwrap(),
            1,
            "hash reducer missing discard sink flag"
        );
    }
}

#[test]
fn create_model_plans_its_select_as_a_row_graph() {
    let file = fixture();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"CREATE MODEL m OPTIONS (model_type='kmeans', num_clusters=2) AS SELECT d, p FROM t WHERE d > 0"}"#;
    let plan = job(body, &[&file], 2);
    let JobPlan::CreateModel {
        exec,
        name,
        options,
        ..
    } = plan
    else {
        panic!("expected iterative model plan");
    };
    assert_eq!(name, "m");
    assert_eq!(options.clusters, 2);
    let ExecutionPlan::Graph {
        tasks, root_output, ..
    } = exec
    else {
        panic!("model input should be a physical row graph");
    };
    assert!(!tasks.is_empty());
    assert_eq!(root_output, RootOutputKind::Rows);
}

#[test]
fn resident_graph_retains_rows_and_resumes_through_the_stream_pool() {
    use wcol::exec::stream_pool::{StreamPoolSession, STEP_RESIDENT_READY};
    use wcol::ops::iterative::{wire, IterativeKernel, KMeansConfig, KMeansKernel};
    let file = fixture();
    let body = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"CREATE MODEL m OPTIONS (model_type='kmeans', num_clusters=2) AS SELECT d, p FROM t WHERE d > 0"}"#;
    let JobPlan::CreateModel {
        exec: ExecutionPlan::Graph { tasks, edges, .. },
        ..
    } = job(body, &[&file], 1)
    else {
        panic!("expected resident graph");
    };
    assert!(edges.is_empty());
    assert_eq!(tasks.len(), 1);
    let mut pool =
        StreamPoolSession::open_graph(&tasks[0].program, RING, 0, 1, 64 * 1024, 128 * 1024, 1)
            .unwrap();
    assert!(pool.is_resident());
    let mut reads = 0;
    for _ in 0..10_000 {
        let status = pool.step(100);
        assert_eq!(status & 8, 0, "{:?}", pool.error());
        assert_eq!(
            status & 4,
            0,
            "resident input EOF must not complete the graph"
        );
        assert_eq!(
            pool.output_len(0),
            0,
            "resident rows must not be encoded on a WCS edge"
        );
        if status & STEP_RESIDENT_READY != 0 {
            break;
        }
        let (ticket, _, offset, len) = pool.read_at();
        if len > 0 {
            pool.take_read_at();
            pool.complete_read_at(
                ticket,
                file[offset as usize..offset as usize + len as usize].to_vec(),
            )
            .unwrap();
            reads += 1;
        }
    }
    assert!(reads > 0);
    assert!(pool.resident_ready());
    assert_eq!(pool.output_done(0), 1);
    let (moments, sample) = wire::decode_kmeans_moments(pool.resident_response(), 2).unwrap();
    let (means, scales) = wire::kmeans_normalization(&moments);
    let kernel = KMeansKernel {
        config: KMeansConfig {
            clusters: 2,
            dimensions: 2,
            max_iterations: 100,
            tolerance: 1e-4,
        },
    };
    let state = kernel
        .seeded_centroids(&moments.standardize(&sample).unwrap(), 42)
        .unwrap();
    pool.resident_command(1, &wire::encode_kmeans_normalization(&means, &scales))
        .unwrap();
    assert!(!pool.resident_ready());
    assert_eq!(pool.step(100) & STEP_RESIDENT_READY, STEP_RESIDENT_READY);
    let encoded = kernel.encode_state(&state).unwrap();
    let mut first = Vec::new();
    for round in 0..2 {
        pool.resident_command(2, &encoded).unwrap();
        assert_eq!(pool.step(100) & STEP_RESIDENT_READY, STEP_RESIDENT_READY);
        assert_eq!(
            pool.read_at().3,
            0,
            "rounds must reuse resident data without reads"
        );
        if round == 0 {
            first = pool.resident_response().to_vec();
        } else {
            assert_eq!(pool.resident_response(), first);
        }
        assert!(
            kernel
                .decode_partial(pool.resident_response())
                .unwrap()
                .counts
                .iter()
                .sum::<u64>()
                > 0
        );
    }
    assert_eq!(pool.step(100) & 1, 0, "an idle resident graph must park");
}

/// Exchange keys of the one hash exchange in `sql`'s plan on the 500k hits subset.
fn hits_exchange_keys(sql: &str) -> Vec<u32> {
    use wcol::plan::physical_plan::OpKind;
    use wcol::plan::{plan_query_job, PhysicalPlannerConfig};
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    let (_, footer) = wcol::formats::parquet::open_plan(&path).unwrap();
    let mut config = PhysicalPlannerConfig::for_coordinator(32);
    config.hash_aggregate.reducers = Some(4);
    let (_, plan, _) = plan_query_job(sql, &[("hits", &footer)], 32, 1024, &config).unwrap();
    plan.operators
        .iter()
        .find_map(|op| match &op.kind {
            OpKind::HashExchange { keys, .. } => Some(keys.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no hash exchange\n{}", plan.canonical_text()))
}

#[test]
fn distinct_keeps_many_groups_whole_and_spreads_few() {
    // Q13: SearchPhrase has no footer bound, so each phrase stays on one reducer, its count is
    // final there, and reducers rank before the finalizer.
    let q13 = "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits \
               WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10";
    assert_eq!(hits_exchange_keys(q13).len(), 1);
    // Q8: RegionID's range is small; one region's users must spread over the reducers.
    let q8 = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits \
              GROUP BY RegionID ORDER BY u DESC LIMIT 10";
    assert_eq!(hits_exchange_keys(q8).len(), 2);
}

/// Distinct exchange keys on the full hits footer at the hosted shape (32 scans, 64 reducers).
#[test]
fn full_hits_distinct_exchange_keys() {
    use wcol::plan::physical_plan::OpKind;
    use wcol::plan::{plan_query_job, PhysicalPlannerConfig};
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits.footer.bin");
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    let footer = plan_footer(&bytes).unwrap();
    let mut config = PhysicalPlannerConfig::for_coordinator(32);
    config.hash_aggregate.reducers = Some(64);
    let keys = |sql: &str| {
        let (_, plan, _) = plan_query_job(sql, &[("hits", &footer)], 32, 1024, &config).unwrap();
        plan.operators
            .iter()
            .find_map(|op| match &op.kind {
                OpKind::HashExchange { keys, .. } => Some(keys.len()),
                _ => None,
            })
            .expect("hash exchange")
    };
    // Q13: millions of phrases, each kept whole on one reducer so reducers can rank.
    assert_eq!(
        keys(
            "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits \
              WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"
        ),
        1
    );
    // Q10/Q11: a few hundred models (dictionary pages under 1 KB), so users spread too.
    assert_eq!(
        keys(
            "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits \
              WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10"
        ),
        2
    );
    assert_eq!(
        keys(
            "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits \
              WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel \
              ORDER BY u DESC LIMIT 10"
        ),
        3
    );
}

/// Q24 prints all 105 columns of its ten rows: on the full hits footer it picks rows from
/// URL and EventTime alone, then reads the rest back in column groups. Q25–Q27 print one
/// column and stay one pass.
#[test]
fn full_hits_wide_topk_reads_rows_late() {
    use wcol::exec::operators::RowBatchWcsEgress;
    use wcol::plan::late_rows::{plan_late_pick, plan_late_read, LateRead};
    use wcol::plan::physical_plan::OpKind;
    use wcol::plan::PhysicalPlannerConfig;
    use wcol::program::row_id::{row_id, RowPosition};
    use wcol::stream::OwnedWcolBatch;
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits.footer.bin");
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    let footer = plan_footer(&bytes).unwrap();
    let objects = [("hits", &footer)];
    let config = PhysicalPlannerConfig::for_coordinator(32);
    let q24 = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10";
    let (_, plan, columns) = plan_late_pick(q24, &objects, 1024, &config)
        .unwrap()
        .expect("Q24 reads its rows late");
    let row_id_column = columns[0].column;
    let decoded = plan
        .operators
        .iter()
        .find_map(|op| match &op.kind {
            OpKind::Decode { groups } => groups.first().map(|group| group.columns.clone()),
            _ => None,
        })
        .unwrap();
    let names: Vec<&str> = decoded
        .iter()
        .filter(|&&column| column != row_id_column)
        .map(|&column| footer.leaves[column as usize].name.as_str())
        .collect();
    assert_eq!(names, ["EventTime", "URL"]);
    for sql in [
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' \
          ORDER BY EventTime, SearchPhrase LIMIT 10",
    ] {
        assert!(
            plan_late_pick(sql, &objects, 1024, &config)
                .unwrap()
                .is_none(),
            "{sql}"
        );
    }
    // Ten winners in the largest row groups: each read-back scan stays near its budget.
    let ids: Vec<i64> = (0..10u32)
        .map(|index| {
            row_id(RowPosition {
                source: 0,
                row_group: index * 20,
                row: u64::from(index),
            })
            .unwrap()
        })
        .collect();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(row_id_column, ids.clone(), vec![1; ids.len()]);
    batch.set_row_count(ids.len()).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(&batch).unwrap();
    egress.push_end().unwrap();
    let mut picked = Vec::new();
    let mut buf = vec![0u8; 4096];
    while egress.has_pending_output() {
        let n = egress.poll_write(&mut buf).unwrap();
        picked.extend_from_slice(&buf[..n]);
    }
    let LateRead::Graph(ExecutionPlan::Graph { tasks, edges, .. }, columns) =
        plan_late_read(q24, &objects, &picked, 1024, &config).unwrap()
    else {
        panic!("Q24 reads its rows back in a graph");
    };
    assert_eq!(columns.len(), footer.leaves.len());
    let scans = edges.len();
    assert!((8..=32).contains(&scans), "{scans} read-back scans");
    let slots: std::collections::BTreeSet<_> = tasks.iter().map(|task| task.slot).collect();
    assert_eq!(slots.len(), tasks.len(), "every task has its own slot");
}

/// A grouped `approx_count_distinct` charges each group a dense sketch: on the gather plan
/// every scan would ship every region's sketch to one merge (52 MB for A2 on full hits),
/// so RegionID partitions. A few OS values stay gathered.
#[test]
fn full_hits_grouped_sketches_partition_by_size() {
    use wcol::plan::{plan_query_job, PhysicalPlannerConfig};
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits.footer.bin");
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    let footer = plan_footer(&bytes).unwrap();
    let config = PhysicalPlannerConfig::for_coordinator(32);
    let reducers = |sql: &str| {
        let (_, plan, _) = plan_query_job(sql, &[("hits", &footer)], 32, 1024, &config).unwrap();
        plan.metrics.hash_reducers
    };
    assert!(
        reducers(
            "SELECT RegionID, approx_count_distinct(UserID) AS u FROM hits \
              GROUP BY RegionID ORDER BY u DESC LIMIT 10"
        ) > 1
    );
    assert!(
        reducers(
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), \
              approx_count_distinct(UserID) FROM hits GROUP BY RegionID"
        ) > 1
    );
    assert_eq!(
        reducers(
            "SELECT OS, approx_count_distinct(ClientIP), approx_count_distinct(RegionID) \
              FROM hits WHERE EventDate > '2013-07-02' GROUP BY OS"
        ),
        1
    );
    // Without a sketch RegionID's groups fit the gather plan, as before.
    assert_eq!(
        reducers(
            "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"
        ),
        1
    );
}

/// A hash Parquet shard export streams its reducer's groups to Parquet frame by frame
/// instead of merging them first. Fed the same one-producer partial stream, it writes the
/// rows the merging finalizer writes.
#[test]
fn shard_export_streams_the_rows_a_finalizer_merges() {
    use wcol::program::coordinator::encode_shard_export_program;
    let file = fixture();
    let sql = "SELECT d, COUNT(*) AS c, SUM(p) AS s, AVG(p) AS a FROM t GROUP BY d";
    let body = format!(
        r#"{{"sources":{{"t":{{"bucket":"output","key":"t.parquet"}}}},"sql":{sql:?},"output":{{"format":"parquet","bucket":"output","prefix":"out/","targetRowGroupRows":16}}}}"#
    );
    let JobPlan::Parquet {
        exec:
            ExecutionPlan::Graph {
                tasks,
                edges,
                root_task_id,
                ..
            },
        ..
    } = job(&body, &[&file], 1)
    else {
        panic!("expected a parquet graph");
    };
    let root = tasks.iter().find(|task| task.id == root_task_id).unwrap();
    assert_eq!(root.program[5], OP_FINALIZE_FRAGMENT);
    let [edge] = edges.as_slice() else {
        panic!("one producer feeds the finalizer");
    };
    let producer = tasks.iter().find(|task| task.id == edge.from).unwrap();
    let partials = run_task(&[&file], &producer.program, &[]);
    let steps = decode_coordinator_steps(&root.program[14..]).unwrap();
    let merged = run_task(&[&file], &root.program, std::slice::from_ref(&partials));
    let streamed = run_task(&[&file], &encode_shard_export_program(&steps), &[partials]);
    let rows = |parquet: &[u8]| query(parquet, "SELECT d, c, s, a FROM t ORDER BY d", 1);
    assert_eq!(rows(&streamed), rows(&merged));
    assert_eq!(
        rows(&streamed),
        query(&file, &format!("{sql} ORDER BY d"), 1)
    );
}

#[test]
fn json_job_reports_the_aggregate_memory_estimate() {
    let file = fixture();
    let grouped = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d, COUNT(*) AS c FROM t GROUP BY d","output":{"format":"json"}}"#;
    let JobPlan::Json { group_bytes, .. } = job(grouped, &[&file], 4) else {
        panic!("expected json");
    };
    assert!(
        group_bytes > 0,
        "a GROUP BY carries its group-state estimate"
    );

    let rows = r#"{"sources":{"t":{"key":"http://x/t.parquet"}},"sql":"SELECT d FROM t LIMIT 3","output":{"format":"json"}}"#;
    let JobPlan::Json { group_bytes, .. } = job(rows, &[&file], 4) else {
        panic!("expected json");
    };
    assert_eq!(group_bytes, 0, "a plan without an aggregate has none");
}

#[test]
fn min_and_max_come_from_the_footer_and_match_a_scan() {
    let file = fixture();
    let body = |sql: &str| {
        format!(
            r#"{{"sources":{{"t":{{"key":"http://x/t.parquet"}}}},"sql":{sql:?},"output":{{"format":"json"}}}}"#
        )
    };
    let footer_only = |sql: &str| {
        matches!(
            job(&body(sql), &[&file], 4),
            JobPlan::Json {
                exec: ExecutionPlan::MetadataPartial(_),
                ..
            }
        )
    };
    assert!(footer_only("SELECT MIN(d), MAX(d), COUNT(*) FROM t"));
    // DuckDB: min 0, max 49 over 9411 non-null values of 10000 rows.
    assert_eq!(
        query(&file, "SELECT MIN(d), MAX(d), COUNT(*) FROM t", 4),
        r#"{"columns":["min(d)","max(d)","count_star()"],"rows":[[0,49,10000]]}"#
    );
    // A filter that keeps every non-null value forces a scan; both answers agree.
    for (footer, scanned) in [
        (
            "SELECT MIN(d), MAX(d) FROM t",
            "SELECT MIN(d), MAX(d) FROM t WHERE d > -2147483648",
        ),
        (
            "SELECT MIN(p), MAX(p) FROM t",
            "SELECT MIN(p), MAX(p) FROM t WHERE p > -9223372036854775807",
        ),
    ] {
        assert!(!footer_only(scanned), "{scanned} reads rows");
        assert_eq!(
            query(&file, footer, 4),
            query(&file, scanned, 4),
            "{footer}"
        );
    }
    // Anything the footer cannot answer still reads rows.
    assert!(!footer_only("SELECT SUM(d) FROM t"));
    assert!(!footer_only("SELECT MIN(d) FROM t WHERE p > 3"));
}
