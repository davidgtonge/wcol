//! Time filter + aggregate queries through the colocated graph on a local Parquet file.
//!
//! ```text
//! cargo run --release -p wcol --features native --bin query_bench -- data/hits_subset_500k.parquet
//! ```
//! Options: `--sql "<query>"` (repeatable; default is a ClickBench subset), `--batch-rows N`,
//! `--runs N`, `--print-groups` (print every result group as one JSON line per query instead of
//! timings), `--plan-nodes N` (with `--print-groups`: run through the physical plan on N node
//! slots, each node as its own `wcs_open` program, then merge). The file is read into memory
//! first, so timings exclude disk I/O.

use std::time::Instant;

use wcol::formats::parquet::open_plan;
use wcol::ir::ColumnKind;
use wcol::ops::ParquetSourceOp;
use wcol::plan::physical_plan::OpKind;
use wcol::plan::{
    plan_query_dispatch, IngestMemoryProfile, PhysicalPlannerConfig, WcolPhysicalPlan,
};
use wcol::stream::partials::{partial_snapshot_from_state, PartialStateWcol};
use wcol::{ingest_groups_from_plan, CatalogColumn, GraphPump, IngestGroup, LinearColocatedGraph};

const DEFAULT_QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    "SELECT AVG(UserID) FROM hits",
    "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    "SELECT ClientIP, COUNT(*) AS c FROM hits GROUP BY ClientIP ORDER BY c DESC LIMIT 10",
    "SELECT COUNT(*) FROM hits WHERE CounterID = 62 AND DontCountHits = 0 AND IsRefresh = 0",
];

struct Args {
    path: String,
    queries: Vec<String>,
    batch_rows: usize,
    runs: usize,
    print_groups: bool,
    /// Run through the physical plan on this many node slots (0: the single SQL graph).
    plan_nodes: u32,
    /// Force a power-of-two hash-reducer count. `1` keeps the gather plan.
    hash_reducers: Option<u32>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut path = None;
    let mut queries = Vec::new();
    let mut batch_rows = 8192;
    let mut runs = 3;
    let mut print_groups = false;
    let mut plan_nodes = 0u32;
    let mut hash_reducers = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--sql" => queries.push(args.next().ok_or("--sql needs a value")?),
            "--batch-rows" => {
                batch_rows = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--batch-rows needs a number")?
            }
            "--runs" => {
                runs = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--runs needs a number")?
            }
            "--print-groups" => print_groups = true,
            "--plan-nodes" => {
                plan_nodes = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or("--plan-nodes needs a number")?
            }
            "--hash-reducers" => {
                hash_reducers = Some(
                    args.next()
                        .and_then(|v| v.parse().ok())
                        .ok_or("--hash-reducers needs a number")?,
                );
            }
            _ if path.is_none() => path = Some(arg),
            other => return Err(format!("unexpected argument {other}")),
        }
    }
    if queries.is_empty() {
        queries = DEFAULT_QUERIES.iter().map(|q| q.to_string()).collect();
    }
    Ok(Args {
        path: path
            .ok_or("usage: query_bench <file.parquet> [--sql Q]... [--batch-rows N] [--runs N]")?,
        queries,
        batch_rows,
        runs: runs.max(1),
        print_groups,
        plan_nodes,
        hash_reducers,
    })
}

/// Column ids the physical plan decodes. A footer-only count has none.
fn scanned_columns(plan: &WcolPhysicalPlan) -> Vec<u32> {
    let mut ids = Vec::new();
    for op in &plan.operators {
        if let OpKind::Decode { groups } = &op.kind {
            for group in groups {
                for column in &group.columns {
                    if !ids.contains(column) {
                        ids.push(*column);
                    }
                }
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn catalog_of(plan: &wcol::Plan) -> Vec<CatalogColumn> {
    plan.leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect()
}

/// Run one query; returns elapsed milliseconds and the encoded partial (empty for decode-only).
fn run(
    bytes: &[u8],
    catalog: &[CatalogColumn],
    groups: &[IngestGroup],
    batch_rows: usize,
) -> Result<f64, String> {
    if groups.iter().all(|g| g.chunks.is_empty()) {
        return Ok(0.0);
    }
    let source = ParquetSourceOp::new(
        batch_rows,
        catalog.to_vec(),
        groups.to_vec(),
        IngestMemoryProfile::default(),
    );
    let mut graph =
        LinearColocatedGraph::open_custom(Some(Box::new(source)), Vec::new(), Vec::new());
    let start = Instant::now();
    loop {
        match graph.pump()? {
            GraphPump::ReadAt {
                ticket,
                offset,
                length,
                ..
            } => {
                let at = offset as usize;
                let end = at + length as usize;
                let slice = bytes.get(at..end).ok_or("read past end of file")?;
                graph.complete_ingest_read(ticket, slice.to_vec())?;
            }
            GraphPump::Progress => {}
            GraphPump::AwaitHost => {
                return Err("graph waiting on host with no reads pending".to_string())
            }
            GraphPump::RowsReady => return Err("decode scan queued a row batch".to_string()),
            GraphPump::Done => break,
        }
    }
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

/// Plan `sql` over the file's footer, run each node's program through GraphHost as a worker would
/// (ReadAt served from `bytes`, responses committed in ring-sized parts), and merge the partials.
fn run_planned(
    bytes: &[u8],
    sql: &str,
    footer: &wcol::Plan,
    nodes: u32,
    batch_rows: usize,
    hash_reducers: Option<u32>,
) -> Result<PartialStateWcol, String> {
    use wcol::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
    use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
    use wcol::stream::partial_wcs::partial_state_from_wire_payload;
    const RING: usize = 256 * 1024;

    let mut config = PhysicalPlannerConfig::default();
    config.hash_aggregate.reducers = hash_reducers;
    let (dispatch, plan) = plan_query_dispatch(
        sql,
        "query_bench",
        footer,
        nodes,
        batch_rows as u32,
        &config,
    )?;
    let programs = match dispatch {
        QueryDispatch::Partial(partial) => return partial_state_from_wire_payload(&partial),
        QueryDispatch::Nodes(programs) => programs,
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => {
            return Err(
                "query_bench runs aggregate queries; row queries use the fragment host".to_string(),
            );
        }
    };
    if plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::HashExchange { .. }))
    {
        let (state, stats) =
            wcol::plan::run_hash_aggregate(&programs, &plan, &config, &mut |offset, len| {
                let at = offset as usize;
                bytes
                    .get(at..at + len as usize)
                    .map(|slice| slice.to_vec())
                    .ok_or_else(|| "read past end of file".to_string())
            })?;
        if std::env::var_os("WCOL_PHASE").is_some() {
            eprintln!(
                "hash reducers={} groups={:?} shuffle_bytes={}",
                stats.reducers, stats.peak_groups, stats.shuffle_bytes
            );
        }
        return Ok(state);
    }
    let mut partials = Vec::with_capacity(programs.len());
    for node in &programs {
        let mut pipe = wcol::open_wcol_plan(&node.program, RING).map_err(|e| e.to_string())?;
        let mut wire = Vec::new();
        loop {
            match pipe.poll() {
                Action::ReadAt => {
                    let (ticket, _source, offset, len) = pipe.read_at();
                    let at = offset as usize;
                    let slice = bytes
                        .get(at..at + len as usize)
                        .ok_or("read past end of file")?;
                    for part in slice.chunks(RING) {
                        pipe.input()[..part.len()].copy_from_slice(part);
                        pipe.commit_input(part.len()).map_err(|e| e.to_string())?;
                        pipe.complete_read(ticket).map_err(|e| e.to_string())?;
                    }
                }
                Action::NeedInput | Action::Yield => {}
                Action::HaveOutput => {
                    wire.extend_from_slice(pipe.output());
                    pipe.consume_output(pipe.output().len())
                        .map_err(|e| e.to_string())?;
                }
                Action::Finished => break,
                Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
                Action::Failed => {
                    return Err(pipe.error().unwrap_or("node program failed").to_string())
                }
            }
        }
        let mut offset = 0usize;
        let mut last = None;
        while offset + WCS_HEADER_BYTES <= wire.len() {
            let header = &wire[offset..offset + WCS_HEADER_BYTES];
            if u32::from_le_bytes(header[..4].try_into().unwrap()) != WCS_MAGIC {
                break;
            }
            let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            if header[5] == 4 {
                last = Some(&wire[offset + WCS_HEADER_BYTES..offset + WCS_HEADER_BYTES + len]);
            }
            offset += WCS_HEADER_BYTES + len;
        }
        partials.push(partial_state_from_wire_payload(
            last.ok_or("node produced no partial")?,
        )?);
    }
    wcol::coordinator_reduce_partials(partials)
}

fn best_of(
    runs: usize,
    bytes: &[u8],
    catalog: &[CatalogColumn],
    groups: &[IngestGroup],
    batch_rows: usize,
) -> Result<f64, String> {
    let mut best = f64::INFINITY;
    for _ in 0..runs {
        best = best.min(run(bytes, catalog, groups, batch_rows)?);
    }
    Ok(best)
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let bytes = std::fs::read(&args.path).map_err(|e| format!("reading {}: {e}", args.path))?;
    let (_file, plan) = open_plan(std::path::Path::new(&args.path)).map_err(|e| e.to_string())?;
    let catalog = catalog_of(&plan);
    println!(
        "{}: {} rows, batch_rows={}, best of {} runs (ms)",
        args.path, plan.rows, args.batch_rows, args.runs
    );
    println!("{:>8} {:>8}  query", "decode", "batch");
    for sql in &args.queries {
        let physical = match plan_query_dispatch(
            sql,
            "query_bench",
            &plan,
            1,
            args.batch_rows as u32,
            &PhysicalPlannerConfig::default(),
        ) {
            Ok((_, physical)) => physical,
            Err(err) => {
                println!("skip ({err}): {sql}");
                continue;
            }
        };
        let columns = scanned_columns(&physical);
        let groups = ingest_groups_from_plan(&plan, &columns);
        if args.print_groups {
            let nodes = if args.plan_nodes > 0 {
                args.plan_nodes
            } else {
                1
            };
            let result = run_planned(
                &bytes,
                sql,
                &plan,
                nodes,
                args.batch_rows,
                args.hash_reducers,
            )
            .map(|state| (0.0, Some(state)));
            match result {
                Ok((_, Some(state))) => {
                    println!("{}", groups_json(&partial_snapshot_from_state(&state)?)?)
                }
                Ok((_, None)) => println!("{{\"error\":\"query produced no partial\"}}"),
                Err(err) => println!("{{\"error\":{:?}}}", err),
            }
            continue;
        }
        let outcome = (|| -> Result<_, String> {
            let decode = best_of(args.runs, &bytes, &catalog, &groups, args.batch_rows)?;
            let mut batch = f64::INFINITY;
            let phases = std::env::var_os("WCOL_PHASE").is_some();
            for _ in 0..args.runs {
                if phases {
                    wcol::ops::query::batch::reset_phase_timers();
                }
                let start = Instant::now();
                run_planned(&bytes, sql, &plan, 1, args.batch_rows, args.hash_reducers)?;
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                if phases && elapsed <= batch {
                    let (hash_ns, finish_ns, filter_ns, groups_in, groups_out) =
                        wcol::ops::query::batch::phase_timers();
                    eprintln!(
                        "phase hash_ms={:.1} finish_ms={:.1} filter_ms={:.1} groups_in={groups_in} groups_out={groups_out}  {sql}",
                        hash_ns as f64 / 1e6,
                        finish_ns as f64 / 1e6,
                        filter_ns as f64 / 1e6,
                    );
                }
                batch = batch.min(elapsed);
            }
            Ok((decode, batch))
        })();
        match outcome {
            Ok((decode, batch)) => println!("{decode:>8.1} {batch:>8.1}  {sql}"),
            Err(err) => println!("FAIL ({err}): {sql}"),
        }
    }
    Ok(())
}

/// `{"groups":[[key.., agg..], ..]}` for one partial (see `--print-groups`).
fn groups_json(snap: &wcol::ops::query::PartialSnapshot) -> Result<String, String> {
    use wcol::ops::query::PartialKeyPart;
    let groups = wcol::ops::query::finalize_groups(snap)?;
    let keys = groups.first().map_or(0, |g| g.key.len());
    let mut out = format!("{{\"keys\":{keys},\"groups\":[");
    for (index, group) in groups.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let mut cells: Vec<String> = group
            .key
            .iter()
            .map(|part| match part {
                PartialKeyPart::Null => "null".to_string(),
                PartialKeyPart::Int(v) => v.to_string(),
                PartialKeyPart::Float(bits) => json_f64(f64::from_bits(*bits)),
                // Full JSON string escaping: keys can hold newlines and other control bytes.
                PartialKeyPart::Bytes(b) => {
                    serde_json::to_string(&String::from_utf8_lossy(b)).expect("strings serialise")
                }
            })
            .collect();
        for (agg, stat) in snap.aggs.iter().zip(&group.stats) {
            let integer = stat.flags & 1 != 0;
            cells.push(match agg.kind {
                0
                | 1
                | wcol::ops::query::KIND_APPROX_DISTINCT
                | wcol::ops::query::KIND_COUNT_DISTINCT => stat.count.to_string(),
                _ if stat.count == 0 => "null".to_string(),
                2 if integer => stat.sum_i.to_string(),
                2 => json_f64(stat.sum_f),
                3 if integer => format!("{}", stat.sum_i as f64 / stat.count as f64),
                3 => json_f64(stat.sum_f / stat.count as f64),
                4 if integer => stat.min_i.to_string(),
                4 => json_f64(stat.min_f),
                5 if integer => stat.max_i.to_string(),
                5 => json_f64(stat.max_f),
                other => return Err(format!("unknown aggregate kind {other}")),
            });
        }
        out.push('[');
        out.push_str(&cells.join(","));
        out.push(']');
    }
    out.push_str("]}");
    Ok(out)
}

/// DuckDB CSV spellings, quoted so the groups document stays JSON.
fn json_f64(v: f64) -> String {
    if v.is_nan() {
        "\"nan\"".to_string()
    } else if v == f64::INFINITY {
        "\"inf\"".to_string()
    } else if v == f64::NEG_INFINITY {
        "\"-inf\"".to_string()
    } else if v == 0.0 {
        "0".to_string()
    } else {
        format!("{v}")
    }
}
