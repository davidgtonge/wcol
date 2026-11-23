//! Execution from the physical plan, for every ClickBench query the hand parser lowers.
//!
//! Each node runs its fragment as a `wcs_open` program ([`OP_NODE_FRAGMENT`]): no SQL in the
//! bytes, reads are the plan's fetches, operators come from the fragment. One, two, and four
//! node slots must finish the same groups. Host reads must equal the plan's per-node metrics,
//! and resident fetch bytes stay bounded.
//!
//! [`OP_NODE_FRAGMENT`]: wcol::plan::stream_program::OP_NODE_FRAGMENT

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use wcol::plan::catalog_from_footer;

use wcol::exec::operator::{Operator, OperatorIo, StreamProgress};
use wcol::formats::parquet::open_plan;
use wcol::ops::parquet_source::ParquetSourceOp;
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::{Column, ColumnKind};
use wcol::plan::{
    encode_node_fragment_program, lower_sql_v0_to_logical, lower_to_physical, node_fragments,
    NodeFragment, NodeId, OpKind, ParquetMetadataCatalog, PhysicalPlannerConfig,
};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::stream::partials::partial_snapshot_from_wire;
use wcol::{coordinator_reduce_partials, open_wcol_plan};

const BATCH_ROWS: u32 = 32_768;
/// Smaller than a planned fetch (2 MiB), so large fetches arrive in several ring-sized parts.
const RING_BYTES: usize = 256 * 1024;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Finalized groups, cut to the query's top-k when it orders groups by count. One node holds
/// every group whole and applies the top-k itself; merged partials of several nodes do not,
/// so both sides are cut the same way here: count descending, then key.
fn top_groups(snap: &wcol::ops::query::PartialSnapshot) -> Vec<wcol::ops::query::PartialGroup> {
    let mut groups = wcol::ops::query::finalize_groups(snap).unwrap();
    if snap.order_groups_by_count && snap.limit > 0 {
        let count = |group: &wcol::ops::query::PartialGroup| {
            group.stats.first().map_or(0, |stat| stat.count)
        };
        groups.sort_by(|left, right| {
            count(right)
                .cmp(&count(left))
                .then_with(|| left.key.cmp(&right.key))
        });
        groups.truncate((snap.offset + snap.limit) as usize);
    }
    groups
}

struct ProgramRun {
    /// Every partial frame the node sent: a node splits its partial across several frames.
    partials: Vec<Vec<u8>>,
    host_requests: u32,
    host_bytes: u64,
}

/// Drive a `wcs_open` program like the Wasm host does. Responses larger than the input ring
/// are committed in ring-sized parts under the same ticket.
fn run_program(path: &Path, program: &[u8]) -> Result<ProgramRun, String> {
    let mut file = File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, RING_BYTES).map_err(|e| e.to_string())?;
    let mut wire = Vec::new();
    let (mut host_requests, mut host_bytes) = (0u32, 0u64);
    loop {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                host_requests += 1;
                host_bytes += u64::from(len);
                for part in bytes.chunks(RING_BYTES) {
                    let dst = pipe.input();
                    dst[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len()).map_err(|e| e.to_string())?;
                    pipe.complete_read(ticket).map_err(|e| e.to_string())?;
                }
            }
            Action::NeedInput | Action::Yield => {}
            Action::HaveOutput => {
                wire.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => break,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => return Err(pipe.error().unwrap_or("failed").to_string()),
        }
    }
    let mut offset = 0usize;
    let mut partials = Vec::new();
    while offset + WCS_HEADER_BYTES <= wire.len() {
        let hdr = &wire[offset..offset + WCS_HEADER_BYTES];
        if u32::from_le_bytes(hdr[..4].try_into().unwrap()) != WCS_MAGIC {
            break;
        }
        let payload_len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let end = offset + WCS_HEADER_BYTES + payload_len;
        if hdr[5] == 4 {
            partials.push(wire[offset + WCS_HEADER_BYTES..end].to_vec());
        }
        offset = end;
    }
    if partials.is_empty() {
        return Err("no partial frame".into());
    }
    Ok(ProgramRun {
        partials,
        host_requests,
        host_bytes,
    })
}

/// Peak fetch bytes held at once while the fragment's source drains (no operators needed).
fn peak_resident(path: &Path, fragment: &NodeFragment, catalog: &[CatalogColumn]) -> u64 {
    let mut source = ParquetSourceOp::with_fetches(
        BATCH_ROWS as usize,
        catalog.to_vec(),
        fragment.ingest_groups().unwrap(),
        Default::default(),
        fragment.fetches.clone(),
        fragment.logical_reads.clone(),
        fragment.fetch_depth as usize,
    );
    let mut file = File::open(path).unwrap();
    loop {
        let mut out = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: None,
            input_eof: false,
            output: &mut out,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match source.poll(&mut io).unwrap() {
            StreamProgress::Finished => break,
            StreamProgress::NeedHostRead {
                ticket,
                offset,
                length,
                ..
            } => {
                let mut bytes = vec![0u8; length as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                source.complete_host_read(ticket, bytes).unwrap();
            }
            StreamProgress::HaveOutput | StreamProgress::Blocked | StreamProgress::NeedInput => {}
        }
    }
    source.fetch_metrics().unwrap().peak_resident_bytes
}

#[test]
fn clickbench_queries_run_from_node_fragment_programs() {
    let path = root().join("data/hits_subset_500k.parquet");
    if !path.is_file() {
        eprintln!("skip: missing {}", path.display());
        return;
    }
    let (_file, footer) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&footer);
    let columns: Vec<Column<'static>> = footer
        .leaves
        .iter()
        .map(|leaf| Column {
            name: Box::leak(leaf.name.clone().into_boxed_str()),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
        })
        .collect();
    let metadata = ParquetMetadataCatalog::from_footer("hits", "hits_subset_500k.parquet", &footer);
    // Gather aggregates: each node's fragment ends in a partial, which is what this test merges
    // and compares. Left to size itself, the planner hash-partitions grouped queries, whose scan
    // fragments send rows to reducers instead (covered by the hash exchange tests).
    let mut config = PhysicalPlannerConfig::default();
    config.hash_aggregate.reducers = Some(1);
    let tsv = std::fs::read_to_string(root().join("scripts/clickbench-queries.tsv")).unwrap();

    let mut baseline = std::collections::HashMap::<String, Vec<String>>::new();
    for node_count in [1u32, 2, 4] {
        let nodes: Vec<NodeId> = (0..node_count).map(NodeId).collect();
        let mut ran = 0usize;
        let mut not_run = Vec::new();
        for line in tsv.lines() {
            let Some((label, sql)) = line.split_once('\t') else {
                continue;
            };
            let sql = sql.trim().trim_end_matches(';');
            let Ok(logical) = lower_sql_v0_to_logical(sql, &columns, "hits") else {
                continue;
            };
            let plan = lower_to_physical(&logical, &metadata, &nodes, &config).unwrap();
            if plan
                .operators
                .iter()
                .any(|op| matches!(op.kind, OpKind::MetadataAggregate { .. }))
            {
                not_run.push(format!("{label}: footer metadata (no node work)"));
                continue;
            }
            if plan
                .operators
                .iter()
                .any(|op| matches!(op.kind, OpKind::Sink { .. }))
            {
                not_run.push(format!("{label}: row query"));
                continue;
            }
            let fragments = node_fragments(&plan, config.fetch_depth).unwrap();
            let mut partials = Vec::new();
            let mut skipped = None;
            for (node, fragment) in &fragments {
                let program = encode_node_fragment_program(BATCH_ROWS, &catalog, fragment);
                assert!(
                    !program
                        .windows(6)
                        .any(|w| w.eq_ignore_ascii_case(b"select")),
                    "{label}: SQL text in the node program"
                );
                let run = match run_program(&path, &program) {
                    Ok(run) => run,
                    Err(err) => {
                        skipped = Some(err);
                        break;
                    }
                };
                let planned = plan.metrics.nodes.iter().find(|m| m.node == *node).unwrap();
                assert_eq!(
                    run.host_requests, planned.request_count,
                    "{label} node {} requests",
                    node.0
                );
                assert_eq!(
                    run.host_bytes, planned.fetched_bytes,
                    "{label} node {} bytes",
                    node.0
                );
                if node_count == 2 {
                    // Speculative prefetch holds at most `fetch_depth` fetches; demand loads add at
                    // most one fetch per column the decoder is reading at once.
                    let columns = fragment
                        .decode
                        .iter()
                        .map(|g| g.columns.len())
                        .max()
                        .unwrap_or(0);
                    let bound = (columns + config.fetch_depth) as u64 * config.max_request_bytes;
                    let peak = peak_resident(&path, fragment, &catalog);
                    assert!(
                        peak <= bound,
                        "{label} node {} peak resident {peak} > {bound}",
                        node.0
                    );
                }
                for payload in &run.partials {
                    partials.push(
                        wcol::stream::partial_wcs::partial_state_from_wire_payload(payload)
                            .unwrap(),
                    );
                }
            }
            if let Some(err) = skipped {
                not_run.push(format!("{label}: {err}"));
                continue;
            }
            let merged = coordinator_reduce_partials(partials).unwrap();
            let got = wcol::stream::partial_wcs::partial_state_to_wcs_payload(&merged).unwrap();
            // Finalized groups, as text, since unused float fields are NaN and NaN != NaN.
            // Sorted, since the order of groups with equal counts depends on merge order.
            let snap = partial_snapshot_from_wire(&got).unwrap();
            let mut got_groups: Vec<String> = top_groups(&snap)
                .iter()
                .map(|group| format!("{group:?}"))
                .collect();
            got_groups.sort_unstable();
            if node_count == 1 {
                baseline.insert(label.to_string(), got_groups);
            } else {
                let expect_groups = baseline.get(label).unwrap_or_else(|| {
                    panic!("{label} ran on {node_count} nodes but not on one node")
                });
                assert_eq!(
                    &got_groups, expect_groups,
                    "{label} on {node_count} nodes disagrees with one node"
                );
            }
            ran += 1;
        }
        eprintln!(
            "{node_count} node(s): ran {ran} queries from fragments; not run:\n  {}",
            not_run.join("\n  ")
        );
        // Footer-only answers (COUNT(*), and MIN/MAX of Q6) need no fragment; job_plan.rs
        // checks them against a scan.
        assert!(
            ran >= 46,
            "expected 46 ClickBench queries from fragments on {node_count} nodes, got {ran}"
        );
    }
}
