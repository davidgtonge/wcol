//! Distributed row-group jobs with string `GROUP BY` keys (coordinator `ReduceOp`).

#[path = "../common/claims.rs"]
mod claims;

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use wcol::formats::parquet::open_plan;
use wcol::ops::query::encoded_partials_equivalent;
use wcol::plan::{
    coordinator_reduce_partials, plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch,
};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::{open_wcol_plan, partial_state_from_wcs_bytes};

/// `HITS_PARQUET`, else the local ClickBench subset when `data/` has it.
fn hits_path() -> Option<PathBuf> {
    std::env::var("HITS_PARQUET")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            Some(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../data/hits_subset_500k.parquet"),
            )
        })
        .filter(|p| p.exists())
}

/// Every partial frame on a node's wire. A node sends its partial in several frames, each a
/// partial over some of its groups, so a check that read only the last one compared chunks.
fn partial_payloads(wire: &[u8]) -> Vec<&[u8]> {
    let mut offset = 0usize;
    let mut payloads = Vec::new();
    while offset + WCS_HEADER_BYTES <= wire.len() {
        let hdr = &wire[offset..offset + WCS_HEADER_BYTES];
        if u32::from_le_bytes(hdr[..4].try_into().unwrap()) != WCS_MAGIC {
            break;
        }
        let frame_type = hdr[5];
        let payload_len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let end = offset + WCS_HEADER_BYTES + payload_len;
        if end > wire.len() {
            break;
        }
        if frame_type == 4 {
            payloads.push(&wire[offset + WCS_HEADER_BYTES..end]);
        }
        offset = end;
    }
    payloads
}

/// One node's whole partial: its frames decoded and merged.
fn node_partial(
    path: &Path,
    program: &[u8],
    own_units: &[u32],
) -> wcol::stream::partials::PartialStateWcol {
    let wire = run_wire_program(path, program, own_units);
    let frames: Vec<_> = partial_payloads(&wire)
        .into_iter()
        .map(|payload| partial_state_from_wcs_bytes(payload).expect("decode node partial"))
        .collect();
    assert!(!frames.is_empty(), "node sent no partial");
    coordinator_reduce_partials(frames).expect("merge node partial frames")
}

fn run_wire_program(path: &Path, program: &[u8], own_units: &[u32]) -> Vec<u8> {
    let mut claims = claims::Claims::new(own_units);
    let mut file = std::fs::File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, 512 * 1024).unwrap();
    let mut out = Vec::new();
    for _ in 0..500_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, source, offset, len) = pipe.read_at();
                if let Some(unit) = claims.answer(source) {
                    pipe.input()[..4].copy_from_slice(&unit);
                    pipe.commit_input(4).unwrap();
                    pipe.complete_read(ticket).unwrap();
                    continue;
                }
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                // A planned fetch can exceed the input ring: commit it in ring-sized parts.
                for part in bytes.chunks(512 * 1024) {
                    let dst = pipe.input();
                    dst[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len()).unwrap();
                    pipe.complete_read(ticket).unwrap();
                }
            }
            Action::NeedInput => {}
            Action::HaveOutput => {
                out.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Yield => {}
            Action::Finished => break,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", pipe.error().unwrap()),
        }
    }
    out
}

/// ClickBench Q12 without its `ORDER BY c DESC LIMIT 10`. One node holds every group whole, so
/// it applies the top-10 itself; split nodes cannot, so their partials keep every group. With
/// the limit, the two partial states would differ by design.
const SEARCH_PHRASE_COUNTS: &str =
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase";

#[test]
fn distributed_string_group_by_matches_monolithic_on_hits_slice() {
    let path = match hits_path() {
        Some(p) => p,
        None => {
            eprintln!("skip: set HITS_PARQUET or add data/hits_subset_500k.parquet");
            return;
        }
    };
    let (_file, plan) = open_plan(&path).unwrap();
    let batch_rows = 32_768u32;
    let mut footer = plan.clone();
    footer.chunks.retain(|chunk| chunk.row_group < 14);
    let group_count = footer
        .chunks
        .iter()
        .map(|chunk| chunk.row_group)
        .collect::<std::collections::HashSet<_>>()
        .len();
    if group_count < 2 {
        eprintln!("skip: need at least two row groups to shard");
        return;
    }

    let (mono, _) = plan_query_dispatch(
        SEARCH_PHRASE_COUNTS,
        "hits",
        &footer,
        1,
        batch_rows,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let mono_partial = match mono {
        QueryDispatch::Partial(bytes) => bytes,
        QueryDispatch::Nodes(programs) => wcol::partial_state_to_wcs_bytes(&node_partial(
            &path,
            &programs[0].program,
            programs[0]
                .work
                .as_ref()
                .map_or(&[][..], |work| &work.own_units),
        ))
        .expect("encode one-node partial"),
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => {
            panic!("a GROUP BY is an aggregate")
        }
    };
    let (dispatch, _plan) = plan_query_dispatch(
        SEARCH_PHRASE_COUNTS,
        "hits",
        &footer,
        2,
        batch_rows,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let QueryDispatch::Nodes(programs) = dispatch else {
        panic!("a GROUP BY needs node work");
    };
    assert!(programs.len() >= 2);

    let worker_partials: Vec<_> = programs
        .iter()
        .map(|node| {
            node_partial(
                &path,
                &node.program,
                node.work.as_ref().map_or(&[][..], |work| &work.own_units),
            )
        })
        .collect();

    let merged = coordinator_reduce_partials(worker_partials).expect("coordinator reduce");
    let merged_bytes = wcol::partial_state_to_wcs_bytes(&merged).expect("encode merged partial");

    encoded_partials_equivalent(&mono_partial, &merged_bytes)
        .expect("distributed string GROUP BY must match the one-node plan");
}
