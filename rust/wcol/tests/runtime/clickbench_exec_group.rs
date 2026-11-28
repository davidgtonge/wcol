//! ClickBench Q8 / Q3 / Q1 through the ingest graph on the committed 500k hits sample,
//! checked against DuckDB's answers on the same file.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use wcol::ops::query::{finalize_groups, PartialKeyPart, PartialSnapshot};
use wcol::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::stream::partials::partial_snapshot_from_wire;
use wcol::{open_plan, open_wcol_plan};

const Q8: &str = "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC";
const Q3: &str = "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits";
const Q1: &str = "SELECT COUNT(*) FROM hits";

fn sample_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
}

fn last_partial_payload(wire: &[u8]) -> Option<Vec<u8>> {
    let mut offset = 0usize;
    let mut last = None;
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
            last = Some(wire[offset + WCS_HEADER_BYTES..end].to_vec());
        }
        offset = end;
    }
    last
}

fn run_execution_group(path: &Path, program: &[u8]) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, 1024 * 1024).unwrap();
    let mut out = Vec::new();
    for _ in 0..10_000_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                let dst = pipe.input();
                assert!(dst.len() >= bytes.len());
                dst[..bytes.len()].copy_from_slice(&bytes);
                pipe.commit_input(bytes.len()).unwrap();
                pipe.complete_read(ticket).unwrap();
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

/// The query's partial state on one node, as the ingest graph emits it.
fn sample_partial(sql: &str) -> PartialSnapshot {
    let path = sample_path();
    let (_file, plan) = open_plan(&path).unwrap();
    let (dispatch, _) = plan_query_dispatch(
        sql,
        "hits",
        &plan,
        1,
        32_768,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let wire = match dispatch {
        QueryDispatch::Partial(partial) => partial,
        QueryDispatch::Nodes(programs) => {
            assert_eq!(programs.len(), 1, "{sql}");
            let wire = run_execution_group(&path, &programs[0].program);
            last_partial_payload(&wire).expect("WCS partial")
        }
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => panic!("{sql} is a row query"),
    };
    partial_snapshot_from_wire(&wire).unwrap()
}

#[test]
fn clickbench_q8_matches_oracle() {
    let groups = finalize_groups(&sample_partial(Q8)).unwrap();
    let got: BTreeMap<i128, u64> = groups
        .iter()
        .filter(|group| group.stats[0].count > 0)
        .map(|group| {
            let PartialKeyPart::Int(key) = group.key[0] else {
                panic!("expected int group key");
            };
            (key, group.stats[0].count)
        })
        .collect();
    assert_eq!(
        got,
        BTreeMap::from([(2, 9409), (13, 4256), (50, 4), (52, 32)])
    );
}

#[test]
fn clickbench_q3_matches_oracle() {
    let snap = sample_partial(Q3);
    let stats = &snap.groups[0].stats;
    assert_eq!(stats[0].sum_i, 76_010);
    assert_eq!(stats[1].count, 500_000);
    let mean = stats[2].sum_i as f64 / stats[2].count as f64;
    assert!((mean - 1601.941292).abs() < 1e-6, "{mean}");
}

#[test]
fn clickbench_q1_metadata_count() {
    assert_eq!(sample_partial(Q1).groups[0].stats[0].count, 500_000);
}
