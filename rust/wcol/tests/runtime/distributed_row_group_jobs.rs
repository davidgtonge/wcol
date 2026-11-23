//! Node programs from the physical plan + coordinator [`ReduceOp`] match a one-node plan.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use wcol::formats::parquet::open_plan;
use wcol::ops::query::encoded_partials_equivalent;
use wcol::plan::{
    coordinator_reduce_partials, plan_query_dispatch, Frontend, PhysicalPlannerConfig,
    QueryDispatch,
};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::{open_wcol_plan, partial_state_from_wcs_bytes};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

fn last_partial_payload(wire: &[u8]) -> Option<&[u8]> {
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
            last = Some(&wire[offset + WCS_HEADER_BYTES..end]);
        }
        offset = end;
    }
    last
}

fn run_wire_program(path: &Path, program: &[u8]) -> Vec<u8> {
    let mut file = std::fs::File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, 512 * 1024).unwrap();
    let mut out = Vec::new();
    for _ in 0..500_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
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

#[test]
fn distributed_workers_match_monolithic_partial_on_flat() {
    let path = fixture_path();
    if !path.exists() {
        eprintln!("skip: flat.parquet missing");
        return;
    }
    let (file, plan) = open_plan(&path).unwrap();
    drop(file);
    let sql = "SELECT SUM(a) FROM flat";
    let batch_rows = 4096u32;

    let (mono, _) = plan_query_dispatch(
        sql,
        "flat.parquet",
        &plan,
        1,
        batch_rows,
        &PhysicalPlannerConfig::default(),
        &[Frontend::Sql],
    )
    .unwrap();
    let mono_partial = match mono {
        QueryDispatch::Partial(bytes) => bytes,
        QueryDispatch::Nodes(programs) => {
            assert_eq!(programs.len(), 1);
            let wire = run_wire_program(&path, &programs[0].program);
            last_partial_payload(&wire)
                .expect("one-node partial")
                .to_vec()
        }
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => panic!("SUM(a) is an aggregate"),
    };

    // Two node slots: the physical planner decides which row groups each one reads.
    let (dispatch, _plan) = plan_query_dispatch(
        sql,
        "flat.parquet",
        &plan,
        2,
        batch_rows,
        &PhysicalPlannerConfig::default(),
        &[Frontend::Sql],
    )
    .unwrap();
    let QueryDispatch::Nodes(programs) = dispatch else {
        panic!("SUM(a) needs node work");
    };
    assert!(!programs.is_empty());

    let worker_partials: Vec<_> = programs
        .iter()
        .map(|node| {
            let wire = run_wire_program(&path, &node.program);
            let payload = last_partial_payload(&wire).expect("worker partial");
            partial_state_from_wcs_bytes(payload).expect("decode worker partial")
        })
        .collect();

    let merged = coordinator_reduce_partials(worker_partials).expect("coordinator reduce");
    let merged_bytes = wcol::partial_state_to_wcs_bytes(&merged).expect("encode merged partial");

    encoded_partials_equivalent(&mono_partial, &merged_bytes)
        .expect("two-node merge must match the one-node plan");
}
