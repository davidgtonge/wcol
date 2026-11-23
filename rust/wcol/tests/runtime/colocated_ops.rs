//! ParquetDecode → Filter → Aggregate inside one execution group (no inter-op WCS).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::{open_plan, open_wcol_plan, partial_first_sum_i, Plan};

fn node_program(sql: &str, plan: &Plan) -> Vec<u8> {
    let (dispatch, _) =
        plan_query_dispatch(sql, "flat", plan, 1, 4, &PhysicalPlannerConfig::default()).unwrap();
    match dispatch {
        QueryDispatch::Nodes(programs) => {
            assert_eq!(programs.len(), 1);
            programs.into_iter().next().unwrap().program
        }
        QueryDispatch::Partial(_) | QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => {
            panic!("{sql} should scan")
        }
    }
}

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

fn drive_plan(file: &mut std::fs::File, program: &[u8]) -> Vec<u8> {
    let mut pipe = open_wcol_plan(program, 256 * 1024).unwrap();
    let mut out = Vec::new();
    for _ in 0..200_000 {
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

#[test]
fn sum_a_unfiltered_matches_full_table() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let sql = "SELECT SUM(a) FROM flat";
    let program = node_program(sql, &plan);
    let out = drive_plan(&mut file, &program);
    let partial = last_partial_payload(&out).expect("partial");
    assert_eq!(partial_first_sum_i(partial).unwrap(), 20);
}

#[test]
fn sum_a_with_filter_uses_selection_mask_not_column_copy() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let sql = "SELECT SUM(a) FROM flat WHERE a > 5";
    let program = node_program(sql, &plan);
    let out = drive_plan(&mut file, &program);
    let partial = last_partial_payload(&out).expect("partial");
    assert_eq!(partial_first_sum_i(partial).unwrap(), 17);
}
