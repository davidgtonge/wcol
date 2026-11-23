//! Regression: root merge task (`OP_WCS_PARTIAL_REDUCE`) must finish when fed worker WCS bytes.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use wcol::plan::stream_program::encode_wcs_partial_reduce_program;
use wcol::stream::core::{Action, WCS_HEADER_BYTES};
use wcol::stream::FRAME_PARTIAL;
use wcol::{encode_frame, encoded_partials_equivalent, open_wcol_plan, FRAME_END};

const RING_BYTES: usize = 256 * 1024;

fn run_worker_wire(path: &std::path::Path, program: &[u8]) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, RING_BYTES).unwrap();
    let mut wire = Vec::new();
    loop {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                for part in bytes.chunks(RING_BYTES) {
                    let dst = pipe.input();
                    dst[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len()).unwrap();
                    pipe.complete_read(ticket).unwrap();
                }
            }
            Action::NeedInput | Action::Yield => {}
            Action::HaveOutput => {
                wire.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => return wire,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", pipe.error().unwrap_or("worker failed")),
        }
    }
}

fn drive_partial_reduce(program: &[u8], input: &[u8]) -> Result<Vec<u8>, String> {
    let mut pipe = open_wcol_plan(program, RING_BYTES).map_err(|e| e.to_string())?;
    let mut out_wire = Vec::new();
    let mut input_at = 0usize;
    let mut input_eof = false;
    let mut counts: std::collections::HashMap<&'static str, u32> = std::collections::HashMap::new();
    for _ in 0..100_000 {
        match pipe.poll() {
            Action::ReadAt => return Err("partial reduce must not ReadAt".into()),
            Action::NeedInput => {
                *counts.entry("NeedInput").or_insert(0) += 1;
                if input_at < input.len() {
                    let end = (input_at + RING_BYTES).min(input.len());
                    let chunk = &input[input_at..end];
                    for part in chunk.chunks(RING_BYTES) {
                        let dst = pipe.input();
                        let n = part.len().min(dst.len());
                        dst[..n].copy_from_slice(&part[..n]);
                        pipe.commit_input(n).map_err(|e| e.to_string())?;
                    }
                    input_at = end;
                } else if !input_eof {
                    pipe.end_input().map_err(|e| e.to_string())?;
                    input_eof = true;
                }
            }
            Action::Yield => {
                *counts.entry("Yield").or_insert(0) += 1;
            }
            Action::HaveOutput => {
                *counts.entry("HaveOutput").or_insert(0) += 1;
                out_wire.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => return Ok(out_wire),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => {
                return Err(format!("failed: {}", pipe.error().unwrap_or("failed")));
            }
        }
    }
    Err(format!("partial reduce exceeded poll budget: {counts:?}"))
}

#[test]
fn wcs_partial_reduce_merges_one_worker_stream() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = root.join("tests/fixtures/flat.parquet");
    if !fixture.is_file() {
        eprintln!("skip: missing flat.parquet");
        return;
    }

    let (_plan, footer) = wcol::formats::parquet::open_plan(&fixture).unwrap();
    let catalog: Vec<_> = footer
        .leaves
        .iter()
        .map(|leaf| wcol::ops::query::CatalogColumn {
            name: leaf.name.clone(),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                wcol::plan::sql_parser::ColumnKind::Text
            } else {
                wcol::plan::sql_parser::ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();

    let columns: Vec<_> = footer
        .leaves
        .iter()
        .map(|leaf| wcol::plan::sql_parser::Column {
            name: Box::leak(leaf.name.clone().into_boxed_str()),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                wcol::plan::sql_parser::ColumnKind::Text
            } else {
                wcol::plan::sql_parser::ColumnKind::Number
            },
        })
        .collect();
    let metadata = wcol::plan::ParquetMetadataCatalog::from_footer(
        "flat",
        fixture.file_name().unwrap().to_str().unwrap(),
        &footer,
    );
    let logical =
        wcol::plan::lower_sql_v0_to_logical("SELECT SUM(a) FROM flat", &columns, "flat").unwrap();
    let physical = wcol::plan::lower_to_physical(
        &logical,
        &metadata,
        &[wcol::plan::NodeId(0)],
        &wcol::plan::PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let fragments = wcol::plan::node_fragments(
        &physical,
        wcol::plan::PhysicalPlannerConfig::default().fetch_depth,
    )
    .unwrap();
    let fragment = fragments
        .get(&wcol::plan::NodeId(0))
        .expect("node 0 fragment");
    let worker_program = wcol::plan::encode_node_fragment_program(4, &catalog, fragment);
    let worker_wire = run_worker_wire(&fixture, &worker_program);
    assert!(worker_wire.len() > 32, "worker produced WCS wire");

    let reduce_program = encode_wcs_partial_reduce_program(1);
    let merged_wire = drive_partial_reduce(&reduce_program, &worker_wire).expect("partial reduce");
    assert!(!merged_wire.is_empty(), "merge produced output");
}

fn last_partial_payload(wire: &[u8]) -> Option<&[u8]> {
    let mut last = None;
    let mut at = 0usize;
    while at + WCS_HEADER_BYTES <= wire.len() {
        let ty = wire[at + 5];
        let len = u32::from_le_bytes(wire[at + 8..at + 12].try_into().unwrap()) as usize;
        let end = at + WCS_HEADER_BYTES + len;
        if end > wire.len() {
            break;
        }
        if ty == FRAME_PARTIAL {
            last = Some(&wire[at + WCS_HEADER_BYTES..end]);
        }
        at = end;
    }
    last
}

fn encode_end_frame() -> Vec<u8> {
    let mut out = vec![0u8; 64];
    let n = encode_frame(FRAME_END, 0, &[], &mut out).unwrap();
    out.truncate(n);
    out
}

fn drive_ports(program: &[u8], wires: &[Vec<u8>], chunk: usize) -> Result<Vec<u8>, String> {
    let mut pipe = open_wcol_plan(program, RING_BYTES).map_err(|e| e.to_string())?;
    let mut at = vec![0usize; wires.len()];
    let mut eof = vec![false; wires.len()];
    let mut rr = 0usize;
    let mut out = Vec::new();
    for _ in 0..100_000 {
        match pipe.poll() {
            Action::NeedInput => {
                let nports = wires.len();
                let mut fed = false;
                for step in 0..nports {
                    let port = (rr + step) % nports;
                    if at[port] < wires[port].len() {
                        let dst = pipe.input();
                        if dst.is_empty() {
                            break;
                        }
                        let n = dst.len().min(wires[port].len() - at[port]).min(chunk);
                        dst[..n].copy_from_slice(&wires[port][at[port]..at[port] + n]);
                        pipe.commit_input_port(port as u32, n)
                            .map_err(|e| e.to_string())?;
                        at[port] += n;
                        rr = port + 1;
                        fed = true;
                        break;
                    }
                }
                if !fed {
                    if let Some(port) = eof.iter().position(|done| !done) {
                        pipe.end_input_port(port as u32)
                            .map_err(|e| e.to_string())?;
                        eof[port] = true;
                    }
                }
            }
            Action::Yield => {}
            Action::HaveOutput => {
                out.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => return Ok(out),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => {
                return Err(pipe.error().unwrap_or("failed").to_string());
            }
            Action::ReadAt => return Err("partial reduce must not ReadAt".into()),
        }
    }
    Err("poll budget".into())
}

fn worker_sum_wire() -> Option<Vec<u8>> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = root.join("tests/fixtures/flat.parquet");
    if !fixture.is_file() {
        return None;
    }
    let (_plan, footer) = wcol::formats::parquet::open_plan(&fixture).unwrap();
    let catalog: Vec<_> = footer
        .leaves
        .iter()
        .map(|leaf| wcol::ops::query::CatalogColumn {
            name: leaf.name.clone(),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                wcol::plan::sql_parser::ColumnKind::Text
            } else {
                wcol::plan::sql_parser::ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();
    let columns: Vec<_> = footer
        .leaves
        .iter()
        .map(|leaf| wcol::plan::sql_parser::Column {
            name: Box::leak(leaf.name.clone().into_boxed_str()),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                wcol::plan::sql_parser::ColumnKind::Text
            } else {
                wcol::plan::sql_parser::ColumnKind::Number
            },
        })
        .collect();
    let metadata = wcol::plan::ParquetMetadataCatalog::from_footer(
        "flat",
        fixture.file_name().unwrap().to_str().unwrap(),
        &footer,
    );
    let logical =
        wcol::plan::lower_sql_v0_to_logical("SELECT SUM(a) FROM flat", &columns, "flat").unwrap();
    let physical = wcol::plan::lower_to_physical(
        &logical,
        &metadata,
        &[wcol::plan::NodeId(0)],
        &wcol::plan::PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let fragments = wcol::plan::node_fragments(
        &physical,
        wcol::plan::PhysicalPlannerConfig::default().fetch_depth,
    )
    .unwrap();
    let fragment = fragments
        .get(&wcol::plan::NodeId(0))
        .expect("node 0 fragment");
    let worker_program = wcol::plan::encode_node_fragment_program(4, &catalog, fragment);
    Some(run_worker_wire(&fixture, &worker_program))
}

#[test]
fn interleaved_ports_match_sequential_partial_reduce() {
    let Some(wire) = worker_sum_wire() else {
        eprintln!("skip: missing flat.parquet");
        return;
    };
    let program = encode_wcs_partial_reduce_program(2);
    let sequential = drive_ports(&program, &[wire.clone(), wire.clone()], usize::MAX).unwrap();
    let interleaved = drive_ports(&program, &[wire.clone(), wire], 1).unwrap();
    let left = last_partial_payload(&sequential).expect("sequential partial");
    let right = last_partial_payload(&interleaved).expect("interleaved partial");
    encoded_partials_equivalent(left, right).expect("chunk order must not change the reduce");
}

#[test]
fn eof_before_frame_end_is_rejected() {
    let program = encode_wcs_partial_reduce_program(1);
    let err = drive_ports(&program, &[Vec::new()], 8).unwrap_err();
    assert!(err.contains("transport EOF before FRAME_END"), "{err}");
}

#[test]
fn incomplete_frame_at_eof_is_rejected() {
    let program = encode_wcs_partial_reduce_program(1);
    let err = drive_ports(&program, &[encode_end_frame()[..4].to_vec()], 8).unwrap_err();
    assert!(err.contains("incomplete WCS frame at EOF"), "{err}");
}

#[test]
fn duplicate_frame_end_is_rejected() {
    let program = encode_wcs_partial_reduce_program(1);
    let mut wire = encode_end_frame();
    wire.extend_from_slice(&encode_end_frame());
    let err = drive_ports(&program, &[wire], 8).unwrap_err();
    assert!(
        err.contains("duplicate FRAME_END") || err.contains("data after FRAME_END"),
        "{err}"
    );
}

#[test]
fn data_after_frame_end_is_rejected() {
    let program = encode_wcs_partial_reduce_program(1);
    let mut wire = encode_end_frame();
    wire.extend_from_slice(&[1, 2, 3, 4]);
    let err = drive_ports(&program, &[wire], 8).unwrap_err();
    assert!(err.contains("data after FRAME_END"), "{err}");
}

#[test]
fn invalid_port_is_rejected() {
    let program = encode_wcs_partial_reduce_program(1);
    let mut pipe = open_wcol_plan(&program, RING_BYTES).unwrap();
    match pipe.poll() {
        Action::NeedInput => {}
        other => panic!("expected NeedInput, got {other:?}"),
    }
    let frame = encode_end_frame();
    pipe.input()[..frame.len()].copy_from_slice(&frame);
    let err = pipe.commit_input_port(3, frame.len()).unwrap_err();
    assert!(err.as_str().contains("invalid WCS input port"), "{err}");
}
