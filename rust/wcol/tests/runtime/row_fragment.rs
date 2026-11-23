//! Row queries run from the physical plan: node fragments emit typed frames, the coordinator
//! sorts and limits them. Node programs contain no SQL.

use wcol::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use wcol::exec::operators::RowBatchWcsIngress;
use wcol::formats::parquet::{open_plan, ParquetEncode, ParquetEncodeConfig, ParquetSinkColumn};
use wcol::open_wcol_plan;
use wcol::plan::{plan_query_dispatch, Frontend, PhysicalPlannerConfig, QueryDispatch};
use wcol::stream::core::Action;
use wcol::stream::{OwnedWcolBatch, WcolColumn};

fn drive_encode(enc: &mut ParquetEncode, batch: OwnedWcolBatch) -> Result<Vec<u8>, String> {
    let mut file = Vec::new();
    let mut ring = vec![0u8; 64 * 1024];
    let mut pending = Some(WcolStreamItem::from_row_batch(batch));
    let mut eof = false;
    for _ in 0..100_000 {
        if pending.is_none() {
            eof = true;
        }
        let mut written = 0usize;
        let mut unused = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof: eof,
            output: &mut unused,
            host_bytes: None,
            output_port: Some(&mut ring),
            bytes_written: &mut written,
        };
        match enc.poll(&mut io)? {
            StreamProgress::Finished => {
                file.extend_from_slice(&ring[..written]);
                return Ok(file);
            }
            StreamProgress::HaveOutput => file.extend_from_slice(&ring[..written]),
            StreamProgress::NeedInput | StreamProgress::Blocked => {}
            StreamProgress::NeedHostRead { .. } => return Err("unexpected host read".to_string()),
        }
    }
    Err("encode did not finish".to_string())
}

fn sample_file() -> Vec<u8> {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![0, 1, 2, 3, 4, 5], vec![1, 1, 1, 1, 1, 1]);
    batch.set_row_count(6).unwrap();
    let mut enc = ParquetEncode::open_typed(
        vec![ParquetSinkColumn {
            name: "a".into(),
            optional: false,
            physical: 2,
            type_length: 0,
            date: false,
        }],
        ParquetEncodeConfig {
            max_row_group_rows: 3,
            max_row_groups: 4,
        },
    )
    .unwrap();
    drive_encode(&mut enc, batch).unwrap()
}

fn run_node(program: &[u8], file: &[u8]) -> (Vec<u8>, u32, u64) {
    let mut host = open_wcol_plan(program, 64 * 1024).unwrap();
    let mut wire = Vec::new();
    let mut reads = 0u32;
    let mut bytes = 0u64;
    for _ in 0..100_000 {
        match host.poll() {
            Action::ReadAt => {
                let (ticket, _, offset, len) = host.read_at();
                reads += 1;
                bytes += u64::from(len);
                let slice = &file[offset as usize..offset as usize + len as usize];
                host.input()[..slice.len()].copy_from_slice(slice);
                host.commit_input(slice.len()).unwrap();
                host.complete_read(ticket).unwrap();
            }
            Action::HaveOutput => {
                wire.extend_from_slice(host.output());
                host.consume_output(host.output().len()).unwrap();
            }
            Action::NeedInput | Action::Yield => {}
            Action::Finished => return (wire, reads, bytes),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", host.error().unwrap()),
        }
    }
    panic!("node did not finish");
}

fn run_coordinator(program: &[u8], wires: &[Vec<u8>]) -> Vec<u8> {
    let mut host = open_wcol_plan(program, 64 * 1024).unwrap();
    let mut at = vec![0usize; wires.len()];
    let mut eof = vec![false; wires.len()];
    let mut rr = 0usize;
    let mut out = Vec::new();
    for _ in 0..100_000 {
        match host.poll() {
            Action::NeedInput => {
                let nports = wires.len();
                let mut fed = false;
                for step in 0..nports {
                    let port = (rr + step) % nports;
                    if at[port] < wires[port].len() {
                        let dst = host.input();
                        if dst.is_empty() {
                            break;
                        }
                        let n = dst.len().min(wires[port].len() - at[port]);
                        dst[..n].copy_from_slice(&wires[port][at[port]..at[port] + n]);
                        host.commit_input_port(port as u32, n).unwrap();
                        at[port] += n;
                        rr = port + 1;
                        fed = true;
                        break;
                    }
                }
                if !fed {
                    if let Some(port) = eof.iter().position(|done| !done) {
                        host.end_input_port(port as u32).unwrap();
                        eof[port] = true;
                    }
                }
            }
            Action::Yield => {}
            Action::HaveOutput => {
                out.extend_from_slice(host.output());
                host.consume_output(host.output().len()).unwrap();
            }
            Action::Finished => return out,
            Action::ReadAt => panic!("coordinator requested a host read"),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", host.error().unwrap()),
        }
    }
    panic!("coordinator did not finish");
}

fn column_ints(wire: &[u8]) -> Vec<i64> {
    let mut ingress = RowBatchWcsIngress::new();
    let mut at = 0usize;
    while at < wire.len() {
        at += ingress.feed(&wire[at..]).unwrap();
    }
    ingress.finish_feed().unwrap();
    let mut values = Vec::new();
    for batch in ingress.take_batches() {
        let mut buf = Vec::new();
        let view = batch.view_with(&mut buf);
        let WcolColumn::I64 {
            values: rows,
            nulls,
            ..
        } = &view.columns[0]
        else {
            panic!("expected an i64 column");
        };
        for (row, value) in rows.iter().enumerate() {
            if nulls.get(row).copied().unwrap_or(1) != 0 {
                values.push(*value);
            }
        }
    }
    values
}

fn dispatch(sql: &str, file: &[u8], nodes: u32) -> QueryDispatch {
    let path = std::env::temp_dir().join(format!(
        "wcol-row-fragment-{nodes}-{}.parquet",
        std::process::id()
    ));
    std::fs::write(&path, file).unwrap();
    let (_, plan) = open_plan(&path).unwrap();
    let _ = std::fs::remove_file(&path);
    plan_query_dispatch(
        sql,
        "rows.parquet",
        &plan,
        nodes,
        8,
        &PhysicalPlannerConfig::default(),
        &[Frontend::Sql],
    )
    .unwrap()
    .0
}

#[test]
fn ordered_row_query_matches_on_one_two_and_four_nodes() {
    let file = sample_file();
    for nodes in [1u32, 2, 4] {
        let QueryDispatch::Rows {
            nodes: programs,
            coordinator,
        } = dispatch("SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1", &file, nodes)
        else {
            panic!("expected a row dispatch");
        };
        assert!(!programs.is_empty());
        let mut wires = Vec::new();
        for node in &programs {
            assert!(
                !node
                    .program
                    .windows(6)
                    .any(|window| window.eq_ignore_ascii_case(b"select")),
                "node program contains SQL"
            );
            assert_eq!(node.program[5], 12, "node opcode");
            let (wire, reads, bytes) = run_node(&node.program, &file);
            assert_eq!(reads, node.request_count, "node {} reads", node.node.0);
            assert_eq!(bytes, node.fetched_bytes, "node {} bytes", node.node.0);
            wires.push(wire);
        }
        assert_eq!(coordinator[5], 13, "coordinator opcode");
        assert!(
            !coordinator
                .windows(6)
                .any(|window| window.eq_ignore_ascii_case(b"select")),
            "coordinator program contains SQL"
        );
        let values = column_ints(&run_coordinator(&coordinator, &wires));
        assert_eq!(values, vec![1, 2], "{nodes} nodes");
    }
}

#[test]
fn unordered_row_query_keeps_every_matching_row() {
    let file = sample_file();
    let QueryDispatch::Rows {
        nodes: programs,
        coordinator,
    } = dispatch("SELECT a FROM t WHERE a > 2", &file, 2)
    else {
        panic!("expected a row dispatch");
    };
    let mut wires = Vec::new();
    for node in &programs {
        let (wire, reads, bytes) = run_node(&node.program, &file);
        assert_eq!(reads, node.request_count);
        assert_eq!(bytes, node.fetched_bytes);
        wires.push(wire);
    }
    let mut values = column_ints(&run_coordinator(&coordinator, &wires));
    values.sort_unstable();
    assert_eq!(values, vec![3, 4, 5]);
}
