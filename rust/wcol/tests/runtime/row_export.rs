//! Ordered row exports of files WCOL wrote. Export jobs plan through `plan_job` and run on the
//! host (`tests/large-parquet-export.test.ts` writes one end to end); this checks the planning
//! that depends on our own footers.

use std::io::Write;
use std::path::PathBuf;

use wcol::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use wcol::formats::parquet::{open_plan, ParquetEncode, ParquetEncodeConfig};
use wcol::plan::{plan_query_dispatch, Frontend, OpKind, PhysicalPlannerConfig};
use wcol::stream::OwnedWcolBatch;

fn drive_encode(enc: &mut ParquetEncode, batch: OwnedWcolBatch) -> Vec<u8> {
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
        match enc.poll(&mut io).unwrap() {
            StreamProgress::Finished => {
                file.extend_from_slice(&ring[..written]);
                return file;
            }
            StreamProgress::HaveOutput => file.extend_from_slice(&ring[..written]),
            StreamProgress::NeedInput | StreamProgress::Blocked => {}
            StreamProgress::NeedHostRead { .. } => panic!("encode requested a host read"),
        }
    }
    panic!("encode did not finish");
}

fn source_bytes() -> Vec<u8> {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, (0..40).collect(), vec![1; 40]);
    batch.set_row_count(40).unwrap();
    let mut enc = ParquetEncode::open(
        vec!["a".into()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 10,
            max_row_groups: 8,
        },
    )
    .unwrap();
    drive_encode(&mut enc, batch)
}

fn write_temp(name: &str, bytes: &[u8]) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "wcol-export-{}-{}-{name}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(bytes).unwrap();
    path
}

/// Files WCOL writes carry footer min/max, so an ordered export over two nodes plans a
/// range-partitioned sort; the sampler used to refuse with "no usable statistics".
#[test]
fn ordered_export_on_two_nodes_range_partitions_our_own_file() {
    let source = write_temp("src-range", &source_bytes());
    let (_, footer) = open_plan(&source).unwrap();
    let config = PhysicalPlannerConfig {
        parquet_row_group_rows: Some(8),
        ..PhysicalPlannerConfig::default()
    };
    let (_, physical) = plan_query_dispatch(
        "SELECT a FROM t ORDER BY a DESC",
        "rows.parquet",
        &footer,
        2,
        8,
        &config,
        &[Frontend::Sql],
    )
    .unwrap();
    let partitions = physical.operators.iter().find_map(|op| match &op.kind {
        OpKind::RangeExchange {
            partitions, splits, ..
        } => Some((*partitions, splits.len())),
        _ => None,
    });
    let (partitions, splits) = partitions.expect("no range exchange planned");
    assert_eq!(splits as u32, partitions - 1);
    let _ = std::fs::remove_file(&source);
}
