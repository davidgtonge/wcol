//! [`NumericScan::poll`] batch-boundary and completion semantics (no ExecutionGroup).

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{open_plan, NumericScan};

fn temp_parquet(bytes: &[u8]) -> PathBuf {
    // Tests run in parallel and clock ticks can repeat, so a counter keeps names unique.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "wcol-numeric-scan-{}-{}.parquet",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::write(&path, bytes).unwrap();
    path
}

fn write_i64_chunk(rows: i64, values: &[i64], present: Option<&[u8]>) -> Vec<u8> {
    let column = Column {
        name: "v",
        rows: rows as usize,
        codec: SNAPPY,
        present,
        values: ColumnValues::I64(values),
    };
    let mut places = [ChunkPlace::default()];
    let value_bytes = values.len().saturating_mul(8).max(8);
    let mut scratch = vec![0u8; scratch_len(rows as usize, value_bytes).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 64 * 1024];
    let n = write_file(
        &mut file,
        rows,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file.truncate(n);
    file
}

fn open_scan(path: &Path, batch_rows: usize, leaf: Leaf) -> NumericScan {
    let (mut file, plan) = open_plan(path).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(batch_rows);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    scan
}

fn collect_poll_batches(scan: &mut NumericScan, cap: usize) -> Vec<usize> {
    let mut batches = Vec::new();
    loop {
        if scan.decode_exhausted() {
            break;
        }
        match scan.poll(cap).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                assert!(rows > 0 && rows <= cap);
                scan.advance_batch(rows).unwrap();
                batches.push(rows);
            }
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => panic!("unexpected NeedInput with full chunk fed"),
        }
    }
    assert!(scan.decode_exhausted(), "must report chunk finished");
    batches
}

fn assert_batches(rows: i64, cap: usize, expect: &[usize]) {
    let values: Vec<i64> = (1..=rows).collect();
    let bytes = write_i64_chunk(rows, &values, None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (_file, plan) = open_plan(&path).unwrap();
    assert_eq!(plan.chunks[0].num_values, rows, "fixture row count");
    let mut scan = open_scan(&path, cap, leaf);
    let got = collect_poll_batches(&mut scan, cap);
    assert_eq!(got, expect, "rows={rows} cap={cap}");
    assert_eq!(scan.replay_metrics().page_replay_count, 0);
    let _ = fs::remove_file(path);
}

fn collect_poll_values(scan: &mut NumericScan, cap: usize) -> (Vec<usize>, Vec<i64>) {
    let mut sizes = Vec::new();
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    loop {
        if scan.decode_exhausted() {
            break;
        }
        match scan.poll(cap).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                nulls.resize(rows, 0);
                scan.copy_materialized_rows(&mut values, &mut nulls, rows)
                    .unwrap();
                scan.advance_batch(rows).unwrap();
                sizes.push(rows);
            }
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => panic!("unexpected NeedInput with full chunk fed"),
        }
    }
    assert!(scan.decode_exhausted());
    (sizes, values)
}

#[test]
fn poll_plain_i64_values_and_single_inflate() {
    let expected: Vec<i64> = (1..=9).collect();
    let bytes = write_i64_chunk(9, &expected, None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 3, leaf);
    let (sizes, got) = collect_poll_values(&mut scan, 3);
    assert_eq!(sizes, vec![3, 3, 3]);
    assert_eq!(got, expected);
    let m = scan.replay_metrics();
    assert_eq!(m.page_replay_count, 0);
    assert_eq!(m.snappy_page_inflates, 1);
    let _ = fs::remove_file(path);
}

#[test]
fn poll_batch_boundaries_plain_i64() {
    assert_batches(4, 4, &[4]);
    assert_batches(4, 3, &[3, 1]);
    assert_batches(4, 5, &[4]);
    assert_batches(8, 4, &[4, 4]);
    assert_batches(9, 4, &[4, 4, 1]);
}

#[test]
fn poll_zero_rows_chunk_finishes() {
    let bytes = write_i64_chunk(0, &[], None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 4, leaf);
    assert_eq!(scan.poll(4).unwrap(), DecodeProgress::Finished);
    assert!(scan.decode_exhausted());
    let _ = fs::remove_file(path);
}

#[test]
fn poll_exact_boundary_then_finished() {
    let bytes = write_i64_chunk(4, &[10, 20, 30, 40], None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 4, leaf);
    assert_eq!(
        scan.poll(4).unwrap(),
        DecodeProgress::BatchReady { rows: 4 }
    );
    scan.advance_batch(4).unwrap();
    assert_eq!(scan.poll(4).unwrap(), DecodeProgress::Finished);
    assert!(scan.decode_exhausted());
    let _ = fs::remove_file(path);
}

#[test]
fn poll_nullable_exact_boundary_flat_column_a() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
    if !path.is_file() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = plan
        .chunks
        .iter()
        .find(|c| plan.leaves[c.leaf as usize].name == "a")
        .unwrap();
    let leaf = Leaf {
        name: "a".into(),
        physical: plan.leaves[chunk.leaf as usize].physical,
        type_length: plan.leaves[chunk.leaf as usize].type_length,
        max_def: plan.leaves[chunk.leaf as usize].max_def,
        max_rep: plan.leaves[chunk.leaf as usize].max_rep,
    };
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(4);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    assert_eq!(collect_poll_batches(&mut scan, 4), &[4]);
}

#[test]
fn incremental_push_before_full_chunk() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
    if !path.is_file() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = plan
        .chunks
        .iter()
        .find(|c| plan.leaves[c.leaf as usize].name == "a")
        .expect("column a chunk");
    let leaf = Leaf {
        name: plan.leaves[chunk.leaf as usize].name.clone(),
        physical: plan.leaves[chunk.leaf as usize].physical,
        type_length: plan.leaves[chunk.leaf as usize].type_length,
        max_def: plan.leaves[chunk.leaf as usize].max_def,
        max_rep: plan.leaves[chunk.leaf as usize].max_rep,
    };
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(2);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(chunk.num_values as u64),
    )
    .unwrap();
    assert_eq!(scan.total_fed_bytes(), 0);
    let split = bytes.len() / 2;
    assert!(split > 0 && split < bytes.len());
    let n1 = scan.push_compressed(&bytes[..split]).unwrap();
    assert_eq!(n1, split);
    assert_eq!(scan.total_fed_bytes(), split);
    assert!(!scan.chunk_complete());
    let n2 = scan.push_compressed(&bytes[split..]).unwrap();
    assert_eq!(n2, bytes.len() - split);
    assert!(scan.chunk_complete());
    assert_eq!(collect_poll_batches(&mut scan, 2), &[2, 2]);
}

#[test]
fn next_batch_into_matches_poll_sequence() {
    let bytes = write_i64_chunk(9, &(1..=9).collect::<Vec<_>>(), None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 4, leaf);
    let mut via_into = Vec::new();
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    values.reserve(4);
    nulls.reserve(4);
    while let Some(n) = scan.next_batch_into(&mut values, &mut nulls, 4).unwrap() {
        via_into.push(n);
    }
    assert_eq!(via_into, &[4, 4, 1]);
    assert!(scan.decode_exhausted());
    let _ = fs::remove_file(path);
}
