//! Dictionary-encoded columns through [`NumericScan`] (bounded store, batch boundaries).

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::schema::Leaf;
use wcol::{open_plan, NumericScan, ScanValueMode};

fn temp_parquet(bytes: &[u8]) -> PathBuf {
    // Tests run in parallel and clock ticks can repeat, so a counter keeps names unique.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "wcol-dict-scan-{}-{}.parquet",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::write(&path, bytes).unwrap();
    path
}

fn write_dict_i32_chunk(
    rows: i64,
    dictionary: &[i32],
    ids: &[u32],
    present: Option<&[u8]>,
) -> Vec<u8> {
    let column = Column {
        name: "v",
        rows: rows as usize,
        codec: SNAPPY,
        present,
        values: ColumnValues::DictI32 { dictionary, ids },
    };
    let mut places = [ChunkPlace::default()];
    let value_bytes = dictionary.len().saturating_mul(4).max(8);
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

fn collect_batches(scan: &mut NumericScan, cap: usize) -> Vec<usize> {
    let mut out = Vec::new();
    loop {
        if scan.decode_exhausted() {
            break;
        }
        match scan.poll(cap).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                scan.advance_batch(rows).unwrap();
                out.push(rows);
            }
            DecodeProgress::Advanced => {
                for _ in 0..64 {
                    if scan.decode_exhausted() {
                        break;
                    }
                    match scan.poll(0).unwrap() {
                        DecodeProgress::Finished | DecodeProgress::Advanced => continue,
                        DecodeProgress::BatchReady { .. } => panic!("unexpected batch on drain"),
                        DecodeProgress::NeedInput => panic!("unexpected NeedInput on drain"),
                    }
                }
            }
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => panic!("unexpected NeedInput"),
        }
    }
    out
}

#[test]
fn dictionary_i32_four_rows_batch_three() {
    let dictionary = [10i32, 20, 30, 40];
    let ids = [0u32, 1, 2, 3];
    let file_bytes = write_dict_i32_chunk(4, &dictionary, &ids, None);
    let path = temp_parquet(&file_bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let mut chunk_bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut chunk_bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(3);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        SNAPPY,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&chunk_bytes).unwrap();
    assert_eq!(collect_batches(&mut scan, 3), vec![3, 1]);
    let _ = fs::remove_file(path);
}

#[test]
fn dictionary_i32_nullable_batch_boundary() {
    let dictionary = [1i32, 2, 3];
    let ids = [0u32, 1, 2]; // one id per present row (3 present of 4)
    let present = [0b0000_1110u8]; // row0 null, rows 1..=3 present
    let bytes = write_dict_i32_chunk(4, &dictionary, &ids, Some(&present));
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 3, leaf);
    assert_eq!(collect_batches(&mut scan, 3), vec![3, 1]);
    assert_eq!(scan.replay_metrics().page_replay_count, 0);
    let _ = fs::remove_file(path);
}

#[test]
fn dictionary_ids_batch_three_without_expansion() {
    let dictionary = [10i32, 20, 30, 40];
    let ids = [0u32, 1, 2, 3];
    let file_bytes = write_dict_i32_chunk(4, &dictionary, &ids, None);
    let path = temp_parquet(&file_bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let mut chunk_bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut chunk_bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(3);
    let mut scan = NumericScan::begin_dict_ids_with_limits(
        leaf,
        SNAPPY,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&chunk_bytes).unwrap();
    assert_eq!(scan.value_mode(), ScanValueMode::DictionaryIds);
    let (ids0, nulls0) = scan.next_dict_owned(3).unwrap().unwrap();
    assert_eq!(ids0, vec![0, 1, 2]);
    assert_eq!(nulls0, vec![1, 1, 1]);
    let (ids1, nulls1) = scan.next_dict_owned(3).unwrap().unwrap();
    assert_eq!(ids1, vec![3]);
    assert_eq!(nulls1, vec![1]);
    assert!(scan.next_dict_owned(3).unwrap().is_none());
    assert_eq!(scan.replay_metrics().page_replay_count, 0);
    let _ = fs::remove_file(path);
}

#[test]
fn dictionary_entry_budget_enforced() {
    let dictionary = [1i32, 2, 3];
    let ids = [0u32, 1, 2];
    let bytes = write_dict_i32_chunk(3, &dictionary, &ids, None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes_buf = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes_buf).unwrap();
    let limits = ScanLimits {
        max_batch_rows: 8,
        max_pending_rows: 8,
        max_dictionary_entries: 2,
        ..ScanLimits::for_batch_rows(8)
    };
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(3),
    )
    .unwrap();
    scan.push_compressed(&bytes_buf).unwrap();
    assert!(scan.poll(8).is_err());
    let _ = fs::remove_file(path);
}

fn collect_materialized_values(scan: &mut NumericScan, cap: usize) -> (Vec<usize>, Vec<i64>) {
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
            DecodeProgress::Advanced => {
                for _ in 0..64 {
                    if scan.decode_exhausted() {
                        break;
                    }
                    match scan.poll(0).unwrap() {
                        DecodeProgress::Finished | DecodeProgress::Advanced => continue,
                        DecodeProgress::BatchReady { .. } => panic!("unexpected batch on drain"),
                        DecodeProgress::NeedInput => panic!("unexpected NeedInput on drain"),
                    }
                }
            }
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => panic!("unexpected NeedInput"),
        }
    }
    (sizes, values)
}

#[test]
fn dictionary_rle_nine_values_batch_three_zero_replay() {
    let dictionary: [i32; 9] = [10, 20, 30, 40, 50, 60, 70, 80, 90];
    let ids: Vec<u32> = (0..9).map(|i| i % 9).collect();
    let expected: Vec<i64> = ids
        .iter()
        .map(|&id| i64::from(dictionary[id as usize]))
        .collect();
    let bytes = write_dict_i32_chunk(9, &dictionary, &ids, None);
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 3, leaf);
    let (sizes, got) = collect_materialized_values(&mut scan, 3);
    assert_eq!(sizes, vec![3, 3, 3]);
    assert_eq!(got, expected);
    let m = scan.replay_metrics();
    assert_eq!(m.page_replay_count, 0);
    assert_eq!(m.snappy_page_inflates, 2, "dictionary page and data page");
    let _ = fs::remove_file(path);
}

fn write_f64_chunk(rows: i64, values: &ColumnValues<'_>, present: Option<&[u8]>) -> Vec<u8> {
    let column = Column {
        name: "v",
        rows: rows as usize,
        codec: SNAPPY,
        present,
        values: *values,
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(rows as usize, 64).unwrap()];
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

fn collect_f64(scan: &mut NumericScan, cap: usize) -> Vec<(f64, u8)> {
    let mut out = Vec::new();
    loop {
        match scan.poll(cap).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                let (mut values, mut nulls) = (Vec::new(), Vec::new());
                scan.copy_f64_rows(&mut values, &mut nulls, rows).unwrap();
                out.extend(values.into_iter().zip(nulls));
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => return out,
            DecodeProgress::Advanced | DecodeProgress::NeedInput => continue,
        }
    }
}

#[test]
fn plain_and_dictionary_f64_keep_nan_infinities_signed_zero_and_null() {
    let plain = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0];
    // row 1 is null; the value slice is the four present rows.
    let present = [0b0001_1101u8];
    let bytes = write_f64_chunk(5, &ColumnValues::F64(&plain), Some(&present));
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 5,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 8, leaf);
    let got = collect_f64(&mut scan, 8);
    assert_eq!(got.len(), 5);
    assert!(got[0].0.is_nan() && got[0].1 == 1);
    assert_eq!(got[1], (0.0, 0));
    assert_eq!(got[2].0, f64::INFINITY);
    assert_eq!(got[3].0, f64::NEG_INFINITY);
    assert_eq!(got[4].0.to_bits(), (-0.0f64).to_bits());
    let _ = fs::remove_file(&path);

    let dictionary = [f64::NAN, -0.0, 0.0, 4.5];
    let ids = [0u32, 1, 2, 3];
    let bytes = write_f64_chunk(
        4,
        &ColumnValues::DictF64 {
            dictionary: &dictionary,
            ids: &ids,
        },
        None,
    );
    let path = temp_parquet(&bytes);
    let leaf = Leaf {
        name: "v".into(),
        physical: 5,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = open_scan(&path, 8, leaf.clone());
    let got = collect_f64(&mut scan, 8);
    assert!(got[0].0.is_nan());
    assert_eq!(got[1].0.to_bits(), (-0.0f64).to_bits());
    assert_eq!(got[2].0.to_bits(), 0.0f64.to_bits());
    assert_eq!(got[3].0, 4.5);
    let mut scan = NumericScan::begin_dict_ids_with_limits(
        leaf,
        scan_codec(&path),
        ScanLimits::for_batch_rows(8),
        scan_chunk_len(&path),
        Some(4),
    )
    .unwrap();
    let mut chunk = vec![0u8; scan_chunk_len(&path) as usize];
    read_chunk(&path, &mut chunk);
    scan.push_compressed(&chunk).unwrap();
    loop {
        match scan.poll(8).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => break,
            DecodeProgress::Advanced | DecodeProgress::NeedInput => continue,
        }
    }
    let entries = scan.dictionary().copy_float_entries().unwrap();
    assert!(entries[0].is_nan());
    assert_eq!(entries[1].to_bits(), (-0.0f64).to_bits());
    assert_eq!(entries[2].to_bits(), 0.0f64.to_bits());
    assert_eq!(entries[3], 4.5);
    let _ = fs::remove_file(path);
}

fn scan_codec(path: &Path) -> i32 {
    open_plan(path).unwrap().1.chunks[0].codec
}

fn scan_chunk_len(path: &Path) -> u32 {
    open_plan(path).unwrap().1.chunks[0].compressed_size as u32
}

fn read_chunk(path: &Path, buf: &mut [u8]) {
    let (mut file, plan) = open_plan(path).unwrap();
    file.seek(SeekFrom::Start(plan.chunks[0].start)).unwrap();
    file.read_exact(buf).unwrap();
}
