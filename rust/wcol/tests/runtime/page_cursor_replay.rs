//! Owned [`PageCursor`] path: no mid-page replay on PLAIN SNAPPY materialized pages.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{
    ingest_replay_reset, ingest_replay_totals, open_plan, NumericScan, ParquetDecode,
};
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::OwnedWcolBatch;

fn write_plain_i64_parquet(rows: i64, values: &[i64]) -> (Vec<u8>, PathBuf) {
    let column = Column {
        name: "v",
        rows: rows as usize,
        codec: SNAPPY,
        present: None,
        values: ColumnValues::I64(values),
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(rows as usize, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 256 * 1024];
    let n = write_file(
        &mut file_bytes,
        rows,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-page-cursor-plain-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();
    (file_bytes, path)
}

fn pump_all(decode: &mut ParquetDecode, file: &mut std::fs::File) {
    let mut batch = OwnedWcolBatch::empty();
    loop {
        if decode.fill_batch(&mut batch).unwrap() {
            batch.clear();
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        let read = decode.host_read_request().unwrap().expect("read");
        let mut bytes = vec![0u8; read.length as usize];
        file.seek(SeekFrom::Start(read.offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        decode.complete_read(read.ticket, &bytes).unwrap();
    }
}

fn collect_poll_batches(scan: &mut NumericScan, cap: usize) -> Vec<usize> {
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
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => panic!("unexpected need input"),
        }
    }
    out
}

#[test]
fn numeric_scan_plain_snappy_no_page_replay() {
    const ENABLE_OWNED: bool = true;
    if !ENABLE_OWNED {
        return;
    }
    let values: Vec<i64> = (1..=9).collect();
    let (_, path) = write_plain_i64_parquet(9, &values);
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let leaf = Leaf {
        name: "v".into(),
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
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
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    assert_eq!(collect_poll_batches(&mut scan, 2), vec![2, 2, 2, 2, 1]);
    assert_eq!(scan.replay_metrics().page_replay_count, 0);
    assert_eq!(scan.replay_metrics().snappy_page_inflates, 1);
    let _ = std::fs::remove_file(path);
}

#[test]
fn plain_multi_batch_ingest_zero_page_replays() {
    const ENABLE_OWNED: bool = true;
    if !ENABLE_OWNED {
        return;
    }
    let values: Vec<i64> = (1..=9).collect();
    let (_, path) = write_plain_i64_parquet(9, &values);
    ingest_replay_reset();
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = vec![CatalogColumn {
        name: "v".into(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let groups = vec![(
        0u32,
        plan.rows as u64,
        vec![(
            0u32,
            plan.chunks[0].start,
            plan.chunks[0].codec,
            plan.chunks[0].compressed_size as u32,
        )],
    )];
    let mut decode = ParquetDecode::new(2, catalog, groups, HashSet::new());
    pump_all(&mut decode, &mut file);
    assert_eq!(ingest_replay_totals().page_replay_count, 0);
    let _ = std::fs::remove_file(path);
}

#[test]
fn dict_id_multi_batch_zero_page_replays() {
    use compact_parquet_thrift::encode::ColumnValues::DictI32;

    let dictionary = [10i32, 20, 30, 40, 50, 60, 70, 80, 90];
    let ids: Vec<u32> = (0..9).map(|i| i % 9).collect();
    let column = Column {
        name: "v",
        rows: 9,
        codec: SNAPPY,
        present: None,
        values: DictI32 {
            dictionary: &dictionary,
            ids: &ids,
        },
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(9, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 64 * 1024];
    let n = write_file(
        &mut file_bytes,
        9,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-page-cursor-dict-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();

    ingest_replay_reset();
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = vec![CatalogColumn {
        name: "v".into(),
        kind: ColumnKind::Number,
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let mut dict_cols = HashSet::new();
    dict_cols.insert(0);
    let groups = vec![(
        0u32,
        plan.rows as u64,
        vec![(
            0u32,
            plan.chunks[0].start,
            plan.chunks[0].codec,
            plan.chunks[0].compressed_size as u32,
        )],
    )];
    let mut decode = ParquetDecode::new(3, catalog, groups, dict_cols);
    pump_all(&mut decode, &mut file);
    let replay = ingest_replay_totals();
    let _ = std::fs::remove_file(path);
    assert_eq!(replay.page_replay_count, 0);
}

#[test]
fn dict_id_nullable_multi_batch_zero_page_replays() {
    use compact_parquet_thrift::encode::ColumnValues::DictI32;

    let dictionary = [1i32, 2, 3];
    let ids = [0u32, 1, 2];
    let present = [0b0000_1110u8];
    let column = Column {
        name: "v",
        rows: 4,
        codec: SNAPPY,
        present: Some(&present),
        values: DictI32 {
            dictionary: &dictionary,
            ids: &ids,
        },
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(4, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 64 * 1024];
    let n = write_file(
        &mut file_bytes,
        4,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-page-cursor-dict-null-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();

    ingest_replay_reset();
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = vec![CatalogColumn {
        name: "v".into(),
        kind: ColumnKind::Number,
        physical: 1,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let mut dict_cols = HashSet::new();
    dict_cols.insert(0);
    let groups = vec![(
        0u32,
        plan.rows as u64,
        vec![(
            0u32,
            plan.chunks[0].start,
            plan.chunks[0].codec,
            plan.chunks[0].compressed_size as u32,
        )],
    )];
    let mut decode = ParquetDecode::new(2, catalog, groups, dict_cols);
    pump_all(&mut decode, &mut file);
    let replay = ingest_replay_totals();
    let _ = std::fs::remove_file(path);
    assert_eq!(replay.page_replay_count, 0);
}
