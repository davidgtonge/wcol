//! PLAIN and dictionary BYTE_ARRAY columns decode to [`WcolColumn::Utf8`] / byte [`DictEntries`].

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use std::collections::HashSet;
use wcol::formats::parquet::batch_decode::ParquetDecode;
use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{open_plan, NumericScan};
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::{OwnedWcolBatch, WcolColumn};

fn temp_parquet(bytes: &[u8]) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "wcol-string-scan-{}-{}.parquet",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::write(&path, bytes).unwrap();
    path
}

fn write_plain_utf8(rows: i64, values: &[&[u8]]) -> Vec<u8> {
    let column = Column {
        name: "s",
        rows: rows as usize,
        codec: SNAPPY,
        present: None,
        values: ColumnValues::Bytes(values),
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(rows as usize, 64 * 1024).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 256 * 1024];
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

fn write_dict_utf8(rows: i64, dictionary: &[&[u8]], ids: &[u32]) -> Vec<u8> {
    let column = Column {
        name: "s",
        rows: rows as usize,
        codec: SNAPPY,
        present: None,
        values: ColumnValues::DictBytes { dictionary, ids },
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(rows as usize, 4096).unwrap()];
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

#[test]
fn plain_utf8_scan_emits_offsets_and_bytes() {
    let file = temp_parquet(&write_plain_utf8(3, &[b"aa", b"", b"bbb"]));
    let leaf = Leaf {
        name: "s".into(),
        physical: 6,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut f, plan) = open_plan(&file).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    f.seek(SeekFrom::Start(chunk.start)).unwrap();
    f.read_exact(&mut bytes).unwrap();
    let mut scan = NumericScan::begin_plain_utf8_with_limits(
        leaf,
        chunk.codec,
        ScanLimits::for_batch_rows(8),
        chunk.compressed_size as u32,
        Some(3),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    let rows = match scan.poll(8).unwrap() {
        DecodeProgress::BatchReady { rows } => rows,
        other => panic!("expected batch, got {other:?}"),
    };
    let mut offsets = Vec::new();
    let mut data = Vec::new();
    let mut nulls = Vec::new();
    scan.copy_plain_utf8_rows(&mut offsets, &mut data, &mut nulls, rows)
        .unwrap();
    assert_eq!(nulls, vec![1, 1, 1]);
    assert_eq!(&data, b"aabbb");
    assert_eq!(offsets, vec![0, 2, 2, 5]);
    let _ = fs::remove_file(file);
}

#[test]
fn dict_utf8_scan_emits_byte_dictionary() {
    let file = temp_parquet(&write_dict_utf8(2, &[b"x", b"yy"], &[0, 1]));
    let leaf = Leaf {
        name: "s".into(),
        physical: 6,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut f, plan) = open_plan(&file).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    f.seek(SeekFrom::Start(chunk.start)).unwrap();
    f.read_exact(&mut bytes).unwrap();
    let mut scan = NumericScan::begin_dict_ids_with_limits(
        leaf,
        chunk.codec,
        ScanLimits::for_batch_rows(8),
        chunk.compressed_size as u32,
        Some(2),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    loop {
        match scan.poll(8).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => break,
            DecodeProgress::NeedInput => continue,
        }
        if scan.decode_exhausted() {
            break;
        }
    }
    let (offsets, arena) = scan.dictionary().copy_byte_entries().unwrap();
    assert_eq!(arena, b"xyy");
    assert_eq!(offsets, vec![0, 1, 3]);
    let _ = fs::remove_file(file);
}

#[test]
fn parquet_decode_round_trips_utf8_columns() {
    let file = temp_parquet(&write_plain_utf8(2, &[b"hi", b"there"]));
    let catalog = vec![CatalogColumn {
        name: "s".into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let (mut f, plan) = open_plan(&file).unwrap();
    let groups = vec![(
        0u32,
        plan.rows as u64,
        plan.chunks
            .iter()
            .map(|c| (0u32, c.start, c.codec, c.compressed_size as u32))
            .collect::<Vec<_>>(),
    )];
    let mut decode = ParquetDecode::new(8, catalog, groups, HashSet::new());
    let mut batch = OwnedWcolBatch::empty();
    while !decode.fill_batch(&mut batch).unwrap() {
        if decode.ingest_complete() {
            break;
        }
        let Some(read) = decode.host_read_request().unwrap() else {
            break;
        };
        let mut chunk = vec![0u8; read.length as usize];
        f.seek(SeekFrom::Start(read.offset)).unwrap();
        f.read_exact(&mut chunk).unwrap();
        decode.complete_read(read.ticket, &chunk).unwrap();
    }
    assert_eq!(batch.row_count(), 2);
    let mut view_buf = Vec::new();
    let view = batch.view_with(&mut view_buf);
    match view.columns[0] {
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            assert_eq!(nulls, &[1, 1]);
            assert_eq!(&bytes[offsets[0] as usize..offsets[1] as usize], b"hi");
            assert_eq!(&bytes[offsets[1] as usize..offsets[2] as usize], b"there");
        }
        _ => panic!("expected utf8 column"),
    }
    let _ = fs::remove_file(file);
}

/// A small batch byte budget ends batches early instead of failing, and a single value larger
/// than the whole budget still decodes on its own.
#[test]
fn plain_utf8_respects_batch_byte_budget() {
    let mut values: Vec<Vec<u8>> = (0..40)
        .map(|i| format!("{i:03}{}", "x".repeat(97)).into_bytes())
        .collect();
    values.insert(17, vec![b'L'; 1000]);
    let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    let file = temp_parquet(&write_plain_utf8(refs.len() as i64, &refs));
    let leaf = Leaf {
        name: "s".into(),
        physical: 6,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let (mut f, plan) = open_plan(&file).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    f.seek(SeekFrom::Start(chunk.start)).unwrap();
    f.read_exact(&mut bytes).unwrap();
    let mut limits = ScanLimits::for_batch_rows(64);
    limits.budget.batch_bytes = 450;
    let mut scan = NumericScan::begin_plain_utf8_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(refs.len() as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for _ in 0..1000 {
        match scan.poll(64).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                let (mut offsets, mut data, mut nulls) = (Vec::new(), Vec::new(), Vec::new());
                scan.copy_plain_utf8_rows(&mut offsets, &mut data, &mut nulls, rows)
                    .unwrap();
                assert!(
                    data.len() <= 450 || rows == 1,
                    "batch of {rows} rows holds {} bytes",
                    data.len()
                );
                for r in 0..rows {
                    got.push(data[offsets[r] as usize..offsets[r + 1] as usize].to_vec());
                }
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(got, values);
    let _ = fs::remove_file(file);
}

fn write_column(rows: usize, values: ColumnValues<'_>) -> Vec<u8> {
    let column = Column {
        name: "v",
        rows,
        codec: SNAPPY,
        present: None,
        values,
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(rows, 64 * 1024).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 256 * 1024];
    let n = write_file(
        &mut file,
        rows as i64,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file.truncate(n);
    file
}

/// The pages of a file's only column chunk.
fn chunk_pages(file_bytes: &[u8]) -> Vec<u8> {
    let path = temp_parquet(file_bytes);
    let (mut f, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    f.seek(SeekFrom::Start(chunk.start)).unwrap();
    f.read_exact(&mut bytes).unwrap();
    let _ = fs::remove_file(path);
    bytes
}

#[derive(Debug, PartialEq)]
enum Cell {
    Bytes(Vec<u8>),
    Int(i64),
}

/// Drain a dictionary-id scan, reading each batch in whatever mode the scan is in.
fn drain(scan: &mut NumericScan, batch_rows: usize) -> Vec<Cell> {
    use wcol::formats::parquet::ScanValueMode;
    let mut out = Vec::new();
    for _ in 0..10_000 {
        match scan.poll(batch_rows).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                match scan.value_mode() {
                    ScanValueMode::DictionaryIds => {
                        let (mut ids, mut nulls) = (Vec::new(), Vec::new());
                        scan.copy_dict_id_rows(&mut ids, &mut nulls, rows).unwrap();
                        let dict = scan.dictionary();
                        if let Ok((offsets, bytes)) = dict.copy_byte_entries() {
                            for id in ids {
                                let id = id as usize;
                                out.push(Cell::Bytes(
                                    bytes[offsets[id] as usize..offsets[id + 1] as usize].to_vec(),
                                ));
                            }
                        } else {
                            let ints = dict.copy_int_entries().unwrap();
                            out.extend(ids.into_iter().map(|id| Cell::Int(ints[id as usize])));
                        }
                    }
                    ScanValueMode::PlainUtf8 => {
                        let (mut offsets, mut data, mut nulls) =
                            (Vec::new(), Vec::new(), Vec::new());
                        scan.copy_plain_utf8_rows(&mut offsets, &mut data, &mut nulls, rows)
                            .unwrap();
                        for r in 0..rows {
                            out.push(Cell::Bytes(
                                data[offsets[r] as usize..offsets[r + 1] as usize].to_vec(),
                            ));
                        }
                    }
                    ScanValueMode::MaterializedI64 => {
                        let (mut values, mut nulls) = (Vec::new(), Vec::new());
                        scan.copy_materialized_rows(&mut values, &mut nulls, rows)
                            .unwrap();
                        out.extend(values.into_iter().map(Cell::Int));
                    }
                    ScanValueMode::MaterializedF64 => {
                        panic!("string drain saw a float scan");
                    }
                }
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    out
}

/// Writers fall back from dictionary to PLAIN pages inside one column chunk. A dictionary page
/// and its data page, followed by a PLAIN page of the same column, must decode in order.
#[test]
fn dictionary_chunk_with_plain_fallback_page_decodes() {
    let dict: Vec<&[u8]> = vec![b"red", b"green", b"blue"];
    let ids: Vec<u32> = (0..10).map(|i| i % 3).collect();
    let plain: Vec<Vec<u8>> = (0..7)
        .map(|i| format!("fallback-{i}").into_bytes())
        .collect();
    let plain_refs: Vec<&[u8]> = plain.iter().map(|v| v.as_slice()).collect();
    let mut pages = chunk_pages(&write_column(
        10,
        ColumnValues::DictBytes {
            dictionary: &dict,
            ids: &ids,
        },
    ));
    pages.extend(chunk_pages(&write_column(
        7,
        ColumnValues::Bytes(&plain_refs),
    )));
    let mut expected: Vec<Cell> = ids
        .iter()
        .map(|id| Cell::Bytes(dict[*id as usize].to_vec()))
        .collect();
    expected.extend(plain.iter().map(|v| Cell::Bytes(v.clone())));

    let leaf = Leaf {
        name: "v".into(),
        physical: 6,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    for batch_rows in [1usize, 4, 8, 32] {
        let mut scan = NumericScan::begin_dict_ids_with_limits(
            leaf.clone(),
            SNAPPY,
            ScanLimits::for_batch_rows(batch_rows),
            pages.len() as u32,
            Some(17),
        )
        .unwrap();
        scan.push_compressed(&pages).unwrap();
        assert_eq!(drain(&mut scan, batch_rows), expected, "batch {batch_rows}");
    }

    let int_dict = [10i32, 20, 30];
    let int_ids: Vec<u32> = (0..9).map(|i| (i * 2) % 3).collect();
    let tail = [7i32, 8, 9, 10, 11];
    let mut pages = chunk_pages(&write_column(
        9,
        ColumnValues::DictI32 {
            dictionary: &int_dict,
            ids: &int_ids,
        },
    ));
    pages.extend(chunk_pages(&write_column(5, ColumnValues::I32(&tail))));
    let mut expected: Vec<Cell> = int_ids
        .iter()
        .map(|id| Cell::Int(i64::from(int_dict[*id as usize])))
        .collect();
    expected.extend(tail.iter().map(|v| Cell::Int(i64::from(*v))));
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    for batch_rows in [1usize, 4, 16] {
        let mut scan = NumericScan::begin_dict_ids_with_limits(
            leaf.clone(),
            SNAPPY,
            ScanLimits::for_batch_rows(batch_rows),
            pages.len() as u32,
            Some(14),
        )
        .unwrap();
        scan.push_compressed(&pages).unwrap();
        assert_eq!(
            drain(&mut scan, batch_rows),
            expected,
            "int batch {batch_rows}"
        );
    }
}
