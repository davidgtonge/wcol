//! Ticketed Parquet ReadAt: depth 2, reserved credit, out-of-order completion.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::formats::parquet::ParquetDecode;
use wcol::open_plan;
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::OwnedWcolBatch;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

fn open_flat_decode(batch_rows: usize) -> (std::fs::File, ParquetDecode) {
    let path = fixture();
    let (file, plan) = open_plan(&path).unwrap();
    let catalog: Vec<CatalogColumn> = plan
        .leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();
    let columns: Vec<u32> = vec![0, 4];
    let chunks: Vec<_> = plan
        .chunks
        .iter()
        .filter(|c| columns.contains(&c.leaf))
        .map(|c| (c.leaf, c.start, c.codec, c.compressed_size as u32))
        .collect();
    let groups = vec![(0u32, plan.rows as u64, chunks)];
    let decode = ParquetDecode::new(batch_rows, catalog, groups, HashSet::new());
    (file, decode)
}

#[test]
fn two_outstanding_reads_reserve_credit() {
    let path = fixture();
    if !path.exists() {
        return;
    }
    let (mut file, mut decode) = open_flat_decode(4);
    let mut batch = OwnedWcolBatch::empty();
    assert!(!decode.fill_batch(&mut batch).unwrap());

    let r1 = decode.host_read_request().unwrap().expect("first read");
    let r2 = decode.host_read_request().unwrap().expect("second read");
    assert_ne!(r1.ticket, r2.ticket);
    assert_ne!(
        r1.offset, r2.offset,
        "expect parallel reads on different columns"
    );
    assert!(
        decode.host_read_request().unwrap().is_none(),
        "depth capped at 2"
    );

    let mut b1 = vec![0u8; r1.length as usize];
    let mut b2 = vec![0u8; r2.length as usize];
    file.seek(SeekFrom::Start(r1.offset)).unwrap();
    file.read_exact(&mut b1).unwrap();
    file.seek(SeekFrom::Start(r2.offset)).unwrap();
    file.read_exact(&mut b2).unwrap();

    decode.complete_read(r2.ticket, &b2).unwrap();
    decode.complete_read(r1.ticket, &b1).unwrap();

    while decode.fill_batch(&mut batch).unwrap() {
        batch.clear();
    }
}

#[test]
fn cancel_read_releases_reservation() {
    let path = fixture();
    if !path.exists() {
        return;
    }
    let (_file, mut decode) = open_flat_decode(4);
    let read = decode.host_read_request().unwrap().expect("read");
    decode.cancel_read(read.ticket).unwrap();
    assert!(
        decode.host_read_request().unwrap().is_some(),
        "credit must return after cancel"
    );
}

#[test]
fn reverse_completion_matches_serial_order() {
    let path = fixture();
    if !path.exists() {
        return;
    }
    let (mut file, mut decode) = open_flat_decode(4);
    let mut serial_rows = Vec::new();
    loop {
        let mut batch = OwnedWcolBatch::empty();
        if decode.fill_batch(&mut batch).unwrap() {
            serial_rows.push(batch.row_count());
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

    let (mut file, mut decode) = open_flat_decode(4);
    let mut reverse_rows = Vec::new();
    let mut queued: Vec<(u32, Vec<u8>)> = Vec::new();
    loop {
        let mut batch = OwnedWcolBatch::empty();
        if decode.fill_batch(&mut batch).unwrap() {
            reverse_rows.push(batch.row_count());
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        while queued.len() < 2 {
            if let Some(read) = decode.host_read_request().unwrap() {
                let mut bytes = vec![0u8; read.length as usize];
                file.seek(SeekFrom::Start(read.offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                queued.push((read.ticket, bytes));
            } else {
                break;
            }
        }
        let (ticket, bytes) = queued.pop().expect("queued read");
        decode.complete_read(ticket, &bytes).unwrap();
    }

    assert_eq!(serial_rows, reverse_rows);
}

#[test]
fn completion_length_mismatch_is_error() {
    let path = fixture();
    if !path.exists() {
        return;
    }
    let (_file, mut decode) = open_flat_decode(4);
    let read = decode.host_read_request().unwrap().expect("read");
    let short = vec![0u8; read.length as usize / 2];
    assert!(decode.complete_read(read.ticket, &short).is_err());
    decode.cancel_read(read.ticket).unwrap();
}
