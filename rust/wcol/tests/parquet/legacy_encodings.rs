//! pyarrow data page v2, dictionary off: DELTA integers, DELTA byte arrays, PLAIN float/double.
//! Values were recorded from that writer and must survive the owned page cursor.

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::formats::parquet::decode::{DecodeProgress, ScanLimits};
use wcol::formats::parquet::{open_plan, NumericScan};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/legacy_encodings.parquet")
}

fn open_i64(leaf_index: usize) -> NumericScan {
    let (mut file, plan) = open_plan(&fixture()).unwrap();
    let chunk = plan
        .chunks
        .iter()
        .find(|chunk| chunk.leaf == leaf_index as u32)
        .unwrap();
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let mut scan = NumericScan::begin_with_limits(
        plan.leaves[leaf_index].clone(),
        chunk.codec,
        ScanLimits::for_batch_rows(64),
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    scan
}

fn collect_f64(scan: &mut NumericScan) -> Vec<f64> {
    let mut out = Vec::new();
    for _ in 0..100 {
        match scan.poll(64).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                let (mut values, mut nulls) = (Vec::new(), Vec::new());
                scan.copy_f64_rows(&mut values, &mut nulls, rows).unwrap();
                for (value, present) in values.iter().zip(&nulls) {
                    assert_eq!(*present, 1);
                    out.push(*value);
                }
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => return out,
            DecodeProgress::Advanced | DecodeProgress::NeedInput => continue,
        }
    }
    panic!("float scan did not finish");
}

fn collect_i64(scan: &mut NumericScan) -> Vec<i64> {
    let mut out = Vec::new();
    let mut values = Vec::with_capacity(64);
    let mut nulls = Vec::with_capacity(64);
    loop {
        match scan.next_batch_into(&mut values, &mut nulls, 64).unwrap() {
            Some(_) => {
                for (value, present) in values.iter().zip(&nulls) {
                    assert_eq!(*present, 1);
                    out.push(*value);
                }
            }
            None => return out,
        }
    }
}

fn open_utf8(leaf_index: usize) -> NumericScan {
    let (mut file, plan) = open_plan(&fixture()).unwrap();
    let chunk = plan
        .chunks
        .iter()
        .find(|chunk| chunk.leaf == leaf_index as u32)
        .unwrap();
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let mut scan = NumericScan::begin_plain_utf8_with_limits(
        plan.leaves[leaf_index].clone(),
        chunk.codec,
        ScanLimits::for_batch_rows(64),
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    scan
}

fn collect_utf8(scan: &mut NumericScan) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for _ in 0..100 {
        match scan.poll(64).unwrap() {
            DecodeProgress::BatchReady { rows } => {
                let (mut offsets, mut data, mut nulls) = (Vec::new(), Vec::new(), Vec::new());
                scan.copy_plain_utf8_rows(&mut offsets, &mut data, &mut nulls, rows)
                    .unwrap();
                for row in 0..rows {
                    assert_eq!(nulls[row], 1);
                    out.push(data[offsets[row] as usize..offsets[row + 1] as usize].to_vec());
                }
                scan.advance_batch(rows).unwrap();
            }
            DecodeProgress::Finished => return out,
            DecodeProgress::Advanced | DecodeProgress::NeedInput => continue,
        }
    }
    panic!("utf8 scan did not finish");
}

#[test]
fn pyarrow_v2_delta_and_plain_float_match_the_writer() {
    assert!(fixture().exists(), "legacy_encodings.parquet missing");
    assert_eq!(
        collect_i64(&mut open_i64(0)),
        vec![1, 2, 3, 100, -4, 8, 9, 10]
    );
    assert_eq!(
        collect_i64(&mut open_i64(1)),
        vec![10, 20, 30, 40, -5, 6, 7, 8]
    );
    assert_eq!(
        collect_f64(&mut open_i64(2)),
        vec![1.5, -2.25, 0.0, 4.0, 8.5, 3.25, -1.0, 9.5]
    );
    assert_eq!(
        collect_f64(&mut open_i64(3)),
        vec![1.5, 2.5, 3.0, 4.0, -0.5, 6.25, 7.0, 8.125]
    );
    assert_eq!(
        collect_utf8(&mut open_utf8(4)),
        vec![
            b"aa".to_vec(),
            b"bbb".to_vec(),
            b"".to_vec(),
            b"cdef".to_vec(),
            b"z".to_vec(),
            b"hello".to_vec(),
            b"x".to_vec(),
            b"yy".to_vec()
        ]
    );
    assert_eq!(
        collect_utf8(&mut open_utf8(5)),
        ["cat", "catalog", "car", "card", "care", "ca", "dog", "dot"]
            .into_iter()
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>()
    );
}
