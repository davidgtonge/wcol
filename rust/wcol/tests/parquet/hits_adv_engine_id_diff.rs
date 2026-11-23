#![cfg(test)]

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::formats::parquet::decode::ScanLimits;
use wcol::formats::parquet::{open_plan, NumericScan};

fn hits_path() -> Option<PathBuf> {
    std::env::var("HITS_PARQUET")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

#[test]
fn first_chunk_emits_footer_row_count() {
    let Some(path) = hits_path() else {
        return;
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let leaf_idx = plan
        .leaves
        .iter()
        .position(|l| l.name == "AdvEngineID")
        .unwrap();
    let leaf = plan.leaves[leaf_idx].clone();
    let chunk = plan
        .chunks
        .iter()
        .find(|c| c.leaf == leaf_idx as u32)
        .unwrap();
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let batch = 32_768;
    let limits = ScanLimits::for_batch_rows(batch);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(chunk.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&bytes).unwrap();
    let mut scan_vals = Vec::new();
    while let Some((values, nulls)) = scan.next_owned(batch).unwrap() {
        for (v, p) in values.iter().zip(nulls.iter()) {
            scan_vals.push((*p, *v));
        }
    }
    assert_eq!(scan_vals.len() as u64, chunk.rows as u64);
}
