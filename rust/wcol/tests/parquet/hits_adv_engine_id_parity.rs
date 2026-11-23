//! [`NumericScan`] row counts for ClickBench `AdvEngineID` match the footer (optional HITS_PARQUET).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::formats::parquet::decode::ScanLimits;
use wcol::{open_plan, NumericScan};

fn hits_path() -> Option<PathBuf> {
    std::env::var("HITS_PARQUET")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

#[test]
fn adv_engine_id_scan_matches_footer_rows() {
    let Some(path) = hits_path() else {
        eprintln!("skip: set HITS_PARQUET for AdvEngineID parity");
        return;
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let leaf_idx = plan
        .leaves
        .iter()
        .position(|l| l.name == "AdvEngineID")
        .expect("AdvEngineID leaf");
    let leaf = plan.leaves[leaf_idx].clone();
    let mut scan_rows = 0u64;
    let limits = ScanLimits::for_batch_rows(32_768);
    for chunk in plan.chunks.iter().filter(|c| c.leaf == leaf_idx as u32) {
        let mut bytes = vec![0u8; chunk.compressed_size as usize];
        file.seek(SeekFrom::Start(chunk.start)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        let mut chunk_scan_rows = 0u64;
        let mut scan = NumericScan::begin_with_limits(
            leaf.clone(),
            chunk.codec,
            limits,
            chunk.compressed_size as u32,
            Some(chunk.rows as u64),
        )
        .unwrap();
        scan.push_compressed(&bytes).unwrap();
        while let Some((values, _nulls)) = scan.next_owned(32_768).unwrap() {
            chunk_scan_rows += values.len() as u64;
        }
        assert_eq!(scan.rows_emitted(), chunk.rows as u64, "chunk row budget");
        assert_eq!(
            chunk_scan_rows, chunk.rows as u64,
            "chunk rg={}",
            chunk.row_group
        );
        scan_rows += chunk_scan_rows;
    }
    assert_eq!(scan_rows, plan.rows as u64);
}
