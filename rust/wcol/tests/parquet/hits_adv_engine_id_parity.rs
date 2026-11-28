//! [`NumericScan`] decodes every ClickBench `AdvEngineID` chunk of the committed sample to its
//! footer row count and values, checked against DuckDB's answer on the same file.

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::formats::parquet::decode::ScanLimits;
use wcol::{open_plan, NumericScan};

/// `SELECT SUM(AdvEngineID), COUNT(*) FILTER (WHERE AdvEngineID <> 0)` on the sample, by DuckDB.
const SAMPLE_SUM: i64 = 76_010;
const SAMPLE_NONZERO: u64 = 13_701;

#[test]
fn adv_engine_id_scan_matches_footer_rows_and_values() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    let (mut file, plan) = open_plan(&path).unwrap();
    let leaf_idx = plan
        .leaves
        .iter()
        .position(|l| l.name == "AdvEngineID")
        .expect("AdvEngineID leaf");
    let leaf = plan.leaves[leaf_idx].clone();
    let (mut scan_rows, mut sum, mut nonzero) = (0u64, 0i64, 0u64);
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
        while let Some((values, nulls)) = scan.next_owned(32_768).unwrap() {
            chunk_scan_rows += values.len() as u64;
            for (&value, &present) in values.iter().zip(nulls.iter()) {
                if present != 0 && value != 0 {
                    sum += value;
                    nonzero += 1;
                }
            }
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
    assert_eq!((sum, nonzero), (SAMPLE_SUM, SAMPLE_NONZERO));
}
