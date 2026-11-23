//! Named Definition-of-Done acceptance tests (section D).

use std::fs;
use std::io::{Read, Seek, SeekFrom};

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};
use wcol::formats::parquet::decode::ScanLimits;
use wcol::formats::parquet::schema::Leaf;
use wcol::{open_plan, NumericScan};

fn write_dict_i32_chunk(rows: i64, dictionary: &[i32], ids: &[u32]) -> Vec<u8> {
    let column = Column {
        name: "v",
        rows: rows as usize,
        codec: SNAPPY,
        present: None,
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

#[test]
fn dod_dictionary_encoded_column_roundtrip() {
    let dictionary = [10i32, 20, 30];
    let bytes = write_dict_i32_chunk(5, &dictionary, &[0u32, 1, 2, 1, 0]);
    let path = std::env::temp_dir().join(format!(
        "wcol-dod-dict-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::write(&path, &bytes).unwrap();

    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let leaf = Leaf {
        name: "v".into(),
        physical: plan.leaves[0].physical,
        type_length: plan.leaves[0].type_length,
        max_def: plan.leaves[0].max_def,
        max_rep: plan.leaves[0].max_rep,
    };
    let mut compressed = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut compressed).unwrap();

    let limits = ScanLimits::for_batch_rows(2);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    scan.push_compressed(&compressed).unwrap();

    let mut values = Vec::new();
    let mut nulls = Vec::new();
    values.reserve(2);
    nulls.reserve(2);
    let mut total = 0usize;
    let mut sum = 0i128;
    while let Some(n) = scan.next_batch_into(&mut values, &mut nulls, 2).unwrap() {
        total += n;
        for (value, flag) in values.iter().zip(nulls.iter()).take(n) {
            if *flag != 0 {
                sum += i128::from(*value);
            }
        }
    }
    assert_eq!(total, 5);
    assert_eq!(sum, (10 + 20 + 30 + 20 + 10) as i128);
    let _ = fs::remove_file(path);
}
