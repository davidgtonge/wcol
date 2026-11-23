use std::fs;
use std::path::PathBuf;

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY, UNCOMPRESSED,
};
use wcol::{aggregate_column, open_plan, Agg, Extreme, Sum};

fn fixture() -> Vec<u8> {
    let rows = 4i64;
    let a_present = [0b0000_1101u8];
    let a_values = [10i64, 3, 7];
    let b_present = [0b0000_0111u8];
    let dictionary: [&[u8]; 2] = [b"aa", b"bb"];
    let b_ids = [0u32, 1, 0];
    let c_values = [1.5f64, 2.5, 3.0, 4.0];
    let d_values = [true, false, true, true];
    let e_values = [1i32, 2, 3, 4];
    let big_values = [9_007_199_254_740_995i64, 1, -9_007_199_254_740_995, 42];
    let columns = [
        Column {
            name: "a",
            rows: 4,
            codec: SNAPPY,
            present: Some(&a_present),
            values: ColumnValues::I64(&a_values),
        },
        Column {
            name: "b",
            rows: 4,
            codec: SNAPPY,
            present: Some(&b_present),
            values: ColumnValues::DictBytes {
                dictionary: &dictionary,
                ids: &b_ids,
            },
        },
        Column {
            name: "c",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::F64(&c_values),
        },
        Column {
            name: "d",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::Bool(&d_values),
        },
        Column {
            name: "e",
            rows: 4,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::I32(&e_values),
        },
        Column {
            name: "big",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::I64(&big_values),
        },
    ];
    let mut places = [ChunkPlace::default(); 6];
    let mut scratch = vec![0u8; scratch_len(4, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 32 * 1024];
    let n = write_file(
        &mut file,
        rows,
        &columns,
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file.truncate(n);
    file
}

fn approx(agg: &Agg, sum: Sum, min: Extreme, max: Extreme, count: u64, nulls: u64) {
    assert_eq!(agg.count, count);
    assert_eq!(agg.nulls, nulls);
    assert_eq!(agg.sum, sum);
    assert_eq!(agg.min, min);
    assert_eq!(agg.max, max);
}

#[test]
fn flat_file_aggregates_match_the_encoder() {
    let bytes = fixture();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, &bytes).unwrap();

    let (_file, plan) = open_plan(&path).unwrap();
    assert_eq!(plan.rows, 4);
    assert_eq!(
        plan.leaves
            .iter()
            .map(|leaf| leaf.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d", "e", "big"]
    );
    assert!(plan.leaves[0].max_def == 1 && plan.leaves[0].max_rep == 0);
    assert_eq!(plan.chunks.len(), 6);

    approx(
        &aggregate_column(&path, "a").unwrap(),
        Sum::Int(20),
        Extreme::Int(3),
        Extreme::Int(10),
        3,
        1,
    );
    approx(
        &aggregate_column(&path, "d").unwrap(),
        Sum::Int(3),
        Extreme::Int(0),
        Extreme::Int(1),
        4,
        0,
    );
    approx(
        &aggregate_column(&path, "e").unwrap(),
        Sum::Int(10),
        Extreme::Int(1),
        Extreme::Int(4),
        4,
        0,
    );
    approx(
        &aggregate_column(&path, "big").unwrap(),
        Sum::Int(43),
        Extreme::Int(-9_007_199_254_740_995),
        Extreme::Int(9_007_199_254_740_995),
        4,
        0,
    );
}

#[test]
fn nested_column_is_rejected() {
    use wcol::formats::parquet::decode::ScanLimits;
    use wcol::{Leaf, NumericScan};
    let leaf = Leaf {
        name: "nested".into(),
        physical: 1,
        type_length: -1,
        max_def: 2,
        max_rep: 1,
    };
    let err = match NumericScan::begin_with_limits(leaf, 0, ScanLimits::for_batch_rows(8), 0, None)
    {
        Ok(_) => panic!("nested column should be rejected"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("nested"), "{err}");
}
