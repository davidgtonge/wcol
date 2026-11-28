//! Canonical import path: `wcol` crate only.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use wcol::{LinearColocatedGraph, ParquetDecode};

#[path = "runtime/decode_query_harness.rs"]
mod decode_query_harness;
use decode_query_harness::decode_query_to_json;
use wcol::columns_for_sql;
use wcol::formats::parquet::open_plan;
use wcol::ops::query::CatalogColumn;
use wcol::plan::logical::ColumnKind;

fn fixture_flat() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

#[test]
fn wcol_crate_parquet_sum_json() {
    let path = fixture_flat();
    let sql = "SELECT SUM(a) FROM flat";
    let (mut file, plan) = open_plan(&path).unwrap();
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
    let needed = columns_for_sql(sql, &catalog).unwrap().columns;
    let mut groups = Vec::new();
    let mut seen = HashSet::new();
    for chunk in &plan.chunks {
        if !needed.contains(&chunk.leaf) || !seen.insert(chunk.row_group) {
            continue;
        }
        let row_chunks: Vec<_> = plan
            .chunks
            .iter()
            .filter(|c| c.row_group == chunk.row_group && needed.contains(&c.leaf))
            .map(|c| (c.leaf, c.start, c.codec, c.compressed_size as u32))
            .collect();
        groups.push((chunk.row_group, chunk.rows as u64, row_chunks));
    }
    let mut source = ParquetDecode::new(4, catalog.clone(), groups, HashSet::new());
    let mut query = LinearColocatedGraph::open_custom(
        None,
        vec![
            Box::new(wcol::plan::bind::open_filter_op(sql, &catalog).unwrap()),
            Box::new(wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap()),
        ],
        vec![Box::new(wcol::ReduceOp::new())],
    );
    let json = decode_query_to_json(&mut source, &mut query, |offset, len| {
        let mut bytes = vec![0u8; len as usize];
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| e.to_string())?;
        file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    })
    .unwrap();
    assert_eq!(json, r#"{"columns":["agg0"],"rows":[[20]]}"#);
}
