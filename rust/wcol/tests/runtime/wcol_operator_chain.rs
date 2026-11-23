//! Filter → Aggregate → Reduce → Finalize on typed partial-state WCOL (Phase 2).

#[path = "decode_query_harness.rs"]
mod decode_query_harness;

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use wcol::plan::catalog_from_footer;

use wcol::{
    columns_for_sql, drive_operator, open_plan, partial_first_sum_i, partial_state_to_wcs_bytes,
    CatalogColumn, FinalizeOp, LinearColocatedGraph, OwnedWcolBatch, ParquetDecode, ReduceOp,
    WcolStreamItem,
};

fn fixture_flat() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

fn decode_all_batches(
    path: &Path,
    sql: &str,
) -> (Vec<CatalogColumn>, Vec<Vec<u8>>, Vec<OwnedWcolBatch>) {
    let (mut file, plan) = open_plan(path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let needed = columns_for_sql(sql, &catalog).unwrap().columns;
    let mut groups = Vec::new();
    let mut chunks = Vec::new();
    let mut chunk_map: HashMap<(u32, u32), Vec<u8>> = HashMap::new();
    for chunk in &plan.chunks {
        if !needed.contains(&chunk.leaf) {
            continue;
        }
        let mut bytes = vec![0u8; chunk.compressed_size as usize];
        file.seek(SeekFrom::Start(chunk.start)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        chunk_map.insert((chunk.row_group, chunk.leaf), bytes);
    }
    let mut seen_rg = HashSet::new();
    for chunk in &plan.chunks {
        if !needed.contains(&chunk.leaf) {
            continue;
        }
        if !seen_rg.insert(chunk.row_group) {
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
    for group in &groups {
        for (leaf, _, _, _) in &group.2 {
            chunks.push(chunk_map.get(&(group.0, *leaf)).cloned().unwrap());
        }
    }

    let mut decode = ParquetDecode::new(4, catalog.clone(), groups, HashSet::new());
    let mut scratch = OwnedWcolBatch::empty();
    let mut row_batches = Vec::new();
    loop {
        if decode.fill_batch(&mut scratch).unwrap() {
            row_batches.push(std::mem::take(&mut scratch));
            scratch = OwnedWcolBatch::empty();
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        let Some(read) = decode.host_read_request().unwrap() else {
            panic!("decode stalled");
        };
        let mut bytes = vec![0u8; read.length as usize];
        file.seek(SeekFrom::Start(read.offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        decode.complete_read(read.ticket, &bytes).unwrap();
    }
    (catalog, chunks, row_batches)
}

use std::collections::HashMap;

#[test]
fn operator_chain_matches_execute_sum() {
    let path = fixture_flat();
    let sql = "SELECT SUM(a) FROM flat";
    let (catalog, _chunks, row_batches) = decode_all_batches(&path, sql);

    let items: Vec<_> = row_batches
        .iter()
        .cloned()
        .map(WcolStreamItem::from_row_batch)
        .collect();

    let mut filter = wcol::plan::bind::open_filter_op(sql, &catalog).unwrap();
    let mut agg = wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap();
    let mut reduce = ReduceOp::new();
    let mut finalize = FinalizeOp::new();

    let filtered = drive_operator(&mut filter, items, false).unwrap();
    let partials = drive_operator(&mut agg, filtered, true).unwrap();
    assert_eq!(partials.len(), 1);
    assert!(partials[0].is_partial_batch());
    let reduced = drive_operator(&mut reduce, partials, true).unwrap();
    let results = drive_operator(&mut finalize, reduced, true).unwrap();

    let sum_via_chain = result_sum_i(&results[0]);

    let (_, _chunks2, _) = decode_all_batches(&path, sql);
    let (mut file, plan) = open_plan(&path).unwrap();
    let needed = columns_for_sql(sql, &catalog).unwrap().columns;
    let mut groups = Vec::new();
    let mut seen_rg = HashSet::new();
    for chunk in &plan.chunks {
        if !needed.contains(&chunk.leaf) || !seen_rg.insert(chunk.row_group) {
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
    let json =
        decode_query_harness::decode_query_to_json(&mut source, &mut query, |offset, len| {
            let mut bytes = vec![0u8; len as usize];
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| e.to_string())?;
            file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
            Ok(bytes)
        })
        .unwrap();
    assert_eq!(json, r#"{"columns":["agg0"],"rows":[[20]]}"#);
    assert_eq!(sum_via_chain, 20);
}

#[test]
fn reduce_merges_two_partials() {
    let sql = "SELECT SUM(a) FROM flat";
    let path = fixture_flat();
    let (catalog, _, _) = decode_all_batches(&path, sql);

    let row = |v: i64| -> WcolStreamItem {
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![v], vec![1]);
        batch.set_row_count(1).unwrap();
        WcolStreamItem::from_row_batch(batch)
    };

    let mut agg1 = wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap();
    let mut agg2 = wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap();
    let p1 = drive_operator(&mut agg1, vec![row(5)], true).unwrap();
    let p2 = drive_operator(&mut agg2, vec![row(15)], true).unwrap();

    let mut reduce = ReduceOp::new();
    let merged = drive_operator(&mut reduce, vec![p1[0].clone(), p2[0].clone()], true).unwrap();
    let mut finalize = FinalizeOp::new();
    let out = drive_operator(&mut finalize, merged, true).unwrap();
    assert_eq!(result_sum_i(&out[0]), 20);
}

#[test]
fn partial_state_wcs_edge_matches_oracle() {
    let path = fixture_flat();
    let sql = "SELECT SUM(a) FROM flat";
    let (catalog, _, row_batches) = decode_all_batches(&path, sql);
    let items: Vec<_> = row_batches
        .iter()
        .cloned()
        .map(WcolStreamItem::from_row_batch)
        .collect();
    let mut agg = wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap();
    let partials = drive_operator(&mut agg, items, true).unwrap();
    let state = partials[0]
        .try_partial_state()
        .expect("expected partial state");
    let payload = partial_state_to_wcs_bytes(&state).unwrap();
    assert!(payload.starts_with(b"PWCL"));
    assert_eq!(partial_first_sum_i(&payload).unwrap(), 20);
}

fn result_sum_i(item: &WcolStreamItem) -> i64 {
    let b = item.as_row_batch().expect("expected result row batch");
    let mut view = Vec::new();
    b.view_with(&mut view).slot_i64(0).unwrap().0[0]
}
