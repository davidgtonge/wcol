//! Sliding compressed window + canonical [`ParquetDecode`] ingest (I-09 milestone).

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use compact_parquet_thrift::encode::{write_file, ChunkPlace, Column, ColumnValues, UNCOMPRESSED};
use wcol::formats::parquet::batch_decode::HOST_READ_LIMIT;
use wcol::formats::parquet::decode::ScanLimits;
use wcol::formats::parquet::ingest_memory::ScanMemoryBudget;
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{open_plan, schema::Plan, NumericScan, ParquetDecode};
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::OwnedWcolBatch;

fn flat_catalog(plan: &Plan) -> Vec<CatalogColumn> {
    plan.leaves
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
        .collect()
}

fn numeric_leaf(plan: &Plan, leaf: u32) -> bool {
    matches!(
        plan.leaves.get(leaf as usize).map(|l| l.physical),
        Some(0..=2)
    )
}

fn planned_chunks(plan: &Plan, row_group: u32, leaves: &[u32]) -> Vec<(u32, u64, i32, u32)> {
    plan.chunks
        .iter()
        .filter(|c| {
            c.row_group == row_group && leaves.contains(&c.leaf) && numeric_leaf(plan, c.leaf)
        })
        .map(|c| (c.leaf, c.start, c.codec, c.compressed_size as u32))
        .collect()
}

fn fixture_flat() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

fn hits_subset() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
}

fn hits_catalog(plan: &Plan) -> Vec<CatalogColumn> {
    plan.leaves
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
        .collect()
}

fn row_group_chunks(plan: &Plan, row_group: u32) -> Vec<(u32, u64, i32, u32)> {
    plan.chunks
        .iter()
        .filter(|chunk| chunk.row_group == row_group)
        .map(|chunk| {
            (
                chunk.leaf,
                chunk.start,
                chunk.codec,
                chunk.compressed_size as u32,
            )
        })
        .collect()
}

#[test]
fn full_row_group_with_wide_plain_columns_decodes() {
    let path = hits_subset();
    if !path.is_file() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = hits_catalog(&plan);
    let rg0 = 0u32;
    let row_chunks = row_group_chunks(&plan, rg0);
    assert!(row_chunks.len() > 10, "hits row group should be wide");
    let rows = plan
        .chunks
        .iter()
        .find(|chunk| chunk.row_group == rg0)
        .map(|chunk| chunk.rows as u64)
        .unwrap_or(plan.rows as u64);
    let groups = vec![(rg0, rows, row_chunks)];
    let dict_columns: HashSet<u32> = plan
        .chunks
        .iter()
        .filter(|chunk| chunk.row_group == rg0 && chunk.has_dictionary)
        .map(|chunk| chunk.leaf)
        .collect();
    let mut decode = ParquetDecode::new(8192, catalog, groups, dict_columns);
    let sizes = pump_decode(&mut decode, &mut file);
    assert_eq!(sizes.iter().sum::<usize>(), rows as usize);
}

#[test]
fn invalid_plan_column_surfaces_on_fill_not_stall() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (_file, plan) = open_plan(&path).unwrap();
    let catalog = flat_catalog(&plan);
    let groups = vec![(
        0u32,
        plan.rows as u64,
        vec![(999u32, 0, 0, 1)], // column id out of catalog range
    )];
    let mut decode = ParquetDecode::new(4, catalog, groups, HashSet::new());
    let mut batch = OwnedWcolBatch::empty();
    let err = decode.fill_batch(&mut batch).unwrap_err().to_string();
    assert!(err.contains("column") || err.contains("scan"), "{err}");
}

fn pump_decode(decode: &mut ParquetDecode, file: &mut std::fs::File) -> Vec<usize> {
    let mut batch = OwnedWcolBatch::empty();
    let mut sizes = Vec::new();
    loop {
        if decode.fill_batch(&mut batch).unwrap() {
            sizes.push(batch.row_count());
            batch.clear();
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        let Some(read) = decode.host_read_request().unwrap() else {
            panic!("decode stalled");
        };
        assert!(read.length as usize <= HOST_READ_LIMIT);
        let mut bytes = vec![0u8; read.length as usize];
        file.seek(SeekFrom::Start(read.offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        decode.complete_read(read.ticket, &bytes).unwrap();
    }
    sizes
}

#[test]
fn early_batch_before_full_column_chunks() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = flat_catalog(&plan);
    let rg0 = plan.chunks[0].row_group;
    // One projected column: a single bounded read can finish the chunk and emit before ingest completes.
    let row_chunks = planned_chunks(&plan, rg0, &[0]);
    let groups = vec![(rg0, plan.rows as u64, row_chunks)];
    let mut decode = ParquetDecode::new(4, catalog, groups, HashSet::new());
    let mut batch = OwnedWcolBatch::empty();
    assert!(
        !decode.fill_batch(&mut batch).unwrap(),
        "no rows before any compressed bytes are ingested"
    );
    let Some(read) = decode.host_read_request().unwrap() else {
        panic!("expected initial read");
    };
    let mut bytes = vec![0u8; read.length as usize];
    file.seek(SeekFrom::Start(read.offset)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    decode.complete_read(read.ticket, &bytes).unwrap();
    assert!(
        decode.fill_batch(&mut batch).unwrap(),
        "expected an early batch before ingest completes"
    );
    assert!(batch.row_count() > 0);
    assert!(
        !decode.ingest_complete(),
        "ingest must still be in progress after the first bounded read and batch"
    );
}

#[test]
fn two_column_batches_stay_row_aligned() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = flat_catalog(&plan);
    let groups = vec![(0u32, plan.rows as u64, planned_chunks(&plan, 0, &[0, 4]))];
    let mut decode = ParquetDecode::new(3, catalog, groups, HashSet::new());
    let sizes = pump_decode(&mut decode, &mut file);
    assert_eq!(sizes.iter().sum::<usize>(), plan.rows as usize);
    assert!(sizes.iter().all(|&n| n > 0 && n <= 3));
}

#[test]
fn min_pending_alignment_emits_shorter_side_first() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = flat_catalog(&plan);
    let rg0 = plan.chunks[0].row_group;
    let row_chunks = planned_chunks(&plan, rg0, &[0, 4]);
    let groups = vec![(rg0, plan.rows as u64, row_chunks)];
    let mut decode = ParquetDecode::new(3, catalog, groups, HashSet::new());
    let sizes = pump_decode(&mut decode, &mut file);
    let total: usize = sizes.iter().sum();
    assert_eq!(total, plan.rows as usize);
    assert!(
        sizes.len() >= 2,
        "expected multiple aligned batches, got {sizes:?}"
    );
}

#[test]
fn multi_page_chunk_streams_within_window() {
    let dictionary = [1i32, 2, 3, 4];
    let ids: Vec<u32> = (0..8).map(|i| i % 4).collect();
    let column = Column {
        name: "v",
        rows: 8,
        codec: UNCOMPRESSED,
        present: None,
        values: ColumnValues::DictI32 {
            dictionary: &dictionary,
            ids: &ids,
        },
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; 4096];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 16 * 1024];
    let n = write_file(
        &mut file_bytes,
        8,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-window-multipage-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();

    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let mut budget = ScanMemoryBudget::permissive_guardrails_only();
    budget.compressed_bytes = (chunk.compressed_size as usize / 2).max(48);
    let limits = ScanLimits::for_batch_rows_and_budget(4, budget);
    let leaf = Leaf {
        name: "v".into(),
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    };
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    let mut chunk_bytes = vec![0u8; chunk.compressed_size as usize];
    file.read_exact(&mut chunk_bytes).unwrap();
    let mut at = 0usize;
    while at < chunk_bytes.len() {
        let n = scan.push_compressed(&chunk_bytes[at..]).unwrap();
        if n == 0 {
            let _ = scan.poll(4).unwrap();
        } else {
            at += n;
        }
        assert!(scan.resident_compressed_bytes() <= limits.budget.compressed_bytes);
    }
    while scan.available_rows() > 0 || !scan.decode_exhausted() {
        match scan.poll(4).unwrap() {
            wcol::formats::parquet::DecodeProgress::BatchReady { rows } => {
                scan.advance_batch(rows).unwrap();
            }
            wcol::formats::parquet::DecodeProgress::Finished => break,
            wcol::formats::parquet::DecodeProgress::Advanced => {}
            wcol::formats::parquet::DecodeProgress::NeedInput => break,
        }
    }
    assert_eq!(scan.rows_emitted(), plan.rows as u64);
    let _ = std::fs::remove_file(path);
}

#[test]
fn full_window_with_incomplete_page_errors() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let leaf = Leaf {
        name: plan.leaves[chunk.leaf as usize].name.clone(),
        physical: plan.leaves[chunk.leaf as usize].physical,
        type_length: plan.leaves[chunk.leaf as usize].type_length,
        max_def: plan.leaves[chunk.leaf as usize].max_def,
        max_rep: plan.leaves[chunk.leaf as usize].max_rep,
    };
    let mut budget = ScanMemoryBudget::permissive_guardrails_only();
    budget.compressed_bytes = 32;
    let limits = ScanLimits::for_batch_rows_and_budget(4, budget);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    let mut chunk_bytes = vec![0u8; chunk.compressed_size as usize];
    file.read_exact(&mut chunk_bytes).unwrap();
    assert_eq!(scan.push_compressed(&chunk_bytes[..32]).unwrap(), 32);
    assert_eq!(scan.read_credit(), 0);
    let err = scan.poll(4).unwrap_err().to_string();
    assert!(
        err.contains("memory budget"),
        "expected oversized-page style error, got {err}"
    );
}

#[test]
fn execution_group_memory_within_shared_budget() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = flat_catalog(&plan);
    let groups = vec![(0u32, plan.rows as u64, planned_chunks(&plan, 0, &[0, 4, 5]))];
    let mut decode = ParquetDecode::new(4, catalog, groups, HashSet::new());
    let group_budget = ScanLimits::for_execution_group(4).budget;
    let _ = pump_decode(&mut decode, &mut file);
    let resident = decode.resident_compressed_bytes();
    assert!(
        resident <= group_budget.compressed_bytes + group_budget.codec_scratch_bytes,
        "resident={resident} budget={:?}",
        group_budget
    );
}
