//! I-09: scan budgets, bounded batch buffers, and shared dictionary storage.

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use wcol::formats::parquet::decode::ScanLimits;
use wcol::formats::parquet::ingest_memory::ScanMemoryBudget;
use wcol::formats::parquet::schema::Leaf;
use wcol::formats::parquet::{open_plan, NumericScan};

fn fixture_flat() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

#[test]
fn scan_accepts_chunk_larger_than_window_budget() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (_file, plan) = open_plan(&path).unwrap();
    let chunk = &plan.chunks[0];
    let leaf = Leaf {
        name: plan.leaves[chunk.leaf as usize].name.clone(),
        physical: plan.leaves[chunk.leaf as usize].physical,
        type_length: plan.leaves[chunk.leaf as usize].type_length,
        max_def: plan.leaves[chunk.leaf as usize].max_def,
        max_rep: plan.leaves[chunk.leaf as usize].max_rep,
    };
    let mut budget = ScanMemoryBudget::permissive_guardrails_only();
    budget.compressed_bytes = 16;
    let limits = ScanLimits::for_batch_rows_and_budget(4, budget);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .expect("whole chunk may exceed window; scan still opens");
    assert_eq!(scan.read_credit(), 16);
    assert_eq!(scan.push_compressed(&[0u8; 32]).unwrap(), 16);
    assert_eq!(scan.resident_compressed_bytes(), 16);
}

#[test]
fn compressed_resident_bytes_grow_with_read_at_not_at_begin() {
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
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(4);
    let mut scan = NumericScan::begin_with_limits(
        leaf,
        chunk.codec,
        limits,
        chunk.compressed_size as u32,
        Some(plan.rows as u64),
    )
    .unwrap();
    assert_eq!(scan.resident_compressed_bytes(), 0);
    let half = bytes.len() / 2;
    scan.push_compressed(&bytes[..half]).unwrap();
    assert_eq!(scan.resident_compressed_bytes(), half);
    scan.push_compressed(&bytes[half..]).unwrap();
    assert_eq!(scan.resident_compressed_bytes(), bytes.len());
    assert!(scan.resident_compressed_bytes() <= limits.budget.compressed_bytes);
}

#[test]
fn dict_id_batches_share_one_arc_dictionary() {
    use compact_parquet_thrift::encode::{
        scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
    };
    use wcol::formats::parquet::ParquetDecode;
    use wcol::ops::query::CatalogColumn;
    use wcol::plan::sql_parser::ColumnKind;
    use wcol::stream::OwnedWcolBatch;

    let dictionary = [10i32, 20, 30, 40, 50, 60, 70, 80, 90];
    let ids: Vec<u32> = (0..9).map(|i| i % 9).collect();
    let column = Column {
        name: "v",
        rows: 9,
        codec: SNAPPY,
        present: None,
        values: ColumnValues::DictI32 {
            dictionary: &dictionary,
            ids: &ids,
        },
    };
    let mut places = [ChunkPlace::default()];
    let mut scratch = vec![0u8; scratch_len(9, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file_bytes = vec![0u8; 64 * 1024];
    let n = write_file(
        &mut file_bytes,
        9,
        std::slice::from_ref(&column),
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    file_bytes.truncate(n);
    let path = std::env::temp_dir().join(format!(
        "wcol-bounded-dict-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &file_bytes).unwrap();

    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = vec![CatalogColumn {
        name: "v".into(),
        kind: ColumnKind::Number,
        physical: 1,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let groups = vec![(
        0u32,
        plan.rows as u64,
        vec![(
            0u32,
            plan.chunks[0].start,
            plan.chunks[0].codec,
            plan.chunks[0].compressed_size as u32,
        )],
    )];
    let mut dict_cols = std::collections::HashSet::new();
    dict_cols.insert(0);
    let mut decode = ParquetDecode::new(3, catalog, groups, dict_cols);
    let mut arcs: Vec<Arc<Vec<i64>>> = Vec::new();
    let mut batch = OwnedWcolBatch::empty();
    for _ in 0..100_000 {
        if decode.fill_batch(&mut batch).unwrap() && batch.row_count() > 0 {
            let mut cols = Vec::new();
            let view = batch.view_with(&mut cols);
            for col in view.columns {
                if let wcol::stream::WcolColumn::DictId { dictionary, .. } = col {
                    arcs.push(Arc::clone(dictionary.int_entries().expect("int dict")));
                }
            }
            if arcs.len() >= 2 {
                break;
            }
            continue;
        }
        if decode.ingest_complete() && !decode.scans_active() {
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
    let _ = std::fs::remove_file(path);
    assert!(arcs.len() >= 2, "expected multiple dict-id batches");
    assert!(
        Arc::ptr_eq(&arcs[0], &arcs[1]),
        "dictionary entries must be shared across batches (Arc)"
    );
}
