//! Slow spill coverage — not part of `npm run test:spill-rust` (10s budget).

use std::path::PathBuf;

use wcol::ir::{AggExpr, AggKind, CatalogColumn, ColumnKind};
use wcol::ops::aggregate::AggregateOp;
use wcol::spill::{DiscardRunStore, FileAppendSpillStore, SpillConfig, SpillProgress, SpillStore};
use wcol::stream::partials::partial_state_concat;
use wcol::stream::{OwnedWcolBatch, SelectionMask};

fn spill_db_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "wcol-spill-stress-{}-{}-{}.sqlite",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn consume_spill(agg: &mut AggregateOp, batch: &OwnedWcolBatch, n: usize) {
    let mask = SelectionMask::all(n);
    loop {
        match agg.consume_row_batch(batch, &mask) {
            Ok(SpillProgress::Ready(())) => return,
            Ok(SpillProgress::AwaitHost) => continue,
            Err(err) => panic!("{err}"),
        }
    }
}

fn key_catalog() -> Vec<CatalogColumn> {
    vec![CatalogColumn {
        name: "k".into(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }]
}

#[test]
#[ignore = "stress: cargo test -p wcol --test spill_groups_stress -- --ignored"]
fn spill_finalize_emits_bounded_chunks() {
    let catalog = key_catalog();
    let config = SpillConfig {
        combined_budget_bytes: 64 * 1024,
        scratch_reserve_bytes: 16 * 1024,
        max_batch_rows: 32,
        finalize_batch_rows: 64,
        scan_page_rows: 8,
        ..SpillConfig::default()
    };
    let store: Box<dyn SpillStore> =
        Box::new(FileAppendSpillStore::open(spill_db_path("chunks")).unwrap());
    let mut agg = AggregateOp::from_aggregate(
        &[0],
        &[AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog,
    )
    .unwrap();
    agg.enable_group_spill(config, store).unwrap();
    let n = 512usize;
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, (0..n as i64).collect(), vec![1; n]);
    batch.set_row_count(n).unwrap();
    consume_spill(&mut agg, &batch, n);

    agg.begin_finish_chunks().unwrap();
    let mut chunks = Vec::new();
    let mut peak_finalize_rows = 0usize;
    while let Some(chunk) = agg.finish_next_chunk().unwrap().expect_ready("chunk") {
        peak_finalize_rows = peak_finalize_rows.max(chunk.row_count);
        chunks.push(chunk);
    }
    agg.finish_chunks_done().unwrap();
    assert!(chunks.len() > 1);
    assert!(peak_finalize_rows <= config.finalize_batch_rows);
    assert_eq!(chunks.iter().map(|c| c.row_count).sum::<usize>(), n);
}

#[test]
#[ignore = "stress: cargo test -p wcol --test spill_groups_stress -- --ignored"]
fn spill_high_cardinality_memory_plateau_ingest_and_finalize() {
    let catalog = vec![CatalogColumn {
        name: "k".into(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let distinct = std::env::var("WCOL_SPILL_STRESS")
        .ok()
        .map(|_| 1_000_000usize)
        .unwrap_or(100_000);
    let config = SpillConfig {
        combined_budget_bytes: 256 * 1024,
        scratch_reserve_bytes: 64 * 1024,
        max_batch_rows: 128,
        finalize_batch_rows: 128,
        scan_page_rows: 32,
        ..SpillConfig::default()
    };
    let store: Box<dyn SpillStore> =
        Box::new(FileAppendSpillStore::open(spill_db_path("high-card")).unwrap());
    let mut agg = AggregateOp::from_aggregate(
        &[0],
        &[wcol::ir::AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog,
    )
    .unwrap();
    agg.enable_group_spill(config, store).unwrap();

    let mut peak = agg.resident_state_bytes();
    let chunk = 10_000usize;
    for start in (0..distinct).step_by(chunk) {
        let end = (start + chunk).min(distinct);
        let len = end - start;
        let keys: Vec<i64> = (start as i64..end as i64).collect();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, keys, vec![1; len]);
        batch.set_row_count(len).unwrap();
        consume_spill(&mut agg, &batch, len);
        peak = peak.max(agg.resident_state_bytes());
    }

    agg.begin_finish_chunks().unwrap();
    let mut finalize_peak_rows = 0usize;
    let mut total = 0usize;
    let mut merged = None;
    while let Some(chunk) = agg.finish_next_chunk().unwrap().expect_ready("chunk") {
        finalize_peak_rows = finalize_peak_rows.max(chunk.row_count);
        total += chunk.row_count;
        merged = Some(match merged.take() {
            None => chunk,
            Some(left) => partial_state_concat(left, chunk).unwrap(),
        });
    }
    agg.finish_chunks_done().unwrap();
    assert_eq!(total, distinct);
    assert!(merged.as_ref().unwrap().row_count >= distinct);
    assert!(peak <= config.combined_budget_bytes.saturating_add(96 * 1024));
    assert!(finalize_peak_rows <= config.finalize_batch_rows);
}

/// Millions of distinct groups, discard sink: live state and table capacity must plateau.
#[test]
#[ignore = "stress: cargo test -p wcol --test spill_groups_stress -- --ignored"]
fn flush_reset_discard_millions_of_groups_plateau() {
    let catalog = key_catalog();
    let distinct = std::env::var("WCOL_SPILL_STRESS")
        .ok()
        .map(|_| 2_000_000usize)
        .unwrap_or(1_000_000);
    let config = SpillConfig {
        combined_budget_bytes: 4 * 1024 * 1024,
        scratch_reserve_bytes: 256 * 1024,
        max_batch_rows: 256,
        max_batch_bytes: 64 * 1024,
        ..SpillConfig::default()
    };
    let mut agg = AggregateOp::from_aggregate(
        &[0],
        &[wcol::ir::AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog,
    )
    .unwrap();
    agg.enable_group_spill(config, Box::new(DiscardRunStore::new()))
        .unwrap();

    let chunk = 8_192usize;
    let mut capacity_after_first_flush = None;
    let mut peak_live = 0u64;
    let mut peak_table = 0usize;
    let mut peak_interner = 0usize;
    for start in (0..distinct).step_by(chunk) {
        let end = (start + chunk).min(distinct);
        let len = end - start;
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, (start as i64..end as i64).collect(), vec![1; len]);
        batch.set_row_count(len).unwrap();
        consume_spill(&mut agg, &batch, len);
        peak_live = peak_live.max(agg.peak_live_bytes());
        peak_table = peak_table.max(agg.live_table_bytes());
        peak_interner = peak_interner.max(agg.live_interner_bytes());
        if agg.spill_runs_flushed() > 0 {
            let cap = agg.live_table_bytes();
            match capacity_after_first_flush {
                None => capacity_after_first_flush = Some(cap),
                Some(first) => {
                    assert!(
                        cap <= first.saturating_add(first / 2).saturating_add(64 * 1024),
                        "WASM-analog table capacity grew: first={first} later={cap} at {end} keys"
                    );
                }
            }
        }
    }
    assert!(
        agg.spill_runs_flushed() >= 10,
        "runs={}",
        agg.spill_runs_flushed()
    );
    assert!(
        agg.spill_groups_flushed() >= (distinct as u64 / 2),
        "flushed {} of {distinct}",
        agg.spill_groups_flushed()
    );
    let slack = 512 * 1024;
    assert!(
        peak_live <= config.combined_budget_bytes.saturating_add(slack),
        "peak live {peak_live}"
    );
    assert!(
        peak_table as u64 <= config.combined_budget_bytes.saturating_add(slack),
        "peak table {peak_table}"
    );
    assert!(
        peak_interner < 64 * 1024,
        "i64 keys should not grow the interner"
    );
    agg.close_spill().unwrap();
}
