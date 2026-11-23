//! Append-only GROUP BY spill (native SQLite): `cargo test -p wcol --features native`.

use std::path::PathBuf;

use wcol::ir::{AggExpr, AggKind, CatalogColumn, ColumnKind};
use wcol::ops::aggregate::AggregateOp;
use wcol::ops::query::{PartialAggSpec, PartialKeyPart};
use wcol::spill::{
    encode_canonical_key, DiscardRunStore, FileAppendSpillStore, SpillConfig, SpillProgress,
    SpillStore,
};
use wcol::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, SelectionMask};

fn spill_db_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "wcol-spill-{}-{}-{}.sqlite",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn count_star_group_by_user(catalog: &[CatalogColumn]) -> AggregateOp {
    AggregateOp::from_aggregate(
        &[0],
        &[AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        None,
        0,
        false,
        catalog,
    )
    .unwrap()
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

fn i64_batch(keys: Vec<i64>) -> OwnedWcolBatch {
    let n = keys.len();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, keys, vec![1; n]);
    batch.set_row_count(n).unwrap();
    batch
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

fn dict_i64_batch(keys: Vec<i64>) -> OwnedWcolBatch {
    let n = keys.len();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(
        0,
        (0..n as u32).collect(),
        vec![1; n],
        DictionaryValues {
            scope: 0,
            column: 0,
            entries: DictEntries::Int(keys.into()),
        },
    );
    batch.set_row_count(n).unwrap();
    batch
}

#[test]
fn spill_matches_in_memory_for_distinct_keys() {
    let catalog = key_catalog();
    let keys: Vec<i64> = (0..350).collect();
    let batch = i64_batch(keys.clone());
    let mask = SelectionMask::all(keys.len());

    let mut baseline = count_star_group_by_user(&catalog);
    baseline.consume_row_batch(&batch, &mask).unwrap();
    let base_state = baseline
        .finish_partial_state()
        .unwrap()
        .expect_ready("baseline finish");

    let config = SpillConfig {
        combined_budget_bytes: 96 * 1024,
        scratch_reserve_bytes: 24 * 1024,
        max_batch_rows: 64,
        ..SpillConfig::default()
    };

    let store: Box<dyn SpillStore> =
        Box::new(FileAppendSpillStore::open(spill_db_path("parity")).unwrap());
    let mut spilled = count_star_group_by_user(&catalog);
    spilled
        .enable_group_spill(config, store)
        .expect("enable spill");
    consume_spill(&mut spilled, &batch, keys.len());
    let spill_state = spilled
        .finish_partial_state()
        .unwrap()
        .expect_ready("spill finish");
    spilled.close_spill().unwrap();
    assert_eq!(spill_state.row_count, base_state.row_count);
}

/// Replay-from-start after `scratch full` used to spin on the same overflow keys.
#[test]
fn spill_ingest_resumes_after_scratch_flush() {
    let catalog = key_catalog();
    let n = 512usize;
    let batch = i64_batch((0..n as i64).collect());
    let mask = SelectionMask::all(n);

    let mut baseline = count_star_group_by_user(&catalog);
    baseline.consume_row_batch(&batch, &mask).unwrap();
    let base_state = baseline
        .finish_partial_state()
        .unwrap()
        .expect_ready("baseline finish");

    let config = SpillConfig {
        combined_budget_bytes: 64 * 1024,
        scratch_reserve_bytes: 16 * 1024,
        max_batch_rows: 32,
        finalize_batch_rows: 64,
        scan_page_rows: 8,
        ..SpillConfig::default()
    };
    let store: Box<dyn SpillStore> =
        Box::new(FileAppendSpillStore::open(spill_db_path("resume")).unwrap());
    let mut spilled = count_star_group_by_user(&catalog);
    spilled.enable_group_spill(config, store).unwrap();
    consume_spill(&mut spilled, &batch, n);
    let spill_state = spilled
        .finish_partial_state()
        .unwrap()
        .expect_ready("spill finish");
    spilled.close_spill().unwrap();
    assert_eq!(spill_state.row_count as usize, n);
    assert_eq!(spill_state.row_count, base_state.row_count);
}

#[test]
fn spill_memory_plateau_under_combined_budget() {
    let catalog = key_catalog();
    let config = SpillConfig {
        combined_budget_bytes: 96 * 1024,
        scratch_reserve_bytes: 24 * 1024,
        max_batch_rows: 32,
        ..SpillConfig::default()
    };
    let store: Box<dyn SpillStore> =
        Box::new(FileAppendSpillStore::open(spill_db_path("plateau")).unwrap());
    let mut agg = count_star_group_by_user(&catalog);
    agg.enable_group_spill(config, store).unwrap();

    let mut peak = agg.resident_state_bytes();
    for chunk in 0..4 {
        let start = chunk * 80;
        let batch = i64_batch((start..start + 80).map(|v| v as i64).collect());
        consume_spill(&mut agg, &batch, 80);
        peak = peak.max(agg.resident_state_bytes());
    }
    agg.close_spill().unwrap();
    assert!(
        peak <= config.combined_budget_bytes.saturating_add(48 * 1024),
        "peak resident {peak} exceeds budget band"
    );
}

/// Live aggregation must depend on the budget, not on distinct keys processed.
#[test]
fn flush_reset_discard_plateau_is_independent_of_cardinality() {
    let catalog = key_catalog();
    let config = SpillConfig {
        combined_budget_bytes: 64 * 1024,
        scratch_reserve_bytes: 16 * 1024,
        max_batch_rows: 64,
        max_batch_bytes: 16 * 1024,
        ..SpillConfig::default()
    };
    let mut agg = count_star_group_by_user(&catalog);
    agg.enable_group_spill(config, Box::new(DiscardRunStore::new()))
        .unwrap();

    let chunk = 512usize;
    let chunks = 40usize;
    let mut peak_live = 0u64;
    let mut peak_interner = 0usize;
    let mut peak_table = 0usize;
    for i in 0..chunks {
        let start = (i * chunk) as i64;
        let batch = i64_batch((start..start + chunk as i64).collect());
        consume_spill(&mut agg, &batch, chunk);
        peak_live = peak_live.max(agg.peak_live_bytes());
        peak_interner = peak_interner.max(agg.live_interner_bytes());
        peak_table = peak_table.max(agg.live_table_bytes());
        let cap = agg.live_table_bytes();
        assert!(
            (cap as u64) < config.main_lock_threshold(),
            "table still trips the flush lock after the batch: {cap}"
        );
    }
    assert!(
        agg.spill_runs_flushed() >= 2,
        "expected multiple flush-and-reset runs, got {}",
        agg.spill_runs_flushed()
    );
    assert!(
        agg.spill_groups_flushed() >= (chunk * 2) as u64,
        "flushed {} groups",
        agg.spill_groups_flushed()
    );
    let slack = 48 * 1024;
    assert!(
        peak_live <= config.combined_budget_bytes.saturating_add(slack),
        "peak live table {peak_live} exceeds budget"
    );
    assert!(
        peak_table as u64 <= config.combined_budget_bytes.saturating_add(slack),
        "peak table bytes {peak_table} exceeds budget"
    );
    assert!(
        (peak_interner as u64) <= config.combined_budget_bytes,
        "interner {peak_interner} should stay inside the combined budget for i64 keys"
    );
    agg.close_spill().unwrap();
}

/// Hash reducers see dictionary-encoded keys. Flush must not drop `dict_slots`.
#[test]
fn flush_reset_survives_dictionary_keys() {
    let catalog = key_catalog();
    let config = SpillConfig {
        combined_budget_bytes: 64 * 1024,
        scratch_reserve_bytes: 16 * 1024,
        max_batch_rows: 64,
        ..SpillConfig::default()
    };
    let mut agg = count_star_group_by_user(&catalog);
    agg.enable_group_spill(config, Box::new(DiscardRunStore::new()))
        .unwrap();
    let n = 4_000usize;
    let batch = dict_i64_batch((0..n as i64).collect());
    consume_spill(&mut agg, &batch, n);
    assert!(
        agg.spill_runs_flushed() >= 1,
        "expected a flush, got {}",
        agg.spill_runs_flushed()
    );
    agg.close_spill().unwrap();
}

#[test]
fn spill_hot_key_merge_across_sql_pages() {
    let hot = PartialKeyPart::Int(42);
    let key_blob = encode_canonical_key(std::slice::from_ref(&hot)).unwrap();
    let mut store = FileAppendSpillStore::open(spill_db_path("hot-key")).unwrap();
    store.begin().unwrap();
    let partial_aggs = vec![PartialAggSpec { kind: 0, column: 0 }];
    for i in 0..36u64 {
        let group = wcol::ops::query::PartialGroup {
            key: vec![hot.clone()],
            stats: vec![wcol::ops::query::PartialStat {
                count: 1,
                flags: 0,
                sum_i: i as i128,
                sum_f: 0.0,
                min_i: 0,
                max_i: 0,
                min_f: 0.0,
                max_f: 0.0,
                sketch: None,
            }],
        };
        let payload = wcol::spill::encode_partial_group_payload(&group).unwrap();
        store
            .append_partials(&[(key_blob.clone(), payload)])
            .unwrap();
    }
    store.begin_ordered_scan().unwrap();
    let mut resume = wcol::spill::SpillScanResume::default();
    let mut merged_count = 0u64;
    let mut current_key: Option<Vec<u8>> = None;
    let mut current = 0u64;
    loop {
        let batch = store
            .next_ordered_batch(&mut resume, 7, 1024 * 1024)
            .unwrap()
            .expect_ready("ordered page");
        if batch.rows.is_empty() {
            if batch.exhausted {
                break;
            }
            continue;
        }
        for (_seq, kb, payload) in batch.rows {
            let key = wcol::spill::decode_canonical_key(&kb).unwrap();
            let group =
                wcol::spill::decode_partial_group_payload(&payload, key, &partial_aggs).unwrap();
            if current_key.as_ref() == Some(&kb) {
                current += group.stats[0].count;
            } else {
                if current_key.is_some() {
                    merged_count += current;
                }
                current_key = Some(kb);
                current = group.stats[0].count;
            }
        }
        if batch.exhausted {
            break;
        }
    }
    if current_key.is_some() {
        merged_count += current;
    }
    assert_eq!(merged_count, 36);
}
