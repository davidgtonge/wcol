use super::*;

#[test]
fn a_reducer_finishes_from_memory_up_to_half_its_budget() {
    const MIB: u64 = 1024 * 1024;
    // The Wasm default (16 MiB) keeps its old threshold.
    assert_eq!(eager_finish_max_bytes(16 * MIB), 16 * MIB);
    assert_eq!(eager_finish_max_bytes(4 * MIB), 16 * MIB);
    // A container reducer with room keeps a table of half its budget in memory.
    assert_eq!(eager_finish_max_bytes(768 * MIB), 384 * MIB);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn pair_groups_spill_matches_the_in_memory_aggregate() {
    let catalog = vec![
        CatalogColumn {
            name: "a".into(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "b".into(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "v".into(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
    ];
    let aggs = [
        AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        },
        AggExpr {
            column: Some(2),
            kind: AggKind::Sum,
            offset: 0,
        },
    ];
    let mut memory = AggregateOp::from_aggregate(&[0, 1], &aggs, None, 0, false, &catalog).unwrap();
    let mut spilled =
        AggregateOp::from_aggregate(&[0, 1], &aggs, None, 0, false, &catalog).unwrap();
    let path = std::env::temp_dir().join(format!(
        "wcol-pair-spill-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    spilled
        .enable_group_spill(
            crate::spill::SpillConfig {
                combined_budget_bytes: 384 * 3 + 128,
                scratch_reserve_bytes: 128,
                max_batch_rows: 2,
                max_batch_bytes: 64 * 1024,
                scan_page_rows: 2,
                finalize_batch_rows: 2,
            },
            Box::new(crate::spill::FileAppendSpillStore::open(path.clone()).unwrap()),
        )
        .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 1, 2, 2, 3, 4, 1], vec![1, 1, 1, 1, 1, 1, 0]);
    batch.push_i64_column(1, vec![10, 10, 20, 20, 30, 40, 7], vec![1; 7]);
    batch.push_i64_column(2, vec![5, 6, 7, 8, 9, 2, 4], vec![1; 7]);
    batch.set_row_count(7).unwrap();
    let feed = |agg: &mut AggregateOp, batch: &OwnedWcolBatch| {
        for _ in 0..8 {
            match agg.consume_row_batch(batch, &SelectionMask::all(batch.row_count())) {
                Ok(SpillProgress::Ready(())) => return,
                Ok(SpillProgress::AwaitHost) => continue,
                Err(e) => panic!("{e}"),
            }
        }
        panic!("spill did not resume");
    };
    feed(&mut memory, &batch);
    feed(&mut spilled, &batch);
    assert!(
        spilled.spill_runs_flushed() >= 1,
        "pair table should have spilled"
    );
    let mut again = OwnedWcolBatch::empty();
    again.push_i64_column(0, vec![1], vec![1]);
    again.push_i64_column(1, vec![10], vec![1]);
    again.push_i64_column(2, vec![3], vec![1]);
    again.set_row_count(1).unwrap();
    feed(&mut memory, &again);
    feed(&mut spilled, &again);

    fn signature(state: &crate::stream::partials::PartialStateWcol) -> Vec<(String, u64, i128)> {
        let snap = crate::stream::partials::partial_snapshot_from_state(state).unwrap();
        let mut rows: Vec<_> = snap
            .groups
            .iter()
            .map(|group| {
                (
                    format!("{:?}", group.key),
                    group.stats[0].count,
                    group.stats[1].sum_i,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    let left = finish_state(&mut memory);
    let right = finish_state(&mut spilled);
    assert_eq!(signature(&left), signature(&right));
    assert!(right.row_count >= 4);
    let _ = std::fs::remove_file(path);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn two_runs_under_a_tight_budget_match_in_memory_groups() {
    let catalog = vec![
        CatalogColumn {
            name: "k".into(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "v".into(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
    ];
    let aggs = [
        AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        },
        AggExpr {
            column: Some(1),
            kind: AggKind::Sum,
            offset: 0,
        },
    ];
    let mut memory = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
    let mut spilled = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
    let path = std::env::temp_dir().join(format!(
        "wcol-two-run-spill-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    // Two groups fill the lock; further keys flush another run before finish.
    spilled
        .enable_group_spill(
            crate::spill::SpillConfig {
                combined_budget_bytes: 384 * 2 + 128,
                scratch_reserve_bytes: 128,
                max_batch_rows: 8,
                max_batch_bytes: 64 * 1024,
                scan_page_rows: 4,
                finalize_batch_rows: 4,
            },
            Box::new(crate::spill::FileAppendSpillStore::open(path.clone()).unwrap()),
        )
        .unwrap();
    let keys = vec![1_i64, 1, 2, 3, 2, 4, 5, 3, 6, 6];
    let values = vec![10_i64, 1, 4, 7, 2, 8, 3, 5, 9, 6];
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, keys, vec![1; 10]);
    batch.push_i64_column(1, values, vec![1; 10]);
    batch.set_row_count(10).unwrap();
    let feed = |agg: &mut AggregateOp| {
        for _ in 0..32 {
            match agg.consume_row_batch(&batch, &SelectionMask::all(10)) {
                Ok(SpillProgress::Ready(())) => return,
                Ok(SpillProgress::AwaitHost) => continue,
                Err(e) => panic!("{e}"),
            }
        }
        panic!("spill did not resume, runs {}", agg.spill_runs_flushed());
    };
    feed(&mut memory);
    feed(&mut spilled);
    assert!(
        spilled.spill_runs_flushed() >= 2,
        "tight budget should flush at least two runs, got {}",
        spilled.spill_runs_flushed()
    );
    fn signature(state: &crate::stream::partials::PartialStateWcol) -> Vec<(String, u64, i128)> {
        let snap = crate::stream::partials::partial_snapshot_from_state(state).unwrap();
        let mut rows: Vec<_> = snap
            .groups
            .iter()
            .map(|group| {
                (
                    format!("{:?}", group.key),
                    group.stats[0].count,
                    group.stats[1].sum_i,
                )
            })
            .collect();
        rows.sort();
        rows
    }
    let left = finish_state(&mut memory);
    let right = finish_state(&mut spilled);
    assert_eq!(signature(&left), signature(&right));
    assert_eq!(right.row_count, 6);
    let _ = std::fs::remove_file(path);
}
