use super::*;

#[test]
fn reducer_that_never_spills_finishes_without_host_io() {
    let catalog: Vec<CatalogColumn> = (0..2)
        .map(|physical| CatalogColumn {
            name: format!("c{physical}"),
            kind: crate::ir::ColumnKind::Number,
            physical,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        })
        .collect();
    let aggs = [AggExpr {
        column: Some(1),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let mut agg = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
    agg.set_final_aggregate(true);
    let bridge = std::rc::Rc::new(std::cell::RefCell::new(crate::spill::HostSpillBridge::new(
        1,
    )));
    agg.enable_group_spill(
        crate::spill::SpillConfig::default(),
        Box::new(crate::spill::HostR2SortedRunStore::new(bridge.clone())),
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, (0..100).map(|i| i % 7).collect(), vec![1; 100]);
    batch.push_i64_column(1, (0..100).collect(), vec![1; 100]);
    batch.set_row_count(100).unwrap();
    assert!(matches!(
        agg.consume_row_batch(&batch, &SelectionMask::all(100))
            .unwrap(),
        SpillProgress::Ready(())
    ));
    assert!(!agg.needs_spill_chunk_finish());
    let SpillProgress::Ready(state) = agg.finish_partial_state().unwrap() else {
        panic!("finish waited on the host without having spilled");
    };
    assert!(
        bridge.borrow().pending_request().is_none(),
        "finish asked the host for I/O"
    );
    assert_eq!(agg.spill_runs_flushed(), 0);
    let snap = crate::stream::partials::partial_snapshot_from_state(&state).unwrap();
    let counts = crate::ops::query::distinct_counts_snapshot(&snap).unwrap();
    assert_eq!(counts.groups.len(), 7);
    assert_eq!(
        counts.groups.iter().map(|g| g.stats[0].count).sum::<u64>(),
        100
    );
}

#[test]
fn count_distinct_spills_for_every_key_shape() {
    let number = |name: &str| CatalogColumn {
        name: name.to_string(),
        kind: crate::ir::ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    let text = |name: &str| CatalogColumn {
        kind: crate::ir::ColumnKind::Text,
        physical: 6,
        ..number(name)
    };
    // Columns: 0 int group, 1 text group, 2 int value, 3 text value, 4 int group, 5 int group.
    let catalog = vec![
        number("g"),
        text("t"),
        number("v"),
        text("w"),
        number("h"),
        number("k"),
    ];
    let rows = 600usize;
    let mut x: u64 = 0x2545F4914F6CDD1D;
    let mut next = |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    let g: Vec<i64> = (0..rows).map(|_| next(7) as i64).collect();
    let t: Vec<String> = (0..rows).map(|_| format!("model-{}", next(5))).collect();
    let v: Vec<i64> = (0..rows).map(|_| next(90) as i64).collect();
    let w: Vec<String> = (0..rows)
        .map(|_| format!("https://x/{}", next(120)))
        .collect();
    let h: Vec<i64> = (0..rows).map(|_| next(3) as i64).collect();
    let k: Vec<i64> = (0..rows).map(|_| next(2) as i64).collect();
    // Every tenth value is NULL, so NULL handling is covered too.
    let nulls: Vec<u8> = (0..rows).map(|i| u8::from(i % 10 != 0)).collect();
    let utf8 = |values: &[String]| {
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(value.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        (offsets, bytes)
    };
    let batch_of = |from: usize, to: usize| {
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, g[from..to].to_vec(), vec![1; to - from]);
        let (offsets, bytes) = utf8(&t[from..to]);
        batch.push_utf8_column(1, offsets, bytes, vec![1; to - from]);
        batch.push_i64_column(2, v[from..to].to_vec(), nulls[from..to].to_vec());
        let (offsets, bytes) = utf8(&w[from..to]);
        batch.push_utf8_column(3, offsets, bytes, nulls[from..to].to_vec());
        batch.push_i64_column(4, h[from..to].to_vec(), vec![1; to - from]);
        batch.push_i64_column(5, k[from..to].to_vec(), vec![1; to - from]);
        batch.set_row_count(to - from).unwrap();
        batch
    };
    let feed = |agg: &mut AggregateOp, serve: &mut dyn FnMut(&mut AggregateOp)| {
        for from in (0..rows).step_by(50) {
            let batch = batch_of(from, (from + 50).min(rows));
            let mut done = false;
            for _ in 0..10_000 {
                match agg
                    .consume_row_batch(&batch, &SelectionMask::all(batch.row_count()))
                    .unwrap()
                {
                    SpillProgress::Ready(()) => {
                        done = true;
                        break;
                    }
                    SpillProgress::AwaitHost => serve(agg),
                }
            }
            assert!(done, "batch at row {from} never finished");
        }
    };
    // (label, group by, counted column): global int, global text (hashed), grouped int,
    // grouped text, int + text groups (Q11), and four keys (the general group table).
    let shapes: [(&str, &[u32], u32); 6] = [
        ("global int", &[], 2),
        ("global text", &[], 3),
        ("grouped int", &[0], 2),
        ("grouped text", &[1], 2),
        ("int and text groups", &[0, 1], 2),
        ("general table", &[0, 4, 5], 2),
    ];
    for (index, (label, group_by, column)) in shapes.into_iter().enumerate() {
        let aggs = [AggExpr {
            column: Some(column),
            kind: AggKind::CountDistinct,
            offset: 0,
        }];
        let mut memory =
            AggregateOp::from_aggregate(group_by, &aggs, None, 0, false, &catalog).unwrap();
        memory.set_final_aggregate(true);
        let mut spilled =
            AggregateOp::from_aggregate(group_by, &aggs, None, 0, false, &catalog).unwrap();
        spilled.set_final_aggregate(true);
        let path = std::env::temp_dir().join(format!(
            "wcol-distinct-spill-{}-{index}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = crate::spill::SpillConfig {
            combined_budget_bytes: 384 * 3 + 128,
            scratch_reserve_bytes: 128,
            max_batch_rows: 2,
            max_batch_bytes: 64 * 1024,
            scan_page_rows: 2,
            finalize_batch_rows: 2,
        };
        spilled
            .enable_group_spill(
                config,
                Box::new(crate::spill::FileAppendSpillStore::open(path.clone()).unwrap()),
            )
            .unwrap();
        // The R2 store writes pages as flushed, without re-sorting: it fails on any
        // flush that is not in canonical key order, and it waits on every run's commit.
        let bridge = std::rc::Rc::new(std::cell::RefCell::new(crate::spill::HostSpillBridge::new(
            index as u64 + 1,
        )));
        let mut r2 =
            AggregateOp::from_aggregate(group_by, &aggs, None, 0, false, &catalog).unwrap();
        r2.set_final_aggregate(true);
        r2.enable_group_spill(
            config,
            Box::new(crate::spill::HostR2SortedRunStore::new(bridge.clone())),
        )
        .unwrap();
        let mut bodies = std::collections::HashMap::new();
        let mut serve = |agg: &mut AggregateOp| serve_r2(agg, &bridge, &mut bodies);
        let mut no_host = |_: &mut AggregateOp| {};
        feed(&mut memory, &mut no_host);
        feed(&mut spilled, &mut no_host);
        feed(&mut r2, &mut serve);
        assert!(
            spilled.spill_runs_flushed() >= 2,
            "{label}: did not spill ({} runs)",
            spilled.spill_runs_flushed()
        );
        assert!(
            r2.spill_runs_flushed() >= 2,
            "{label}: R2 store did not spill"
        );
        let expected = reducer_counts(&mut memory);
        assert_eq!(reducer_counts(&mut spilled), expected, "{label}");
        assert_eq!(
            reducer_counts_served(&mut r2, &mut serve),
            expected,
            "{label}: R2 runs"
        );
        let _ = std::fs::remove_file(&path);
    }
}
