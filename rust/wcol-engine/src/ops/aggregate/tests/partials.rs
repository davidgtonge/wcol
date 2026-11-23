use super::*;

#[test]
fn merged_partials_equal_the_rows_they_summarise_for_every_key_shape() {
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
    let double = |name: &str| CatalogColumn {
        physical: 5,
        ..number(name)
    };
    // 0 int group, 1 text group, 2 int value, 3 text value, 4 int group, 5 int group,
    // 6 double value and group.
    let catalog = vec![
        number("g"),
        text("t"),
        number("v"),
        text("w"),
        number("h"),
        number("k"),
        double("f"),
    ];
    let rows = 900usize;
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut next = |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    let g: Vec<i64> = (0..rows).map(|_| next(7) as i64 - 3).collect();
    let t: Vec<String> = (0..rows).map(|_| format!("model-{}", next(40))).collect();
    let v: Vec<i64> = (0..rows)
        .map(|_| next(1_000_000) as i64 - 500_000)
        .collect();
    let w: Vec<String> = (0..rows)
        .map(|_| format!("https://x/{}", next(150)))
        .collect();
    let h: Vec<i64> = (0..rows).map(|_| next(3) as i64).collect();
    let k: Vec<i64> = (0..rows).map(|_| next(2) as i64).collect();
    // Halves, so float sums are exact in any order.
    let f: Vec<f64> = (0..rows).map(|_| next(20) as f64 * 0.5 - 4.0).collect();
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
        let n = to - from;
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, g[from..to].to_vec(), vec![1; n]);
        let (offsets, bytes) = utf8(&t[from..to]);
        batch.push_utf8_column(1, offsets, bytes, nulls[from..to].to_vec());
        batch.push_i64_column(2, v[from..to].to_vec(), nulls[from..to].to_vec());
        let (offsets, bytes) = utf8(&w[from..to]);
        batch.push_utf8_column(3, offsets, bytes, nulls[from..to].to_vec());
        batch.push_i64_column(4, h[from..to].to_vec(), vec![1; n]);
        batch.push_i64_column(5, k[from..to].to_vec(), vec![1; n]);
        batch.push_f64_column(6, f[from..to].to_vec(), nulls[from..to].to_vec());
        batch.set_row_count(n).unwrap();
        batch
    };
    let agg = |kind, column: Option<u32>, offset| AggExpr {
        column,
        kind,
        offset,
    };
    use AggKind::*;
    let all_kinds = vec![
        agg(CountStar, None, 0),
        agg(Count, Some(2), 0),
        agg(Sum, Some(2), 0),
        agg(Avg, Some(2), 0),
        agg(Min, Some(2), 0),
        agg(Max, Some(2), 0),
        agg(ApproxDistinct, Some(2), 0),
        agg(Sum, Some(6), 0),
        agg(Min, Some(6), 0),
        agg(Max, Some(6), 0),
    ];
    let mut shifted = all_kinds.clone();
    shifted.push(agg(Sum, Some(2), 5));
    let int_only = vec![
        agg(CountStar, None, 0),
        agg(Sum, Some(2), 0),
        agg(Max, Some(2), 0),
    ];
    let distinct = |column| vec![agg(CountDistinct, Some(column), 0)];
    // (label, group by, aggregates). Distinct shapes finish as the final aggregate.
    let shapes: Vec<(&str, Vec<u32>, Vec<AggExpr>)> = vec![
        ("ungrouped", vec![], all_kinds.clone()),
        ("int key with offset", vec![0], shifted),
        ("text key", vec![1], all_kinds.clone()),
        ("float key", vec![6], all_kinds),
        ("two int keys (pair table)", vec![0, 4], int_only.clone()),
        ("three int keys", vec![0, 4, 5], int_only),
        ("global text distinct (hashed)", vec![], distinct(3)),
        ("grouped int distinct", vec![0], distinct(2)),
        ("grouped text distinct", vec![1], distinct(3)),
    ];
    let spill_config = crate::spill::SpillConfig {
        combined_budget_bytes: 384 * 3 + 128,
        scratch_reserve_bytes: 128,
        max_batch_rows: 2,
        max_batch_bytes: 64 * 1024,
        scan_page_rows: 2,
        finalize_batch_rows: 2,
    };
    for (index, (label, group_by, aggs)) in shapes.into_iter().enumerate() {
        let is_distinct = aggs.iter().any(|a| a.kind == CountDistinct);
        let new_agg = |final_aggregate: bool| {
            let mut op =
                AggregateOp::from_aggregate(&group_by, &aggs, None, 0, false, &catalog).unwrap();
            op.set_final_aggregate(final_aggregate);
            op
        };
        let mut direct = new_agg(is_distinct);
        for from in (0..rows).step_by(100) {
            consume_all(&mut direct, &batch_of(from, (from + 100).min(rows)));
        }
        // Three scans each summarise a third of the rows; their partials keep any hidden
        // distinct key, as a scan's do.
        let partials: Vec<PartialStateWcol> = (0..3)
            .map(|part| {
                let mut scan = new_agg(false);
                consume_all(&mut scan, &batch_of(part * 300, part * 300 + 300));
                finish_state(&mut scan)
            })
            .collect();
        let bridge = std::rc::Rc::new(std::cell::RefCell::new(crate::spill::HostSpillBridge::new(
            index as u64 + 1,
        )));
        let mut spilled = new_agg(is_distinct);
        spilled
            .enable_group_spill(
                spill_config,
                Box::new(crate::spill::HostR2SortedRunStore::new(bridge.clone())),
            )
            .unwrap();
        let mut bodies = std::collections::HashMap::new();
        let mut serve = |agg: &mut AggregateOp| serve_r2(agg, &bridge, &mut bodies);
        let mut merged = new_agg(is_distinct);
        for partial in &partials {
            spill_ready(merged.consume_partial(partial).unwrap());
            let mut done = false;
            for _ in 0..100_000 {
                match spilled.consume_partial(partial).unwrap() {
                    SpillProgress::Ready(()) => {
                        done = true;
                        break;
                    }
                    SpillProgress::AwaitHost => serve(&mut spilled),
                }
            }
            assert!(done, "{label}: partial never finished merging");
        }
        assert_eq!(
            merged.passed_rows(),
            direct.passed_rows(),
            "{label}: passed rows"
        );
        assert_eq!(
            spilled.passed_rows(),
            direct.passed_rows(),
            "{label}: spilled passed rows"
        );
        if !group_by.is_empty() {
            assert!(spilled.spill_runs_flushed() >= 2, "{label}: did not spill");
        }
        let mut no_host = |_: &mut AggregateOp| {};
        if is_distinct {
            let expected = reducer_counts(&mut direct);
            assert_eq!(reducer_counts(&mut merged), expected, "{label}");
            assert_eq!(
                reducer_counts_served(&mut spilled, &mut serve),
                expected,
                "{label}: spilled"
            );
        } else {
            let expected = finished_groups_served(&mut direct, &mut no_host);
            assert!(!expected.is_empty(), "{label}: no groups");
            assert_same_groups(
                &finished_groups_served(&mut merged, &mut no_host),
                &expected,
                label,
            );
            assert_same_groups(
                &finished_groups_served(&mut spilled, &mut serve),
                &expected,
                &format!("{label}: spilled"),
            );
        }
    }
}

#[test]
fn a_combiner_hands_on_partials_that_merge_to_the_whole() {
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
    let double = |name: &str| CatalogColumn {
        physical: 5,
        ..number(name)
    };
    // 0 int key, 1 text key, 2 int value, 3 int key, 4 double key.
    let catalog = vec![
        number("g"),
        text("t"),
        number("v"),
        number("h"),
        double("f"),
    ];
    let rows = 3000usize;
    let batch_of = |from: usize, to: usize| {
        let n = to - from;
        let (mut offsets, mut bytes) = (vec![0u32], Vec::new());
        for i in from..to {
            bytes.extend_from_slice(format!("https://site/{}", (i * 7919) % 1100).as_bytes());
            offsets.push(bytes.len() as u32);
        }
        let nulls: Vec<u8> = (from..to).map(|i| u8::from(i % 13 != 0)).collect();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(
            0,
            (from..to).map(|i| (i % 400) as i64).collect(),
            vec![1; n],
        );
        batch.push_utf8_column(1, offsets, bytes, nulls.clone());
        batch.push_i64_column(
            2,
            (from..to).map(|i| (i * 31 % 997) as i64 - 400).collect(),
            nulls,
        );
        batch.push_i64_column(3, (from..to).map(|i| (i % 3) as i64).collect(), vec![1; n]);
        batch.push_f64_column(
            4,
            (from..to).map(|i| (i % 90) as f64 * 0.5).collect(),
            vec![1; n],
        );
        batch.set_row_count(n).unwrap();
        batch
    };
    let agg = |kind, column| AggExpr {
        column,
        kind,
        offset: 0,
    };
    use AggKind::*;
    let sums = vec![agg(CountStar, None), agg(Sum, Some(2)), agg(Max, Some(2))];
    let shapes: Vec<(&str, Vec<u32>, Vec<AggExpr>)> = vec![
        ("int key", vec![0], sums.clone()),
        ("text key", vec![1], sums.clone()),
        ("float key", vec![4], sums.clone()),
        ("pair table", vec![0, 3], sums),
        (
            "grouped distinct",
            vec![3],
            vec![agg(CountDistinct, Some(1))],
        ),
        (
            "global text distinct",
            vec![],
            vec![agg(CountDistinct, Some(1))],
        ),
    ];
    for (label, group_by, aggs) in shapes {
        let is_distinct = aggs.iter().any(|a| a.kind == CountDistinct);
        let new_agg = |final_aggregate: bool| {
            let mut op =
                AggregateOp::from_aggregate(&group_by, &aggs, None, 0, false, &catalog).unwrap();
            op.set_final_aggregate(final_aggregate);
            op
        };
        let mut direct = new_agg(is_distinct);
        let mut combiner = new_agg(false);
        // Route by the first partial key (the counted value for a global distinct).
        let first_key = group_by.first().copied().unwrap_or(1);
        let route = crate::ops::exchange::CombinerRoute {
            keys: vec![(0, first_key)],
            partitions: 4,
            hash_version: 1,
        };
        // Tiny, so the table fills many times.
        combiner.set_combiner(16 * 1024, route.clone()).unwrap();
        let mut handed_on: Vec<(u32, PartialStateWcol)> = Vec::new();
        let drain = |combiner: &mut AggregateOp, into: &mut Vec<(u32, PartialStateWcol)>| {
            while let Some((partition, payload)) = combiner.pop_routed_payload() {
                let state =
                    crate::stream::partial_wcs::partial_state_from_wire_payload(&payload).unwrap();
                into.push((partition, state));
            }
        };
        let mut flushes = 0;
        for from in (0..rows).step_by(100) {
            let batch = batch_of(from, from + 100);
            consume_all(&mut direct, &batch);
            let out = crate::exec::runtime::wcol_stream::drive_operator(
                &mut combiner,
                vec![WcolStreamItem::from_row_batch(batch)],
                false,
            )
            .unwrap();
            assert!(out.is_empty(), "{label}: a combiner emits no stream items");
            let before = handed_on.len();
            drain(&mut combiner, &mut handed_on);
            flushes += usize::from(handed_on.len() > before);
        }
        let last =
            crate::exec::runtime::wcol_stream::drive_operator(&mut combiner, Vec::new(), true)
                .unwrap();
        assert!(last.is_empty(), "{label}: a combiner emits no stream items");
        drain(&mut combiner, &mut handed_on);
        assert!(flushes >= 2, "{label}: the combiner never flushed mid-scan");
        // Every group went to the partition its key routes to.
        let mut buf = Vec::new();
        for (partition, state) in &handed_on {
            let rows = crate::stream::partials::PartialRows::new(state).unwrap();
            for row in 0..state.row_count {
                let part = rows.key(row, 0).unwrap();
                assert_eq!(
                    route.partition([(first_key, part)], &mut buf).unwrap(),
                    *partition,
                    "{label}: a group went to the wrong reducer"
                );
            }
        }
        let mut reducer = new_agg(is_distinct);
        for (_, partial) in &handed_on {
            spill_ready(reducer.consume_partial(partial).unwrap());
        }
        assert_eq!(
            reducer.passed_rows(),
            direct.passed_rows(),
            "{label}: passed rows"
        );
        let mut no_host = |_: &mut AggregateOp| {};
        if is_distinct {
            assert_eq!(
                reducer_counts(&mut reducer),
                reducer_counts(&mut direct),
                "{label}"
            );
        } else {
            let expected = finished_groups_served(&mut direct, &mut no_host);
            assert_same_groups(
                &finished_groups_served(&mut reducer, &mut no_host),
                &expected,
                label,
            );
        }
    }
}

#[test]
fn a_spilling_aggregate_cannot_also_flush_partials() {
    let catalog = vec![CatalogColumn {
        name: "k".to_string(),
        kind: crate::ir::ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let count = [AggExpr {
        column: None,
        kind: AggKind::CountStar,
        offset: 0,
    }];
    let mut agg = AggregateOp::from_aggregate(&[0], &count, None, 0, false, &catalog).unwrap();
    let bridge = std::rc::Rc::new(std::cell::RefCell::new(crate::spill::HostSpillBridge::new(
        1,
    )));
    agg.enable_group_spill(
        crate::spill::SpillConfig::default(),
        Box::new(crate::spill::HostR2SortedRunStore::new(bridge)),
    )
    .unwrap();
    let route = crate::ops::exchange::CombinerRoute {
        keys: vec![(0, 0)],
        partitions: 2,
        hash_version: 1,
    };
    assert!(agg.set_combiner(1 << 20, route).is_err());
}

#[test]
fn partial_from_another_plan_is_refused() {
    let catalog: Vec<CatalogColumn> = (0..2)
        .map(|physical| CatalogColumn {
            name: format!("c{physical}"),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        })
        .collect();
    let count = [AggExpr {
        column: None,
        kind: AggKind::CountStar,
        offset: 0,
    }];
    let sum = [AggExpr {
        column: Some(1),
        kind: AggKind::Sum,
        offset: 0,
    }];
    let mut scan = AggregateOp::from_aggregate(&[0], &count, None, 0, false, &catalog).unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 2], vec![1, 1]);
    batch.push_i64_column(1, vec![3, 4], vec![1, 1]);
    batch.set_row_count(2).unwrap();
    consume_all(&mut scan, &batch);
    let partial = finish_state(&mut scan);
    let mut other = AggregateOp::from_aggregate(&[0], &sum, None, 0, false, &catalog).unwrap();
    assert!(other.consume_partial(&partial).is_err());
    let mut ungrouped = AggregateOp::from_aggregate(&[], &count, None, 0, false, &catalog).unwrap();
    assert!(ungrouped.consume_partial(&partial).is_err());
}

#[test]
fn group_budget_uses_resident_table_bytes() {
    let catalog = vec![CatalogColumn {
        name: "k".to_string(),
        kind: crate::ir::ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
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
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, (0..64).collect(), vec![1; 64]);
    batch.set_row_count(64).unwrap();
    spill_ready(
        agg.consume_row_batch(&batch, &SelectionMask::all(64))
            .unwrap(),
    );
    let bytes = agg.resident_state_bytes();
    assert!(bytes > 0);
    assert!(bytes < 64 * ESTIMATED_GROUP_BYTES);
    agg.set_group_budget(bytes);
    spill_ready(
        agg.consume_row_batch(&batch, &SelectionMask::all(64))
            .unwrap(),
    );
    agg.set_group_budget(bytes.saturating_sub(1).max(1));
    let err = agg
        .consume_row_batch(&batch, &SelectionMask::all(64))
        .unwrap_err();
    assert!(err.contains("resource limit"), "{err}");
}
