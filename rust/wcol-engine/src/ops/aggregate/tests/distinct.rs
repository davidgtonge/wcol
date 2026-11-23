use super::*;

#[test]
fn final_ungrouped_count_distinct_is_the_cardinality() {
    let catalog = vec![CatalogColumn {
        name: "k".to_string(),
        kind: crate::ir::ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let aggs = [AggExpr {
        column: Some(0),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let mut partial = AggregateOp::from_aggregate(&[], &aggs, None, 0, false, &catalog).unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 1, 2, 0, 3], vec![1, 1, 1, 0, 1]);
    batch.set_row_count(5).unwrap();
    consume_all(&mut partial, &batch);
    let merged = finish_state(&mut partial);
    assert!(merged.row_count > 1, "a partial keeps the set");
    assert_eq!(distinct_count(&merged), 3);

    let mut final_agg = AggregateOp::from_aggregate(&[], &aggs, None, 0, false, &catalog).unwrap();
    final_agg.set_final_aggregate(true);
    consume_all(&mut final_agg, &batch);
    let state = finish_state(&mut final_agg);
    assert_eq!(state.row_count, 1);
    assert_eq!(distinct_count(&state), 3);
}

#[test]
fn reducer_distinct_counts_sum_across_partitions() {
    // Each reducer owns whole (group, value) pairs, so its count is final and the finalizer
    // only sums them. Here group 1 has values {1, 2, 3} split across two reducers.
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
    let reducer_counts = |rows: (Vec<i64>, Vec<i64>)| {
        let mut agg = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
        agg.set_final_aggregate(true);
        let mut batch = OwnedWcolBatch::empty();
        let n = rows.0.len();
        batch.push_i64_column(0, rows.0, vec![1; n]);
        batch.push_i64_column(1, rows.1, vec![1; n]);
        batch.set_row_count(n).unwrap();
        consume_all(&mut agg, &batch);
        let snap =
            crate::stream::partials::partial_snapshot_from_state(&finish_state(&mut agg)).unwrap();
        crate::ops::query::distinct_counts_snapshot(&snap).unwrap()
    };
    let left = reducer_counts((vec![1, 1, 2], vec![1, 2, 5]));
    let right = reducer_counts((vec![1, 2], vec![3, 6]));
    // Kind 1 is KIND_COUNT, which the finalizer sums.
    assert!(left.aggs.iter().all(|agg| agg.kind == 1));
    let merged = crate::ops::query::merge_partial_snapshots(&left, &right).unwrap();
    let count_of = |key: i128| {
        merged
            .groups
            .iter()
            .find(|group| group.key == vec![crate::ops::query::PartialKeyPart::Int(key)])
            .map(|group| group.stats[0].count)
    };
    assert_eq!(count_of(1), Some(3));
    assert_eq!(count_of(2), Some(2));
}

#[test]
fn mixed_sum_and_count_distinct_merge_across_partitions() {
    // Q9: SUM beside COUNT(DISTINCT). Each reducer owns some (group, value) pairs.
    // Folding the value sums the amounts and counts the values; the finalizer sums both.
    let catalog: Vec<CatalogColumn> = (0..3)
        .map(|physical| CatalogColumn {
            name: format!("c{physical}"),
            kind: crate::ir::ColumnKind::Number,
            physical,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        })
        .collect();
    let aggs = [
        AggExpr {
            column: Some(2),
            kind: AggKind::Sum,
            offset: 0,
        },
        AggExpr {
            column: Some(1),
            kind: AggKind::CountDistinct,
            offset: 0,
        },
    ];
    let reducer = |rows: (Vec<i64>, Vec<i64>, Vec<i64>)| {
        let mut agg = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
        agg.set_final_aggregate(true);
        let mut batch = OwnedWcolBatch::empty();
        let n = rows.0.len();
        batch.push_i64_column(0, rows.0, vec![1; n]);
        batch.push_i64_column(1, rows.1, vec![1; n]);
        batch.push_i64_column(2, rows.2, vec![1; n]);
        batch.set_row_count(n).unwrap();
        consume_all(&mut agg, &batch);
        let snap =
            crate::stream::partials::partial_snapshot_from_state(&finish_state(&mut agg)).unwrap();
        crate::ops::query::distinct_counts_snapshot(&snap).unwrap()
    };
    let left = reducer((vec![1, 1, 2], vec![10, 10, 10], vec![5, 7, 4]));
    let right = reducer((vec![1], vec![11], vec![3]));
    let merged = crate::ops::query::merge_partial_snapshots(&left, &right).unwrap();
    let group = |key: i128| {
        merged
            .groups
            .iter()
            .find(|group| group.key == vec![crate::ops::query::PartialKeyPart::Int(key)])
            .unwrap_or_else(|| panic!("missing group {key}"))
    };
    let one = group(1);
    assert_eq!(one.stats[0].sum_i, 15);
    assert_eq!(one.stats[1].count, 2);
    let two = group(2);
    assert_eq!(two.stats[0].sum_i, 4);
    assert_eq!(two.stats[1].count, 1);
}

#[test]
fn global_text_count_distinct_hashes_values() {
    let catalog = vec![CatalogColumn {
        name: "t".to_string(),
        kind: crate::ir::ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let aggs = [AggExpr {
        column: Some(0),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let text_batch = |values: &[Option<&str>]| {
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        let mut nulls = Vec::new();
        for value in values {
            match value {
                Some(v) => {
                    bytes.extend_from_slice(v.as_bytes());
                    nulls.push(1);
                }
                None => nulls.push(0),
            }
            offsets.push(bytes.len() as u32);
        }
        let mut batch = OwnedWcolBatch::empty();
        batch.push_utf8_column(0, offsets, bytes, nulls);
        batch.set_row_count(values.len()).unwrap();
        batch
    };
    let partial = |values: &[Option<&str>]| {
        let mut agg = AggregateOp::from_aggregate(&[], &aggs, None, 0, false, &catalog).unwrap();
        consume_all(&mut agg, &text_batch(values));
        crate::stream::partials::partial_snapshot_from_state(&finish_state(&mut agg)).unwrap()
    };
    let mut final_agg = AggregateOp::from_aggregate(&[], &aggs, None, 0, false, &catalog).unwrap();
    final_agg.set_final_aggregate(true);
    consume_all(
        &mut final_agg,
        &text_batch(&[Some("a"), Some("b"), Some("a"), None, Some("c")]),
    );
    assert_eq!(distinct_count(&finish_state(&mut final_agg)), 3);

    let merged = crate::ops::query::merge_partial_snapshots(
        &partial(&[Some("a"), Some("b"), None]),
        &partial(&[Some("b"), Some("c")]),
    )
    .unwrap();
    let groups = crate::ops::query::finalize_groups(&merged).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].stats[0].count, 3);
}

#[test]
fn grouped_text_count_distinct_keeps_group_strings() {
    let catalog = vec![
        CatalogColumn {
            name: "g".to_string(),
            kind: crate::ir::ColumnKind::Text,
            physical: 6,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "x".to_string(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
    ];
    let aggs = [AggExpr {
        column: Some(1),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let partial = |rows: &[(&str, i64)]| {
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        for (g, _) in rows {
            bytes.extend_from_slice(g.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        let n = rows.len();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_utf8_column(0, offsets, bytes, vec![1; n]);
        batch.push_i64_column(1, rows.iter().map(|(_, x)| *x).collect(), vec![1; n]);
        batch.set_row_count(n).unwrap();
        let mut agg = AggregateOp::from_aggregate(&[0], &aggs, None, 0, false, &catalog).unwrap();
        consume_all(&mut agg, &batch);
        crate::stream::partials::partial_snapshot_from_state(&finish_state(&mut agg)).unwrap()
    };
    let merged = crate::ops::query::merge_partial_snapshots(
        &partial(&[("a", 1), ("a", 2), ("b", 3), ("a", 2)]),
        &partial(&[("a", 2), ("b", 4), ("b", 3)]),
    )
    .unwrap();
    let groups = crate::ops::query::finalize_groups(&merged).unwrap();
    let count = |name: &str| {
        groups
            .iter()
            .find(|group| {
                group.key
                    == vec![crate::ops::query::PartialKeyPart::Bytes(
                        name.as_bytes().to_vec(),
                    )]
            })
            .map(|group| group.stats[0].count)
    };
    assert_eq!(count("a"), Some(2));
    assert_eq!(count("b"), Some(2));
}

#[test]
fn int_then_text_group_count_distinct_merges_partials() {
    // Q11 shape: GROUP BY int, text with COUNT(DISTINCT int).
    let catalog = vec![
        CatalogColumn {
            name: "m".to_string(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "t".to_string(),
            kind: crate::ir::ColumnKind::Text,
            physical: 6,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumn {
            name: "u".to_string(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        },
    ];
    let aggs = [AggExpr {
        column: Some(2),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let partial = |rows: &[(i64, &str, i64)]| {
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        for (_, t, _) in rows {
            bytes.extend_from_slice(t.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        let n = rows.len();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, rows.iter().map(|r| r.0).collect(), vec![1; n]);
        batch.push_utf8_column(1, offsets, bytes, vec![1; n]);
        batch.push_i64_column(2, rows.iter().map(|r| r.2).collect(), vec![1; n]);
        batch.set_row_count(n).unwrap();
        let mut agg =
            AggregateOp::from_aggregate(&[0, 1], &aggs, None, 0, false, &catalog).unwrap();
        consume_all(&mut agg, &batch);
        crate::stream::partials::partial_snapshot_from_state(&finish_state(&mut agg)).unwrap()
    };
    let merged = crate::ops::query::merge_partial_snapshots(
        &partial(&[(1, "a", 10), (1, "a", 10), (1, "a", 11), (2, "a", 10)]),
        &partial(&[(1, "a", 11), (2, "a", 10), (1, "b", 10)]),
    )
    .unwrap();
    let groups = crate::ops::query::finalize_groups(&merged).unwrap();
    let count = |m: i128, t: &str| {
        groups
            .iter()
            .find(|g| {
                g.key
                    == vec![
                        crate::ops::query::PartialKeyPart::Int(m),
                        crate::ops::query::PartialKeyPart::Bytes(t.as_bytes().to_vec()),
                    ]
            })
            .map(|g| g.stats[0].count)
    };
    assert_eq!(count(1, "a"), Some(2));
    assert_eq!(count(2, "a"), Some(1));
    assert_eq!(count(1, "b"), Some(1));
    assert_eq!(groups.len(), 3);
}
