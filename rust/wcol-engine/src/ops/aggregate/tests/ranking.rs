use super::*;

#[test]
fn finish_partial_state_roundtrips_through_pwcl() {
    let catalog = vec![CatalogColumn {
        name: "a".to_string(),
        kind: crate::ir::ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }];
    let mut agg = AggregateOp::from_aggregate(
        &[],
        &[AggExpr {
            column: Some(0),
            kind: AggKind::Sum,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog,
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![7, 13], vec![1, 1]);
    batch.set_row_count(2).unwrap();
    consume_all(&mut agg, &batch);
    let state = finish_state(&mut agg);
    let via_pwcl =
        partial_state_from_wire_payload(&partial_state_to_wcs_payload(&state).unwrap()).unwrap();
    assert_eq!(state, via_pwcl);
}

#[test]
fn final_rank_keeps_limit_plus_offset_groups() {
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
        Some(2),
        1,
        true,
        &catalog,
    )
    .unwrap();
    agg.set_final_rank(Some(0));
    // counts: key 1 → 1, key 2 → 2, key 3 → 2, key 4 → 3
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 2, 2, 3, 3, 4, 4, 4], vec![1; 8]);
    batch.set_row_count(8).unwrap();
    consume_all(&mut agg, &batch);
    let state = finish_state(&mut agg);
    assert_eq!(state.row_count, 3, "offset 1 + limit 2");
    let mut keys = state.columns[0].values.clone();
    keys.sort();
    assert_eq!(keys, vec![2, 3, 4]);

    let mut tied = AggregateOp::from_aggregate(
        &[0],
        &[AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        Some(1),
        0,
        true,
        &catalog,
    )
    .unwrap();
    tied.set_final_rank(Some(0));
    let mut tied_batch = OwnedWcolBatch::empty();
    tied_batch.push_i64_column(0, vec![3, 3, 2, 2], vec![1; 4]);
    tied_batch.set_row_count(4).unwrap();
    consume_all(&mut tied, &tied_batch);
    let tied_state = finish_state(&mut tied);
    assert_eq!(tied_state.columns[0].values, vec![2]);
}

#[test]
fn two_integer_keys_keep_the_top_count() {
    let catalog = vec![
        CatalogColumn {
            name: "a".to_string(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        },
        CatalogColumn {
            name: "b".to_string(),
            kind: crate::ir::ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        },
    ];
    let mut agg = AggregateOp::from_aggregate(
        &[0, 1],
        &[AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        Some(1),
        0,
        true,
        &catalog,
    )
    .unwrap();
    agg.set_final_rank(Some(0));
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 1, 1, 2, 2, 2], vec![1; 6]);
    batch.push_i64_column(1, vec![1, 1, 2, 2, 2, 2], vec![1; 6]);
    batch.set_row_count(6).unwrap();
    consume_all(&mut agg, &batch);
    let state = finish_state(&mut agg);
    assert_eq!(state.row_count, 1);
    assert_eq!(state.columns[0].values, vec![2]);
    assert_eq!(state.columns[1].values, vec![2]);
}

#[test]
fn a_partial_that_will_merge_emits_every_group() {
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
        Some(1),
        0,
        true,
        &catalog,
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 2, 2], vec![1, 1, 1]);
    batch.set_row_count(3).unwrap();
    consume_all(&mut agg, &batch);
    let state = finish_state(&mut agg);
    assert_eq!(state.row_count, 2);
}
