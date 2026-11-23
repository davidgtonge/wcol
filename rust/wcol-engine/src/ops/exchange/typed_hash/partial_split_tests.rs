use super::*;
use crate::ir::{AggExpr, AggKind, ColumnKind};
use crate::ops::query::{CatalogColumn, PartialKeyPart};
use crate::stream::partials::{partial_snapshot_from_state, PartialStateWcol};
use crate::stream::{OwnedWcolBatch, SelectionMask};

const PARTITIONS: u32 = 8;

fn catalog() -> Vec<CatalogColumn> {
    let column = |name: &str, kind, physical| CatalogColumn {
        name: name.into(),
        kind,
        physical,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    vec![
        column("g", ColumnKind::Number, 2),
        column("t", ColumnKind::Text, 6),
        column("v", ColumnKind::Number, 2),
        column("f", ColumnKind::Number, 5),
    ]
}

/// 400 rows: int `g`, text `t`, int `v`, double `f`; every seventh key is NULL.
fn batch() -> OwnedWcolBatch {
    let rows = 400usize;
    let nulls: Vec<u8> = (0..rows).map(|i| u8::from(i % 7 != 0)).collect();
    let (mut offsets, mut bytes) = (vec![0u32], Vec::new());
    for i in 0..rows {
        bytes.extend_from_slice(format!("phrase {}", i % 23).as_bytes());
        offsets.push(bytes.len() as u32);
    }
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(
        0,
        (0..rows).map(|i| (i % 13) as i64 - 6).collect(),
        nulls.clone(),
    );
    batch.push_utf8_column(1, offsets, bytes, nulls.clone());
    batch.push_i64_column(
        2,
        (0..rows).map(|i| (i % 31) as i64).collect(),
        vec![1; rows],
    );
    batch.push_f64_column(
        3,
        (0..rows).map(|i| (i % 9) as f64 * 0.25 - 1.0).collect(),
        nulls,
    );
    batch.set_row_count(rows).unwrap();
    batch
}

fn partial(group_by: &[u32], aggs: &[AggExpr], batch: &OwnedWcolBatch) -> PartialStateWcol {
    let mut query = crate::ops::query::batch::BatchQuery::from_ir(
        &[],
        group_by,
        aggs,
        None,
        0,
        false,
        &catalog(),
    )
    .unwrap();
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    query
        .consume(&view, &SelectionMask::all(batch.row_count()))
        .unwrap();
    query.finish_partial_state()
}

/// A combiner's flush of `batch`: each partition's partial, as `flush_routed` hands them on.
fn routed(
    group_by: &[u32],
    aggs: &[AggExpr],
    batch: &OwnedWcolBatch,
    keys: &[(usize, u32)],
) -> Result<Vec<(u32, PartialStateWcol)>, String> {
    let mut query = crate::ops::query::batch::BatchQuery::from_ir(
        &[],
        group_by,
        aggs,
        None,
        0,
        false,
        &catalog(),
    )
    .unwrap();
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    query
        .consume(&view, &SelectionMask::all(batch.row_count()))
        .unwrap();
    let route = CombinerRoute {
        keys: keys.to_vec(),
        partitions: PARTITIONS,
        hash_version: 1,
    };
    let mut out = Vec::new();
    query.flush_routed(&route, u64::MAX, |partition, state| {
        out.push((partition, state));
        Ok(())
    })?;
    Ok(out)
}

/// The partial key part a raw row's value becomes.
fn raw_part(batch: &OwnedWcolBatch, column: u32, row: usize) -> PartialKeyPart {
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    match find_column(&view, column).unwrap() {
        WcolColumn::I64 { values, nulls, .. } if nulls[row] != 0 => {
            PartialKeyPart::Int(i128::from(values[row]))
        }
        WcolColumn::F64 { values, nulls, .. } if nulls[row] != 0 => PartialKeyPart::Float(
            crate::ops::query::float_key::canonical_f64(values[row]).to_bits(),
        ),
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } if nulls[row] != 0 => {
            PartialKeyPart::Bytes(byte_span(offsets, bytes, row).unwrap().to_vec())
        }
        _ => PartialKeyPart::Null,
    }
}

#[test]
fn partial_rows_go_where_their_raw_rows_go() {
    let batch = batch();
    let count = [AggExpr {
        column: None,
        kind: AggKind::CountStar,
        offset: 0,
    }];
    let distinct = [AggExpr {
        column: Some(2),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    // (group by, aggregates, exchange keys as (partial key position, source column)).
    let shapes: Vec<(&[u32], &[AggExpr], Vec<(usize, u32)>)> = vec![
        (&[0], &count, vec![(0, 0)]),
        (&[1], &count, vec![(0, 1)]),
        (&[3], &count, vec![(0, 3)]),
        (&[0, 1], &count, vec![(0, 0), (1, 1)]),
        // COUNT(DISTINCT): routed by group and counted value, or by the group alone.
        (&[0], &distinct, vec![(0, 0), (1, 2)]),
        (&[1], &distinct, vec![(0, 1)]),
    ];
    let mut view_cols = Vec::new();
    let view = batch.view_with(&mut view_cols);
    for (group_by, aggs, keys) in shapes {
        let state = partial(group_by, aggs, &batch);
        let columns: Vec<u32> = keys.iter().map(|(_, column)| *column).collect();
        // Where the raw rows of each exchange key went.
        let mut raw = std::collections::HashMap::new();
        for row in 0..batch.row_count() {
            let key: Vec<PartialKeyPart> =
                columns.iter().map(|&c| raw_part(&batch, c, row)).collect();
            let cache = ExchangeHashCache::default();
            let partition = partition_for_row(&view, row, &columns, PARTITIONS, 1, &cache).unwrap();
            assert_eq!(*raw.entry(key).or_insert(partition), partition);
        }
        let split = routed(group_by, aggs, &batch, &keys).unwrap();
        assert!(
            split.len() > 1,
            "{group_by:?}: everything went to one partition"
        );
        let mut rows = 0;
        let mut passed = 0;
        for (partition, part) in &split {
            rows += part.row_count;
            passed += part.passed_rows;
            for group in partial_snapshot_from_state(part).unwrap().groups {
                let key: Vec<PartialKeyPart> = keys
                    .iter()
                    .map(|(position, _)| group.key[*position].clone())
                    .collect();
                assert_eq!(raw.get(&key), Some(partition), "{group_by:?}: {key:?}");
            }
        }
        assert_eq!(
            rows, state.row_count,
            "{group_by:?}: rows lost or duplicated"
        );
        assert_eq!(passed, state.passed_rows, "{group_by:?}: passed rows");
    }
}

#[test]
fn split_partials_rebuild_the_original() {
    let batch = batch();
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
        AggExpr {
            column: Some(2),
            kind: AggKind::ApproxDistinct,
            offset: 0,
        },
    ];
    let state = partial(&[1, 0], &aggs, &batch);
    let split = routed(&[1, 0], &aggs, &batch, &[(0, 1), (1, 0)]).unwrap();
    let sorted = |state: &PartialStateWcol| {
        let mut groups: Vec<String> = partial_snapshot_from_state(state)
            .unwrap()
            .groups
            .iter()
            .map(|group| format!("{group:?}"))
            .collect();
        groups.sort();
        groups
    };
    let mut rebuilt: Vec<String> = split.iter().flat_map(|(_, part)| sorted(part)).collect();
    rebuilt.sort();
    assert_eq!(rebuilt, sorted(&state));
}

#[test]
fn hashed_text_distinct_keeps_each_value_on_one_partition() {
    let batch = batch();
    let distinct = [AggExpr {
        column: Some(1),
        kind: AggKind::CountDistinct,
        offset: 0,
    }];
    let state = partial(&[], &distinct, &batch);
    assert_eq!(
        state.schema.group_key_string_mask, 0,
        "text distinct should hash its values"
    );
    let split = routed(&[], &distinct, &batch, &[(0, 1)]).unwrap();
    let mut seen = std::collections::HashMap::new();
    for (partition, part) in &split {
        for group in partial_snapshot_from_state(part).unwrap().groups {
            assert!(seen.insert(group.key.clone(), *partition).is_none());
        }
    }
    assert_eq!(seen.len(), state.row_count);
    assert!(routed(&[], &distinct, &batch, &[(1, 1)]).is_err());
}
