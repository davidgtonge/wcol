use super::*;
use crate::ir::ColumnKind;
use crate::ops::query::{encoded_partials_equivalent, CatalogColumn, SelectionMask};
use crate::stream::OwnedWcolBatch;

fn catalog() -> Vec<CatalogColumn> {
    vec![CatalogColumn {
        name: "a".into(),
        kind: ColumnKind::Number,
        physical: 0,
        type_length: 0,
        max_def: 0,
        max_rep: 0,
    }]
}

fn sum_pwcl(values: &[i64]) -> Vec<u8> {
    let mut query = crate::ops::query::batch::BatchQuery::from_ir(
        &[],
        &[],
        &[crate::ir::AggExpr {
            column: Some(0),
            kind: crate::ir::AggKind::Sum,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog(),
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, values.to_vec(), vec![1; values.len()]);
    batch.set_row_count(values.len()).unwrap();
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    query
        .consume(&view, &SelectionMask::all(values.len()))
        .unwrap();
    partial_state_to_wcs_payload(&query.finish_partial_state()).unwrap()
}

#[test]
fn pwcl_roundtrip_preserves_sum() {
    let pwcl = sum_pwcl(&[3, 7]);
    assert!(pwcl.starts_with(b"PWCL"));
    let back = partial_state_from_wire_payload(&pwcl).unwrap();
    let again = partial_state_to_wcs_payload(&back).unwrap();
    encoded_partials_equivalent(&pwcl, &again).unwrap();
    assert_eq!(partial_first_sum_from_wire(&pwcl).unwrap(), 10);
}

#[test]
fn wire_sum_reads_pwcl() {
    assert_eq!(partial_first_sum_from_wire(&sum_pwcl(&[5])).unwrap(), 5);
}

#[test]
fn empty_string_reducer_keeps_the_string_mask_on_the_wire() {
    let state = PartialStateWcol {
        schema: PartialStateSchemaV1 {
            version: PARTIAL_SCHEMA_VERSION,
            grouped: true,
            order_groups_by_count: true,
            limit: 10,
            offset: 0,
            group_key_len: 1,
            group_key_float_mask: 0,
            group_key_string_mask: 1,
            aggs: vec![PartialAggSpec {
                kind: 0,
                column: u32::MAX,
            }],
        },
        passed_rows: 0,
        row_count: 0,
        columns: Vec::new(),
        sketches: Vec::new(),
        string_keys: Vec::new(),
    };
    let wire = partial_state_to_wcs_payload(&state).unwrap();
    let back = partial_state_from_wire_payload(&wire).unwrap();
    assert_eq!(back.schema.group_key_string_mask, 1);
    assert_eq!(wire[4], 5);
}

fn grouped_state(rows: usize) -> PartialStateWcol {
    use crate::ir::AggKind::*;
    grouped_state_of(
        rows,
        &[0, 1],
        &[
            (CountStar, None),
            (Count, Some(2)),
            (Sum, Some(2)),
            (Avg, Some(2)),
            (Min, Some(2)),
            (Max, Some(2)),
            (ApproxDistinct, Some(2)),
        ],
    )
}

/// `rows` rows of (url text, region, x) grouped by `group_by`.
fn grouped_state_of(
    rows: usize,
    group_by: &[u32],
    aggs: &[(crate::ir::AggKind, Option<u32>)],
) -> PartialStateWcol {
    let text = |name: &str| CatalogColumn {
        name: name.into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    let number = |name: &str| CatalogColumn {
        name: name.into(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    let catalog = vec![text("url"), number("region"), number("x")];
    let aggs: Vec<crate::ir::AggExpr> = aggs
        .iter()
        .map(|(kind, column)| crate::ir::AggExpr {
            column: *column,
            kind: *kind,
            offset: 0,
        })
        .collect();
    let mut query = crate::ops::query::batch::BatchQuery::from_ir(
        &[],
        group_by,
        &aggs,
        None,
        0,
        false,
        &catalog,
    )
    .unwrap();
    let (mut offsets, mut bytes, mut text_nulls) = (vec![0u32], Vec::new(), Vec::new());
    let (mut regions, mut xs, mut x_nulls) = (Vec::new(), Vec::new(), Vec::new());
    for row in 0..rows {
        if row % 17 == 0 {
            text_nulls.push(0);
        } else {
            bytes.extend_from_slice(format!("https://example.com/{}", row % 97).as_bytes());
            text_nulls.push(1);
        }
        offsets.push(bytes.len() as u32);
        regions.push((row % 5) as i64 - 2);
        xs.push(if row % 3 == 0 {
            -1_000_000_000_000
        } else {
            row as i64 * 31
        });
        x_nulls.push(u8::from(row % 11 != 0));
    }
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(0, offsets, bytes, text_nulls);
    batch.push_i64_column(1, regions, vec![1; rows]);
    batch.push_i64_column(2, xs, x_nulls);
    batch.set_row_count(rows).unwrap();
    let mut cols = Vec::new();
    let view = batch.view_with(&mut cols);
    query.consume(&view, &SelectionMask::all(rows)).unwrap();
    query.finish_partial_state()
}

#[test]
fn every_aggregate_kind_and_key_type_round_trips_exactly() {
    let state = grouped_state(2000);
    assert!(state.row_count > 100 && !state.string_keys.is_empty() && !state.sketches.is_empty());
    let wire = partial_state_to_wcs_payload(&state).unwrap();
    assert_eq!(partial_state_from_wire_payload(&wire).unwrap(), state);
}

#[test]
fn unused_stat_columns_and_small_counts_cost_almost_nothing() {
    let state = grouped_state(2000);
    let columns = encode_columns(&state.columns, state.row_count).unwrap();
    // Fixed cells were 9 bytes per column per row, whatever the column held.
    let fixed: usize = state.columns.len() * state.row_count * 9;
    assert!(
        columns.len() * 2 < fixed,
        "{} encoded against {fixed} fixed",
        columns.len()
    );

    // Q33's shape: a string key and COUNT(*). Ten of the eleven stat columns are constant
    // and the count is a small varint, so the columns add about a byte per group.
    let counted = grouped_state_of(2000, &[0], &[(crate::ir::AggKind::CountStar, None)]);
    let columns = encode_columns(&counted.columns, counted.row_count).unwrap();
    assert!(
        columns.len() < counted.row_count * 3 + 400,
        "{} column bytes for {} groups",
        columns.len(),
        counted.row_count
    );
}

#[test]
fn extreme_values_round_trip() {
    let values = vec![i64::MIN, i64::MAX, 0, -1, 1, i64::MIN + 1];
    let state = PartialStateWcol {
        schema: PartialStateSchemaV1 {
            version: PARTIAL_SCHEMA_VERSION,
            grouped: true,
            order_groups_by_count: false,
            limit: 0,
            offset: 0,
            group_key_len: 1,
            group_key_float_mask: 0,
            group_key_string_mask: 0,
            aggs: vec![PartialAggSpec {
                kind: 0,
                column: u32::MAX,
            }],
        },
        passed_rows: 6,
        row_count: values.len(),
        columns: vec![
            PartialColumn {
                id: 1,
                values: values.clone(),
                nulls: vec![1, 0, 1, 1, 0, 1],
            },
            PartialColumn {
                id: 2,
                values: vec![7; 6],
                nulls: vec![1; 6],
            },
            PartialColumn {
                id: 3,
                values: vec![1, 2, 3, 4, 5, 300],
                nulls: vec![1; 6],
            },
        ],
        sketches: Vec::new(),
        string_keys: Vec::new(),
    };
    let wire = partial_state_to_wcs_payload(&state).unwrap();
    assert_eq!(partial_state_from_wire_payload(&wire).unwrap(), state);
}

#[test]
fn truncated_or_corrupt_payloads_are_refused() {
    let wire = partial_state_to_wcs_payload(&grouped_state(40)).unwrap();
    for len in 0..wire.len() {
        assert!(
            partial_state_from_wire_payload(&wire[..len]).is_err(),
            "prefix {len} decoded"
        );
    }
    let mut huge_rows = wire.clone();
    // row_count sits after the header, the aggregates and passed_rows.
    let aggs = u32::from_le_bytes(wire[23..27].try_into().unwrap()) as usize;
    let at = 27 + aggs * 5 + 8;
    huge_rows[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(partial_state_from_wire_payload(&huge_rows).is_err());
}

#[test]
fn counts_beyond_the_payload_are_refused_before_allocating() {
    let pwcl = sum_pwcl(&[3, 7]);
    let count_at = |at: usize| u32::from_le_bytes(pwcl[at..at + 4].try_into().unwrap()) as usize;
    // Magic, version, flags, limit, offset, key length and the two key masks precede it.
    let aggs_at = 23;
    let rows_at = aggs_at + 4 + count_at(aggs_at) * AGG_BYTES + 8;
    let columns_at = rows_at + 4;
    for (at, want) in [
        (aggs_at, "aggregate count"),
        (rows_at, "row count"),
        (columns_at, "column count"),
    ] {
        let mut wire = pwcl.clone();
        wire[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = partial_state_from_wire_payload(&wire).unwrap_err();
        assert!(err.contains(want), "{want}: {err}");
    }
    // A row count the payload could hold, but its columns cannot.
    let mut wire = pwcl.clone();
    wire[rows_at..rows_at + 4].copy_from_slice(&((pwcl.len() - 1) as u32).to_le_bytes());
    assert!(partial_state_from_wire_payload(&wire).is_err());
}
