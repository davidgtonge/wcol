use super::*;
use crate::exec::operator::{Operator, OperatorIo, WcolStreamItem};
use crate::ir::SortKey;
use crate::ops::query::float_key::float_key;
use crate::ops::topk::TopKOp;
use std::sync::Arc;

fn i64_batch(ids: &[i64], keys: &[Option<i64>]) -> OwnedWcolBatch {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, ids.to_vec(), vec![1; ids.len()]);
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    for key in keys {
        match key {
            Some(v) => {
                values.push(*v);
                nulls.push(1);
            }
            None => {
                values.push(0);
                nulls.push(0);
            }
        }
    }
    batch.push_i64_column(1, values, nulls);
    batch.set_row_count(ids.len()).unwrap();
    batch
}

fn utf8_batch(ids: &[i64], keys: &[Option<&[u8]>]) -> OwnedWcolBatch {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, ids.to_vec(), vec![1; ids.len()]);
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    let mut nulls = Vec::new();
    for key in keys {
        match key {
            Some(value) => {
                bytes.extend_from_slice(value);
                nulls.push(1);
            }
            None => nulls.push(0),
        }
        offsets.push(bytes.len() as u32);
    }
    batch.push_utf8_column(1, offsets, bytes, nulls);
    batch.set_row_count(ids.len()).unwrap();
    batch
}

fn ids_of(batch: &OwnedWcolBatch) -> Vec<i64> {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    match view.columns[0] {
        WcolColumn::I64 { values, .. } => values.to_vec(),
        _ => panic!("id"),
    }
}

fn reference_order<T: Ord>(keys: &[Option<T>], desc: bool, nulls_first: bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|&a, &b| match (&keys[a], &keys[b]) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (Some(x), Some(y)) => {
            let ord = x.cmp(y);
            if desc {
                ord.reverse()
            } else {
                ord
            }
        }
    });
    order
}

#[test]
fn integer_and_string_order_matches_a_reference_sort() {
    let ints = [
        Some(3i64),
        None,
        Some(-1),
        Some(3),
        Some(i64::MIN),
        None,
        Some(0),
    ];
    for desc in [false, true] {
        for nulls_first in [false, true] {
            let batch = i64_batch(&[0, 1, 2, 3, 4, 5, 6], &ints);
            let sorted = sort_rows(
                &[batch],
                &[RowSortKey {
                    column: 1,
                    descending: desc,
                    nulls_first,
                }],
            )
            .unwrap();
            let expect: Vec<i64> = reference_order(&ints, desc, nulls_first)
                .into_iter()
                .map(|row| row as i64)
                .collect();
            assert_eq!(
                ids_of(&sorted),
                expect,
                "desc={desc} nulls_first={nulls_first}"
            );
        }
    }
    let texts: Vec<Option<&[u8]>> = vec![
        Some(b"b"),
        Some("é".as_bytes()),
        None,
        Some(b""),
        Some(b"a"),
        Some("你好".as_bytes()),
        Some(b"b"),
    ];
    for desc in [false, true] {
        for nulls_first in [false, true] {
            let batch = utf8_batch(&[0, 1, 2, 3, 4, 5, 6], &texts);
            let sorted = sort_rows(
                &[batch],
                &[RowSortKey {
                    column: 1,
                    descending: desc,
                    nulls_first,
                }],
            )
            .unwrap();
            let expect: Vec<i64> = reference_order(&texts, desc, nulls_first)
                .into_iter()
                .map(|row| row as i64)
                .collect();
            assert_eq!(
                ids_of(&sorted),
                expect,
                "text desc={desc} nulls_first={nulls_first}"
            );
        }
    }
}

#[test]
fn dictionary_ids_sort_as_their_values_across_scopes() {
    let mut left = OwnedWcolBatch::empty();
    left.set_scope(1);
    left.push_i64_column(0, vec![0, 1], vec![1, 1]);
    left.push_dict_id_column(
        1,
        vec![0, 1],
        vec![1, 1],
        crate::stream::DictionaryValues {
            scope: 1,
            column: 1,
            entries: crate::stream::DictEntries::Bytes {
                offsets: Arc::from(vec![0, 1, 2]),
                bytes: Arc::from(b"ba".to_vec()),
            },
        },
    );
    left.set_row_count(2).unwrap();
    let mut right = OwnedWcolBatch::empty();
    right.set_scope(2);
    right.push_i64_column(0, vec![2], vec![1]);
    right.push_dict_id_column(
        1,
        vec![0],
        vec![1],
        crate::stream::DictionaryValues {
            scope: 2,
            column: 1,
            entries: crate::stream::DictEntries::Bytes {
                offsets: Arc::from(vec![0, 1]),
                bytes: Arc::from(b"a".to_vec()),
            },
        },
    );
    right.set_row_count(1).unwrap();
    let sorted = sort_rows(
        &[left, right],
        &[RowSortKey {
            column: 1,
            descending: false,
            nulls_first: false,
        }],
    )
    .unwrap();
    // values are "b", "a", "a" so ids 1 then 2 then 0 (stable between the two "a"?
    // left row 1 is "a" (id 1), right row is "a". Input order: row0 "b", row1 "a", row2 "a".
    assert_eq!(ids_of(&sorted), vec![1, 2, 0]);
}

#[test]
fn float_nan_and_signed_zero_follow_float_key() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![0, 1, 2, 3], vec![1; 4]);
    batch.push_f64_column(1, vec![-0.0, f64::NAN, 1.0, 0.0], vec![1, 1, 1, 1]);
    batch.set_row_count(4).unwrap();
    let sorted = sort_row_batches(
        &[batch],
        &[SortKey {
            column: 1,
            descending: false,
            nulls_first: false,
        }],
    )
    .unwrap();
    let got = ids_of(&sorted);
    assert_eq!(float_key(-0.0), float_key(0.0));
    assert_eq!(&got[..2], &[0, 3]);
    assert_eq!(got[3], 1);
}

#[test]
fn sort_budget_trips_before_the_rows_are_kept() {
    let batch = i64_batch(&[1, 2, 3, 4], &[Some(1), Some(2), Some(3), Some(4)]);
    let mut op = SortOp::with_budget(
        vec![RowSortKey {
            column: 1,
            descending: false,
            nulls_first: false,
        }],
        8,
    );
    let mut slot = Some(WcolStreamItem::from_row_batch(batch));
    let mut output = None;
    let mut written = 0usize;
    let err = op
        .poll(&mut OperatorIo {
            input: None,
            input_slot: Some(&mut slot),
            input_eof: false,
            output: &mut output,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut written,
        })
        .unwrap_err();
    assert!(err.contains(SORT_TOO_LARGE), "{err}");
}

#[test]
fn topk_matches_sort_then_limit_for_every_n() {
    let keys = [Some(5i64), None, Some(1), Some(5), Some(-2), Some(1), None];
    let batch = i64_batch(&[0, 1, 2, 3, 4, 5, 6], &keys);
    let sorted = sort_rows(
        std::slice::from_ref(&batch),
        &[RowSortKey {
            column: 1,
            descending: true,
            nulls_first: false,
        }],
    )
    .unwrap();
    let sorted_ids = ids_of(&sorted);
    for n in 0..=keys.len() {
        let top = TopKOp::take(
            vec![RowSortKey {
                column: 1,
                descending: true,
                nulls_first: false,
            }],
            n,
            std::slice::from_ref(&batch),
        )
        .unwrap();
        assert_eq!(ids_of(&top), sorted_ids[..n]);
    }
}
