//! Range-partition row batches by sampled global sort boundaries.

use std::cmp::Ordering;

use crate::ir::Scalar;
use crate::ops::query::float_key::float_key;
use crate::ops::sort::cmp_sort_presence;
use crate::program::types::{SortKey, SortTarget};
use crate::stream::{DictEntries, OwnedWcolBatch, WcolColumn};

/// Range shuffle parameters: composite sort keys and interior split points.
#[derive(Clone, Debug, PartialEq)]
pub struct RangePartitionPlan {
    pub keys: Vec<SortKey>,
    pub partitions: u32,
    /// One component per sort key. `None` is a NULL boundary.
    pub splits: Vec<Vec<Option<Scalar>>>,
}

impl RangePartitionPlan {
    pub fn new(
        keys: Vec<SortKey>,
        partitions: u32,
        splits: Vec<Vec<Option<Scalar>>>,
    ) -> Result<Self, String> {
        if keys.is_empty() {
            return Err("range partition requires sort keys".to_string());
        }
        if partitions < 2 {
            return Err("range partition requires at least two partitions".to_string());
        }
        let expected = (partitions - 1) as usize;
        if splits.len() != expected {
            return Err(format!(
                "range partition expected {expected} split keys, got {}",
                splits.len()
            ));
        }
        for (index, split) in splits.iter().enumerate() {
            if split.len() != keys.len() {
                return Err(format!(
                    "range partition split {index} has {} components, sort keys {}",
                    split.len(),
                    keys.len()
                ));
            }
        }
        for index in 1..splits.len() {
            if compare_composite(&keys, &splits[index - 1], &splits[index]) != Ordering::Less {
                return Err("range partition splits are not strictly increasing".to_string());
            }
        }
        Ok(Self {
            keys,
            partitions,
            splits,
        })
    }
}

/// One batch per partition. Empty partitions are empty batches.
pub fn split_owned_batch(
    plan: &RangePartitionPlan,
    batch: &OwnedWcolBatch,
) -> Result<Vec<OwnedWcolBatch>, String> {
    let parts = plan.partitions as usize;
    let mut staging = vec![Vec::new(); parts];
    let rows = batch.row_count();
    if rows > 0 {
        let mask = batch.effective_selection_mask();
        for row in 0..rows {
            if !mask.is_selected(row) {
                continue;
            }
            let part = partition_for_row(batch, row, plan)? as usize;
            staging[part].push(row);
        }
    }
    let mut out = Vec::with_capacity(parts);
    for indices in staging {
        if indices.is_empty() {
            out.push(OwnedWcolBatch::empty());
            continue;
        }
        let mut part = OwnedWcolBatch::empty();
        part.append_rows_from(batch, &indices)?;
        out.push(part);
    }
    Ok(out)
}

fn partition_for_row(
    batch: &OwnedWcolBatch,
    row: usize,
    plan: &RangePartitionPlan,
) -> Result<u32, String> {
    let row_key = row_composite(batch, row, &plan.keys)?;
    for (index, split) in plan.splits.iter().enumerate() {
        if compare_composite(&plan.keys, &row_key, split) == Ordering::Less {
            return Ok(index as u32);
        }
    }
    Ok(plan.splits.len() as u32)
}

fn row_composite(
    batch: &OwnedWcolBatch,
    row: usize,
    keys: &[SortKey],
) -> Result<Vec<Option<Scalar>>, String> {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let column = match key.target {
            SortTarget::Column(column) => column,
            SortTarget::Aggregate(_) => {
                return Err("range partition sort keys must be scan columns".to_string());
            }
        };
        let col = view
            .columns
            .iter()
            .find(|col| column_id(col) == column)
            .ok_or_else(|| format!("range partition column {column} was not decoded"))?;
        out.push(cell_scalar(col, row)?);
    }
    Ok(out)
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

fn cell_scalar(col: &WcolColumn<'_>, row: usize) -> Result<Option<Scalar>, String> {
    let present = match col {
        WcolColumn::I64 { nulls, .. }
        | WcolColumn::F64 { nulls, .. }
        | WcolColumn::DictId { nulls, .. }
        | WcolColumn::Utf8 { nulls, .. } => nulls[row] != 0,
    };
    if !present {
        return Ok(None);
    }
    Ok(Some(match col {
        WcolColumn::I64 { values, .. } => Scalar::Int(values[row]),
        WcolColumn::F64 { values, .. } => Scalar::Number(values[row]),
        WcolColumn::Utf8 { offsets, bytes, .. } => Scalar::Text(
            String::from_utf8_lossy(&bytes[offsets[row] as usize..offsets[row + 1] as usize])
                .into_owned(),
        ),
        WcolColumn::DictId {
            ids, dictionary, ..
        } => {
            let id = ids[row] as usize;
            match &dictionary.entries {
                DictEntries::Int(entries) => {
                    Scalar::Int(*entries.get(id).ok_or("dictionary index out of range")?)
                }
                DictEntries::Float(entries) => {
                    Scalar::Number(*entries.get(id).ok_or("dictionary index out of range")?)
                }
                DictEntries::Bytes { offsets, bytes } => {
                    if id + 1 >= offsets.len() {
                        return Err("dictionary index out of range".to_string());
                    }
                    let start = offsets[id] as usize;
                    let end = offsets[id + 1] as usize;
                    Scalar::Text(String::from_utf8_lossy(&bytes[start..end]).into_owned())
                }
            }
        }
    }))
}

fn compare_composite(
    keys: &[SortKey],
    left: &[Option<Scalar>],
    right: &[Option<Scalar>],
) -> Ordering {
    for (index, key) in keys.iter().enumerate() {
        let ordering = cmp_sort_presence(
            left.get(index).and_then(|value| value.as_ref()).is_some(),
            right.get(index).and_then(|value| value.as_ref()).is_some(),
            key.nulls_first,
            key.descending,
            || {
                scalar_cmp(
                    left[index].as_ref().expect("present"),
                    right[index].as_ref().expect("present"),
                )
            },
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::types::{SortKey, SortTarget};

    fn key(descending: bool, nulls_first: bool) -> SortKey {
        SortKey {
            target: SortTarget::Column(0),
            descending,
            nulls_first,
        }
    }

    fn i64_batch(values: &[Option<i64>]) -> OwnedWcolBatch {
        let mut batch = OwnedWcolBatch::empty();
        let mut data = Vec::new();
        let mut nulls = Vec::new();
        for value in values {
            match value {
                Some(v) => {
                    data.push(*v);
                    nulls.push(1);
                }
                None => {
                    data.push(0);
                    nulls.push(0);
                }
            }
        }
        batch.push_i64_column(0, data, nulls);
        batch.set_row_count(values.len()).unwrap();
        batch
    }

    #[test]
    fn descending_nulls_last_does_not_pull_nulls_forward() {
        let plan =
            RangePartitionPlan::new(vec![key(true, false)], 2, vec![vec![Some(Scalar::Int(5))]])
                .unwrap();
        let batch = i64_batch(&[Some(10), Some(1), None]);
        let parts = split_owned_batch(&plan, &batch).unwrap();
        assert_eq!(parts[0].row_count(), 1);
        assert_eq!(parts[1].row_count(), 2);
        let mut cols = Vec::new();
        let high = parts[0].view_with(&mut cols);
        match &high.columns[0] {
            WcolColumn::I64 { values, .. } => assert_eq!(values, &[10]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn rejects_split_arity_and_order() {
        let err = RangePartitionPlan::new(
            vec![key(false, false), key(false, false)],
            2,
            vec![vec![Some(Scalar::Int(1))]],
        )
        .unwrap_err();
        assert!(err.contains("components"));
        let err = RangePartitionPlan::new(
            vec![key(false, false)],
            3,
            vec![vec![Some(Scalar::Int(5))], vec![Some(Scalar::Int(5))]],
        )
        .unwrap_err();
        assert!(err.contains("strictly increasing"));
    }
}

#[cfg(test)]
#[path = "range_sort_order.rs"]
mod range_sort_order;

fn scalar_cmp(left: &Scalar, right: &Scalar) -> Ordering {
    match (left, right) {
        (Scalar::Int(a), Scalar::Int(b)) => a.cmp(b),
        (Scalar::Number(a), Scalar::Number(b)) => float_key(*a).cmp(&float_key(*b)),
        (Scalar::Text(a), Scalar::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        _ => Ordering::Equal,
    }
}
