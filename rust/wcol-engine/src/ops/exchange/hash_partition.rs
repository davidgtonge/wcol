//! Hash-partition one row batch across the outputs of a hash exchange.

use crate::stream::OwnedWcolBatch;

use super::partitioning::HashPartitionPlan;
use super::typed_hash::{partition_for_row, ExchangeHashCache};

/// One batch per partition, in partition order. Empty partitions are empty batches.
/// Every selected row is copied to exactly one partition.
pub fn split_owned_batch(
    plan: &HashPartitionPlan,
    batch: &OwnedWcolBatch,
) -> Result<Vec<OwnedWcolBatch>, String> {
    let parts = plan.partitions as usize;
    let mut staging = vec![Vec::new(); parts];
    let rows = batch.row_count();
    if rows > 0 {
        let mask = batch.effective_selection_mask();
        let mut columns = Vec::new();
        let view = batch.view_with(&mut columns);
        let cache = ExchangeHashCache::prepare(&view, &plan.keys, plan.hash_version)?;
        for row in 0..rows {
            if !mask.is_selected(row) {
                continue;
            }
            let part = partition_for_row(
                &view,
                row,
                &plan.keys,
                plan.partitions,
                plan.hash_version,
                &cache,
            )? as usize;
            staging[part].push(row);
        }
    }
    // Rows are chosen from `batch` (its selection and dictionary ids); values come from
    // the expanded copy, so no partition receives a numeric dictionary.
    let expanded = batch.with_numeric_dicts_expanded()?;
    let source = expanded.as_ref().unwrap_or(batch);
    let mut out = Vec::with_capacity(parts);
    for indices in staging {
        if indices.is_empty() {
            out.push(OwnedWcolBatch::empty());
            continue;
        }
        let mut part = OwnedWcolBatch::empty();
        part.append_rows_from(source, &indices)?;
        out.push(part);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::SelectionMask;

    #[test]
    fn partitions_receive_numeric_dictionary_values_not_the_dictionary() {
        use crate::stream::{DictEntries, DictionaryValues, WcolColumn};
        use std::sync::Arc;
        // More entries than the batch has rows: cheaper to send the values.
        let dict = DictionaryValues {
            scope: 3,
            column: 0,
            entries: DictEntries::Int(Arc::new((1..=9).map(|n| n * 100).collect())),
        };
        let mut batch = OwnedWcolBatch::empty();
        batch.push_dict_id_column(0, vec![2, 0, 7, 1], vec![1, 1, 0, 1], dict);
        batch.set_row_count(4).unwrap();
        let plan = HashPartitionPlan::new(vec![0], 4, 1).unwrap();
        let mut rows = Vec::new();
        for part in split_owned_batch(&plan, &batch).unwrap() {
            let mut cols = Vec::new();
            let view = part.view_with(&mut cols);
            for col in view.columns {
                let WcolColumn::I64 { values, nulls, .. } = col else {
                    panic!("partition kept a dictionary column");
                };
                rows.extend(values.iter().zip(nulls.iter()).map(|(v, n)| (*v, *n)));
            }
        }
        rows.sort();
        // The null row keeps its null and expands to zero, whatever its id.
        assert_eq!(rows, vec![(0, 0), (100, 1), (200, 1), (300, 1)]);
    }

    #[test]
    fn a_small_numeric_dictionary_stays_a_dictionary() {
        use crate::stream::{DictEntries, DictionaryValues, WcolColumn};
        use std::sync::Arc;
        let dict = DictionaryValues {
            scope: 3,
            column: 0,
            entries: DictEntries::Int(Arc::new(vec![100, 200])),
        };
        let mut batch = OwnedWcolBatch::empty();
        batch.push_dict_id_column(0, vec![1, 0, 1, 1], vec![1; 4], dict);
        batch.set_row_count(4).unwrap();
        let plan = HashPartitionPlan::new(vec![0], 4, 1).unwrap();
        for part in split_owned_batch(&plan, &batch).unwrap() {
            let mut cols = Vec::new();
            let view = part.view_with(&mut cols);
            assert!(view
                .columns
                .iter()
                .all(|col| matches!(col, WcolColumn::DictId { .. })));
        }
    }

    fn i64_keys(keys: Vec<i64>) -> OwnedWcolBatch {
        let rows = keys.len();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, keys, vec![1; rows]);
        batch.set_row_count(rows).unwrap();
        batch
    }

    fn row_counts(plan: &HashPartitionPlan, batch: &OwnedWcolBatch) -> Vec<usize> {
        split_owned_batch(plan, batch)
            .unwrap()
            .iter()
            .map(OwnedWcolBatch::row_count)
            .collect()
    }

    #[test]
    fn conserves_row_count_across_partitions() {
        let plan = HashPartitionPlan::new(vec![0], 4, 1).unwrap();
        let counts = row_counts(&plan, &i64_keys((0..100).collect()));
        assert_eq!(counts.len(), 4);
        assert_eq!(counts.iter().sum::<usize>(), 100);
    }

    #[test]
    fn same_key_goes_to_the_same_partition() {
        let plan = HashPartitionPlan::new(vec![0], 4, 1).unwrap();
        let once = row_counts(&plan, &i64_keys(vec![42]));
        let thrice = row_counts(&plan, &i64_keys(vec![42, 42, 42]));
        let part = once.iter().position(|&rows| rows == 1).unwrap();
        assert_eq!(thrice[part], 3);
    }

    #[test]
    fn respects_selection_mask() {
        let mut batch = i64_keys(vec![1, 2, 3]);
        let mut mask = SelectionMask::with_rows(3);
        mask.set_selected(0, true);
        mask.set_selected(2, true);
        batch.set_selection_mask(mask);
        let plan = HashPartitionPlan::new(vec![0], 2, 1).unwrap();
        assert_eq!(row_counts(&plan, &batch).iter().sum::<usize>(), 2);
    }
}
