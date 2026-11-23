//! Hash-partition row batches across multiple logical output ports with bounded buffering.

use crate::exec::operator::{StreamProgress, WcolStreamItem};
use crate::stream::OwnedWcolBatch;

use super::partitioning::HashPartitionPlan;
use super::typed_hash::{partition_for_row, ExchangeHashCache, WCOL_EXCHANGE_HASH_VERSION_DEFAULT};

/// Per-partition row and approximate byte totals (updated on emit).
#[derive(Clone, Debug, Default)]
pub struct HashPartitionStats {
    pub row_counts: Vec<u64>,
    pub byte_counts: Vec<u64>,
}

/// Bounded multi-output shuffle operator (one input stream, `plan.partitions` outputs).
pub struct HashPartitionOp {
    plan: HashPartitionPlan,
    max_rows_per_partition: usize,
    max_total_buffered_rows: usize,
    builders: Vec<OwnedWcolBatch>,
    row_staging: Vec<Vec<usize>>,
    buffered_rows: usize,
    stats: HashPartitionStats,
    input_done: bool,
    finished: bool,
    pending_emit: Option<usize>,
}

/// Multi-output extension for local tests and future graph scheduler wiring.
pub struct HashPartitionIo<'a, 'b> {
    pub input: Option<&'a WcolStreamItem>,
    pub input_slot: Option<&'b mut Option<WcolStreamItem>>,
    pub input_eof: bool,
    pub outputs: &'b mut [Option<WcolStreamItem>],
    /// When true, partition `p` must not emit (downstream backpressure).
    pub outputs_blocked: &'b [bool],
}

impl HashPartitionOp {
    pub fn new(plan: HashPartitionPlan) -> Result<Self, String> {
        Self::with_limits(plan, 4096, 16_384)
    }

    pub fn with_limits(
        plan: HashPartitionPlan,
        max_rows_per_partition: usize,
        max_total_buffered_rows: usize,
    ) -> Result<Self, String> {
        if max_rows_per_partition == 0 || max_total_buffered_rows == 0 {
            return Err("hash partition buffer limits must be > 0".to_string());
        }
        let parts = plan.partitions as usize;
        Ok(Self {
            plan,
            max_rows_per_partition,
            max_total_buffered_rows,
            builders: (0..parts).map(|_| OwnedWcolBatch::empty()).collect(),
            row_staging: (0..parts).map(|_| Vec::new()).collect(),
            buffered_rows: 0,
            stats: HashPartitionStats {
                row_counts: vec![0; parts],
                byte_counts: vec![0; parts],
            },
            input_done: false,
            finished: false,
            pending_emit: None,
        })
    }

    pub fn stats(&self) -> &HashPartitionStats {
        &self.stats
    }

    pub fn poll(&mut self, io: &mut HashPartitionIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.finished {
            return Ok(StreamProgress::Finished);
        }

        if let Some(part) = self.pending_emit {
            if io.outputs.get(part).is_none() {
                return Err("hash partition output slot missing".to_string());
            }
            if io.outputs_blocked.get(part).copied().unwrap_or(false) {
                return Ok(StreamProgress::Blocked);
            }
            if io.outputs[part].is_some() {
                return Ok(StreamProgress::Blocked);
            }
            let batch = std::mem::take(&mut self.builders[part]);
            let rows = batch.row_count();
            let bytes = estimate_batch_bytes(&batch);
            io.outputs[part] = Some(WcolStreamItem::from_row_batch(batch));
            self.stats.row_counts[part] += rows as u64;
            self.stats.byte_counts[part] += bytes;
            self.buffered_rows = self.buffered_rows.saturating_sub(rows);
            self.pending_emit = None;
            return Ok(StreamProgress::HaveOutput);
        }

        if self.schedule_flush(io)? {
            return Ok(StreamProgress::HaveOutput);
        }

        if self.buffered_rows > 0 && !self.can_emit_any(io) {
            return Ok(StreamProgress::Blocked);
        }

        if let Some(slot) = io.input_slot.as_mut() {
            if let Some(item) = slot.take() {
                self.partition_batch(item.into_batch())?;
                if self.schedule_flush(io)? {
                    return Ok(StreamProgress::HaveOutput);
                }
                if self.buffered_rows > 0 && !self.can_emit_any(io) {
                    return Ok(StreamProgress::Blocked);
                }
                return Ok(StreamProgress::NeedInput);
            }
        } else if io.input.is_some() {
            return Err("hash partition requires input_slot for zero-copy batch move".to_string());
        }

        if io.input_eof && !self.input_done {
            self.input_done = true;
            if self.schedule_flush(io)? {
                return Ok(StreamProgress::HaveOutput);
            }
        }

        if self.input_done && self.buffered_rows == 0 && self.pending_emit.is_none() {
            self.finished = true;
            return Ok(StreamProgress::Finished);
        }

        if self.input_done && self.buffered_rows > 0 {
            if self.schedule_flush(io)? {
                return Ok(StreamProgress::HaveOutput);
            }
            return Ok(StreamProgress::Blocked);
        }

        Ok(StreamProgress::NeedInput)
    }

    fn partition_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        if batch.is_partial_batch() {
            return Err("hash partition expects row batches".to_string());
        }
        let mask = batch.effective_selection_mask();
        let rows = batch.row_count();
        if rows == 0 {
            return Ok(());
        }
        let keys = &self.plan.keys;
        let partitions = self.plan.partitions;
        let version = self.plan.hash_version;

        for part in &mut self.row_staging {
            part.clear();
        }

        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        let cache = ExchangeHashCache::prepare(&view, keys, version)?;
        for row in 0..rows {
            if !mask.is_selected(row) {
                continue;
            }
            let p = partition_for_row(&view, row, keys, partitions, version, &cache)? as usize;
            self.row_staging[p].push(row);
        }

        // As in `split_owned_batch`: partitions get values, never a numeric dictionary.
        let expanded = batch.with_numeric_dicts_expanded()?;
        let source = expanded.as_ref().unwrap_or(&batch);
        for (p, indices) in self.row_staging.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            self.builders[p].append_rows_from(source, indices)?;
            self.buffered_rows += indices.len();
        }
        Ok(())
    }

    fn schedule_flush(&mut self, io: &mut HashPartitionIo<'_, '_>) -> Result<bool, String> {
        if self.pending_emit.is_some() {
            return Ok(false);
        }
        let force = self.buffered_rows > self.max_total_buffered_rows;
        if let Some(p) = self.pick_flush_partition(io, force) {
            self.pending_emit = Some(p);
            return Ok(true);
        }
        Ok(false)
    }

    fn pick_flush_partition(&self, io: &HashPartitionIo<'_, '_>, force_any: bool) -> Option<usize> {
        let mut candidate: Option<(usize, usize)> = None;
        for p in 0..self.builders.len() {
            if !self.partition_may_emit(p, io) {
                continue;
            }
            let rows = self.builders[p].row_count();
            if rows == 0 {
                continue;
            }
            let should_flush = self.input_done || force_any || rows >= self.max_rows_per_partition;
            if !should_flush {
                continue;
            }
            candidate = Some(match candidate {
                None => (p, rows),
                Some((_, best_rows)) if rows > best_rows => (p, rows),
                other => other.unwrap(),
            });
        }
        candidate.map(|(p, _)| p)
    }

    fn partition_may_emit(&self, p: usize, io: &HashPartitionIo<'_, '_>) -> bool {
        if io.outputs_blocked.get(p).copied().unwrap_or(false) {
            return false;
        }
        !io.outputs.get(p).map(|s| s.is_some()).unwrap_or(true)
    }

    fn can_emit_any(&self, io: &HashPartitionIo<'_, '_>) -> bool {
        self.builders
            .iter()
            .enumerate()
            .any(|(p, b)| b.row_count() > 0 && self.partition_may_emit(p, io))
    }

    /// Convenience for tests.
    pub fn for_keys(keys: Vec<u32>, partitions: u32) -> Result<Self, String> {
        let plan = HashPartitionPlan::new(keys, partitions, WCOL_EXCHANGE_HASH_VERSION_DEFAULT)?;
        Self::new(plan)
    }
}

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

fn estimate_batch_bytes(batch: &OwnedWcolBatch) -> u64 {
    let rows = batch.row_count() as u64;
    if rows == 0 {
        return 0;
    }
    rows.saturating_mul(16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::SelectionMask;

    fn drive_all(
        op: &mut HashPartitionOp,
        inputs: Vec<OwnedWcolBatch>,
        outputs: &mut [Option<WcolStreamItem>],
    ) -> Result<(), String> {
        let mut input_eof = false;
        let mut inputs = inputs.into_iter();
        let mut pending: Option<WcolStreamItem> = inputs.next().map(WcolStreamItem::from_row_batch);

        loop {
            let blocked = vec![false; outputs.len()];
            let mut io = HashPartitionIo {
                input: None,
                input_slot: Some(&mut pending),
                input_eof,
                outputs,
                outputs_blocked: &blocked,
            };
            match op.poll(&mut io)? {
                StreamProgress::HaveOutput => {
                    for slot in outputs.iter_mut() {
                        slot.take();
                    }
                }
                StreamProgress::NeedInput => {
                    if pending.is_none() && !input_eof {
                        pending = inputs.next().map(WcolStreamItem::from_row_batch);
                        if pending.is_none() {
                            input_eof = true;
                        }
                    }
                }
                StreamProgress::Finished => break,
                StreamProgress::Blocked => {
                    return Err("unexpected blocked in test harness".to_string());
                }
                _ => {}
            }
        }
        Ok(())
    }

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

    #[test]
    fn conserves_row_count_across_partitions() {
        let mut op = HashPartitionOp::for_keys(vec![0], 4).unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, (0..100).collect(), vec![1; 100]);
        batch.set_row_count(100).unwrap();
        let mut outs = vec![None; 4];
        drive_all(&mut op, vec![batch], &mut outs).unwrap();
        let total: u64 = op.stats().row_counts.iter().sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn deterministic_partition_for_same_key() {
        let mut a = OwnedWcolBatch::empty();
        a.push_i64_column(0, vec![42], vec![1]);
        a.set_row_count(1).unwrap();
        let mut b = OwnedWcolBatch::empty();
        b.push_i64_column(0, vec![42], vec![1]);
        b.set_row_count(1).unwrap();

        let mut op = HashPartitionOp::for_keys(vec![0], 4).unwrap();
        let mut outs = vec![None; 4];
        drive_all(&mut op, vec![a], &mut outs).unwrap();
        let p0 = op.stats().row_counts.iter().position(|&c| c > 0).unwrap();

        let mut op2 = HashPartitionOp::for_keys(vec![0], 4).unwrap();
        let mut outs2 = vec![None; 4];
        drive_all(&mut op2, vec![b], &mut outs2).unwrap();
        let p1 = op2.stats().row_counts.iter().position(|&c| c > 0).unwrap();
        assert_eq!(p0, p1);
    }

    #[test]
    fn respects_selection_mask() {
        let mut op = HashPartitionOp::for_keys(vec![0], 2).unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![1, 2, 3], vec![1; 3]);
        batch.set_row_count(3).unwrap();
        let mut mask = SelectionMask::with_rows(3);
        mask.set_selected(0, true);
        mask.set_selected(2, true);
        batch.set_selection_mask(mask);
        let mut outs = vec![None; 2];
        drive_all(&mut op, vec![batch], &mut outs).unwrap();
        assert_eq!(op.stats().row_counts.iter().sum::<u64>(), 2);
    }

    #[test]
    fn blocked_when_all_outputs_full() {
        let mut op = HashPartitionOp::with_limits(
            HashPartitionPlan::new(vec![0], 2, 1).unwrap(),
            1024,
            1024,
        )
        .unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![1, 2], vec![1; 2]);
        batch.set_row_count(2).unwrap();
        let mut pending = Some(WcolStreamItem::from_row_batch(batch));
        let mut outs = vec![Some(WcolStreamItem::from_row_batch(OwnedWcolBatch::empty())); 2];
        let blocked = vec![false; 2];
        let mut io = HashPartitionIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof: false,
            outputs: &mut outs,
            outputs_blocked: &blocked,
        };
        assert_eq!(op.poll(&mut io).unwrap(), StreamProgress::Blocked);
    }
}
