//! Finalize: partial-state WCOL → result row WCOL (ungrouped queries first).

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::ops::query::{
    finalize_groups, PartialKeyPart, PartialSnapshot, PartialStat, KIND_APPROX_DISTINCT,
    KIND_COUNT_DISTINCT,
};
use crate::stream::partials::{partial_snapshot_from_state, PartialStateWcol};
use crate::stream::OwnedWcolBatch;

pub struct FinalizeOp {
    partial: Option<PartialStateWcol>,
    finished: bool,
}

impl FinalizeOp {
    pub fn new() -> Self {
        Self {
            partial: None,
            finished: false,
        }
    }

    pub fn finalize_partial(state: &PartialStateWcol) -> Result<WcolStreamItem, String> {
        let snap = partial_snapshot_from_state(state)?;
        Ok(WcolStreamItem::from_row_batch(finalized_groups_batch(
            &snap,
        )?))
    }
}

/// One row per result group of a merged partial: group keys at columns `0..k`, then each
/// aggregate's SQL value at `k + i`. Rows are in group-key order, so the result does not
/// depend on how rows were split across nodes: a stable sort downstream breaks ties by key,
/// and a retried job returns the same rows.
///
/// Counts are `i64` and never null. Integer `SUM`/`MIN`/`MAX` are `i64`, or exact decimal
/// text when a `SUM` outgrows it; `AVG` and float aggregates are `f64`; a non-count aggregate
/// over no rows is null.
pub fn finalized_groups_batch(snap: &PartialSnapshot) -> Result<OwnedWcolBatch, String> {
    let mut groups = finalize_groups(snap)?;
    groups.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    // An empty partial still has a planned key width. Using 0 here renumbers aggregates
    // onto column 0 and a later sort (`keys + agg`) looks up a column that was never emitted.
    let keys = if groups.is_empty() {
        snap.key_len as usize
    } else {
        groups[0].key.len()
    };
    if groups.iter().any(|group| group.key.len() != keys) {
        return Err("partial groups have different key widths".to_string());
    }
    let mut batch = OwnedWcolBatch::empty();
    if groups.is_empty() {
        for index in 0..keys {
            let column = index as u32;
            let bit = 1u32 << index;
            if snap.key_string_mask & bit != 0 {
                batch.push_utf8_column(column, vec![0], Vec::new(), Vec::new());
            } else if snap.key_float_mask & bit != 0 {
                batch.push_f64_column(column, Vec::new(), Vec::new());
            } else {
                batch.push_i64_column(column, Vec::new(), Vec::new());
            }
        }
        for (index, agg) in snap.aggs.iter().enumerate() {
            let column = (keys + index) as u32;
            push_aggregate_column(&mut batch, column, agg.kind, &[])?;
        }
        batch.set_row_count(0).map_err(|e| e.to_string())?;
        return Ok(batch);
    }
    for index in 0..keys {
        let column = index as u32;
        let parts = groups.iter().map(|group| &group.key[index]);
        push_key_column(&mut batch, column, parts)?;
    }
    for (index, agg) in snap.aggs.iter().enumerate() {
        let column = (keys + index) as u32;
        let stats: Vec<&PartialStat> = groups.iter().map(|group| &group.stats[index]).collect();
        push_aggregate_column(&mut batch, column, agg.kind, &stats)?;
    }
    batch
        .set_row_count(groups.len())
        .map_err(|e| e.to_string())?;
    Ok(batch)
}

fn push_key_column<'a>(
    batch: &mut OwnedWcolBatch,
    column: u32,
    parts: impl Iterator<Item = &'a PartialKeyPart> + Clone,
) -> Result<(), String> {
    let kind = parts
        .clone()
        .find(|part| !matches!(part, PartialKeyPart::Null));
    match kind {
        None | Some(PartialKeyPart::Int(_)) => {
            let mut values = Vec::new();
            let mut valid = Vec::new();
            for part in parts {
                let (value, present) = match part {
                    PartialKeyPart::Null => (0, 0),
                    PartialKeyPart::Int(value) => (
                        i64::try_from(*value).map_err(|_| "group key exceeds i64")?,
                        1,
                    ),
                    _ => return Err("group key mixes types".to_string()),
                };
                values.push(value);
                valid.push(present);
            }
            batch.push_i64_column(column, values, valid);
        }
        Some(PartialKeyPart::Float(_)) => {
            let mut values = Vec::new();
            let mut valid = Vec::new();
            for part in parts {
                let (value, present) = match part {
                    PartialKeyPart::Null => (0.0, 0),
                    PartialKeyPart::Float(bits) => (f64::from_bits(*bits), 1),
                    _ => return Err("group key mixes types".to_string()),
                };
                values.push(value);
                valid.push(present);
            }
            batch.push_f64_column(column, values, valid);
        }
        Some(PartialKeyPart::Bytes(_)) => {
            let mut offsets = vec![0u32];
            let mut bytes = Vec::new();
            let mut valid = Vec::new();
            for part in parts {
                match part {
                    PartialKeyPart::Null => valid.push(0),
                    PartialKeyPart::Bytes(value) => {
                        bytes.extend_from_slice(value);
                        valid.push(1);
                    }
                    _ => return Err("group key mixes types".to_string()),
                }
                offsets.push(u32::try_from(bytes.len()).map_err(|_| "group keys exceed 4 GiB")?);
            }
            batch.push_utf8_column(column, offsets, bytes, valid);
        }
        Some(PartialKeyPart::Null) => unreachable!("filtered above"),
    }
    Ok(())
}

fn push_aggregate_column(
    batch: &mut OwnedWcolBatch,
    column: u32,
    kind: u8,
    stats: &[&PartialStat],
) -> Result<(), String> {
    const COUNT_STAR: u8 = 0;
    const COUNT: u8 = 1;
    const SUM: u8 = 2;
    const AVG: u8 = 3;
    const MIN: u8 = 4;
    const MAX: u8 = 5;
    let count_kind = matches!(
        kind,
        COUNT_STAR | COUNT | KIND_APPROX_DISTINCT | KIND_COUNT_DISTINCT
    );
    // The integer flag is the column's type; a group with no rows may not carry it.
    let integer = stats
        .iter()
        .find(|stat| stat.count > 0)
        .is_none_or(|stat| stat.flags & 1 != 0);
    if count_kind {
        let values = stats
            .iter()
            .map(|stat| i64::try_from(stat.count).map_err(|_| "count exceeds i64".to_string()))
            .collect::<Result<_, _>>()?;
        batch.push_i64_column(column, values, vec![1; stats.len()]);
        return Ok(());
    }
    let valid: Vec<u8> = stats.iter().map(|stat| u8::from(stat.count > 0)).collect();
    if integer && matches!(kind, SUM | MIN | MAX) {
        let wide: Vec<i128> = stats
            .iter()
            .map(|stat| match (stat.count, kind) {
                (0, _) => 0,
                (_, SUM) => stat.sum_i,
                (_, MIN) => stat.min_i,
                _ => stat.max_i,
            })
            .collect();
        if wide.iter().all(|value| i64::try_from(*value).is_ok()) {
            batch.push_i64_column(column, wide.iter().map(|v| *v as i64).collect(), valid);
        } else {
            // A SUM past i64 (DuckDB's HUGEINT): exact decimal text, never a rounded float.
            let mut offsets = vec![0u32];
            let mut bytes = Vec::new();
            for (value, present) in wide.iter().zip(&valid) {
                if *present != 0 {
                    bytes.extend_from_slice(value.to_string().as_bytes());
                }
                offsets
                    .push(u32::try_from(bytes.len()).map_err(|_| "aggregate text exceeds 4 GiB")?);
            }
            batch.push_utf8_column(column, offsets, bytes, valid);
        }
        return Ok(());
    }
    let values = stats
        .iter()
        .map(|stat| {
            if stat.count == 0 {
                return Ok(0.0);
            }
            Ok(match kind {
                SUM => stat.sum_f,
                AVG if integer => stat.sum_i as f64 / stat.count as f64,
                AVG => stat.sum_f / stat.count as f64,
                MIN => stat.min_f,
                MAX => stat.max_f,
                other => return Err(format!("unknown aggregate kind {other}")),
            })
        })
        .collect::<Result<_, String>>()?;
    batch.push_f64_column(column, values, valid);
    Ok(())
}

impl Default for FinalizeOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Operator for FinalizeOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.finished {
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        if let Some(input) = io.input_item() {
            let partial = input.try_partial_state()?;
            self.partial = Some(partial);
            return Ok(StreamProgress::NeedInput);
        }
        if io.input_eof {
            let partial = self
                .partial
                .take()
                .ok_or_else(|| "finalize received no partial".to_string())?;
            *io.output = Some(Self::finalize_partial(&partial)?);
            self.finished = true;
            return Ok(StreamProgress::HaveOutput);
        }
        Ok(StreamProgress::NeedInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::query::{PartialAggSpec, PartialGroup};

    #[test]
    fn a_sum_past_i64_is_exact_text() {
        let stat = |sum: i128| PartialStat {
            count: 2,
            flags: 1,
            sum_i: sum,
            sum_f: sum as f64,
            min_i: 0,
            max_i: 0,
            min_f: 0.0,
            max_f: 0.0,
            sketch: None,
        };
        let snap = PartialSnapshot {
            grouped: true,
            order_groups_by_count: false,
            limit: 0,
            offset: 0,
            passed_rows: 4,
            aggs: vec![PartialAggSpec { kind: 2, column: 0 }],
            groups: vec![
                PartialGroup {
                    key: vec![PartialKeyPart::Int(1)],
                    stats: vec![stat(i128::from(i64::MAX) + 1)],
                },
                PartialGroup {
                    key: vec![PartialKeyPart::Int(2)],
                    stats: vec![stat(5)],
                },
            ],
            key_len: 1,
            key_float_mask: 0,
            key_string_mask: 0,
        };
        let batch = finalized_groups_batch(&snap).unwrap();
        let mut columns = Vec::new();
        let view = batch.view_with(&mut columns);
        let crate::stream::WcolColumn::Utf8 { offsets, bytes, .. } = view.columns[1] else {
            panic!("expected text");
        };
        let text = |row: usize| {
            std::str::from_utf8(&bytes[offsets[row] as usize..offsets[row + 1] as usize]).unwrap()
        };
        assert_eq!((text(0), text(1)), ("9223372036854775808", "5"));
    }

    #[test]
    fn empty_groups_keep_the_planned_sort_column() {
        let snap = PartialSnapshot {
            grouped: true,
            order_groups_by_count: true,
            limit: 10,
            offset: 0,
            passed_rows: 0,
            aggs: vec![PartialAggSpec { kind: 0, column: 0 }],
            groups: Vec::new(),
            key_len: 1,
            key_float_mask: 0,
            key_string_mask: 1,
        };
        let batch = finalized_groups_batch(&snap).unwrap();
        let sorted = crate::ops::sort::sort_rows(
            &[batch],
            &[crate::ops::sort::RowSortKey {
                column: 1,
                descending: true,
                nulls_first: false,
            }],
        )
        .unwrap();
        assert_eq!(sorted.row_count(), 0);
    }
}
