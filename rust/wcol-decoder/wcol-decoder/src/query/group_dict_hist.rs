//! Dense dict-id histogram for `GROUP BY` one dictionary column + `COUNT(*)` / `SUM(f64)`.

use crate::constants::{AGG_KIND_COUNT_STAR, AGG_KIND_SUM, FLAG_DICT, TYPE_F64};
use crate::query::mask::{is_valid, mask_is_full};
use crate::query::scan::for_each_value;
use crate::types::{Column, ColumnData, GroupAgg, Plan, Runtime};
use crate::decode::dict_index_at;

pub(crate) const GROUP_HIST_MAGIC: &[u8; 8] = b"WCDH0001";
pub(crate) const GROUP_HIST_MAGIC_SUM: &[u8; 8] = b"WCDH0002";

pub(crate) fn group_hist_max_dict_len() -> usize {
    std::env::var("WCOL_GROUP_DICT_HIST_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000)
}

pub(crate) fn group_hist_disabled() -> bool {
    std::env::var("WCOL_GROUP_DICT_HIST")
        .map(|v| v == "0")
        .unwrap_or(false)
}

fn hist_dict_len_for_key(runtime: &Runtime, key_col_id: u32, max_dict: usize) -> Option<usize> {
    let col = runtime.schema.get(key_col_id as usize)?;
    if (col.flags & FLAG_DICT) == 0 {
        return None;
    }
    let dict = runtime.dicts.get(&col.dict_id)?;
    let dict_len = dict.len();
    if dict_len == 0 || dict_len > max_dict {
        return None;
    }
    Some(dict_len)
}

/// Enable dense histogram aggregation when the query shape allows it.
pub(crate) fn try_enable_group_dict_histogram(plan: &mut Plan, runtime: &Runtime) {
    plan.group_dict_hist_dict_len = 0;
    plan.group_dict_hist_counts = None;
    plan.group_dict_hist_sums = None;

    if group_hist_disabled() {
        return;
    }
    let max_dict = group_hist_max_dict_len();
    let gb = match &plan.group_by {
        Some(g) if g.keys.len() == 1 => g,
        _ => return,
    };
    if plan.group_emit_raw {
        return;
    }
    let dict_len = match hist_dict_len_for_key(runtime, gb.keys[0], max_dict) {
        Some(n) => n,
        None => return,
    };
    let buckets = dict_len.saturating_add(1);

    let count_only =
        plan.group_aggs.len() == 1 && plan.group_aggs[0].kind == AGG_KIND_COUNT_STAR;
    let sum_only = plan.group_aggs.len() == 2
        && plan.group_aggs.iter().any(|a| a.kind == AGG_KIND_COUNT_STAR)
        && plan
            .group_aggs
            .iter()
            .any(|a| a.kind == AGG_KIND_SUM && a.col_id != gb.keys[0]);

    if count_only {
        plan.group_dict_hist_dict_len = dict_len as u32;
        plan.group_dict_hist_counts = Some(vec![0u32; buckets]);
        return;
    }

    if !sum_only {
        return;
    }
    let sum_agg = plan
        .group_aggs
        .iter()
        .find(|a| a.kind == AGG_KIND_SUM)
        .expect("sum_only");
    let sum_col = match runtime.schema.get(sum_agg.col_id as usize) {
        Some(c) => c,
        None => return,
    };
    if (sum_col.flags & FLAG_DICT) != 0 || sum_col.logical_type != TYPE_F64 {
        return;
    }
    plan.group_dict_hist_dict_len = dict_len as u32;
    plan.group_dict_hist_counts = Some(vec![0u32; buckets]);
    plan.group_dict_hist_sums = Some(vec![0.0f64; buckets]);
}

pub(crate) fn plan_uses_group_dict_histogram(plan: &Plan) -> bool {
    plan.group_dict_hist_dict_len > 0
}

pub(crate) fn is_group_hist_partial(bytes: &[u8]) -> bool {
    bytes.len() >= 12
        && (bytes.starts_with(GROUP_HIST_MAGIC) || bytes.starts_with(GROUP_HIST_MAGIC_SUM))
}

pub(crate) fn encode_group_hist_partial(
    dict_len: u32,
    counts: &[u32],
    sums: Option<&[f64]>,
) -> Vec<u8> {
    let n = dict_len as usize;
    let with_sums = sums.is_some();
    let magic = if with_sums {
        GROUP_HIST_MAGIC_SUM
    } else {
        GROUP_HIST_MAGIC
    };
    let mut out = Vec::with_capacity(12 + n * 4 + if with_sums { n * 8 } else { 0 });
    out.extend_from_slice(magic);
    out.extend_from_slice(&dict_len.to_le_bytes());
    for &c in counts.iter().take(n) {
        out.extend_from_slice(&c.to_le_bytes());
    }
    if let Some(sums) = sums {
        for &s in sums.iter().take(n) {
            out.extend_from_slice(&s.to_le_bytes());
        }
    }
    out
}

pub(crate) fn decode_group_hist_partial(
    bytes: &[u8],
) -> Option<(u32, Vec<u32>, Option<Vec<f64>>)> {
    if bytes.len() < 12 {
        return None;
    }
    let with_sums = bytes.starts_with(GROUP_HIST_MAGIC_SUM);
    if !bytes.starts_with(GROUP_HIST_MAGIC) && !with_sums {
        return None;
    }
    let dict_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    let n = dict_len as usize;
    let need = 12 + n * 4 + if with_sums { n * 8 } else { 0 };
    if bytes.len() < need {
        return None;
    }
    let mut counts = Vec::with_capacity(n);
    for chunk in bytes[12..12 + n * 4].chunks_exact(4) {
        counts.push(u32::from_le_bytes(chunk.try_into().ok()?));
    }
    let sums = if with_sums {
        let mut out = Vec::with_capacity(n);
        for chunk in bytes[12 + n * 4..need].chunks_exact(8) {
            out.push(f64::from_le_bytes(chunk.try_into().ok()?));
        }
        Some(out)
    } else {
        None
    };
    Some((dict_len, counts, sums))
}

pub(crate) fn hist_count_dict_column(
    counts: &mut [u32],
    col: &Column,
    data: &ColumnData,
    mask: &[u32],
    rows: usize,
    key_nulls: &[Option<&[u8]>],
) {
    if key_nulls.iter().any(|n| n.is_some()) {
        hist_count_dict_column_masked(counts, col, data, mask, rows, key_nulls);
        return;
    }
    let mut bump = |id: u32| {
        let idx = id as usize;
        if idx < counts.len() {
            counts[idx] = counts[idx].saturating_add(1);
        }
    };
    if mask_is_full(mask, rows) {
        match data {
            ColumnData::U8(values) => {
                for &v in values.iter().take(rows) {
                    bump(v as u32);
                }
            }
            ColumnData::U16(values) => {
                for &v in values.iter().take(rows) {
                    bump(v as u32);
                }
            }
            ColumnData::U32(values) => {
                for &v in values.iter().take(rows) {
                    bump(v);
                }
            }
            _ => hist_count_dict_column_masked(counts, col, data, mask, rows, key_nulls),
        }
    } else {
        match data {
            ColumnData::U8(values) => {
                for_each_value(values, mask, rows, |v| bump(v as u32));
            }
            ColumnData::U16(values) => {
                for_each_value(values, mask, rows, |v| bump(v as u32));
            }
            ColumnData::U32(values) => for_each_value(values, mask, rows, bump),
            _ => hist_count_dict_column_masked(counts, col, data, mask, rows, key_nulls),
        }
    }
}

fn hist_count_dict_column_masked(
    counts: &mut [u32],
    col: &Column,
    data: &ColumnData,
    mask: &[u32],
    rows: usize,
    key_nulls: &[Option<&[u8]>],
) {
    let row_valid = |row: usize| -> bool {
        key_nulls
            .iter()
            .all(|nulls| nulls.map(|n| is_valid(n, row)).unwrap_or(true))
    };
    let mut bump_row = |row: usize| {
        if !row_valid(row) {
            return;
        }
        if let Some(id) = dict_index_at(data, row) {
            if id < counts.len() {
                counts[id] = counts[id].saturating_add(1);
            }
        }
    };
    if mask_is_full(mask, rows) {
        for row in 0..rows {
            bump_row(row);
        }
    } else {
        for row in crate::query::mask::iter_mask(mask, rows) {
            bump_row(row);
        }
    }
    let _ = col;
}

pub(crate) fn merge_group_hist_counts(target: &mut [u32], source: &[u32]) {
    let n = target.len().min(source.len());
    for i in 0..n {
        target[i] = target[i].saturating_add(source[i]);
    }
}

pub(crate) fn merge_group_hist_sums(target: &mut [f64], source: &[f64]) {
    let n = target.len().min(source.len());
    for i in 0..n {
        target[i] += source[i];
    }
}

pub(crate) fn hist_sum_f64_dict_key(
    sums: &mut [f64],
    key_data: &ColumnData,
    val_data: &ColumnData,
    mask: &[u32],
    rows: usize,
    key_nulls: &[Option<&[u8]>],
) {
    let row_valid = |row: usize| -> bool {
        key_nulls
            .iter()
            .all(|nulls| nulls.map(|n| is_valid(n, row)).unwrap_or(true))
    };
    let mut bump = |row: usize| {
        if !row_valid(row) {
            return;
        }
        let Some(id) = dict_index_at(key_data, row) else {
            return;
        };
        let idx = id as usize;
        if idx >= sums.len() {
            return;
        }
        let v = match val_data {
            ColumnData::F64(values) => values[row],
            _ => return,
        };
        sums[idx] += v;
    };
    if mask_is_full(mask, rows) {
        for row in 0..rows {
            bump(row);
        }
    } else {
        for row in crate::query::mask::iter_mask(mask, rows) {
            bump(row);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CountDescIdAsc {
    count: u32,
    id: u32,
}

impl Ord for CountDescIdAsc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count
            .cmp(&other.count)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for CountDescIdAsc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct SumDescIdAsc {
    sum: f64,
    id: u32,
}

impl Ord for SumDescIdAsc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sum
            .total_cmp(&other.sum)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for SumDescIdAsc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SumDescIdAsc {
    fn eq(&self, other: &Self) -> bool {
        self.sum == other.sum && self.id == other.id
    }
}

impl Eq for SumDescIdAsc {}

/// Keep the best `offset + limit` groups by sum (desc), id (asc).
fn hist_topk_by_sum(sums: &[f64], dict_len: usize, offset: u32, limit: u32) -> Vec<(u32, f64)> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let window = (offset as usize).saturating_add(limit as usize);
    if window == 0 {
        return Vec::new();
    }

    let mut heap: BinaryHeap<Reverse<SumDescIdAsc>> = BinaryHeap::new();
    for (id, &sum) in sums.iter().take(dict_len).enumerate() {
        if sum == 0.0 {
            continue;
        }
        let candidate = SumDescIdAsc {
            sum,
            id: id as u32,
        };
        if heap.len() < window {
            heap.push(Reverse(candidate));
            continue;
        }
        let Some(Reverse(weakest)) = heap.peek() else {
            continue;
        };
        if candidate > *weakest {
            heap.pop();
            heap.push(Reverse(candidate));
        }
    }

    let mut entries: Vec<(u32, f64)> = heap
        .into_iter()
        .map(|Reverse(SumDescIdAsc { sum, id })| (id, sum))
        .collect();
    entries.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect()
}

/// Keep the best `offset + limit` groups by count (desc), id (asc); same order as full sort.
fn hist_topk_by_count(
    counts: &[u32],
    dict_len: usize,
    offset: u32,
    limit: u32,
) -> Vec<(u32, u32)> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let window = (offset as usize).saturating_add(limit as usize);
    if window == 0 {
        return Vec::new();
    }

    let mut heap: BinaryHeap<Reverse<CountDescIdAsc>> = BinaryHeap::new();
    for (id, &count) in counts.iter().take(dict_len).enumerate() {
        if count == 0 {
            continue;
        }
        let candidate = CountDescIdAsc {
            count,
            id: id as u32,
        };
        if heap.len() < window {
            heap.push(Reverse(candidate));
            continue;
        }
        let Some(Reverse(weakest)) = heap.peek() else {
            continue;
        };
        if candidate > *weakest {
            heap.pop();
            heap.push(Reverse(candidate));
        }
    }

    let mut entries: Vec<(u32, u32)> = heap
        .into_iter()
        .map(|Reverse(CountDescIdAsc { count, id })| (id, count))
        .collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect()
}

fn hist_entries_full_sort(
    counts: &[u32],
    dict_len: usize,
    order_by_count: bool,
) -> Vec<(u32, u32)> {
    let mut entries: Vec<(u32, u32)> = counts
        .iter()
        .take(dict_len)
        .enumerate()
        .filter_map(|(id, &c)| (c > 0).then_some((id as u32, c)))
        .collect();
    if order_by_count {
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    } else {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    }
    entries
}

pub(crate) fn group_hist_to_records(
    counts: &[u32],
    sums: Option<&[f64]>,
    dict_len: u32,
    group_aggs: &[GroupAgg],
    order_by_count: bool,
    limit: u32,
    offset: u32,
) -> Vec<u8> {
    let agg_count = group_aggs.len();
    let agg_record_size = 8 + 8 + 8 + 4 + 4;
    let record_size = 16 + agg_count * agg_record_size;
    let n = dict_len as usize;
    let use_sum_order = order_by_count && limit > 0 && sums.is_some();

    let mut out = Vec::new();

    if use_sum_order {
        let sums = sums.expect("use_sum_order");
        let selected = hist_topk_by_sum(sums, n, offset, limit);
        out.reserve(selected.len().saturating_mul(record_size));
        for (id, sum) in selected {
            let count = counts.get(id as usize).copied().unwrap_or(0);
            out.extend_from_slice(&(id as u64).to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
            for agg in group_aggs {
                match agg.kind {
                    AGG_KIND_COUNT_STAR => {
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&count.to_le_bytes());
                        out.extend_from_slice(&0u32.to_le_bytes());
                    }
                    AGG_KIND_SUM => {
                        out.extend_from_slice(&sum.to_le_bytes());
                        out.extend_from_slice(&sum.to_le_bytes());
                        out.extend_from_slice(&sum.to_le_bytes());
                        out.extend_from_slice(&count.to_le_bytes());
                        out.extend_from_slice(&0u32.to_le_bytes());
                    }
                    _ => {
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&0f64.to_le_bytes());
                        out.extend_from_slice(&0u32.to_le_bytes());
                        out.extend_from_slice(&0u32.to_le_bytes());
                    }
                }
            }
        }
        debug_assert_eq!(out.len() % record_size, 0);
        return out;
    }

    let selected: Vec<(u32, u32)> = if order_by_count && limit > 0 {
        hist_topk_by_count(counts, n, offset, limit)
    } else {
        let mut entries = hist_entries_full_sort(counts, n, order_by_count);
        let skip = offset as usize;
        let take = if limit > 0 {
            limit as usize
        } else {
            entries.len()
        };
        entries.drain(..skip.min(entries.len()));
        entries.truncate(take);
        entries
    };

    out.reserve(selected.len().saturating_mul(record_size));
    for (id, count) in selected {
        out.extend_from_slice(&(id as u64).to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        for agg in group_aggs {
            out.extend_from_slice(&0f64.to_le_bytes());
            out.extend_from_slice(&0f64.to_le_bytes());
            out.extend_from_slice(&0f64.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    debug_assert_eq!(out.len() % record_size, 0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{FLAG_DICT, TYPE_STRING};
    use crate::types::Column;

    #[test]
    fn hist_roundtrip_and_merge() {
        let a = encode_group_hist_partial(3, &[1, 0, 2], None);
        let b = encode_group_hist_partial(3, &[0, 1, 1], None);
        let (len_a, mut merged, _) = decode_group_hist_partial(&a).unwrap();
        let (_, cb, _) = decode_group_hist_partial(&b).unwrap();
        assert_eq!(len_a, 3);
        merge_group_hist_counts(&mut merged, &cb);
        assert_eq!(merged, [1, 1, 3]);
        let records = group_hist_to_records(
            &merged,
            None,
            3,
            &[crate::types::GroupAgg {
                col_id: crate::constants::ROW_COUNT_COL_ID,
                kind: AGG_KIND_COUNT_STAR,
            }],
            false,
            0,
            0,
        );
        assert_eq!(records.len(), 3 * (16 + 32));
    }

    #[test]
    fn hist_topk_matches_full_sort() {
        let mut counts = vec![0u32; 100];
        for (id, c) in counts.iter_mut().enumerate() {
            *c = (id as u32).wrapping_mul(17) % 50;
        }
        counts[42] = 999;
        counts[7] = 998;
        counts[3] = 997;

        let full = hist_entries_full_sort(&counts, counts.len(), true)
            .into_iter()
            .take(10)
            .collect::<Vec<_>>();
        let heap = hist_topk_by_count(&counts, counts.len(), 0, 10);
        assert_eq!(heap, full);

        let full_off = hist_entries_full_sort(&counts, counts.len(), true)
            .into_iter()
            .skip(5)
            .take(10)
            .collect::<Vec<_>>();
        let heap_off = hist_topk_by_count(&counts, counts.len(), 5, 10);
        assert_eq!(heap_off, full_off);
    }

    #[test]
    fn hist_count_u32_full_mask() {
        let col = Column {
            id: 0,
            name: "k".to_string(),
            logical_type: TYPE_STRING,
            physical_type: crate::constants::TYPE_U32,
            flags: FLAG_DICT,
            encoding: 0,
            dict_id: 0,
            dict_index_width: 4,
            scale: 0,
        };
        let data = ColumnData::U32(vec![0, 1, 1, 2]);
        let mask = vec![u32::MAX];
        let mut counts = vec![0u32; 4];
        hist_count_dict_column(&mut counts, &col, &data, &mask, 4, &[]);
        assert_eq!(counts, [1, 2, 1, 0]);
    }
}
