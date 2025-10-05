use rustc_hash::FxHashMap;

use crate::ffi::{
    finalize_rows_from_heap, merge_row_candidates_from_bytes, next_handle, row_take_count, PLANS,
};
use crate::runtime::agg_key_make;
use crate::types::{
    AggState, FilterTiming, GroupAggState, GroupKey, GroupState, PlanTiming,
};

use super::{finalize_rows_basic, init_reducer_agg_state};

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_new(base_handle: u32) -> u32 {
    let base = {
        let plans = PLANS.lock().unwrap();
        match plans.get(&base_handle) {
            Some(p) => p.clone(),
            None => return 0,
        }
    };
    let hist_dict_len = base.group_dict_hist_dict_len;
    let hist_counts_len = base.group_dict_hist_counts.as_ref().map(|c| c.len());
    let hist_sums_len = base.group_dict_hist_sums.as_ref().map(|s| s.len());
    let mut plan = base;
    plan.rows.clear();
    plan.agg_state = FxHashMap::default();
    plan.group_state = FxHashMap::default();
    plan.group_keys.clear();
    plan.group_key_repr.clear();
    plan.row_heap = std::collections::BinaryHeap::new();
    plan.hll_state = FxHashMap::default();
    plan.group_emit_raw = false;
    plan.group_rows_raw_with_keys.clear();
    plan.group_dict_hist_dict_len = hist_dict_len;
    plan.group_dict_hist_counts = hist_counts_len.map(|n| vec![0u32; n]);
    plan.group_dict_hist_sums = hist_sums_len.map(|n| vec![0.0f64; n]);
    plan.timing = PlanTiming::default();
    plan.filter_timing = FilterTiming::default();
    init_reducer_agg_state(&mut plan);

    let handle = next_handle();
    PLANS.lock().unwrap().insert(handle, plan);
    handle
}

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_merge_aggs(
    handle: u32,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i32 {
    use crate::constants::{AGG_KIND_APPROX_DISTINCT, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR};

    const RECORD_SIZE: usize = 4 + 1 + 3 + 8 + 8 + 8 + 4;
    if bytes_len == 0 {
        return 0;
    }
    if bytes_len % RECORD_SIZE != 0 {
        return -2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(bytes_ptr, bytes_len) };
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };

    let mut offset = 0usize;
    while offset < bytes_len {
        let col_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let kind = bytes[offset];
        offset += 1;
        let off_raw = bytes[offset] as i8;
        offset += 3;
        let sum = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let min = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let max = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let agg_key = agg_key_make(col_id, kind, off_raw);
        let entry = plan.agg_state.entry(agg_key).or_insert(AggState {
            sum: 0.0,
            min: if kind == AGG_KIND_APPROX_DISTINCT {
                0.0
            } else {
                f64::INFINITY
            },
            max: if kind == AGG_KIND_APPROX_DISTINCT {
                0.0
            } else {
                f64::NEG_INFINITY
            },
            count: 0,
        });

        if kind == AGG_KIND_COUNT || kind == AGG_KIND_COUNT_STAR {
            entry.count = entry.count.saturating_add(count);
            continue;
        }
        let mut sum_adj = sum;
        let mut min_adj = min;
        let mut max_adj = max;
        // plan_copy_aggs serializes SUM/AVG/MIN/MAX with constant offset already applied.
        // Reducer state stores unshifted stats, so remove offset before merging to avoid
        // applying it twice when the final result is serialized.
        if kind != AGG_KIND_APPROX_DISTINCT && count > 0 {
            let off = off_raw as f64;
            sum_adj -= off * (count as f64);
            min_adj -= off;
            max_adj -= off;
        }
        entry.sum += sum_adj;
        if count > 0 {
            if entry.count == 0 {
                entry.min = min_adj;
                entry.max = max_adj;
            } else {
                entry.min = entry.min.min(min_adj);
                entry.max = entry.max.max(max_adj);
            }
            entry.count = entry.count.saturating_add(count);
        }
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_merge_groups(
    handle: u32,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i32 {
    use crate::constants::{AGG_KIND_COUNT, AGG_KIND_COUNT_STAR};
    use crate::query::group_dict_hist::{
        decode_group_hist_partial, is_group_hist_partial, merge_group_hist_counts,
        merge_group_hist_sums,
    };

    if bytes_len == 0 {
        return 0;
    }
    let bytes = unsafe { std::slice::from_raw_parts(bytes_ptr, bytes_len) };
    if is_group_hist_partial(bytes) {
        let mut plans = PLANS.lock().unwrap();
        let plan = match plans.get_mut(&handle) {
            Some(p) => p,
            None => return -1,
        };
        let (dict_len, counts, sums) = match decode_group_hist_partial(bytes) {
            Some(v) => v,
            None => return -2,
        };
        if plan.group_dict_hist_dict_len == 0 {
            plan.group_dict_hist_dict_len = dict_len;
            let n = dict_len as usize + 1;
            plan.group_dict_hist_counts = Some(vec![0u32; n]);
            if sums.is_some() {
                plan.group_dict_hist_sums = Some(vec![0.0f64; n]);
            }
        }
        if let Some(target) = plan.group_dict_hist_counts.as_mut() {
            merge_group_hist_counts(target, &counts);
        }
        if let (Some(target), Some(source)) = (plan.group_dict_hist_sums.as_mut(), sums.as_ref()) {
            merge_group_hist_sums(target, source);
        }
        return 0;
    }

    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let agg_count = plan.group_aggs.len();
    let record_size = 16 + agg_count * (8 + 8 + 8 + 4 + 4);
    if record_size == 0 || bytes_len % record_size != 0 {
        return -2;
    }
    let mut offset = 0usize;
    while offset < bytes_len {
        let key_a = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let key_b = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let key = GroupKey { a: key_a, b: key_b };
        let entry = plan.group_state.entry(key).or_insert_with(|| {
            plan.group_keys.push(key);
            GroupState {
                aggs: plan
                    .group_aggs
                    .iter()
                    .map(|_| {
                        GroupAggState::Numeric(AggState {
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            count: 0,
                        })
                    })
                    .collect(),
            }
        });
        for (idx, agg) in plan.group_aggs.iter().enumerate() {
            let sum = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let min = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let max = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            offset += 4; // padding

            let state = match entry.aggs.get_mut(idx) {
                Some(GroupAggState::Numeric(s)) => s,
                Some(_) => {
                    entry.aggs[idx] = GroupAggState::Numeric(AggState {
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                        count: 0,
                    });
                    match entry.aggs.get_mut(idx) {
                        Some(GroupAggState::Numeric(s)) => s,
                        _ => continue,
                    }
                }
                None => continue,
            };

            if agg.kind == AGG_KIND_COUNT || agg.kind == AGG_KIND_COUNT_STAR {
                state.count = state.count.saturating_add(count);
                continue;
            }
            state.sum += sum;
            if count > 0 {
                if state.count == 0 {
                    state.min = min;
                    state.max = max;
                } else {
                    state.min = state.min.min(min);
                    state.max = state.max.max(max);
                }
                state.count = state.count.saturating_add(count);
            }
        }
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_merge_rows(
    handle: u32,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i32 {
    if bytes_len == 0 {
        return 0;
    }
    if bytes_len % 8 != 0 {
        return -2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(bytes_ptr, bytes_len) };
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let mut offset = 0usize;
    while offset < bytes_len {
        let row = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        plan.rows.push(row);
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_merge_row_candidates(
    handle: u32,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i32 {
    if bytes_len == 0 {
        return 0;
    }
    let bytes = unsafe { std::slice::from_raw_parts(bytes_ptr, bytes_len) };
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    if let Err(code) = merge_row_candidates_from_bytes(plan, bytes) {
        return code;
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn plan_reducer_finalize(handle: u32) -> i32 {
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    if !plan.row_order_by.is_empty() {
        let take = row_take_count(plan);
        if take == 0 || plan.row_heap.is_empty() {
            return 0;
        }
        finalize_rows_from_heap(plan);
        return 0;
    }
    finalize_rows_basic(plan);
    0
}
