use crate::constants::ROW_COUNT_COL_ID;
use crate::ffi::{
    copy_group_hist_partial, copy_groups, copy_groups_with_keys, copy_row_candidates,
    finalize_rows_from_heap, group_output_count, row_take_count, write_f64, write_u32, write_u64,
    PLANS, RUNTIMES,
};
use crate::query::group_dict_hist::plan_uses_group_dict_histogram;
use crate::runtime::{
    agg_key_col_id, agg_key_kind, agg_key_offset, hll_error_estimate, hll_estimate,
};

#[no_mangle]
pub unsafe extern "C" fn plan_copy_timing(handle: u32, out_ptr: *mut u8, out_len: usize) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    const REC_SIZE: usize = 4 + 8 * 13;
    if out_len < REC_SIZE {
        return -(REC_SIZE as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    plan.timing
        .write_copy_buffer(out, write_u32, write_f64);
    REC_SIZE as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_filters_len(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.filters.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_rows_len(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.rows.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_rows(handle: u32, out_ptr: *mut u8, out_len: usize) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let needed = plan.rows.len() * 8;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    let mut offset = 0;
    for row in &plan.rows {
        write_u64(out, offset, *row);
        offset += 8;
    }
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_finalize_rows(handle: u32) -> i32 {
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let take = row_take_count(plan);
    if take == 0 || plan.row_order_by.is_empty() {
        return 0;
    }
    if plan.row_heap.is_empty() {
        return 0;
    }
    finalize_rows_from_heap(plan);
    0
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_row_candidates(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    match copy_row_candidates(plan, out) {
        Ok(needed) => needed as isize,
        Err(needed) => -(needed as isize),
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_limit(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.limit as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_row_order_by_len(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.row_order_by.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_order_by_count(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => i32::from(plan.group_order_by_count),
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_row_order_by_count(handle: u32) -> i32 {
    let plans = PLANS.lock().unwrap();
    match plans.get(&handle) {
        Some(plan) => plan.row_order_by.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_row_order_by(
    handle: u32,
    out_ptr: *mut u32,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let needed = plan.row_order_by.len();
    if out_len < needed {
        return -(needed as isize);
    }
    if needed == 0 {
        return 0;
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    for (idx, col_id) in plan.row_order_by.iter().enumerate() {
        out[idx] = *col_id;
    }
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_agg_count(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.aggregates.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_aggs(handle: u32, out_ptr: *mut u8, out_len: usize) -> isize {
    use crate::constants::{AGG_KIND_APPROX_DISTINCT, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR};
    use crate::runtime::{
        agg_key_col_id, agg_key_kind, agg_key_offset, hll_error_estimate, hll_estimate,
    };

    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    const AGG_RECORD_SIZE: usize = 4 + 1 + 3 + 8 + 8 + 8 + 4;
    let needed = plan.aggregates.len() * AGG_RECORD_SIZE;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    let mut offset = 0;
    for agg_key in &plan.aggregates {
        let col_id = agg_key_col_id(*agg_key);
        let kind = agg_key_kind(*agg_key);
        let agg_offset = agg_key_offset(*agg_key);

        write_u32(
            out,
            offset,
            if kind == AGG_KIND_COUNT_STAR {
                ROW_COUNT_COL_ID
            } else {
                col_id
            },
        );
        offset += 4;
        out[offset] = kind;
        offset += 1;
        out[offset] = agg_offset as u8;
        out[offset + 1..offset + 3].fill(0);
        offset += 3;

        if kind == AGG_KIND_APPROX_DISTINCT {
            let hll = plan.hll_state.get(agg_key);
            let (estimate, err) = match hll {
                Some(h) => (hll_estimate(h), hll_error_estimate(h)),
                None => match plan.agg_state.get(agg_key) {
                    Some(state) => (state.sum, state.min),
                    None => (0.0, 0.0),
                },
            };
            write_f64(out, offset, estimate);
            offset += 8;
            write_f64(out, offset, err);
            offset += 8;
            write_f64(out, offset, 0.0);
            offset += 8;
            write_u32(out, offset, 1);
            offset += 4;
        } else {
            let state = plan.agg_state.get(agg_key).unwrap();
            let (sum, min, max) = if kind == AGG_KIND_COUNT_STAR || kind == AGG_KIND_COUNT {
                let c = state.count as f64;
                (c, c, c)
            } else {
                let off = agg_offset as f64;
                let min = if state.count > 0 {
                    state.min + off
                } else {
                    0.0
                };
                let max = if state.count > 0 {
                    state.max + off
                } else {
                    0.0
                };
                let sum = state.sum + off * (state.count as f64);
                (sum, min, max)
            };
            write_f64(out, offset, sum);
            offset += 8;
            write_f64(out, offset, min);
            offset += 8;
            write_f64(out, offset, max);
            offset += 8;
            write_u32(out, offset, state.count);
            offset += 4;
        }
    }
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_count(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => {
            if plan.group_emit_raw {
                let mut offset = 0usize;
                let mut count = 0usize;
                let agg_count = plan.group_aggs.len();
                let bytes = &plan.group_rows_raw_with_keys;
                while offset < bytes.len() {
                    if offset + 24 > bytes.len() {
                        break;
                    }
                    let key_len =
                        u32::from_le_bytes(bytes[offset + 16..offset + 20].try_into().unwrap())
                            as usize;
                    let payload_len = 24usize
                        .saturating_add(key_len)
                        .saturating_add(agg_count.saturating_mul(8 + 8 + 8 + 4 + 4));
                    if offset + payload_len > bytes.len() {
                        break;
                    }
                    offset += payload_len;
                    count = count.saturating_add(1);
                }
                return count as i32;
            }
            if plan_uses_group_dict_histogram(plan) {
                let n = plan
                    .group_dict_hist_counts
                    .as_ref()
                    .map(|c| {
                        c.iter()
                            .take(plan.group_dict_hist_dict_len as usize)
                            .filter(|&&v| v > 0)
                            .count()
                    })
                    .unwrap_or(0);
                return group_output_count(plan, n) as i32;
            }
            let n = plan.group_state.len();
            group_output_count(plan, n) as i32
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_dict_hist_dict_len(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.group_dict_hist_dict_len as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_dict_hist_active(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => {
            if plan_uses_group_dict_histogram(plan) {
                1
            } else {
                0
            }
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_group_hist_partial(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    match copy_group_hist_partial(plan, out) {
        Ok(needed) => needed as isize,
        Err(needed) => -(needed as isize),
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_key_count(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => match &plan.group_by {
            Some(g) => g.keys.len() as i32,
            None => 0,
        },
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_key_info(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let runtime_handle = {
        let plans = PLANS.lock().unwrap();
        let plan = match plans.get(&handle) {
            Some(p) => p,
            None => return -1,
        };
        plan.runtime
    };
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&runtime_handle) {
        Some(r) => r,
        None => return -1,
    };
    let keys = {
        let plans = PLANS.lock().unwrap();
        let plan = match plans.get(&handle) {
            Some(p) => p,
            None => return -1,
        };
        match &plan.group_by {
            Some(g) => g.keys.clone(),
            None => return 0,
        }
    };
    let record_size = 4 + 1 + 1 + 2;
    let needed = keys.len() * record_size;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    let mut offset = 0;
    for col_id in keys {
        let col = match runtime.schema.get(col_id as usize) {
            Some(c) => c,
            None => return -1,
        };
        write_u32(out, offset, col_id);
        offset += 4;
        out[offset] = col.physical_type;
        offset += 1;
        out[offset] = col.flags;
        offset += 1;
        out[offset] = 0;
        out[offset + 1] = 0;
        offset += 2;
    }
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_group_agg_count(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.group_aggs.len() as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_group_aggs(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let record_size = 4 + 1 + 3;
    let needed = plan.group_aggs.len() * record_size;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    let mut offset = 0;
    for agg in &plan.group_aggs {
        write_u32(out, offset, agg.col_id);
        offset += 4;
        out[offset] = agg.kind;
        offset += 1;
        out[offset..offset + 3].fill(0);
        offset += 3;
    }
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_groups(handle: u32, out_ptr: *mut u8, out_len: usize) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    match copy_groups(plan, out) {
        Ok(needed) => needed as isize,
        Err(needed) => -(needed as isize),
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_copy_groups_with_keys(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    match copy_groups_with_keys(plan, out) {
        Ok(written) => written as isize,
        Err(needed) => -(needed as isize),
    }
}
