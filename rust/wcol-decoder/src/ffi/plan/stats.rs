use crate::ffi::{write_f64, write_u32, write_u64, PLANS};
use crate::types::GroupAggState;

#[no_mangle]
pub unsafe extern "C" fn plan_memory_stats(handle: u32, out_ptr: *mut u8, out_len: usize) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let needed = 80usize;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };

    write_u64(out, 0, plan.rows.len() as u64);
    write_u64(out, 8, plan.rows.capacity() as u64);
    write_u64(out, 16, plan.group_state.len() as u64);
    write_u64(out, 24, plan.group_state.capacity() as u64);
    write_u64(out, 32, plan.group_keys.len() as u64);
    write_u64(out, 40, plan.group_keys.capacity() as u64);
    write_u64(out, 48, plan.row_heap.len() as u64);
    write_u64(out, 56, plan.row_heap.capacity() as u64);
    write_u64(out, 64, plan.row_order_lex_ranks.len() as u64);
    write_u64(out, 72, plan.row_order_lex_ranks.capacity() as u64);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_memory_deep_stats(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    // 14 u64 fields
    let needed = 112usize;
    if out_len < needed {
        return -(needed as isize);
    }

    let mut group_aggs_vec_len_total = 0u64;
    let mut group_aggs_vec_cap_total = 0u64;
    let mut distinct_set_count = 0u64;
    let mut distinct_set_len_total = 0u64;
    let mut distinct_set_cap_total = 0u64;
    for state in plan.group_state.values() {
        group_aggs_vec_len_total = group_aggs_vec_len_total.saturating_add(state.aggs.len() as u64);
        group_aggs_vec_cap_total =
            group_aggs_vec_cap_total.saturating_add(state.aggs.capacity() as u64);
        for agg in &state.aggs {
            if let GroupAggState::Distinct(set) = agg {
                distinct_set_count = distinct_set_count.saturating_add(1);
                distinct_set_len_total = distinct_set_len_total.saturating_add(set.len() as u64);
                distinct_set_cap_total =
                    distinct_set_cap_total.saturating_add(set.capacity() as u64);
            }
        }
    }

    let mut row_order_rank_vec_count = 0u64;
    let mut row_order_rank_vec_cap_total = 0u64;
    for rank in plan.row_order_lex_ranks.values() {
        row_order_rank_vec_count = row_order_rank_vec_count.saturating_add(1);
        row_order_rank_vec_cap_total =
            row_order_rank_vec_cap_total.saturating_add(rank.capacity() as u64);
    }

    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(out, 0, plan.group_state.len() as u64);
    write_u64(out, 8, plan.group_state.capacity() as u64);
    write_u64(out, 16, group_aggs_vec_len_total);
    write_u64(out, 24, group_aggs_vec_cap_total);
    write_u64(out, 32, distinct_set_count);
    write_u64(out, 40, distinct_set_len_total);
    write_u64(out, 48, distinct_set_cap_total);
    write_u64(out, 56, plan.group_keys.len() as u64);
    write_u64(out, 64, plan.group_keys.capacity() as u64);
    write_u64(out, 72, plan.row_heap.len() as u64);
    write_u64(out, 80, plan.row_heap.capacity() as u64);
    write_u64(out, 88, row_order_rank_vec_count);
    write_u64(out, 96, row_order_rank_vec_cap_total);
    write_u64(out, 104, plan.rows.capacity() as u64);
    needed as isize
}

#[no_mangle]
pub unsafe extern "C" fn plan_global_stats(out_ptr: *mut u8, out_len: usize) -> isize {
    let plans = PLANS.lock().unwrap();
    let needed = 48usize;
    if out_len < needed {
        return -(needed as isize);
    }
    let mut rows_cap_total = 0u64;
    let mut group_state_cap_total = 0u64;
    let mut group_keys_cap_total = 0u64;
    let mut row_heap_cap_total = 0u64;
    let mut row_order_ranks_cap_total = 0u64;
    for plan in plans.values() {
        rows_cap_total = rows_cap_total.saturating_add(plan.rows.capacity() as u64);
        group_state_cap_total =
            group_state_cap_total.saturating_add(plan.group_state.capacity() as u64);
        group_keys_cap_total =
            group_keys_cap_total.saturating_add(plan.group_keys.capacity() as u64);
        row_heap_cap_total = row_heap_cap_total.saturating_add(plan.row_heap.capacity() as u64);
        row_order_ranks_cap_total =
            row_order_ranks_cap_total.saturating_add(plan.row_order_lex_ranks.capacity() as u64);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    write_u64(out, 0, plans.len() as u64);
    write_u64(out, 8, rows_cap_total);
    write_u64(out, 16, group_state_cap_total);
    write_u64(out, 24, group_keys_cap_total);
    write_u64(out, 32, row_heap_cap_total);
    write_u64(out, 40, row_order_ranks_cap_total);
    needed as isize
}

#[cfg(feature = "bench_api")]
#[no_mangle]
pub unsafe extern "C" fn plan_filter_timing_len(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.filter_timing.filter_count() as i32,
        None => -1,
    }
}

#[cfg(feature = "bench_api")]
#[no_mangle]
pub unsafe extern "C" fn plan_filter_value_str_len(handle: u32, idx: u32) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let i = idx as usize;
    if i >= plan.filters.len() {
        return -2;
    }
    match plan.filters[i].value_str.as_ref() {
        Some(s) => s.len() as isize,
        None => 0,
    }
}

#[cfg(feature = "bench_api")]
#[no_mangle]
pub unsafe extern "C" fn plan_copy_filter_value_str(
    handle: u32,
    idx: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let i = idx as usize;
    if i >= plan.filters.len() {
        return -2;
    }
    let Some(s) = plan.filters[i].value_str.as_ref() else {
        return 0;
    };
    if out_len < s.len() {
        return -(s.len() as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    out[..s.len()].copy_from_slice(s.as_bytes());
    s.len() as isize
}

#[cfg(feature = "bench_api")]
#[no_mangle]
pub unsafe extern "C" fn plan_copy_filter_timing(
    handle: u32,
    out_ptr: *mut u8,
    out_len: usize,
) -> isize {
    let plans = PLANS.lock().unwrap();
    let plan = match plans.get(&handle) {
        Some(p) => p,
        None => return -1,
    };
    let count = plan.filter_timing.filter_count();
    const REC_SIZE: usize = 4 + 1 + 3 + 8 * 3 + 4 * 5 + 8 * 3;
    let needed = count * REC_SIZE;
    if out_len < needed {
        return -(needed as isize);
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_len) };
    let written = plan
        .filter_timing
        .write_copy_buffer(out, write_u32, write_f64);
    debug_assert_eq!(written, needed);
    needed as isize
}
