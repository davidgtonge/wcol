use crate::constants::{
    AGG_KIND_AVG, AGG_KIND_COUNT_STAR, AGG_KIND_MAX, AGG_KIND_MIN, AGG_KIND_SUM,
    ERR_NON_NUMERIC_AGG, FLAG_DICT, ROW_COUNT_COL_ID, TYPE_STRING,
};
use crate::ffi::{PLANS, RUNTIMES};
use crate::runtime::agg_key_make;
use crate::types::{AggState, Filter, FilterTiming, GroupAgg, GroupBy, PlanTiming};

use super::shrink_plan_buffers;

#[no_mangle]
pub unsafe extern "C" fn plan_set_limit(handle: u32, limit: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.limit = limit;
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_prepare_optimizations(handle: u32) -> i32 {
    let runtime_handle = {
        let plans = PLANS.lock().unwrap();
        let plan = match plans.get(&handle) {
            Some(p) => p.runtime,
            None => return -1,
        };
        plan
    };
    let runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get(&runtime_handle) {
        Some(r) => r,
        None => return -2,
    };
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        crate::query::group_dict_hist::try_enable_group_dict_histogram(plan, runtime);
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_group_order_by_count(handle: u32, enabled: i32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.group_order_by_count = enabled != 0;
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_offset(handle: u32) -> i32 {
    match PLANS.lock().unwrap().get(&handle) {
        Some(plan) => plan.offset as i32,
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_offset(handle: u32, offset: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.offset = offset;
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_add_filter(
    handle: u32,
    col_id: u32,
    op: u8,
    value: f64,
    value2: f64,
) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.filters.push(Filter {
            col_id,
            op,
            value,
            value2,
            in_list: None,
            value_str: None,
            in_list_str: None,
            like_ids: None,
        });
        return (plan.filters.len() - 1) as i32;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_add_filter_in(
    handle: u32,
    col_id: u32,
    op: u8,
    values_ptr: *const f64,
    values_len: usize,
) -> i32 {
    let values = unsafe { std::slice::from_raw_parts(values_ptr, values_len) };
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.filters.push(Filter {
            col_id,
            op,
            value: 0.0,
            value2: 0.0,
            in_list: Some(values.to_vec()),
            value_str: None,
            in_list_str: None,
            like_ids: None,
        });
        return (plan.filters.len() - 1) as i32;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_filter_value_str(
    handle: u32,
    idx: u32,
    value_ptr: *const u8,
    value_len: usize,
) -> i32 {
    let value = unsafe { std::slice::from_raw_parts(value_ptr, value_len) };
    let value_str = match std::str::from_utf8(value) {
        Ok(s) => s.to_string(),
        Err(_) => return -3,
    };
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        let i = idx as usize;
        if i >= plan.filters.len() {
            return -2;
        }
        plan.filters[i].value_str = Some(value_str);
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_combine(
    handle: u32,
    tokens_ptr: *const i32,
    tokens_len: usize,
) -> i32 {
    let tokens = unsafe { std::slice::from_raw_parts(tokens_ptr, tokens_len) };
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.combine = tokens.to_vec();
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_group_by(
    handle: u32,
    key1: i32,
    key2: i32,
    value_col: i32,
) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        let mut keys = Vec::new();
        if key1 >= 0 {
            keys.push(key1 as u32);
        }
        if key2 >= 0 {
            keys.push(key2 as u32);
        }
        if keys.is_empty() {
            plan.group_by = None;
            plan.group_aggs.clear();
            return 0;
        }
        plan.group_by = Some(GroupBy {
            keys,
            value_col: if value_col >= 0 {
                Some(value_col as u32)
            } else {
                None
            },
            value_kind: AGG_KIND_SUM,
            count_kind: AGG_KIND_COUNT_STAR,
        });
        plan.group_aggs.clear();
        plan.group_aggs.push(GroupAgg {
            col_id: ROW_COUNT_COL_ID,
            kind: AGG_KIND_COUNT_STAR,
        });
        if value_col >= 0 {
            plan.group_aggs.push(GroupAgg {
                col_id: value_col as u32,
                kind: AGG_KIND_SUM,
            });
        }
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_add_aggregate(handle: u32, col_id: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        let runtime_handle = plan.runtime;
        let runtimes = RUNTIMES.lock().unwrap();
        let runtime = match runtimes.get(&runtime_handle) {
            Some(r) => r,
            None => return -2,
        };
        let col = match runtime.schema.get(col_id as usize) {
            Some(c) => c,
            None => return -3,
        };
        if (col.flags & FLAG_DICT) != 0 || col.logical_type == TYPE_STRING {
            return ERR_NON_NUMERIC_AGG;
        }

        let agg_key = agg_key_make(col_id as u32, AGG_KIND_SUM, 0);
        plan.aggregates.push(agg_key);
        plan.agg_state.entry(agg_key).or_insert(AggState {
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: 0,
        });
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_reset_results(handle: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.rows.clear();
        for state in plan.agg_state.values_mut() {
            state.sum = 0.0;
            state.min = f64::INFINITY;
            state.max = f64::NEG_INFINITY;
            state.count = 0;
        }
        plan.group_state.clear();
        plan.group_keys.clear();
        plan.group_key_repr.clear();
        plan.group_rows_raw_with_keys.clear();
        if let Some(counts) = plan.group_dict_hist_counts.as_mut() {
            counts.fill(0);
        }
        if let Some(sums) = plan.group_dict_hist_sums.as_mut() {
            sums.fill(0.0);
        }
        plan.row_heap.clear();
        plan.row_projection.clear();
        for hll in plan.hll_state.values_mut() {
            hll.registers.fill(0);
        }
        plan.timing = PlanTiming::default();
        plan.filter_timing.reset_counters();
        shrink_plan_buffers(plan);
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_clear(handle: u32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.filters.clear();
        plan.combine.clear();
        plan.group_by = None;
        plan.aggregates.clear();
        plan.limit = 0;
        plan.offset = 0;
        plan.agg_state.clear();
        plan.group_state.clear();
        plan.group_keys.clear();
        plan.group_key_repr.clear();
        plan.group_order_by_count = false;
        plan.group_aggs.clear();
        plan.group_emit_raw = false;
        plan.group_rows_raw_with_keys.clear();
        plan.group_dict_hist_dict_len = 0;
        plan.group_dict_hist_counts = None;
        plan.group_dict_hist_sums = None;
        plan.row_order_by.clear();
        plan.row_heap.clear();
        plan.row_order_lex_ranks.clear();
        plan.hll_state.clear();
        plan.select_cols.clear();
        plan.row_projection.clear();
        plan.timing = PlanTiming::default();
        plan.filter_timing = FilterTiming::default();
        plan.filters.shrink_to_fit();
        plan.combine.shrink_to_fit();
        plan.aggregates.shrink_to_fit();
        plan.group_aggs.shrink_to_fit();
        plan.group_rows_raw_with_keys.shrink_to_fit();
        plan.row_order_by.shrink_to_fit();
        shrink_plan_buffers(plan);
        return 0;
    }
    -1
}

#[no_mangle]
pub unsafe extern "C" fn plan_set_group_emit_raw(handle: u32, enabled: i32) -> i32 {
    if let Some(plan) = PLANS.lock().unwrap().get_mut(&handle) {
        plan.group_emit_raw = enabled != 0;
        return 0;
    }
    -1
}
