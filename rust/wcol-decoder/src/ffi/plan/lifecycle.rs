use rustc_hash::FxHashMap;

use crate::ffi::{next_handle, PLANS};
#[cfg(feature = "sql_api")]
use crate::ffi::RUNTIMES;
use crate::types::{FilterTiming, Plan, PlanTiming};

#[no_mangle]
pub unsafe extern "C" fn create_plan(runtime_handle: u32) -> u32 {
    let handle = next_handle();
    let plan = Plan {
        runtime: runtime_handle,
        filters: Vec::new(),
        combine: Vec::new(),
        group_by: None,
        aggregates: Vec::new(),
        limit: 0,
        offset: 0,
        rows: Vec::new(),
        agg_state: FxHashMap::default(),
        group_state: FxHashMap::default(),
        group_keys: Vec::new(),
        group_key_repr: FxHashMap::default(),
        group_order_by_count: false,
        group_aggs: Vec::new(),
        row_order_by: Vec::new(),
        row_heap: std::collections::BinaryHeap::new(),
        row_order_lex_ranks: FxHashMap::default(),
        hll_state: FxHashMap::default(),
        group_emit_raw: false,
        group_rows_raw_with_keys: Vec::new(),
        group_dict_hist_dict_len: 0,
        group_dict_hist_counts: None,
        group_dict_hist_sums: None,
        select_cols: Vec::new(),
        row_projection: crate::types::RowProjectionBuf::default(),
        timing: PlanTiming::default(),
        filter_timing: FilterTiming::default(),
    };
    PLANS.lock().unwrap().insert(handle, Box::new(plan));
    handle
}

#[no_mangle]
pub unsafe extern "C" fn destroy_plan(handle: u32) {
    PLANS.lock().unwrap().remove(&handle);
}

#[no_mangle]
#[cfg(feature = "sql_api")]
pub unsafe extern "C" fn plan_apply_sql(
    plan_handle: u32,
    sql_ptr: *const u8,
    sql_len: usize,
) -> i32 {
    let sql_bytes = std::slice::from_raw_parts(sql_ptr, sql_len);
    let sql_str = match std::str::from_utf8(sql_bytes) {
        Ok(s) => s,
        Err(_) => return -2,
    };
    let query = match wcol_sql_parser::parse_sql_v0(sql_str) {
        Ok(q) => q,
        Err(_) => return -3,
    };
    let runtime_handle = {
        let plans = PLANS.lock().unwrap();
        match plans.get(&plan_handle) {
            Some(p) => p.runtime,
            None => return -1,
        }
    };
    let mut runtimes = RUNTIMES.lock().unwrap();
    let runtime = match runtimes.get_mut(&runtime_handle) {
        Some(r) => r,
        None => return -1,
    };
    let mut plans = PLANS.lock().unwrap();
    let plan = match plans.get_mut(&plan_handle) {
        Some(p) => p,
        None => return -1,
    };
    match crate::sql_plan::apply_query_to_plan(&query, plan, runtime) {
        Ok(()) => 0,
        Err(e) => e,
    }
}

#[no_mangle]
#[cfg(not(feature = "sql_api"))]
pub unsafe extern "C" fn plan_apply_sql(_handle: u32, _sql_ptr: *const u8, _sql_len: usize) -> i32 {
    crate::constants::ERR_UNSUPPORTED
}
