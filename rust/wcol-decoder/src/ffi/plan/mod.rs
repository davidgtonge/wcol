mod config;
mod lifecycle;
mod reducer;
mod results;
mod stats;

pub(super) fn shrink_plan_buffers_enabled() -> bool {
    std::env::var("WCOL_SHRINK_PLAN_BUFFERS")
        .map(|v| v != "0")
        .unwrap_or(true)
}

pub(super) fn shrink_filter_timing(plan: &mut crate::types::Plan) {
    plan.filter_timing.shrink_buffers();
}

pub(super) fn shrink_plan_buffers(plan: &mut crate::types::Plan) {
    if !shrink_plan_buffers_enabled() {
        return;
    }
    plan.rows.shrink_to_fit();
    plan.agg_state.shrink_to_fit();
    plan.group_state.shrink_to_fit();
    plan.group_keys.shrink_to_fit();
    plan.group_key_repr.shrink_to_fit();
    plan.row_heap.shrink_to_fit();
    for v in plan.row_order_lex_ranks.values_mut() {
        v.shrink_to_fit();
    }
    plan.row_order_lex_ranks.shrink_to_fit();
    plan.hll_state.shrink_to_fit();
    shrink_filter_timing(plan);
}

pub(super) fn init_reducer_agg_state(plan: &mut crate::types::Plan) {
    use crate::constants::AGG_KIND_APPROX_DISTINCT;
    use crate::runtime::agg_key_kind;
    use crate::types::AggState;

    for agg_key in &plan.aggregates {
        let kind = agg_key_kind(*agg_key);
        if kind == AGG_KIND_APPROX_DISTINCT {
            plan.agg_state.insert(
                *agg_key,
                AggState {
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                    count: 0,
                },
            );
        } else {
            plan.agg_state.insert(
                *agg_key,
                AggState {
                    sum: 0.0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    count: 0,
                },
            );
        }
    }
}

pub(super) fn finalize_rows_basic(plan: &mut crate::types::Plan) {
    if plan.rows.is_empty() {
        return;
    }
    plan.rows.sort();
    let offset = plan.offset as usize;
    if offset > 0 {
        if offset >= plan.rows.len() {
            plan.rows.clear();
            return;
        }
        plan.rows.drain(0..offset);
    }
    if plan.limit > 0 {
        let limit = plan.limit as usize;
        if plan.rows.len() > limit {
            plan.rows.truncate(limit);
        }
    }
}

#[allow(unused_imports)]
pub use config::*;
#[allow(unused_imports)]
pub use lifecycle::*;
#[allow(unused_imports)]
pub use reducer::*;
#[allow(unused_imports)]
pub use results::*;
#[allow(unused_imports)]
pub use stats::*;
