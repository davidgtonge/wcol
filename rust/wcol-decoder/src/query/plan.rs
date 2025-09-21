#![allow(dead_code)]

use crate::constants::AGG_KIND_COUNT_STAR;
use crate::constants::ROW_COUNT_COL_ID;
use crate::runtime::{agg_key_col_id, agg_key_kind};
use crate::types::Plan;

#[inline]
fn mark_required(required: &mut [bool], col_id: u32) {
    let idx = col_id as usize;
    if idx < required.len() {
        required[idx] = true;
    }
}

pub(crate) fn plan_required_columns(plan: &Plan, schema_len: usize) -> Vec<u32> {
    let mut required = vec![false; schema_len];
    for filter in &plan.filters {
        mark_required(&mut required, filter.col_id);
    }
    for agg_key in &plan.aggregates {
        let kind = agg_key_kind(*agg_key);
        let col_id = if kind == AGG_KIND_COUNT_STAR {
            ROW_COUNT_COL_ID
        } else {
            agg_key_col_id(*agg_key)
        };
        if col_id != ROW_COUNT_COL_ID {
            mark_required(&mut required, col_id);
        }
    }
    if let Some(group_by) = &plan.group_by {
        for key in &group_by.keys {
            mark_required(&mut required, *key);
        }
        if let Some(value) = group_by.value_col {
            mark_required(&mut required, value);
        }
    }
    for col_id in &plan.row_order_by {
        mark_required(&mut required, *col_id);
    }
    for agg in &plan.group_aggs {
        if agg.col_id != ROW_COUNT_COL_ID {
            mark_required(&mut required, agg.col_id);
        }
    }
    required
        .iter()
        .enumerate()
        .filter_map(|(idx, needed)| if *needed { Some(idx as u32) } else { None })
        .collect()
}

pub(crate) fn plan_materialize_required_columns(plan: &Plan, schema_len: usize) -> Vec<u32> {
    plan.select_cols
        .iter()
        .copied()
        .filter(|col_id| (*col_id as usize) < schema_len)
        .collect()
}
