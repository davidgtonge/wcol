use crate::constants::{OP_LIKE, OP_NOT_LIKE};
use crate::runtime::dict_string_id;
use crate::types::{Column, Filter, Runtime};

pub(crate) fn resolve_deferred_filter(filter: &Filter, col: &Column, runtime: &Runtime) -> Filter {
    if filter.op == OP_LIKE || filter.op == OP_NOT_LIKE {
        return filter.clone();
    }
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return filter.clone(),
    };
    let mut resolved = filter.clone();
    if let Some(ref s) = filter.value_str {
        let v = dict_string_id(dict, s).unwrap_or(0xffff_ffff) as f64;
        resolved.value = v;
        resolved.value2 = v;
        resolved.value_str = None;
    }
    if let Some(ref strs) = filter.in_list_str {
        let list: Vec<f64> = strs
            .iter()
            .map(|s| dict_string_id(dict, s).unwrap_or(0xffff_ffff) as f64)
            .collect();
        resolved.in_list = Some(list);
        resolved.in_list_str = None;
    }
    resolved
}

/// Resolve string filter literals to dictionary ids (and precompute LIKE sets) using file dicts.
#[cfg(feature = "sql_api")]
pub(crate) fn finalize_plan_filters(plan: &mut crate::types::Plan, runtime: &Runtime) {
    use std::sync::Arc;

    use crate::constants::FLAG_DICT;
    use crate::query::filter::build_like_id_set;

    for filter in &mut plan.filters {
        let Some(col) = runtime.schema.get(filter.col_id as usize) else {
            continue;
        };
        if (col.flags & FLAG_DICT) == 0 {
            continue;
        }
        let resolved = resolve_deferred_filter(filter, col, runtime);
        *filter = resolved;
        if (filter.op == OP_LIKE || filter.op == OP_NOT_LIKE) && filter.like_ids.is_none() {
            if let Some(pattern) = filter.value_str.as_deref() {
                let ids = build_like_id_set(col, pattern, runtime);
                if !ids.is_empty() {
                    filter.like_ids = Some(Arc::new(ids));
                }
            }
        }
    }
}
