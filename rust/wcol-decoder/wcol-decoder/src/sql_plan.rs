//! Apply parsed SQL (wcol_sql_parser) to a Plan without Serde/JSON.
//! Used from WASM: parse SQL and mutate plan in place.

#![allow(dead_code)]

use crate::constants::{
    AGG_KIND_APPROX_DISTINCT, AGG_KIND_AVG, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR, AGG_KIND_MAX,
    AGG_KIND_MIN, AGG_KIND_SUM, COMB_AND, FLAG_DICT, OP_EQ, OP_GT, OP_GTE, OP_LIKE, OP_LT, OP_LTE,
    OP_NEQ, OP_NOT_LIKE, ROW_COUNT_COL_ID, TYPE_STRING,
};
use crate::runtime::agg_key_make;
use crate::runtime::hll_new_default;
use crate::types::{Filter, GroupBy, Plan, Runtime};
use wcol_sql_parser::{AggFn, BinOp, CmpOp, Expr, Ident, Query};

/// Apply a parsed SQL query to an existing plan. Resolves column names and
/// string values (dict lookup) using the runtime. No serialization.
pub fn apply_query_to_plan(
    query: &Query<'_>,
    plan: &mut Plan,
    runtime: &Runtime,
) -> Result<(), i32> {
    if query.having.is_some() {
        // HAVING requires post-aggregation filtering; not supported in the v0 plan.
        return Err(crate::constants::ERR_UNSUPPORTED);
    }
    if let Some(lim) = query.limit {
        plan.limit = lim.min(u32::MAX as u64) as u32;
    }
    plan.offset = query.offset.unwrap_or(0).min(u32::MAX as u64) as u32;

    if let Some(ref pred) = query.where_ {
        let filters = collect_filters(pred);
        for f in filters {
            let filter = convert_filter(f, runtime)?;
            plan.filters.push(filter);
        }
        if plan.combine.is_empty() && plan.filters.len() > 1 {
            let schema = &runtime.schema;
            plan.filters.sort_by_key(|filter| {
                let is_empty = filter.value_str.as_deref() == Some("")
                    && (filter.op == OP_EQ || filter.op == OP_NEQ);
                let is_string = schema
                    .get(filter.col_id as usize)
                    .map(|col| col.logical_type == TYPE_STRING)
                    .unwrap_or(false);
                if is_empty && is_string && filter.op == OP_NEQ {
                    0
                } else if is_empty && is_string {
                    1
                } else {
                    2
                }
            });
        }
        // RPN for AND of N masks: [0, 1, COMB_AND, 2, COMB_AND, ..., n-1, COMB_AND]
        if plan.combine.is_empty() && plan.filters.len() > 1 {
            let n = plan.filters.len();
            let mut comb = vec![0i32, 1i32, COMB_AND];
            for i in 2..n {
                comb.push(i as i32);
                comb.push(COMB_AND);
            }
            plan.combine = comb;
        }
        crate::query::filter_literals::finalize_plan_filters(plan, runtime);
    }

    if !query.group_by.is_empty() {
        let mut keys = Vec::with_capacity(2);
        for e in query.group_by.iter() {
            match e {
                // GROUP BY 1 is a no-op key (constant); we can ignore it.
                Expr::Int(_) => continue,
                Expr::Ident(_) => {
                    if keys.len() >= 2 {
                        return Err(crate::constants::ERR_UNSUPPORTED);
                    }
                    if let Some(id) = expr_to_col_id(e, runtime) {
                        keys.push(id);
                    }
                }
                _ => return Err(crate::constants::ERR_UNSUPPORTED),
            }
        }
        if !keys.is_empty() {
            plan.group_by = Some(GroupBy {
                keys,
                value_col: None,
                value_kind: AGG_KIND_SUM,
                count_kind: AGG_KIND_COUNT_STAR,
            });
        }
    }

    if !query.order_by.is_empty() {
        if plan.group_by.is_some() {
            // v0: only supports ordering groups by count; treat any ORDER BY as count order.
            plan.group_order_by_count = true;
        } else {
            plan.row_order_by.clear();
            for item in query.order_by.iter().take(2) {
                if let Some(id) = expr_to_col_id(&item.expr, runtime) {
                    plan.row_order_by.push(id);
                }
            }
        }
    }

    let has_group_by = plan.group_by.is_some();
    let mut group_approx_col: Option<u32> = None;
    let mut group_has_count_star = false;
    let mut group_has_other_agg = false;

    for item in &query.select {
        if let Expr::Agg { func, arg, .. } = &item.expr {
            let (col_id, kind, agg_offset) =
                if matches!((func, arg.as_ref()), (AggFn::Count, Expr::Star)) {
                    if has_group_by {
                        group_has_count_star = true;
                    }
                    (ROW_COUNT_COL_ID, AGG_KIND_COUNT_STAR, 0i8)
                } else if let Some((cid, off)) = agg_arg_to_col_id_and_offset(arg, runtime) {
                    let col = match runtime.schema.get(cid as usize) {
                        Some(c) => c,
                        None => continue,
                    };
                    if has_group_by {
                        match func {
                            AggFn::ApproxDistinct => {
                                if group_approx_col.is_none() {
                                    group_approx_col = Some(cid);
                                }
                            }
                            _ => {
                                group_has_other_agg = true;
                            }
                        }
                    }
                    // approx_count_distinct works on all types including dict/string
                    let is_approx_distinct = matches!(func, AggFn::ApproxDistinct);
                    if !is_approx_distinct
                        && ((col.flags & FLAG_DICT) != 0 || col.logical_type == TYPE_STRING)
                    {
                        continue;
                    }
                    let k = match func {
                        AggFn::Count => AGG_KIND_COUNT,
                        AggFn::Sum => AGG_KIND_SUM,
                        AggFn::Avg => AGG_KIND_AVG,
                        AggFn::Min => AGG_KIND_MIN,
                        AggFn::Max => AGG_KIND_MAX,
                        AggFn::ApproxDistinct => AGG_KIND_APPROX_DISTINCT,
                    };
                    (cid, k, off)
                } else {
                    return Err(crate::constants::ERR_UNSUPPORTED);
                };

            if has_group_by {
                plan.group_aggs
                    .push(crate::types::GroupAgg { col_id, kind });
            }
            let agg_key = agg_key_make(col_id as u32, kind, agg_offset);
            if !plan.aggregates.contains(&agg_key) {
                plan.aggregates.push(agg_key);
                if kind == AGG_KIND_APPROX_DISTINCT {
                    plan.hll_state.insert(agg_key, hll_new_default());
                } else {
                    plan.agg_state.insert(
                        agg_key,
                        crate::types::AggState {
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            count: 0,
                        },
                    );
                }
            }
        } else if !has_group_by {
            if let Some(id) = expr_to_col_id(&item.expr, runtime) {
                if !plan.select_cols.contains(&id) {
                    plan.select_cols.push(id);
                }
            }
        }
    }

    if let Some(group_by) = &mut plan.group_by {
        if let Some(cid) = group_approx_col {
            group_by.value_col = Some(cid);
            group_by.value_kind = AGG_KIND_APPROX_DISTINCT;
            if !group_has_count_star && !group_has_other_agg {
                group_by.count_kind = AGG_KIND_APPROX_DISTINCT;
            }
        }
        if group_has_count_star {
            group_by.count_kind = AGG_KIND_COUNT_STAR;
        }
    }

    crate::query::group_dict_hist::try_enable_group_dict_histogram(plan, runtime);

    Ok(())
}

enum FilterExpr<'a> {
    Compare {
        col: &'a str,
        op: CmpOp,
        value: FilterValue<'a>,
    },
    In {
        col: &'a str,
        values: Vec<FilterValue<'a>>,
        negated: bool,
    },
    Like {
        col: &'a str,
        pattern: &'a str,
        negated: bool,
    },
}

enum FilterValue<'a> {
    F64(f64),
    Str(&'a str),
}

fn collect_filters<'a>(pred: &'a Expr<'a>) -> Vec<FilterExpr<'a>> {
    let mut out = Vec::new();
    collect_filters_impl(pred, &mut out);
    out
}

fn collect_filters_impl<'a>(pred: &'a Expr<'a>, out: &mut Vec<FilterExpr<'a>>) {
    match pred {
        Expr::And(l, r) => {
            collect_filters_impl(l, out);
            collect_filters_impl(r, out);
        }
        Expr::Compare { op, left, right } => {
            let col = expr_to_col_name(left).or_else(|| expr_to_col_name(right));
            let value = expr_to_value(right).or_else(|| expr_to_value(left));
            if let (Some(col), Some(value)) = (col, value) {
                out.push(FilterExpr::Compare {
                    col,
                    op: *op,
                    value,
                });
            }
        }
        Expr::In {
            left,
            values,
            negated,
        } => {
            if let Some(col) = expr_to_col_name(left) {
                let vals: Vec<FilterValue<'_>> = values.iter().filter_map(expr_to_value).collect();
                if !vals.is_empty() {
                    out.push(FilterExpr::In {
                        col,
                        values: vals,
                        negated: *negated,
                    });
                }
            }
        }
        Expr::Like {
            left,
            pattern,
            negated,
        } => {
            if let Some(col) = expr_to_col_name(left) {
                if let Expr::Str(pat) = pattern.as_ref() {
                    out.push(FilterExpr::Like {
                        col,
                        pattern: pat,
                        negated: *negated,
                    });
                }
            }
        }
        _ => {}
    }
}

fn expr_to_col_name<'a>(expr: &'a Expr<'a>) -> Option<&'a str> {
    match expr {
        Expr::Ident(Ident(name)) => Some(*name),
        _ => None,
    }
}

fn expr_to_value<'a>(expr: &'a Expr<'a>) -> Option<FilterValue<'a>> {
    match expr {
        Expr::Int(i) => Some(FilterValue::F64(*i as f64)),
        Expr::Str(s) => Some(FilterValue::Str(s)),
        Expr::Binary {
            op: wcol_sql_parser::BinOp::Sub,
            left,
            right,
        } => {
            if let (Expr::Int(0), Expr::Int(r)) = (left.as_ref(), right.as_ref()) {
                Some(FilterValue::F64(-*r as f64))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn expr_to_col_id(expr: &Expr, runtime: &Runtime) -> Option<u32> {
    let name = expr_to_col_name(expr)?;
    for (id, col) in runtime.schema.iter().enumerate() {
        if col.name == name {
            return Some(id as u32);
        }
    }
    None
}

fn agg_arg_to_col_id_and_offset(expr: &Expr, runtime: &Runtime) -> Option<(u32, i8)> {
    // Supported forms:
    // - col
    // - col + int
    // - col - int
    // - int + col
    match expr {
        Expr::Ident(_) => expr_to_col_id(expr, runtime).map(|cid| (cid, 0)),
        Expr::Binary { op, left, right } => {
            let (cid, off) = match (op, left.as_ref(), right.as_ref()) {
                (BinOp::Add, Expr::Ident(_), Expr::Int(k)) => (expr_to_col_id(left, runtime)?, *k),
                (BinOp::Sub, Expr::Ident(_), Expr::Int(k)) => (expr_to_col_id(left, runtime)?, -*k),
                (BinOp::Add, Expr::Int(k), Expr::Ident(_)) => (expr_to_col_id(right, runtime)?, *k),
                _ => return None,
            };
            if off < i8::MIN as i64 || off > i8::MAX as i64 {
                return None;
            }
            Some((cid, off as i8))
        }
        _ => None,
    }
}

/// Resolves string to dict id if dict is available. Returns (Some(id), None) when resolved,
/// (None, Some(s)) when dict is missing (defer to execution).
fn resolve_value<'a>(
    value: FilterValue<'a>,
    col_id: u32,
    runtime: &Runtime,
) -> Result<(Option<f64>, Option<String>), i32> {
    match value {
        FilterValue::F64(f) => Ok((Some(f), None)),
        FilterValue::Str(s) => {
            let col = runtime.schema.get(col_id as usize).ok_or(-1)?;
            if col.logical_type != TYPE_STRING && (col.flags & FLAG_DICT) == 0 {
                if let Ok(v) = s.parse::<f64>() {
                    return Ok((Some(v), None));
                }
                if let Some(days) = parse_ymd_days_since_epoch(s) {
                    return Ok((Some(days as f64), None));
                }
            }
            if let Some(dict) = runtime.dicts.get(&col.dict_id) {
                let id = dict.lookup.get(s).copied().or_else(|| {
                    if !dict.offsets.is_empty() {
                        for (idx, _) in dict.offsets.windows(2).enumerate() {
                            let start = dict.offsets[idx] as usize;
                            let end = dict.offsets[idx + 1] as usize;
                            if dict.blob.get(start..end) == Some(s.as_bytes()) {
                                return Some(idx as u32);
                            }
                        }
                        return None;
                    }
                    if dict.lookup.is_empty() {
                        if let Ok(pos) = dict
                            .values
                            .binary_search_by(|v| v.as_bytes().cmp(s.as_bytes()))
                        {
                            return Some(pos as u32);
                        }
                    }
                    None
                });
                Ok((Some(id.unwrap_or(0xffff_ffff) as f64), None))
            } else {
                Ok((None, Some(s.to_string())))
            }
        }
    }
}

fn parse_ymd_days_since_epoch(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = s.get(0..4)?.parse::<i32>().ok()?;
    let month = s.get(5..7)?.parse::<u32>().ok()?;
    let day = s.get(8..10)?.parse::<u32>().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Howard Hinnant's civil-date to days algorithm.
    let y = year - (month <= 2) as i32;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let d = day as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

fn col_name_to_id(name: &str, runtime: &Runtime) -> Result<u32, i32> {
    for (id, col) in runtime.schema.iter().enumerate() {
        if col.name == name {
            return Ok(id as u32);
        }
    }
    Err(-3)
}

fn cmp_op_to_u8(op: CmpOp) -> u8 {
    match op {
        CmpOp::Eq => OP_EQ,
        CmpOp::Ne => OP_NEQ,
        CmpOp::Lt => OP_LT,
        CmpOp::Le => OP_LTE,
        CmpOp::Gt => OP_GT,
        CmpOp::Ge => OP_GTE,
    }
}

fn convert_filter<'a>(f: FilterExpr<'a>, runtime: &Runtime) -> Result<Filter, i32> {
    match f {
        FilterExpr::Compare { col, op, value } => {
            let col_id = col_name_to_id(col, runtime)?;
            let (v_opt, v_str) = resolve_value(value, col_id, runtime)?;
            Ok(Filter {
                col_id,
                op: cmp_op_to_u8(op),
                value: v_opt.unwrap_or(0.0),
                value2: v_opt.unwrap_or(0.0),
                in_list: None,
                value_str: v_str,
                in_list_str: None,
                like_ids: None,
            })
        }
        FilterExpr::In {
            col,
            values,
            negated,
        } => {
            if negated {
                return Err(-5); // NOT IN not supported in plan yet
            }
            let col_id = col_name_to_id(col, runtime)?;
            let mut list = Vec::with_capacity(values.len());
            let mut list_str = Vec::with_capacity(values.len());
            let mut any_deferred = false;
            for v in values {
                let (opt, s) = resolve_value(v, col_id, runtime)?;
                if let Some(id) = opt {
                    list.push(id);
                } else if let Some(owned) = s {
                    list_str.push(owned);
                    any_deferred = true;
                } else {
                    list.push(0xffff_ffffu32 as f64);
                }
            }
            Ok(Filter {
                col_id,
                op: OP_EQ,
                value: 0.0,
                value2: 0.0,
                in_list: if any_deferred { None } else { Some(list) },
                value_str: None,
                in_list_str: if any_deferred { Some(list_str) } else { None },
                like_ids: None,
            })
        }
        FilterExpr::Like {
            col,
            pattern,
            negated,
        } => {
            let col_id = col_name_to_id(col, runtime)?;
            // Normalize pattern: '%foo%' -> "foo" (contains), 'foo%' -> "foo" (prefix), '%foo' -> "foo" (suffix)
            let sub = pattern.trim_matches('%');
            Ok(Filter {
                col_id,
                op: if negated { OP_NOT_LIKE } else { OP_LIKE },
                value: 0.0,
                value2: 0.0,
                in_list: None,
                value_str: Some(sub.to_string()),
                in_list_str: None,
                like_ids: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FilterTiming, PlanTiming};
    use rustc_hash::FxHashMap;
    use std::sync::Arc;
    use wcol_sql_parser::parse_sql_v0;

    fn col(id: u32, name: &str, physical_type: u8, flags: u8) -> crate::types::Column {
        crate::types::Column {
            id,
            name: name.to_string(),
            logical_type: physical_type,
            physical_type,
            flags,
            encoding: 0,
            dict_id: 0,
            dict_index_width: 0,
            scale: 0,
        }
    }

    fn runtime_with_schema(columns: Vec<crate::types::Column>) -> Runtime {
        Runtime {
            header: None,
            schema: Arc::from(columns),
            toc: vec![],
            dicts: FxHashMap::default(),
            index_cache: FxHashMap::default(),
        }
    }

    fn empty_plan(runtime_handle: u32) -> Plan {
        Plan {
            runtime: runtime_handle,
            filters: vec![],
            combine: vec![],
            group_by: None,
            aggregates: vec![],
            limit: 0,
            offset: 0,
            rows: vec![],
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
        }
    }

    #[test]
    fn apply_limit() {
        let runtime = runtime_with_schema(vec![col(0, "x", crate::constants::TYPE_F64, 0)]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT x FROM t LIMIT 10").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.limit, 10);
    }

    #[test]
    fn apply_filter_eq_int() {
        let runtime = runtime_with_schema(vec![
            col(0, "CounterID", crate::constants::TYPE_I32, 0),
            col(1, "UserID", crate::constants::TYPE_I64, 0),
        ]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT 1 FROM t WHERE CounterID = 62").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].col_id, 0);
        assert_eq!(plan.filters[0].op, OP_EQ);
        assert_eq!(plan.filters[0].value, 62.0);
    }

    #[test]
    fn apply_filter_in() {
        let runtime = runtime_with_schema(vec![col(
            0,
            "TraficSourceID",
            crate::constants::TYPE_I32,
            0,
        )]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT 1 FROM t WHERE TraficSourceID IN (-1, 6)").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].col_id, 0);
        assert_eq!(plan.filters[0].in_list, Some(vec![-1.0, 6.0]));
    }

    #[test]
    fn apply_filter_and_combine() {
        let runtime = runtime_with_schema(vec![
            col(0, "CounterID", crate::constants::TYPE_I32, 0),
            col(1, "EventDate", crate::constants::TYPE_F64, 0),
        ]);
        let mut plan = empty_plan(1);
        let query =
            parse_sql_v0("SELECT 1 FROM t WHERE CounterID = 62 AND EventDate >= 1000").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.filters.len(), 2);
        assert_eq!(plan.combine, vec![0, 1, COMB_AND]);
    }

    #[test]
    fn apply_group_by() {
        let runtime = runtime_with_schema(vec![
            col(0, "URLHash", crate::constants::TYPE_I64, 0),
            col(1, "EventDate", crate::constants::TYPE_F64, 0),
        ]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT COUNT(*) FROM t GROUP BY URLHash, EventDate").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert!(plan.group_by.is_some());
        assert_eq!(plan.group_by.as_ref().unwrap().keys, vec![0, 1]);
    }

    #[test]
    fn apply_aggregates() {
        let runtime = runtime_with_schema(vec![
            col(0, "ResolutionWidth", crate::constants::TYPE_I32, 0),
            col(1, "UserID", crate::constants::TYPE_I64, 0),
        ]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT AVG(ResolutionWidth), COUNT(UserID) FROM t").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        let agg_avg = crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_AVG, 0);
        let agg_count = crate::runtime::agg_key_make(1, crate::constants::AGG_KIND_COUNT, 0);
        assert!(plan.aggregates.contains(&agg_avg));
        assert!(plan.aggregates.contains(&agg_count));
        assert_eq!(plan.aggregates.len(), 2);
    }

    #[test]
    fn apply_aggregate_offsets() {
        let runtime = runtime_with_schema(vec![col(
            0,
            "ResolutionWidth",
            crate::constants::TYPE_I32,
            0,
        )]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0(
            "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth - 2) FROM t",
        )
        .unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        let k0 = crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_SUM, 0);
        let k1 = crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_SUM, 1);
        let k2 = crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_SUM, -2);
        assert!(plan.aggregates.contains(&k0));
        assert!(plan.aggregates.contains(&k1));
        assert!(plan.aggregates.contains(&k2));
        assert_eq!(plan.aggregates.len(), 3);
    }

    #[test]
    fn apply_count_star() {
        let runtime = runtime_with_schema(vec![col(0, "x", crate::constants::TYPE_I32, 0)]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT COUNT(*) FROM t").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        let agg_key = crate::runtime::agg_key_make(
            crate::constants::ROW_COUNT_COL_ID,
            crate::constants::AGG_KIND_COUNT_STAR,
            0,
        );
        assert!(plan.aggregates.contains(&agg_key));
        assert_eq!(plan.aggregates.len(), 1);
    }

    #[test]
    fn apply_approx_count_distinct() {
        let runtime = runtime_with_schema(vec![col(0, "UserID", crate::constants::TYPE_I64, 0)]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT approx_count_distinct(UserID) FROM t").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        let agg_key =
            crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_APPROX_DISTINCT, 0);
        assert!(plan.aggregates.contains(&agg_key));
        assert_eq!(plan.aggregates.len(), 1);
        // HLL state should be initialized
        assert!(plan.hll_state.contains_key(&agg_key));
        // Regular agg_state should NOT contain this key
        assert!(!plan.agg_state.contains_key(&agg_key));
    }

    #[test]
    fn apply_approx_count_distinct_on_string_column() {
        // approx_count_distinct should work on string/dict columns
        let runtime = runtime_with_schema(vec![crate::types::Column {
            id: 0,
            name: "SearchPhrase".to_string(),
            logical_type: crate::constants::TYPE_STRING,
            physical_type: crate::constants::TYPE_STRING,
            flags: crate::constants::FLAG_DICT,
            encoding: 0,
            dict_id: 1,
            dict_index_width: 0,
            scale: 0,
        }]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT approx_count_distinct(SearchPhrase) FROM t").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        let agg_key =
            crate::runtime::agg_key_make(0, crate::constants::AGG_KIND_APPROX_DISTINCT, 0);
        assert!(plan.aggregates.contains(&agg_key));
        assert!(plan.hll_state.contains_key(&agg_key));
    }

    #[test]
    fn apply_full_query() {
        let runtime = runtime_with_schema(vec![
            col(0, "CounterID", crate::constants::TYPE_I32, 0),
            col(1, "EventDate", crate::constants::TYPE_F64, 0),
            col(2, "TraficSourceID", crate::constants::TYPE_I32, 0),
            col(3, "URLHash", crate::constants::TYPE_I64, 0),
            col(4, "ResolutionWidth", crate::constants::TYPE_I32, 0),
        ]);
        let mut plan = empty_plan(1);
        let sql = "SELECT URLHash, EventDate, AVG(ResolutionWidth) FROM t \
                   WHERE CounterID = 62 AND EventDate >= 1000 AND EventDate <= 2000 \
                   AND TraficSourceID IN (-1, 6) \
                   GROUP BY URLHash, EventDate LIMIT 10";
        let query = parse_sql_v0(sql).unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.limit, 10);
        assert_eq!(plan.filters.len(), 4);
        assert!(plan.group_by.is_some());
        assert_eq!(plan.group_by.as_ref().unwrap().keys, vec![3, 1]);
        let agg_avg = crate::runtime::agg_key_make(4, crate::constants::AGG_KIND_AVG, 0);
        assert!(plan.aggregates.contains(&agg_avg));
    }

    #[test]
    fn unknown_column_returns_error() {
        let runtime = runtime_with_schema(vec![col(0, "x", crate::constants::TYPE_F64, 0)]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT 1 FROM t WHERE UnknownCol = 5").unwrap();
        let r = apply_query_to_plan(&query, &mut plan, &runtime);
        assert!(r.is_err());
    }

    #[test]
    fn apply_filter_with_string_value_dict_lookup() {
        let mut dict = crate::types::Dictionary::new();
        dict.lookup.insert("abc".to_string(), 5);
        dict.lookup.insert("xyz".to_string(), 12);
        let mut dicts = FxHashMap::default();
        dicts.insert(1u32, dict);

        let runtime = Runtime {
            header: None,
            schema: Arc::from(vec![crate::types::Column {
                id: 0,
                name: "Label".to_string(),
                logical_type: crate::constants::TYPE_STRING,
                physical_type: crate::constants::TYPE_STRING,
                flags: crate::constants::FLAG_DICT,
                encoding: 0,
                dict_id: 1,
                dict_index_width: 0,
                scale: 0,
            }]),
            toc: vec![],
            dicts,
            index_cache: FxHashMap::default(),
        };
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT 1 FROM t WHERE Label = 'abc'").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].value, 5.0);
    }

    #[test]
    fn apply_filter_with_string_value_deferred_when_dict_missing() {
        let runtime = runtime_with_schema(vec![crate::types::Column {
            id: 0,
            name: "SearchPhrase".to_string(),
            logical_type: crate::constants::TYPE_STRING,
            physical_type: crate::constants::TYPE_STRING,
            flags: 0,
            encoding: 0,
            dict_id: 99,
            dict_index_width: 0,
            scale: 0,
        }]);
        let mut plan = empty_plan(1);
        let query = parse_sql_v0("SELECT 1 FROM t WHERE SearchPhrase <> ''").unwrap();
        apply_query_to_plan(&query, &mut plan, &runtime).unwrap();
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(plan.filters[0].value_str, Some("".to_string()));
    }
}
