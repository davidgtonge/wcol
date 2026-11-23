use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    expr::Expr, Aggregate, Filter, Limit, LogicalPlan, Projection, Sort, TableScan,
};

use wcol::plan::sql::{expr_summary, LowerError, WcolCatalog};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WcolLoweredPlan {
    pub table: String,
    /// wcol catalog column indices referenced by scan / filter / group / agg.
    pub source_column_ids: Vec<u32>,
    pub group_by_column_ids: Vec<u32>,
    pub aggregate_summaries: Vec<String>,
    pub filter_summary: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order_by_count: bool,
}

pub fn lower_to_wcol(
    plan: &LogicalPlan,
    catalog: &WcolCatalog,
) -> Result<WcolLoweredPlan, LowerError> {
    let mut limit = None;
    let mut offset = None;
    let mut order_by_count = false;
    let mut current = plan;

    loop {
        match current {
            LogicalPlan::Limit(Limit {
                input, skip, fetch, ..
            }) => {
                offset = skip.as_deref().and_then(literal_usize);
                limit = fetch.as_deref().and_then(literal_usize);
                current = input.as_ref();
            }
            LogicalPlan::Sort(Sort { input, expr, .. }) => {
                order_by_count = expr.iter().any(|sort| match &sort.expr {
                    Expr::AggregateFunction(_) => true,
                    Expr::Alias(a) => matches!(*a.expr, Expr::AggregateFunction(_)),
                    _ => false,
                });
                current = input.as_ref();
            }
            LogicalPlan::Projection(Projection { input, .. }) => {
                current = input.as_ref();
            }
            LogicalPlan::Aggregate(agg) => {
                return lower_aggregate_chain(agg, catalog, limit, offset, order_by_count);
            }
            LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) => {
                if let LogicalPlan::TableScan(scan) = input.as_ref() {
                    return lower_scan_only(
                        scan,
                        Some(predicate),
                        catalog,
                        limit,
                        offset,
                        order_by_count,
                    );
                }
                return Err(LowerError::UnsupportedOperator(
                    "filter not directly on table scan".into(),
                ));
            }
            LogicalPlan::TableScan(scan) => {
                return lower_scan_only(scan, None, catalog, limit, offset, order_by_count);
            }
            other => {
                return Err(LowerError::UnsupportedOperator(
                    other.display_indent().to_string(),
                ));
            }
        }
    }
}

fn lower_aggregate_chain(
    agg: &Aggregate,
    catalog: &WcolCatalog,
    limit: Option<usize>,
    offset: Option<usize>,
    order_by_count: bool,
) -> Result<WcolLoweredPlan, LowerError> {
    let table = table_from_input(agg.input.as_ref())?;
    let group_by_column_ids = agg
        .group_expr
        .iter()
        .map(|expr| column_id_from_expr(expr, catalog, &table))
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate_summaries = agg
        .aggr_expr
        .iter()
        .map(|expr| expr_summary(expr))
        .collect();
    let mut source = group_by_column_ids.clone();
    for expr in agg.aggr_expr.iter().chain(agg.group_expr.iter()) {
        push_expr_columns(expr, catalog, &table, &mut source);
    }
    let (filter_summary, mut filter_cols) = filter_from_input(agg.input.as_ref(), catalog, &table)?;
    source.append(&mut filter_cols);
    source.sort_unstable();
    source.dedup();
    Ok(WcolLoweredPlan {
        table,
        source_column_ids: source,
        group_by_column_ids,
        aggregate_summaries,
        filter_summary,
        limit,
        offset,
        order_by_count,
    })
}

fn lower_scan_only(
    scan: &TableScan,
    extra_predicate: Option<&Expr>,
    catalog: &WcolCatalog,
    limit: Option<usize>,
    offset: Option<usize>,
    order_by_count: bool,
) -> Result<WcolLoweredPlan, LowerError> {
    let table = scan.table_name.table().to_string();
    let mut source = Vec::new();
    for filter in &scan.filters {
        push_expr_columns(filter, catalog, &table, &mut source);
    }
    if let Some(pred) = extra_predicate {
        push_expr_columns(pred, catalog, &table, &mut source);
    }
    source.sort_unstable();
    source.dedup();
    let filter_summary = scan.filters.first().or(extra_predicate).map(expr_summary);
    Ok(WcolLoweredPlan {
        table,
        source_column_ids: source,
        group_by_column_ids: Vec::new(),
        aggregate_summaries: Vec::new(),
        filter_summary,
        limit,
        offset,
        order_by_count,
    })
}

fn literal_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0 => Some(*v as usize),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) if *v >= 0 => Some(*v as usize),
        _ => None,
    }
}

fn table_from_input(plan: &LogicalPlan) -> Result<String, LowerError> {
    match plan {
        LogicalPlan::TableScan(scan) => Ok(scan.table_name.table().to_string()),
        LogicalPlan::Filter(Filter { input, .. }) => table_from_input(input),
        LogicalPlan::Projection(Projection { input, .. }) => table_from_input(input),
        _ => Err(LowerError::UnsupportedOperator(
            "expected scan under aggregate".into(),
        )),
    }
}

fn filter_from_input(
    plan: &LogicalPlan,
    catalog: &WcolCatalog,
    table: &str,
) -> Result<(Option<String>, Vec<u32>), LowerError> {
    match plan {
        LogicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let mut cols = Vec::new();
            push_expr_columns(predicate, catalog, table, &mut cols);
            if matches!(input.as_ref(), LogicalPlan::TableScan(_)) {
                return Ok((Some(expr_summary(predicate)), cols));
            }
            let (inner, mut more) = filter_from_input(input, catalog, table)?;
            cols.append(&mut more);
            Ok((inner.or_else(|| Some(expr_summary(predicate))), cols))
        }
        LogicalPlan::TableScan(_) => Ok((None, Vec::new())),
        LogicalPlan::Projection(Projection { input, .. }) => {
            filter_from_input(input, catalog, table)
        }
        _ => Ok((None, Vec::new())),
    }
}

fn column_id_from_expr(expr: &Expr, catalog: &WcolCatalog, table: &str) -> Result<u32, LowerError> {
    let name = expr_column_name(expr)
        .ok_or_else(|| LowerError::UnsupportedExpression(expr_summary(expr)))?;
    catalog
        .column_index(table, &name)
        .ok_or_else(|| LowerError::Catalog(format!("unknown column {name} on {table}")))
}

fn push_expr_columns(expr: &Expr, catalog: &WcolCatalog, table: &str, out: &mut Vec<u32>) {
    if let Some(name) = expr_column_name(expr) {
        if let Some(id) = catalog.column_index(table, &name) {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    let _ = expr.apply(&mut |e| {
        if let Some(name) = expr_column_name(e) {
            if let Some(id) = catalog.column_index(table, &name) {
                if !out.contains(&id) {
                    out.push(id);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(
            c.name
                .rsplit('.')
                .next()
                .unwrap_or(c.name.as_str())
                .to_string(),
        ),
        Expr::Alias(a) => expr_column_name(&a.expr),
        _ => None,
    }
}
