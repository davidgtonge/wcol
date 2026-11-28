//! SQL text → engine IR. This is the only place the hand parser meets the batch kernels.
//!
//! Operators are built from the IR this module returns. The engine does not parse SQL.

use crate::ir::{self, AggExpr, CatalogColumn, Predicate, SortKey};
use crate::ops::query::batch::{BatchFilter, BatchQuery};
use crate::ops::{AggregateOp, FilterOp};
use crate::plan::sql_parser::{
    parse_sql_v0, plan_query, Aggregate, Column, Filter, OrderKey, QueryPlan, Scalar,
};

pub struct Needed {
    pub columns: Vec<u32>,
    pub grouped: bool,
}

struct Bound {
    predicates: Vec<Predicate>,
    aggregates: Vec<AggExpr>,
    group_by: Vec<u32>,
    select: Vec<u32>,
    limit: Option<u32>,
    offset: u32,
    order_groups_by_count: bool,
    row_order: bool,
    order_rows_by: Vec<SortKey>,
    referenced: Vec<u32>,
}

pub fn columns_for_sql(sql: &str, catalog: &[CatalogColumn]) -> Result<Needed, String> {
    let bound = bind(sql, catalog)?;
    reject_aggregate(&bound, catalog)?;
    Ok(Needed {
        columns: bound.referenced,
        grouped: !bound.group_by.is_empty(),
    })
}

/// Output column ids in `SELECT` list order for a row-returning query.
pub fn output_columns_for_rows(sql: &str, catalog: &[CatalogColumn]) -> Result<Vec<u32>, String> {
    let bound = bind(sql, catalog)?;
    reject_rows(&bound, catalog)?;
    Ok(bound.select)
}

/// `ORDER BY` columns of a row query. Empty when the query does not order rows.
pub fn row_order_columns(sql: &str, catalog: &[CatalogColumn]) -> Result<Vec<SortKey>, String> {
    let bound = bind(sql, catalog)?;
    reject_rows(&bound, catalog)?;
    Ok(bound.order_rows_by)
}

/// Plan `sql`, then build the batch filter. `Ok(None)` means the predicates are valid
/// but use a shape the kernels do not run.
pub fn open_batch_filter(
    sql: &str,
    catalog: &[CatalogColumn],
) -> Result<Option<BatchFilter>, String> {
    let bound = bind(sql, catalog)?;
    if bound.aggregates.is_empty() && bound.group_by.is_empty() {
        reject_rows(&bound, catalog)?;
    } else {
        reject_aggregate(&bound, catalog)?;
    }
    BatchFilter::from_predicates(&bound.predicates, catalog)
}

/// Plan `sql`, then build the batch aggregate. Filters stay on the query so a filtered
/// `COUNT(*)` is not treated as a footer-only count.
pub fn open_batch_query(sql: &str, catalog: &[CatalogColumn]) -> Result<BatchQuery, String> {
    let bound = bind(sql, catalog)?;
    reject_aggregate(&bound, catalog)?;
    BatchQuery::from_ir(
        &bound.predicates,
        &bound.group_by,
        &bound.aggregates,
        bound.limit,
        bound.offset,
        bound.order_groups_by_count,
        catalog,
    )
}

pub fn open_filter_op(sql: &str, catalog: &[CatalogColumn]) -> Result<FilterOp, String> {
    let filter = open_batch_filter(sql, catalog)?
        .ok_or_else(|| "filter is not supported by the batch kernel".to_string())?;
    Ok(FilterOp::from_batch(filter))
}

pub fn open_aggregate_op(sql: &str, catalog: &[CatalogColumn]) -> Result<AggregateOp, String> {
    Ok(AggregateOp::from_batch(open_batch_query(sql, catalog)?))
}

fn bind(sql: &str, catalog: &[CatalogColumn]) -> Result<Bound, String> {
    let query =
        parse_sql_v0(sql).map_err(|err| format!("sql parse error at {}: {}", err.at, err.msg))?;
    let columns: Vec<Column<'_>> = catalog
        .iter()
        .map(|column| Column {
            name: &column.name,
            kind: column.kind,
        })
        .collect();
    let plan = plan_query(&query, &columns).map_err(|err| err.to_string())?;
    let referenced = referenced(&plan);
    Ok(Bound {
        predicates: plan.filters.iter().map(predicate_from_filter).collect(),
        aggregates: plan
            .aggregates
            .iter()
            .copied()
            .map(agg_from_parser)
            .collect(),
        group_by: plan.group_by,
        select: plan.select,
        limit: plan.limit,
        offset: plan.offset,
        order_groups_by_count: plan.order_groups_by_count,
        row_order: plan.row_order,
        order_rows_by: plan
            .order_rows_by
            .iter()
            .copied()
            .map(sort_key_from_order)
            .collect(),
        referenced,
    })
}

fn predicate_from_filter(filter: &Filter<'_>) -> Predicate {
    match filter {
        Filter::Compare { column, op, value } => Predicate::Compare {
            column: *column,
            op: *op,
            value: scalar_from_parser(*value),
        },
        Filter::In {
            column,
            values,
            negated,
        } => Predicate::In {
            column: *column,
            values: values.iter().copied().map(scalar_from_parser).collect(),
            negated: *negated,
        },
        Filter::Like {
            column,
            pattern,
            negated,
        } => Predicate::Like {
            column: *column,
            pattern: (*pattern).to_string(),
            negated: *negated,
        },
    }
}

fn scalar_from_parser(value: Scalar<'_>) -> ir::Scalar {
    match value {
        Scalar::Int(v) => ir::Scalar::Int(v),
        Scalar::Number(v) => ir::Scalar::Number(v),
        Scalar::Text(v) => ir::Scalar::Text(v.to_string()),
    }
}

fn agg_from_parser(aggregate: Aggregate) -> AggExpr {
    AggExpr {
        column: aggregate.column,
        kind: aggregate.kind,
        offset: aggregate.offset,
    }
}

fn sort_key_from_order(key: OrderKey) -> SortKey {
    SortKey {
        column: key.column,
        descending: key.desc,
        nulls_first: false,
    }
}

fn referenced(plan: &QueryPlan<'_>) -> Vec<u32> {
    let mut ids = Vec::new();
    let mut push = |id: u32| {
        if !ids.contains(&id) {
            ids.push(id);
        }
    };
    for filter in &plan.filters {
        match filter {
            Filter::Compare { column, .. }
            | Filter::In { column, .. }
            | Filter::Like { column, .. } => push(*column),
        }
    }
    for column in &plan.group_by {
        push(*column);
    }
    for aggregate in &plan.aggregates {
        if let Some(column) = aggregate.column {
            push(column);
        }
    }
    ids
}

fn reject_rows(bound: &Bound, catalog: &[CatalogColumn]) -> Result<(), String> {
    if !bound.aggregates.is_empty() || !bound.group_by.is_empty() {
        return Err("row query includes aggregation".to_string());
    }
    if bound.select.is_empty() {
        return Err("row query has no columns".to_string());
    }
    if bound.limit.is_some() || bound.offset != 0 {
        return Err("row ordering and limits are not supported".to_string());
    }
    if bound.row_order {
        if bound.order_rows_by.is_empty() {
            return Err("row ordering and limits are not supported".to_string());
        }
        for key in &bound.order_rows_by {
            if catalog.get(key.column as usize).is_none() {
                return Err("order by column is missing".to_string());
            }
        }
    }
    for column in bound
        .select
        .iter()
        .copied()
        .chain(bound.referenced.iter().copied())
    {
        if catalog.get(column as usize).is_none() {
            return Err("row scan column is missing".to_string());
        }
    }
    Ok(())
}

fn reject_aggregate(bound: &Bound, catalog: &[CatalogColumn]) -> Result<(), String> {
    if !bound.select.is_empty() {
        return Err("row projection is not supported".to_string());
    }
    if bound.aggregates.is_empty() {
        return Err("query has no aggregates".to_string());
    }
    crate::ops::query::validate_aggregate(&bound.group_by, &bound.aggregates, catalog)
}
