//! Lower a hand-parsed [`QueryPlan`] into a storage-free [`WcolLogicalPlan`].

use crate::plan::physical_plan::{
    AggExpr, AggKind as LogicalAggKind, CmpOp as LogicalCmpOp, Compare, Literal, LogicalOp,
    Predicate, SortKey, SortTarget, WcolLogicalPlan,
};
use crate::plan::sql_parser::{
    AggKind, Aggregate, Filter, GroupSortTarget, PlanError, QueryPlan, Scalar,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LowerQueryPlanError {
    Unsupported,
    UnknownColumn,
    NotIn,
}

impl From<PlanError> for LowerQueryPlanError {
    fn from(value: PlanError) -> Self {
        match value {
            PlanError::Unsupported => Self::Unsupported,
            PlanError::UnknownColumn => Self::UnknownColumn,
            PlanError::NotIn => Self::NotIn,
        }
    }
}

/// Build a logical plan from `table` (the `FROM` name) and a compiled hand-parser plan.
pub fn lower_query_plan(
    table: &str,
    plan: &QueryPlan<'_>,
) -> Result<WcolLogicalPlan, LowerQueryPlanError> {
    let mut columns = column_ids(plan);
    columns.sort_unstable();
    columns.dedup();

    // A row query whose SELECT list is exactly the scanned columns needs no projection.
    let project_needed = plan.select != columns;
    let mut root: LogicalOp = LogicalOp::Scan {
        table: table.to_string(),
        columns,
    };

    if !plan.filters.is_empty() {
        let predicates = plan
            .filters
            .iter()
            .map(filter_to_predicate)
            .collect::<Result<Vec<_>, _>>()?;
        root = LogicalOp::Filter {
            input: Box::new(root),
            predicates,
        };
    }

    let row_query =
        plan.group_by.is_empty() && plan.aggregates.is_empty() && !plan.select.is_empty();
    if row_query && project_needed {
        root = LogicalOp::Project {
            input: Box::new(root),
            columns: plan.select.clone(),
        };
    }

    if !plan.aggregates.is_empty() || !plan.group_by.is_empty() {
        let aggregates = plan.aggregates.iter().map(aggregate_to_logical).collect();
        root = LogicalOp::Aggregate {
            input: Box::new(root),
            group_by: plan.group_by.clone(),
            aggregates,
        };
    }

    if !plan.group_sort.is_empty() {
        let keys = plan
            .group_sort
            .iter()
            .map(|key| SortKey {
                target: match key.target {
                    GroupSortTarget::Aggregate(index) => SortTarget::Aggregate(index as u32),
                    GroupSortTarget::Key(column) => SortTarget::Column(column),
                },
                descending: key.desc,
                nulls_first: false,
            })
            .collect();
        root = LogicalOp::Sort {
            input: Box::new(root),
            keys,
        };
    } else if plan.row_order && !plan.order_rows_by.is_empty() {
        let keys = plan
            .order_rows_by
            .iter()
            .map(|key| SortKey {
                target: SortTarget::Column(key.column),
                descending: key.desc,
                nulls_first: false,
            })
            .collect();
        root = LogicalOp::Sort {
            input: Box::new(root),
            keys,
        };
    }

    if plan.limit.is_some() || plan.offset != 0 {
        root = LogicalOp::Limit {
            input: Box::new(root),
            skip: u64::from(plan.offset),
            fetch: plan.limit.map(u64::from),
        };
    }

    Ok(WcolLogicalPlan { root })
}

/// Plan `sql` against one Parquet footer with the hand parser. This is the default frontend.
pub fn logical_plan_from_footer(
    sql: &str,
    footer: &crate::formats::parquet::Plan,
) -> Result<WcolLogicalPlan, String> {
    logical_plan_and_output_from_footer(sql, footer).map(|(logical, _)| logical)
}

/// [`logical_plan_from_footer`] plus the `SELECT` list as result columns.
pub fn logical_plan_and_output_from_footer(
    sql: &str,
    footer: &crate::formats::parquet::Plan,
) -> Result<(WcolLogicalPlan, Vec<crate::plan::sql_parser::OutputItem>), String> {
    use crate::plan::sql_parser::{parse_sql_v0, plan_query, Column, ColumnKind};

    let query = parse_sql_v0(sql).map_err(|err| {
        format!(
            "sql is not supported by the planner: {} at {}",
            err.msg, err.at
        )
    })?;
    let columns: Vec<Column<'_>> = footer
        .leaves
        .iter()
        .map(|leaf| Column {
            name: leaf.name.as_str(),
            kind: if matches!(leaf.physical, 6 | 7) {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
        })
        .collect();
    let planned = plan_query(&query, &columns).map_err(|err| match err {
        PlanError::Unsupported => {
            "sql is not supported by the planner: unsupported sql".to_string()
        }
        PlanError::UnknownColumn => {
            "sql is not supported by the planner: unknown column".to_string()
        }
        PlanError::NotIn => {
            "sql is not supported by the planner: NOT IN is not supported".to_string()
        }
    })?;
    let logical = lower_query_plan(query.from.0, &planned).map_err(|err| match err {
        LowerQueryPlanError::Unsupported => {
            "sql is not supported by the planner: unsupported sql".to_string()
        }
        LowerQueryPlanError::UnknownColumn => {
            "sql is not supported by the planner: unknown column".to_string()
        }
        LowerQueryPlanError::NotIn => {
            "sql is not supported by the planner: NOT IN is not supported".to_string()
        }
    })?;
    Ok((logical, planned.output))
}

/// Parse SQL with the hand parser, resolve columns, and lower to a logical plan.
pub fn lower_sql_v0_to_logical(
    sql: &str,
    columns: &[crate::plan::sql_parser::Column<'_>],
    table: &str,
) -> Result<WcolLogicalPlan, LowerQueryPlanError> {
    let query =
        crate::plan::sql_parser::parse_sql_v0(sql).map_err(|_| LowerQueryPlanError::Unsupported)?;
    let plan = crate::plan::sql_parser::plan_query(&query, columns)?;
    lower_query_plan(table, &plan)
}

fn column_ids(plan: &QueryPlan<'_>) -> Vec<u32> {
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
            | Filter::Like { column, .. } => {
                push(*column);
            }
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
    for column in &plan.select {
        push(*column);
    }
    for key in &plan.order_rows_by {
        push(key.column);
    }
    ids
}

fn filter_to_predicate(filter: &Filter<'_>) -> Result<Predicate, LowerQueryPlanError> {
    Ok(match filter {
        Filter::Compare { column, op, value } => Predicate::Compare(Compare {
            column: *column,
            op: cmp_to_logical(*op),
            value: scalar_to_literal(*value),
        }),
        Filter::In {
            column,
            values,
            negated,
        } => Predicate::In {
            column: *column,
            values: values.iter().map(|v| scalar_to_literal(*v)).collect(),
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
    })
}

fn cmp_to_logical(op: crate::plan::sql_parser::CmpOp) -> LogicalCmpOp {
    use crate::plan::sql_parser::CmpOp;
    match op {
        CmpOp::Eq => LogicalCmpOp::Eq,
        CmpOp::Ne => LogicalCmpOp::NotEq,
        CmpOp::Lt => LogicalCmpOp::Lt,
        CmpOp::Le => LogicalCmpOp::LtEq,
        CmpOp::Gt => LogicalCmpOp::Gt,
        CmpOp::Ge => LogicalCmpOp::GtEq,
    }
}

fn scalar_to_literal(value: Scalar<'_>) -> Literal {
    match value {
        Scalar::Int(v) => Literal::Int(v),
        Scalar::Number(v) => Literal::Float(v.to_bits()),
        Scalar::Text(v) => Literal::Text(v.to_string()),
    }
}

fn aggregate_to_logical(aggregate: &Aggregate) -> AggExpr {
    AggExpr {
        kind: match aggregate.kind {
            AggKind::CountStar => LogicalAggKind::CountStar,
            AggKind::Count => LogicalAggKind::Count,
            AggKind::Sum => LogicalAggKind::Sum,
            AggKind::Avg => LogicalAggKind::Avg,
            AggKind::Min => LogicalAggKind::Min,
            AggKind::Max => LogicalAggKind::Max,
            AggKind::ApproxDistinct => LogicalAggKind::ApproxDistinct,
            AggKind::CountDistinct => LogicalAggKind::CountDistinct,
        },
        column: aggregate.column,
        offset: aggregate.offset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::physical_plan::{
        lower_to_physical, NodeId, ParquetMetadataCatalog, PhysicalPlannerConfig,
    };
    use crate::plan::sql_parser::{parse_sql_v0, Column, ColumnKind};

    fn hits_columns() -> Vec<Column<'static>> {
        vec![
            Column {
                name: "AdvEngineID",
                kind: ColumnKind::Number,
            },
            Column {
                name: "UserID",
                kind: ColumnKind::Number,
            },
        ]
    }

    #[test]
    fn aggregate_offsets_survive_lowering_and_the_logical_wire() {
        let sql = "SELECT SUM(UserID), SUM(UserID + 1), SUM(UserID + 2) FROM hits";
        let logical = lower_sql_v0_to_logical(sql, &hits_columns(), "hits").unwrap();
        let LogicalOp::Aggregate { aggregates, .. } = &logical.root else {
            panic!("{}", logical.display());
        };
        let offsets: Vec<i8> = aggregates.iter().map(|agg| agg.offset).collect();
        assert_eq!(offsets, vec![0, 1, 2]);
        let text = logical.display();
        assert!(
            text.contains("sum(1 +1)") && text.contains("sum(1 +2)"),
            "{text}"
        );

        let bytes = crate::plan::logical_wire::encode_logical_plan(&logical);
        let back = crate::plan::logical_wire::decode_logical_plan(&bytes).unwrap();
        assert_eq!(back, logical);

        // Physical aggregates lower to the engine IR with the same offsets.
        let ir = crate::plan::to_ir::aggregates(aggregates).unwrap();
        let offsets: Vec<i8> = ir.iter().map(|agg| agg.offset).collect();
        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[test]
    fn count_star_with_filter_lowers_and_physically_plans() {
        let sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0";
        let query = parse_sql_v0(sql).unwrap();
        let plan = crate::plan::sql_parser::plan_query(&query, &hits_columns()).unwrap();
        let logical = lower_query_plan("hits", &plan).unwrap();
        let text = logical.display();
        assert!(text.contains("Filter"), "{text}");
        assert!(text.contains("count(*)"), "{text}");
    }

    #[test]
    fn metadata_count_star_has_no_reads() {
        let sql = "SELECT COUNT(*) FROM hits";
        let logical = lower_sql_v0_to_logical(sql, &hits_columns(), "hits").unwrap();
        let meta = ParquetMetadataCatalog::default();
        let err = lower_to_physical(
            &logical,
            &meta,
            &[NodeId(0)],
            &PhysicalPlannerConfig::default(),
        )
        .err();
        assert!(matches!(
            err,
            Some(crate::plan::physical_plan::PlanError::UnknownTable(_))
        ));
    }
}
