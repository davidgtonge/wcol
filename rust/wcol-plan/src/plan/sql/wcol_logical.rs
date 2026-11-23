//! Lower an optimized DataFusion logical plan into a storage-free [`WcolLogicalPlan`].

use crate::plan::{
    AggExpr, AggKind, CmpOp, Compare, Literal, LogicalOp, Predicate, SortKey, SortTarget,
    WcolLogicalPlan,
};
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{BinaryExpr, Expr};
use datafusion_expr::{
    Aggregate, Filter, Join, JoinType, Limit, LogicalPlan, Operator, Projection, Sort, TableScan,
};

use crate::plan::sql_parser::{OutputItem, OutputSource};

use super::catalog::WcolCatalog;
use super::error::LowerError;
use super::required::expr_summary;

pub fn lower_logical(
    plan: &LogicalPlan,
    catalog: &WcolCatalog,
) -> Result<WcolLogicalPlan, LowerError> {
    let mut root = lower(plan, catalog)?;
    // A name-only SELECT list is a project under ORDER BY / LIMIT. Sort keys stay on the
    // scan and the coordinator drops them after the global top-k. Aggregate outputs are not
    // source columns, so those plans keep the tree `lower` already built.
    if !has_aggregate(&root) {
        if let Some(columns) = output_source_columns(plan, &root, catalog)? {
            root = project_output(root, columns);
        }
    }
    Ok(WcolLogicalPlan { root })
}

fn has_aggregate(op: &LogicalOp) -> bool {
    match op {
        LogicalOp::Aggregate { .. } => true,
        LogicalOp::Join { left, right, .. } => has_aggregate(left) || has_aggregate(right),
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. } => has_aggregate(input),
        LogicalOp::Scan { .. } => false,
    }
}

fn output_source_columns(
    plan: &LogicalPlan,
    root: &LogicalOp,
    catalog: &WcolCatalog,
) -> Result<Option<Vec<u32>>, LowerError> {
    let Ok(table) = table_name(root) else {
        return Ok(None);
    };
    let mut columns = Vec::new();
    for field in plan.schema().fields() {
        match catalog.column_index(&table, field.name()) {
            Some(id) => columns.push(id),
            None => return Ok(None),
        }
    }
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(columns))
}

fn project_output(op: LogicalOp, columns: Vec<u32>) -> LogicalOp {
    match op {
        LogicalOp::Limit { input, skip, fetch } => LogicalOp::Limit {
            input: Box::new(project_output(*input, columns)),
            skip,
            fetch,
        },
        LogicalOp::Sort { input, keys } => {
            let key_columns: Vec<u32> = keys
                .iter()
                .filter_map(|key| match key.target {
                    SortTarget::Column(column) => Some(column),
                    SortTarget::Aggregate(_) => None,
                })
                .collect();
            let mut input = *input;
            let _ = cover(&mut input, &key_columns);
            let _ = cover(&mut input, &columns);
            LogicalOp::Sort {
                input: Box::new(align_project(input, columns)),
                keys,
            }
        }
        other => {
            let mut other = other;
            let _ = cover(&mut other, &columns);
            align_project(other, columns)
        }
    }
}

/// `columns` is the SELECT list. When it is already what the scan emits, there is no project.
/// Otherwise that list is the project, and the scan still carries sort keys.
fn align_project(op: LogicalOp, columns: Vec<u32>) -> LogicalOp {
    if scan_column_ids(&op).as_deref() == Some(columns.as_slice()) {
        return strip_identity_project(op);
    }
    match op {
        LogicalOp::Project { input, .. } => LogicalOp::Project { input, columns },
        other => LogicalOp::Project {
            input: Box::new(other),
            columns,
        },
    }
}

fn strip_identity_project(op: LogicalOp) -> LogicalOp {
    match op {
        LogicalOp::Project { input, columns }
            if scan_column_ids(&input).as_deref() == Some(columns.as_slice()) =>
        {
            *input
        }
        other => other,
    }
}

fn scan_column_ids(op: &LogicalOp) -> Option<Vec<u32>> {
    match op {
        LogicalOp::Scan { columns, .. } => Some(columns.clone()),
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. }
        | LogicalOp::Aggregate { input, .. } => scan_column_ids(input),
        LogicalOp::Join { .. } => None,
    }
}

fn lower(plan: &LogicalPlan, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    match plan {
        LogicalPlan::Projection(projection) => lower_projection(projection, catalog),
        LogicalPlan::Filter(filter) => lower_filter(filter, catalog),
        LogicalPlan::Aggregate(aggregate) => lower_aggregate(aggregate, catalog),
        LogicalPlan::Sort(sort) => lower_sort(sort, catalog),
        LogicalPlan::Limit(limit) => lower_limit(limit, catalog),
        LogicalPlan::TableScan(scan) => lower_scan(scan, catalog),
        LogicalPlan::Join(join) => lower_join(join, catalog),
        LogicalPlan::SubqueryAlias(alias) => lower(&alias.input, catalog),
        other => Err(LowerError::UnsupportedOperator(plan_name(other))),
    }
}

fn lower_projection(
    projection: &Projection,
    catalog: &WcolCatalog,
) -> Result<LogicalOp, LowerError> {
    let input = lower(&projection.input, catalog)?;
    if matches!(
        input,
        LogicalOp::Aggregate { .. }
            | LogicalOp::Join { .. }
            | LogicalOp::Sort { .. }
            | LogicalOp::Limit { .. }
    ) {
        if projection.expr.iter().all(is_name_only) {
            return Ok(input);
        }
        return Err(LowerError::UnsupportedExpression(
            "projection is not a column list".into(),
        ));
    }
    let table = table_name(&input)?;
    let mut columns = Vec::with_capacity(projection.expr.len());
    for expr in &projection.expr {
        columns.push(column_id(expr, catalog, &table)?);
    }
    Ok(LogicalOp::Project {
        input: Box::new(input),
        columns,
    })
}

fn lower_filter(filter: &Filter, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    let mut input = lower(&filter.input, catalog)?;
    let table = table_name(&input)?;
    let mut predicates = Vec::new();
    flatten(&filter.predicate, &table, catalog, &mut predicates)?;
    let columns: Vec<u32> = predicates.iter().map(predicate_column).collect();
    if !cover(&mut input, &columns) {
        return Err(LowerError::UnsupportedOperator(
            "filter is not on a scan".into(),
        ));
    }
    Ok(LogicalOp::Filter {
        input: Box::new(input),
        predicates,
    })
}

fn lower_aggregate(aggregate: &Aggregate, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    if let Some(op) = try_lower_count_distinct(aggregate, catalog)? {
        return Ok(op);
    }
    let mut input = lower(&aggregate.input, catalog)?;
    let table = scan_table(&input).map(str::to_string);
    let mut group_by = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        let table = table.as_deref().ok_or_else(|| {
            LowerError::UnsupportedOperator("group by is not on a single scan".into())
        })?;
        group_by.push(column_id(expr, catalog, table)?);
    }
    let mut aggregates = Vec::with_capacity(aggregate.aggr_expr.len());
    let mut needed = group_by.clone();
    for expr in &aggregate.aggr_expr {
        let agg = agg_expr(expr, table.as_deref(), catalog)?;
        if let Some(column) = agg.column {
            needed.push(column);
        }
        aggregates.push(agg);
    }
    if !matches!(input, LogicalOp::Join { .. }) && !cover(&mut input, &needed) {
        return Err(LowerError::UnsupportedOperator(
            "aggregate input is not a scan".into(),
        ));
    }
    Ok(LogicalOp::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
    })
}

fn lower_sort(sort: &Sort, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    if let LogicalPlan::Projection(projection) = sort.input.as_ref() {
        if projection.expr.iter().any(|expr| !is_name_only(expr)) {
            return Err(LowerError::UnsupportedExpression(
                "projection is not a column or aggregate list".into(),
            ));
        }
    }
    let inner = match sort.input.as_ref() {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        other => other,
    };
    let input = lower(inner, catalog)?;
    let table = scan_table(&input).map(str::to_string);
    let mut keys = Vec::with_capacity(sort.expr.len());
    for key in &sort.expr {
        let target = sort_target(&key.expr, sort.input.as_ref(), table.as_deref(), catalog)?;
        keys.push(SortKey {
            target,
            descending: !key.asc,
            nulls_first: key.nulls_first,
        });
    }
    let sorted = LogicalOp::Sort {
        input: Box::new(input),
        keys,
    };
    Ok(match sort.fetch {
        Some(fetch) => LogicalOp::Limit {
            input: Box::new(sorted),
            skip: 0,
            fetch: Some(fetch as u64),
        },
        None => sorted,
    })
}

fn sort_target(
    expr: &Expr,
    sort_input: &LogicalPlan,
    table: Option<&str>,
    catalog: &WcolCatalog,
) -> Result<SortTarget, LowerError> {
    if let Some(table) = table {
        if let Some(column) = source_column(expr, catalog, table) {
            return Ok(SortTarget::Column(column));
        }
    }
    if let Some(index) = aggregate_sort_index(expr, sort_input)? {
        return Ok(SortTarget::Aggregate(index));
    }
    Err(LowerError::UnsupportedExpression(expr_summary(expr)))
}

fn source_column(expr: &Expr, catalog: &WcolCatalog, table: &str) -> Option<u32> {
    let Expr::Column(column) = bare(expr) else {
        return None;
    };
    if column
        .relation
        .as_ref()
        .is_some_and(|relation| relation.table() != table)
    {
        return None;
    }
    catalog.column_index(table, &column.name)
}

fn aggregate_sort_index(expr: &Expr, sort_input: &LogicalPlan) -> Result<Option<u32>, LowerError> {
    let Some(aggregate) = find_aggregate(sort_input) else {
        return Ok(None);
    };
    if let Expr::Column(column) = bare(expr) {
        if let LogicalPlan::Projection(projection) = sort_input {
            let item = projection
                .expr
                .iter()
                .zip(projection.schema.fields().iter())
                .find_map(|(item, field)| (field.name() == &column.name).then_some(item));
            if let Some(item) = item {
                if let Some(signature) = agg_signature(item) {
                    return signature_index(aggregate, signature);
                }
                if let Expr::Column(inner) = bare(item) {
                    return schema_agg_index(aggregate, &inner.name);
                }
            }
            return Ok(None);
        }
        return schema_agg_index(aggregate, &column.name);
    }
    match agg_signature(expr) {
        Some(signature) => signature_index(aggregate, signature),
        None => Ok(None),
    }
}

fn schema_agg_index(aggregate: &Aggregate, name: &str) -> Result<Option<u32>, LowerError> {
    let Some(index) = aggregate
        .schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
    else {
        return Ok(None);
    };
    let groups = aggregate.group_expr.len();
    if index >= groups && index - groups < aggregate.aggr_expr.len() {
        return Ok(Some((index - groups) as u32));
    }
    Ok(None)
}

fn signature_index(
    aggregate: &Aggregate,
    signature: (String, Option<String>),
) -> Result<Option<u32>, LowerError> {
    let mut found = None;
    for (index, candidate) in aggregate.aggr_expr.iter().enumerate() {
        if agg_signature(candidate) == Some(signature.clone()) {
            if found.is_some() {
                return Err(LowerError::UnsupportedExpression(
                    "sort aggregate is ambiguous".into(),
                ));
            }
            found = Some(index as u32);
        }
    }
    Ok(found)
}

/// Result columns of an aggregate plan: each output field is a plain reference to one group
/// key or aggregate value of the (single) aggregate under the top projection.
pub fn output_columns(plan: &LogicalPlan) -> Option<Vec<OutputItem>> {
    let mut top = plan;
    while let LogicalPlan::Sort(Sort { input, .. }) | LogicalPlan::Limit(Limit { input, .. }) = top
    {
        top = input;
    }
    let (exprs, mut below) = match top {
        LogicalPlan::Projection(projection) => (Some(&projection.expr), projection.input.as_ref()),
        other => (None, other),
    };
    while let LogicalPlan::Sort(Sort { input, .. })
    | LogicalPlan::Limit(Limit { input, .. })
    | LogicalPlan::Filter(Filter { input, .. }) = below
    {
        below = input;
    }
    let LogicalPlan::Aggregate(aggregate) = below else {
        return None;
    };
    let keys = aggregate.group_expr.len();
    let inner = aggregate.schema.fields();
    let source = |name: &str| {
        let index = inner.iter().position(|field| field.name() == name)?;
        Some(if index < keys {
            OutputSource::Key(index as u32)
        } else {
            OutputSource::Aggregate((index - keys) as u32)
        })
    };
    let names = plan.schema().fields();
    let sources: Vec<OutputSource> = match exprs {
        Some(exprs) => exprs
            .iter()
            .map(|expr| match expr {
                Expr::Column(column) => source(&column.name),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::Column(column) => source(&column.name),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Option<_>>()?,
        None => (0..inner.len())
            .map(|index| source(inner[index].name()))
            .collect::<Option<_>>()?,
    };
    if sources.len() != names.len() {
        return None;
    }
    Some(
        names
            .iter()
            .zip(sources)
            .map(|(field, source)| OutputItem {
                name: field.name().to_string(),
                source,
            })
            .collect(),
    )
}

fn find_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some(aggregate),
        LogicalPlan::Projection(projection) => find_aggregate(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => find_aggregate(&alias.input),
        _ => None,
    }
}

fn agg_signature(expr: &Expr) -> Option<(String, Option<String>)> {
    let Expr::AggregateFunction(fun) = bare(expr) else {
        return None;
    };
    let argument = match fun.params.args.as_slice() {
        [] => None,
        [arg] if is_star_or_literal(arg) => None,
        [arg] => column_label(arg),
        _ => return None,
    };
    Some((fun.func.name().to_ascii_lowercase(), argument))
}

fn column_label(expr: &Expr) -> Option<String> {
    match bare(expr) {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn lower_limit(limit: &Limit, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    let input = lower(&limit.input, catalog)?;
    let skip = match limit.skip.as_deref() {
        None => 0,
        Some(expr) => literal_u64(expr)?,
    };
    let fetch = match limit.fetch.as_deref() {
        None => None,
        Some(expr) => Some(literal_u64(expr)?),
    };
    // DataFusion pushes `OFFSET s LIMIT n` into the sort as `fetch = s + n` and keeps the outer
    // limit. Fold a limit over a limit into one: skip both offsets, keep the tighter fetch.
    if let LogicalOp::Limit {
        input: inner,
        skip: inner_skip,
        fetch: inner_fetch,
    } = input
    {
        let left_after_skip = inner_fetch.map(|n| n.saturating_sub(skip));
        let fetch = match (left_after_skip, fetch) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        return Ok(LogicalOp::Limit {
            input: inner,
            skip: inner_skip.saturating_add(skip),
            fetch,
        });
    }
    Ok(LogicalOp::Limit {
        input: Box::new(input),
        skip,
        fetch,
    })
}

fn lower_scan(scan: &TableScan, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    let table = scan.table_name.table().to_string();
    let all = catalog
        .table_columns(&table)
        .ok_or_else(|| LowerError::Catalog(format!("unknown table {table}")))?;
    let mut columns = match &scan.projection {
        Some(projection) => projection
            .iter()
            .map(|index| {
                u32::try_from(*index)
                    .map_err(|_| LowerError::Catalog(format!("column index {index}")))
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => (0..all.len() as u32).collect(),
    };
    let mut predicates = Vec::new();
    for filter in &scan.filters {
        flatten(filter, &table, catalog, &mut predicates)?;
    }
    for predicate in &predicates {
        columns.push(predicate_column(predicate));
    }
    columns.sort_unstable();
    columns.dedup();
    let mut op = LogicalOp::Scan { table, columns };
    if !predicates.is_empty() {
        op = LogicalOp::Filter {
            input: Box::new(op),
            predicates,
        };
    }
    Ok(op)
}

fn lower_join(join: &Join, catalog: &WcolCatalog) -> Result<LogicalOp, LowerError> {
    if join.join_type != JoinType::Inner || join.filter.is_some() {
        return Err(LowerError::UnsupportedOperator(
            "only inner equi-joins are supported".into(),
        ));
    }
    if join.on.is_empty() {
        return Err(LowerError::UnsupportedOperator("join needs keys".into()));
    }
    let mut left = lower(&join.left, catalog)?;
    let mut right = lower(&join.right, catalog)?;
    let left_table = table_name(&left)?;
    let right_table = table_name(&right)?;
    let mut left_keys = Vec::with_capacity(join.on.len());
    let mut right_keys = Vec::with_capacity(join.on.len());
    for (left_expr, right_expr) in &join.on {
        left_keys.push(column_id(left_expr, catalog, &left_table)?);
        right_keys.push(column_id(right_expr, catalog, &right_table)?);
    }
    if !cover(&mut left, &left_keys) || !cover(&mut right, &right_keys) {
        return Err(LowerError::UnsupportedOperator(
            "join input is not a scan".into(),
        ));
    }
    Ok(LogicalOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        left_keys,
        right_keys,
    })
}

fn agg_expr(
    expr: &Expr,
    table: Option<&str>,
    catalog: &WcolCatalog,
) -> Result<AggExpr, LowerError> {
    let Expr::AggregateFunction(fun) = bare(expr) else {
        return Err(LowerError::UnsupportedExpression(expr_summary(expr)));
    };
    if fun.params.filter.is_some() || !fun.params.order_by.is_empty() {
        return Err(LowerError::UnsupportedExpression(
            "aggregate options are not supported".into(),
        ));
    }
    if fun.params.distinct {
        if fun.func.name().to_ascii_lowercase() != "count" {
            return Err(LowerError::UnsupportedExpression(
                "distinct is only supported for count".into(),
            ));
        }
        let table = table.ok_or_else(|| {
            LowerError::UnsupportedOperator("aggregate is not on a single scan".into())
        })?;
        let column = match fun.params.args.as_slice() {
            [arg] => Some(column_id(arg, catalog, table)?),
            _ => {
                return Err(LowerError::UnsupportedExpression(
                    "count distinct takes one column".into(),
                ));
            }
        };
        return Ok(AggExpr {
            kind: AggKind::CountDistinct,
            column,
            offset: 0,
        });
    }
    let column = match fun.params.args.as_slice() {
        [] => None,
        [arg] if is_star_or_literal(arg) => None,
        [arg] => {
            let table = table.ok_or_else(|| {
                LowerError::UnsupportedOperator("aggregate is not on a single scan".into())
            })?;
            Some(column_id(arg, catalog, table)?)
        }
        _ => {
            return Err(LowerError::UnsupportedExpression(
                "aggregate takes one column".into(),
            ));
        }
    };
    let kind = match fun.func.name().to_ascii_lowercase().as_str() {
        "count" if column.is_none() => AggKind::CountStar,
        "count" => AggKind::Count,
        "sum" => AggKind::Sum,
        "min" => AggKind::Min,
        "max" => AggKind::Max,
        "avg" => AggKind::Avg,
        "approx_distinct" | "approx_count_distinct" => AggKind::ApproxDistinct,
        _ => {
            return Err(LowerError::UnsupportedExpression(format!(
                "aggregate {}",
                fun.func.name()
            )));
        }
    };
    if kind != AggKind::CountStar && column.is_none() {
        return Err(LowerError::UnsupportedExpression(
            "aggregate argument required".into(),
        ));
    }
    Ok(AggExpr {
        kind,
        column,
        offset: 0,
    })
}

fn flatten(
    expr: &Expr,
    table: &str,
    catalog: &WcolCatalog,
    out: &mut Vec<Predicate>,
) -> Result<(), LowerError> {
    if let Some(column) = case_is_not_null_filter(expr, table, catalog)? {
        out.push(Predicate::IsNotNull { column });
        return Ok(());
    }
    if let Expr::Like(like) = expr {
        out.push(Predicate::Like {
            column: column_id(&like.expr, catalog, table)?,
            pattern: like_pattern(&like.pattern)?,
            negated: like.negated,
        });
        return Ok(());
    }
    if let Expr::InList(in_list) = bare(expr) {
        let mut values = Vec::with_capacity(in_list.list.len());
        for item in &in_list.list {
            values.push(literal(item)?);
        }
        out.push(Predicate::In {
            column: column_id(&in_list.expr, catalog, table)?,
            values,
            negated: in_list.negated,
        });
        return Ok(());
    }
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = bare(expr) {
        if *op == Operator::And {
            flatten(left, table, catalog, out)?;
            flatten(right, table, catalog, out)?;
            return Ok(());
        }
    }
    if let Some(predicate) = null_predicate(expr, table, catalog)? {
        out.push(predicate);
        return Ok(());
    }
    out.push(Predicate::Compare(compare(expr, table, catalog)?));
    Ok(())
}

fn null_predicate(
    expr: &Expr,
    table: &str,
    catalog: &WcolCatalog,
) -> Result<Option<Predicate>, LowerError> {
    match bare(expr) {
        Expr::IsNull(inner) => Ok(Some(Predicate::IsNull {
            column: column_id(inner, catalog, table)?,
        })),
        Expr::IsNotNull(inner) => Ok(Some(Predicate::IsNotNull {
            column: column_id(inner, catalog, table)?,
        })),
        Expr::Not(inner) => match bare(inner) {
            Expr::IsNull(column) => Ok(Some(Predicate::IsNotNull {
                column: column_id(column, catalog, table)?,
            })),
            Expr::IsNotNull(column) => Ok(Some(Predicate::IsNull {
                column: column_id(column, catalog, table)?,
            })),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn predicate_column(predicate: &Predicate) -> u32 {
    match predicate {
        Predicate::Compare(compare) => compare.column,
        Predicate::IsNull { column } | Predicate::IsNotNull { column } => *column,
        Predicate::Like { column, .. } | Predicate::In { column, .. } => *column,
    }
}

fn like_pattern(expr: &Expr) -> Result<String, LowerError> {
    match literal(expr)? {
        Literal::Text(pattern) => Ok(pattern),
        _ => Err(LowerError::UnsupportedExpression(
            "like pattern must be text".into(),
        )),
    }
}

fn case_is_not_null_filter(
    expr: &Expr,
    table: &str,
    catalog: &WcolCatalog,
) -> Result<Option<u32>, LowerError> {
    let Expr::Case(case) = bare(expr) else {
        return Ok(None);
    };
    if case.else_expr.is_some() || case.when_then_expr.len() != 1 {
        return Ok(None);
    }
    let (when, then) = &case.when_then_expr[0];
    if !matches!(
        bare(then),
        Expr::Literal(ScalarValue::Boolean(Some(true)), _)
    ) {
        return Ok(None);
    }
    match bare(when) {
        Expr::IsNotNull(inner) => column_id(inner, catalog, table).map(Some),
        _ => Ok(None),
    }
}

fn try_lower_count_distinct(
    outer: &Aggregate,
    catalog: &WcolCatalog,
) -> Result<Option<LogicalOp>, LowerError> {
    let LogicalPlan::Aggregate(inner) = outer.input.as_ref() else {
        return Ok(None);
    };
    if !inner.aggr_expr.is_empty() {
        return Ok(None);
    }
    if outer.aggr_expr.len() != 1 || outer.group_expr.len() + 1 != inner.group_expr.len() {
        return Ok(None);
    }
    let mut input = lower(&inner.input, catalog)?;
    let table = scan_table(&input)
        .ok_or_else(|| LowerError::UnsupportedOperator("count distinct needs a scan".into()))?
        .to_string();
    let mut outer_groups = Vec::with_capacity(outer.group_expr.len());
    for expr in &outer.group_expr {
        outer_groups.push(column_id(expr, catalog, &table)?);
    }
    let mut inner_groups = Vec::with_capacity(inner.group_expr.len());
    for expr in &inner.group_expr {
        inner_groups.push(column_id(expr, catalog, &table)?);
    }
    if inner_groups[..outer_groups.len()] != outer_groups[..] {
        return Ok(None);
    }
    let distinct_col = *inner_groups.last().ok_or(LowerError::UnsupportedOperator(
        "count distinct inner group is empty".into(),
    ))?;
    let distinct_field = inner.schema.field(inner.group_expr.len() - 1).name();
    let Expr::AggregateFunction(fun) = bare(&outer.aggr_expr[0]) else {
        return Ok(None);
    };
    if fun.func.name().to_ascii_lowercase() != "count" || fun.params.distinct {
        return Ok(None);
    }
    let arg = match fun.params.args.as_slice() {
        [arg] => arg,
        _ => return Ok(None),
    };
    let Expr::Column(column) = bare(arg) else {
        return Ok(None);
    };
    if column.name.as_str() != distinct_field && column.name != "alias1" {
        return Ok(None);
    }
    let mut needed = outer_groups.clone();
    needed.push(distinct_col);
    if !cover(&mut input, &needed) {
        return Err(LowerError::UnsupportedOperator(
            "count distinct input is not a scan".into(),
        ));
    }
    Ok(Some(LogicalOp::Aggregate {
        input: Box::new(input),
        group_by: outer_groups,
        aggregates: vec![AggExpr {
            kind: AggKind::CountDistinct,
            column: Some(distinct_col),
            offset: 0,
        }],
    }))
}

fn compare(expr: &Expr, table: &str, catalog: &WcolCatalog) -> Result<Compare, LowerError> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = bare(expr) else {
        return Err(LowerError::UnsupportedExpression(expr_summary(expr)));
    };
    if matches!(bare(left), Expr::Column(_)) {
        return Ok(Compare {
            column: column_id(left, catalog, table)?,
            op: cmp_op(*op, false)?,
            value: literal(right)?,
        });
    }
    if matches!(bare(right), Expr::Column(_)) {
        return Ok(Compare {
            column: column_id(right, catalog, table)?,
            op: cmp_op(*op, true)?,
            value: literal(left)?,
        });
    }
    Err(LowerError::UnsupportedExpression(
        "compare needs a column and a literal".into(),
    ))
}

fn cmp_op(op: Operator, swap: bool) -> Result<CmpOp, LowerError> {
    let op = match op {
        Operator::Eq => CmpOp::Eq,
        Operator::NotEq => CmpOp::NotEq,
        Operator::Lt => CmpOp::Lt,
        Operator::LtEq => CmpOp::LtEq,
        Operator::Gt => CmpOp::Gt,
        Operator::GtEq => CmpOp::GtEq,
        _ => {
            return Err(LowerError::UnsupportedExpression(
                "comparison operator".into(),
            ));
        }
    };
    Ok(if swap { op.flip() } else { op })
}

fn literal(expr: &Expr) -> Result<Literal, LowerError> {
    match bare(expr) {
        Expr::Literal(value, _) => scalar(value),
        other => Err(LowerError::UnsupportedExpression(expr_summary(other))),
    }
}

fn scalar(value: &ScalarValue) -> Result<Literal, LowerError> {
    match value {
        ScalarValue::Int64(Some(v)) => Ok(Literal::Int(*v)),
        ScalarValue::Int32(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::Date32(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::Int16(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::Int8(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::UInt32(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::UInt16(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::UInt8(Some(v)) => Ok(Literal::Int(i64::from(*v))),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map(Literal::Int).map_err(|_| {
            LowerError::UnsupportedExpression("integer literal does not fit i64".into())
        }),
        ScalarValue::Float64(Some(v)) => Ok(Literal::Float(v.to_bits())),
        ScalarValue::Float32(Some(v)) => Ok(Literal::Float(f64::from(*v).to_bits())),
        ScalarValue::Boolean(Some(v)) => Ok(Literal::Bool(*v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Ok(Literal::Text(v.clone())),
        _ => Err(LowerError::UnsupportedExpression(format!(
            "literal {value}"
        ))),
    }
}

fn literal_u64(expr: &Expr) -> Result<u64, LowerError> {
    match literal(expr)? {
        Literal::Int(value) if value >= 0 => Ok(value as u64),
        _ => Err(LowerError::UnsupportedExpression(
            "limit is not an integer".into(),
        )),
    }
}

fn column_id(expr: &Expr, catalog: &WcolCatalog, table: &str) -> Result<u32, LowerError> {
    let Expr::Column(column) = bare(expr) else {
        return Err(LowerError::UnsupportedExpression(expr_summary(expr)));
    };
    if let Some(relation) = &column.relation {
        if relation.table() != table {
            return Err(LowerError::UnsupportedExpression(format!(
                "column {} is not on {table}",
                column.name
            )));
        }
    }
    catalog
        .column_index(table, &column.name)
        .ok_or_else(|| LowerError::Catalog(format!("unknown column {} on {table}", column.name)))
}

fn is_star_or_literal(expr: &Expr) -> bool {
    match bare(expr) {
        Expr::Literal(_, _) => true,
        #[allow(deprecated)]
        Expr::Wildcard { .. } => true,
        _ => false,
    }
}

fn is_name_only(expr: &Expr) -> bool {
    matches!(
        bare(expr),
        Expr::Column(_) | Expr::AggregateFunction(_) | Expr::Literal(_, _)
    )
}

fn bare(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => bare(&alias.expr),
        Expr::Cast(cast) => bare(&cast.expr),
        other => other,
    }
}

fn table_name(op: &LogicalOp) -> Result<String, LowerError> {
    scan_table(op)
        .map(str::to_string)
        .ok_or_else(|| LowerError::UnsupportedOperator("expression is not on a single scan".into()))
}

fn scan_table(op: &LogicalOp) -> Option<&str> {
    match op {
        LogicalOp::Scan { table, .. } => Some(table),
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. }
        | LogicalOp::Aggregate { input, .. } => scan_table(input),
        LogicalOp::Join { .. } => None,
    }
}

fn cover(op: &mut LogicalOp, extra: &[u32]) -> bool {
    match op {
        LogicalOp::Scan { columns, .. } => {
            columns.extend_from_slice(extra);
            columns.sort_unstable();
            columns.dedup();
            true
        }
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. } => cover(input, extra),
        LogicalOp::Aggregate { .. } | LogicalOp::Join { .. } => false,
    }
}

fn plan_name(plan: &LogicalPlan) -> String {
    plan.display_indent()
        .to_string()
        .lines()
        .next()
        .unwrap_or("unsupported operator")
        .to_string()
}
