//! Query planning against a column list. No file format, dictionaries, or execution state.

use super::{AggFn, BinOp, CmpOp, Expr, Ident, Query};
use crate::ops::query::float_key::float_text_literal;

pub use crate::ir::ColumnKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column<'a> {
    pub name: &'a str,
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar<'a> {
    /// Integer literal kept exact. Values above 2^53 must not round through `f64`.
    Int(i64),
    Number(f64),
    Text(&'a str),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Filter<'a> {
    Compare {
        column: u32,
        op: CmpOp,
        value: Scalar<'a>,
    },
    In {
        column: u32,
        values: Vec<Scalar<'a>>,
        negated: bool,
    },
    Like {
        column: u32,
        pattern: &'a str,
        negated: bool,
    },
}

pub use crate::ir::AggKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Aggregate {
    /// `None` is `COUNT(*)`.
    pub column: Option<u32>,
    pub kind: AggKind,
    pub offset: i8,
}

/// One row `ORDER BY` key. Nulls sort last in both directions, as DuckDB does by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    pub column: u32,
    pub desc: bool,
}

/// What a grouped `ORDER BY` key sorts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSortTarget {
    /// Index into [`QueryPlan::aggregates`].
    Aggregate(usize),
    /// A `GROUP BY` source column.
    Key(u32),
}

/// One `ORDER BY` key on a grouped query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupSortKey {
    pub target: GroupSortTarget,
    pub desc: bool,
}

/// Where one result column comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSource {
    /// Position in [`QueryPlan::group_by`].
    Key(u32),
    /// Index into [`QueryPlan::aggregates`].
    Aggregate(u32),
    /// Source column of a row query.
    Column(u32),
}

/// One result column, in `SELECT` order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputItem {
    pub name: String,
    pub source: OutputSource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan<'a> {
    pub limit: Option<u32>,
    pub offset: u32,
    pub filters: Vec<Filter<'a>>,
    pub group_by: Vec<u32>,
    pub aggregates: Vec<Aggregate>,
    pub select: Vec<u32>,
    /// True when [`Self::group_sort`] is non-empty (legacy flag for the batch kernel).
    pub order_groups_by_count: bool,
    pub group_sort: Vec<GroupSortKey>,
    /// True when the query has a non-group `ORDER BY`, even if no column resolved.
    pub row_order: bool,
    pub order_rows_by: Vec<OrderKey>,
    pub group_approx_column: Option<u32>,
    pub group_count_star: bool,
    pub group_other_agg: bool,
    /// Result columns in `SELECT` order, named by alias or by the expression as DuckDB prints it.
    pub output: Vec<OutputItem>,
}

pub use crate::ir::MAX_GROUP_KEYS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("unsupported sql: {0}")]
    Unsupported(&'static str),
    #[error("unknown column")]
    UnknownColumn,
    #[error("a text literal is compared with a numeric column")]
    TextForNumber,
}

pub fn plan_query<'a>(
    query: &'a Query<'a>,
    columns: &[Column<'a>],
) -> Result<QueryPlan<'a>, PlanError> {
    if query.having.is_some() {
        return Err(PlanError::Unsupported("HAVING"));
    }
    let row_count = |value: u64| {
        u32::try_from(value).map_err(|_| PlanError::Unsupported("LIMIT or OFFSET above 2^32 - 1"))
    };
    let mut plan = QueryPlan {
        limit: query.limit.map(row_count).transpose()?,
        offset: row_count(query.offset.unwrap_or(0))?,
        filters: Vec::new(),
        group_by: Vec::new(),
        aggregates: Vec::new(),
        select: Vec::new(),
        order_groups_by_count: false,
        row_order: false,
        order_rows_by: Vec::new(),
        group_approx_column: None,
        group_count_star: false,
        group_other_agg: false,
        group_sort: Vec::new(),
        output: Vec::new(),
    };

    if let Some(pred) = &query.where_ {
        plan.filters = collect_filters(pred, columns)?;
    }

    for expr in &query.group_by {
        // `GROUP BY 1` names the first SELECT item.
        let expr = match expr {
            Expr::Int(ordinal) => &select_ordinal(*ordinal, &query.select)?.expr,
            other => other,
        };
        if !matches!(expr, Expr::Ident(_)) {
            return Err(PlanError::Unsupported("GROUP BY on an expression"));
        }
        let id = expr_to_col_id(expr, columns).ok_or(PlanError::UnknownColumn)?;
        if plan.group_by.contains(&id) {
            continue;
        }
        if plan.group_by.len() >= MAX_GROUP_KEYS {
            return Err(PlanError::Unsupported("too many GROUP BY keys"));
        }
        plan.group_by.push(id);
    }

    let grouped = !plan.group_by.is_empty();
    for item in &query.select {
        if let Expr::Agg {
            func,
            arg,
            distinct,
        } = &item.expr
        {
            let aggregate = if matches!((func, arg.as_ref()), (AggFn::Count, Expr::Star)) {
                if grouped {
                    plan.group_count_star = true;
                }
                Aggregate {
                    column: None,
                    kind: AggKind::CountStar,
                    offset: 0,
                }
            } else if let Some((column, offset)) = agg_arg(arg, columns) {
                let approx = matches!(func, AggFn::ApproxDistinct);
                if grouped {
                    if approx {
                        if plan.group_approx_column.is_none() {
                            plan.group_approx_column = Some(column);
                        }
                    } else {
                        plan.group_other_agg = true;
                    }
                }
                // Text aggregates stay in the plan; executors reject what they cannot run.
                Aggregate {
                    column: Some(column),
                    kind: if *distinct && matches!(func, AggFn::Count) {
                        AggKind::CountDistinct
                    } else {
                        agg_kind(*func)
                    },
                    offset,
                }
            } else {
                return Err(PlanError::Unsupported("aggregate argument is not a column"));
            };
            if !plan.aggregates.iter().any(|existing| {
                existing.column == aggregate.column
                    && existing.kind == aggregate.kind
                    && existing.offset == aggregate.offset
            }) {
                plan.aggregates.push(aggregate);
            }
        } else if !grouped {
            if matches!(item.expr, Expr::Star) {
                for id in 0..columns.len() as u32 {
                    if !plan.select.contains(&id) {
                        plan.select.push(id);
                    }
                }
            } else if let Some(id) = expr_to_col_id(&item.expr, columns) {
                if !plan.select.contains(&id) {
                    plan.select.push(id);
                }
            }
        }
    }

    // An ungrouped aggregate is one row: its ORDER BY is a no-op.
    let single_row = plan.group_by.is_empty() && !plan.aggregates.is_empty();
    if !query.order_by.is_empty() && !single_row {
        if !plan.group_by.is_empty() {
            for item in &query.order_by {
                let target = resolve_group_order(&item.expr, columns, &query.select, &plan)?;
                plan.group_sort.push(GroupSortKey {
                    target,
                    desc: item.desc,
                });
            }
            plan.order_groups_by_count = !plan.group_sort.is_empty();
        } else {
            plan.row_order = true;
            // Every key must be a plain column: dropping one would return rows in the wrong order.
            for item in &query.order_by {
                let expr = order_by_target(&item.expr, &query.select)?;
                let column = expr_to_col_id(expr, columns).ok_or(PlanError::Unsupported(
                    "ORDER BY on a row query needs a column",
                ))?;
                plan.order_rows_by.push(OrderKey {
                    column,
                    desc: item.desc,
                });
            }
        }
    }

    plan.output = output_items(&query.select, columns, &plan)?;
    Ok(plan)
}

fn select_ordinal<'q, 'a>(
    ordinal: i64,
    select: &'q [super::SelectItem<'a>],
) -> Result<&'q super::SelectItem<'a>, PlanError> {
    usize::try_from(ordinal)
        .ok()
        .and_then(|n| n.checked_sub(1))
        .and_then(|index| select.get(index))
        .ok_or(PlanError::Unsupported(
            "ordinal out of range of the SELECT list",
        ))
}

/// The expression an `ORDER BY` item names: a SELECT ordinal or alias resolves to that
/// item's expression (an alias wins over a source column of the same name), anything else
/// is itself.
fn order_by_target<'q, 'a>(
    order_expr: &'q Expr<'a>,
    select: &'q [super::SelectItem<'a>],
) -> Result<&'q Expr<'a>, PlanError> {
    Ok(match order_expr {
        Expr::Int(ordinal) => &select_ordinal(*ordinal, select)?.expr,
        Expr::Ident(Ident(name)) => select
            .iter()
            .find(|item| item.alias.is_some_and(|alias| alias.0 == *name))
            .map_or(order_expr, |item| &item.expr),
        other => other,
    })
}

fn aggregate_index<'a>(
    expr: &Expr<'a>,
    columns: &[Column<'a>],
    aggregates: &[Aggregate],
) -> Option<usize> {
    let Expr::Agg {
        func,
        arg,
        distinct,
    } = expr
    else {
        return None;
    };
    aggregates
        .iter()
        .position(|aggregate| aggregate_matches_expr(*func, arg, *distinct, aggregate, columns))
}

/// Result columns for the `SELECT` list. A grouped or aggregate query may only select group
/// keys and aggregates; a row query selects source columns (`*` expands to all of them).
fn output_items<'a>(
    select: &[super::SelectItem<'a>],
    columns: &[Column<'a>],
    plan: &QueryPlan<'a>,
) -> Result<Vec<OutputItem>, PlanError> {
    let aggregate_query = !plan.group_by.is_empty() || !plan.aggregates.is_empty();
    let mut out = Vec::with_capacity(select.len());
    for item in select {
        let source = if aggregate_query {
            if let Some(index) = aggregate_index(&item.expr, columns, &plan.aggregates) {
                OutputSource::Aggregate(index as u32)
            } else {
                let not_grouped =
                    PlanError::Unsupported("selected column is neither a key nor an aggregate");
                let id = expr_to_col_id(&item.expr, columns).ok_or(not_grouped)?;
                let key = plan
                    .group_by
                    .iter()
                    .position(|&column| column == id)
                    .ok_or(not_grouped)?;
                OutputSource::Key(key as u32)
            }
        } else if matches!(item.expr, Expr::Star) {
            for (id, column) in columns.iter().enumerate() {
                out.push(OutputItem {
                    name: column.name.to_string(),
                    source: OutputSource::Column(id as u32),
                });
            }
            continue;
        } else {
            OutputSource::Column(expr_to_col_id(&item.expr, columns).ok_or(
                PlanError::Unsupported("selected expression is not a column"),
            )?)
        };
        let name = match item.alias {
            Some(alias) => alias.0.to_string(),
            None => output_name(&item.expr),
        };
        out.push(OutputItem { name, source });
    }
    Ok(out)
}

/// DuckDB's column name for an unaliased expression: `count_star()`, `sum(a)`,
/// `count(DISTINCT a)`, `sum((a + 1))`.
fn output_name(expr: &Expr<'_>) -> String {
    match expr {
        Expr::Star => "*".to_string(),
        Expr::Ident(Ident(name)) => name.to_string(),
        Expr::Int(value) => value.to_string(),
        Expr::Number(value) => value.to_string(),
        Expr::Str(value) => format!("'{value}'"),
        Expr::Agg {
            func: AggFn::Count,
            arg,
            ..
        } if matches!(arg.as_ref(), Expr::Star) => "count_star()".to_string(),
        Expr::Agg {
            func,
            distinct,
            arg,
        } => {
            let name = match func {
                AggFn::Count => "count",
                AggFn::Sum => "sum",
                AggFn::Avg => "avg",
                AggFn::Min => "min",
                AggFn::Max => "max",
                AggFn::ApproxDistinct => "approx_count_distinct",
            };
            let distinct = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct}{})", output_name(arg))
        }
        Expr::Binary { op, left, right } => {
            let op = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
            };
            format!("({} {op} {})", output_name(left), output_name(right))
        }
        _ => "?column?".to_string(),
    }
}

fn column_kind(id: u32, columns: &[Column<'_>]) -> Option<ColumnKind> {
    columns.get(id as usize).map(|column| column.kind)
}

fn find_column(name: &str, columns: &[Column<'_>]) -> Option<u32> {
    columns
        .iter()
        .position(|column| column.name == name)
        .map(|id| id as u32)
}

fn expr_to_col_name<'a>(expr: &'a Expr<'a>) -> Option<&'a str> {
    match expr {
        Expr::Ident(Ident(name)) => Some(*name),
        _ => None,
    }
}

fn expr_to_col_id(expr: &Expr, columns: &[Column<'_>]) -> Option<u32> {
    find_column(expr_to_col_name(expr)?, columns)
}

fn expr_to_scalar<'a>(expr: &'a Expr<'a>) -> Option<Scalar<'a>> {
    match expr {
        Expr::Int(value) => Some(Scalar::Int(*value)),
        Expr::Number(value) => Some(Scalar::Number(*value)),
        Expr::Str(value) => Some(Scalar::Text(value)),
        Expr::Binary {
            op: BinOp::Sub,
            left,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Int(0), Expr::Int(value)) => value.checked_neg().map(Scalar::Int),
            (Expr::Int(0), Expr::Number(value)) => Some(Scalar::Number(-value)),
            _ => None,
        },
        _ => None,
    }
}

fn agg_kind(func: AggFn) -> AggKind {
    match func {
        AggFn::Count => AggKind::Count,
        AggFn::Sum => AggKind::Sum,
        AggFn::Avg => AggKind::Avg,
        AggFn::Min => AggKind::Min,
        AggFn::Max => AggKind::Max,
        AggFn::ApproxDistinct => AggKind::ApproxDistinct,
    }
}

fn agg_arg(expr: &Expr, columns: &[Column<'_>]) -> Option<(u32, i8)> {
    match expr {
        Expr::Ident(_) => expr_to_col_id(expr, columns).map(|id| (id, 0)),
        Expr::Binary { op, left, right } => {
            let (id, offset) = match (op, left.as_ref(), right.as_ref()) {
                (BinOp::Add, Expr::Ident(_), Expr::Int(k)) => (expr_to_col_id(left, columns)?, *k),
                (BinOp::Sub, Expr::Ident(_), Expr::Int(k)) => (expr_to_col_id(left, columns)?, -*k),
                (BinOp::Add, Expr::Int(k), Expr::Ident(_)) => (expr_to_col_id(right, columns)?, *k),
                _ => return None,
            };
            if offset < i8::MIN as i64 || offset > i8::MAX as i64 {
                return None;
            }
            Some((id, offset as i8))
        }
        _ => None,
    }
}

/// A grouped `ORDER BY` key: a SELECT alias, ordinal or expression, an aggregate, or a
/// `GROUP BY` column.
fn resolve_group_order<'a>(
    order_expr: &Expr<'a>,
    columns: &[Column<'a>],
    select: &[super::SelectItem<'a>],
    plan: &QueryPlan<'a>,
) -> Result<GroupSortTarget, PlanError> {
    let expr = order_by_target(order_expr, select)?;
    if let Some(index) = aggregate_index(expr, columns, &plan.aggregates) {
        return Ok(GroupSortTarget::Aggregate(index));
    }
    let not_grouped = PlanError::Unsupported("ORDER BY is neither a key nor an aggregate");
    let id = expr_to_col_id(expr, columns).ok_or(not_grouped)?;
    if plan.group_by.contains(&id) {
        Ok(GroupSortTarget::Key(id))
    } else {
        Err(not_grouped)
    }
}

fn aggregate_matches_expr<'a>(
    func: AggFn,
    arg: &Expr<'a>,
    distinct: bool,
    aggregate: &Aggregate,
    columns: &[Column<'a>],
) -> bool {
    if matches!((func, arg), (AggFn::Count, Expr::Star)) {
        return aggregate.kind == AggKind::CountStar;
    }
    let Some((column, offset)) = agg_arg(arg, columns) else {
        return false;
    };
    if aggregate.column != Some(column) || aggregate.offset != offset {
        return false;
    }
    matches!(
        (func, distinct, aggregate.kind),
        (AggFn::Count, true, AggKind::CountDistinct)
            | (AggFn::Count, false, AggKind::Count)
            | (AggFn::Sum, _, AggKind::Sum)
            | (AggFn::Avg, _, AggKind::Avg)
            | (AggFn::Min, _, AggKind::Min)
            | (AggFn::Max, _, AggKind::Max)
            | (AggFn::ApproxDistinct, _, AggKind::ApproxDistinct)
    )
}

fn collect_filters<'a>(
    pred: &'a Expr<'a>,
    columns: &[Column<'a>],
) -> Result<Vec<Filter<'a>>, PlanError> {
    let mut out = Vec::new();
    collect_filters_impl(pred, columns, &mut out)?;
    Ok(out)
}

fn collect_filters_impl<'a>(
    pred: &'a Expr<'a>,
    columns: &[Column<'a>],
    out: &mut Vec<Filter<'a>>,
) -> Result<(), PlanError> {
    match pred {
        Expr::And(left, right) => {
            collect_filters_impl(left, columns, out)?;
            collect_filters_impl(right, columns, out)?;
        }
        Expr::Compare { op, left, right } => {
            // `col op literal`, or `literal op col` with the comparison mirrored.
            let (name, value, op) = match (
                expr_to_col_name(left),
                expr_to_scalar(right),
                expr_to_col_name(right),
                expr_to_scalar(left),
            ) {
                (Some(name), Some(value), _, _) => (name, value, *op),
                (_, _, Some(name), Some(value)) => (name, value, mirror(*op)),
                _ => {
                    return Err(PlanError::Unsupported(
                        "comparison is not column against literal",
                    ))
                }
            };
            let column = find_column(name, columns).ok_or(PlanError::UnknownColumn)?;
            out.push(Filter::Compare {
                column,
                op,
                value: literal_for_column(value, column, columns)?,
            });
        }
        Expr::In {
            left,
            values,
            negated,
        } => {
            let name =
                expr_to_col_name(left).ok_or(PlanError::Unsupported("IN on an expression"))?;
            let column = find_column(name, columns).ok_or(PlanError::UnknownColumn)?;
            let scalars = values
                .iter()
                .map(|value| {
                    let scalar = expr_to_scalar(value)
                        .ok_or(PlanError::Unsupported("IN list item is not a literal"))?;
                    literal_for_column(scalar, column, columns)
                })
                .collect::<Result<Vec<_>, _>>()?;
            out.push(Filter::In {
                column,
                values: scalars,
                negated: *negated,
            });
        }
        Expr::Like {
            left,
            pattern,
            negated,
        } => {
            let (Some(name), Expr::Str(pattern)) = (expr_to_col_name(left), pattern.as_ref())
            else {
                return Err(PlanError::Unsupported(
                    "LIKE needs a column and a string pattern",
                ));
            };
            let column = find_column(name, columns).ok_or(PlanError::UnknownColumn)?;
            out.push(Filter::Like {
                column,
                pattern,
                negated: *negated,
            });
        }
        // Anything else (functions, arithmetic, bare columns) is not a predicate we can run;
        // dropping it would silently widen the result.
        _ => return Err(PlanError::Unsupported("predicate")),
    }
    Ok(())
}

/// `a < b` is `b > a`.
fn mirror(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        other => other,
    }
}

/// A `'YYYY-MM-DD'` literal against a numeric column means days since 1970-01-01, the
/// storage of Parquet `DATE` (and ClickBench `EventDate`). `'NaN'` and `'inf'` pass through
/// for float columns. Any other text against a number is refused: the executor cannot
/// compare them, and would otherwise match nothing.
fn literal_for_column<'a>(
    value: Scalar<'a>,
    column: u32,
    columns: &[Column<'a>],
) -> Result<Scalar<'a>, PlanError> {
    match value {
        Scalar::Text(text) if column_kind(column, columns) == Some(ColumnKind::Number) => {
            if float_text_literal(text).is_some() {
                return Ok(value);
            }
            date_days(text)
                .map(Scalar::Int)
                .ok_or(PlanError::TextForNumber)
        }
        _ => Ok(value),
    }
}

/// Days since 1970-01-01 for an ISO `YYYY-MM-DD` date (Howard Hinnant's civil-date algorithm).
fn date_days(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = text.get(range)?;
        part.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=month_days).contains(&day) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::sql_parser::parse_sql_v0;

    fn columns() -> Vec<Column<'static>> {
        vec![
            Column {
                name: "CounterID",
                kind: ColumnKind::Number,
            },
            Column {
                name: "Income",
                kind: ColumnKind::Number,
            },
            Column {
                name: "URL",
                kind: ColumnKind::Text,
            },
        ]
    }

    #[test]
    fn date_literals_become_days_for_numeric_columns() {
        assert_eq!(date_days("1970-01-01"), Some(0));
        assert_eq!(date_days("2013-07-01"), Some(15_887));
        assert_eq!(date_days("2000-02-29"), Some(11_016));
        assert_eq!(date_days("1969-12-31"), Some(-1));
        assert_eq!(date_days("2013-02-29"), None);
        assert_eq!(date_days("2013-7-1"), None);
        let query = parse_sql_v0(
            "SELECT COUNT(*) FROM hits WHERE CounterID >= '2013-07-01' AND URL = '2013-07-01'",
        )
        .unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        assert_eq!(
            plan.filters[0],
            Filter::Compare {
                column: 0,
                op: CmpOp::Ge,
                value: Scalar::Int(15_887)
            }
        );
        assert_eq!(
            plan.filters[1],
            Filter::Compare {
                column: 2,
                op: CmpOp::Eq,
                value: Scalar::Text("2013-07-01")
            }
        );
    }

    #[test]
    fn other_text_against_a_number_is_refused() {
        for sql in [
            "SELECT COUNT(*) FROM hits WHERE CounterID >= '2013-07-01 00:00:00'",
            "SELECT COUNT(*) FROM hits WHERE CounterID <> 'x'",
            "SELECT COUNT(*) FROM hits WHERE CounterID IN (1, 'x')",
        ] {
            let query = parse_sql_v0(sql).unwrap();
            assert_eq!(
                plan_query(&query, &columns()).err(),
                Some(PlanError::TextForNumber),
                "{sql}"
            );
        }
    }

    #[test]
    fn literal_on_the_left_mirrors_the_comparison() {
        let query = parse_sql_v0("SELECT COUNT(*) FROM hits WHERE 5 < CounterID").unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        assert_eq!(
            plan.filters[0],
            Filter::Compare {
                column: 0,
                op: CmpOp::Gt,
                value: Scalar::Int(5)
            }
        );
    }

    #[test]
    fn unrecognised_predicates_are_rejected_not_dropped() {
        for sql in [
            "SELECT COUNT(*) FROM hits WHERE CounterID = Income",
            "SELECT COUNT(*) FROM hits WHERE length(URL) > 5",
            "SELECT COUNT(*) FROM hits WHERE CounterID IN (1, Income)",
        ] {
            let query = parse_sql_v0(sql).unwrap();
            assert!(
                matches!(
                    plan_query(&query, &columns()),
                    Err(PlanError::Unsupported(_))
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn a_row_order_by_names_the_select_alias_first() {
        // `Income AS URL`: ORDER BY URL sorts on Income, the output column named URL.
        let query =
            parse_sql_v0("SELECT Income AS URL, URL AS u FROM hits ORDER BY URL, u LIMIT 3")
                .unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        let keys: Vec<u32> = plan.order_rows_by.iter().map(|key| key.column).collect();
        assert_eq!(keys, vec![1, 2]);
        let query = parse_sql_v0("SELECT URL, Income FROM hits ORDER BY 2 DESC").unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        assert_eq!(plan.order_rows_by[0].column, 1);
    }

    #[test]
    fn limits_past_u32_are_rejected_not_clamped() {
        for sql in [
            "SELECT URL FROM hits LIMIT 4294967296",
            "SELECT URL FROM hits LIMIT 1 OFFSET 4294967296",
        ] {
            let query = parse_sql_v0(sql).unwrap();
            assert!(
                matches!(
                    plan_query(&query, &columns()),
                    Err(PlanError::Unsupported(_))
                ),
                "{sql}"
            );
        }
        let query = parse_sql_v0("SELECT URL FROM hits LIMIT 4294967295").unwrap();
        assert_eq!(
            plan_query(&query, &columns()).unwrap().limit,
            Some(u32::MAX)
        );
    }

    #[test]
    fn count_distinct_star_is_rejected() {
        assert!(parse_sql_v0("SELECT COUNT(DISTINCT *) FROM hits").is_err());
    }

    #[test]
    fn count_distinct_is_not_planned_as_count() {
        let query = parse_sql_v0("SELECT COUNT(DISTINCT Income), COUNT(Income) FROM hits").unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        let kinds: Vec<AggKind> = plan.aggregates.iter().map(|a| a.kind).collect();
        assert_eq!(kinds, vec![AggKind::CountDistinct, AggKind::Count]);
    }

    #[test]
    fn star_select_expands_to_every_column_in_catalog_order() {
        let query =
            parse_sql_v0("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY URL LIMIT 10")
                .unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        assert_eq!(plan.select, vec![0, 1, 2]);
        assert_eq!(plan.order_rows_by.len(), 1);
        assert_eq!(plan.order_rows_by[0].column, 2);
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn plans_a_sum_from_names_and_types() {
        let query = parse_sql_v0("SELECT SUM(Income) FROM hits WHERE CounterID = 62").unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        assert_eq!(plan.filters.len(), 1);
        assert_eq!(
            plan.filters[0],
            Filter::Compare {
                column: 0,
                op: CmpOp::Eq,
                value: Scalar::Int(62),
            }
        );
        assert_eq!(
            plan.aggregates,
            vec![Aggregate {
                column: Some(1),
                kind: AggKind::Sum,
                offset: 0,
            }]
        );
    }

    #[test]
    fn keeps_text_aggregates_for_the_executor_to_judge() {
        let query = parse_sql_v0("SELECT SUM(URL), COUNT(*) FROM hits").unwrap();
        let plan = plan_query(&query, &columns()).unwrap();
        let kinds: Vec<AggKind> = plan.aggregates.iter().map(|a| a.kind).collect();
        assert_eq!(kinds, vec![AggKind::Sum, AggKind::CountStar]);
    }
}
