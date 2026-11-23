//! Lower DataFusion [`Expr`] into wcol ingest filters (strict: unsupported → error).

use datafusion_common::ScalarValue;
use datafusion_expr::expr::{BinaryExpr, Expr};
use datafusion_expr::Operator;

use crate::catalog::WcolCatalog;
use crate::error::LowerError;
use wcol::plan::ingest_spec::{IngestFilter, IngestScalar};
use wcol::plan::sql_parser::CmpOp;

pub fn lower_filter_expr(
    expr: &Expr,
    table: &str,
    catalog: &WcolCatalog,
) -> Result<IngestFilter, LowerError> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (column_name, literal, op) = compare_parts(left, right, *op)?;
            let column = catalog
                .column_index(table, &column_name)
                .ok_or_else(|| LowerError::Catalog(format!("unknown column {column_name}")))?;
            let value = lower_literal(literal)?;
            Ok(IngestFilter::Compare { column, op, value })
        }
        Expr::Alias(a) => lower_filter_expr(&a.expr, table, catalog),
        Expr::Cast(datafusion_expr::Cast { expr, .. }) => lower_filter_expr(expr, table, catalog),
        other => Err(LowerError::UnsupportedExpression(format!(
            "filter expression: {}",
            crate::required::expr_summary(other)
        ))),
    }
}

pub fn lower_filter_exprs(
    exprs: &[Expr],
    table: &str,
    catalog: &WcolCatalog,
) -> Result<Vec<IngestFilter>, LowerError> {
    let mut out = Vec::new();
    for expr in exprs {
        flatten_and_lower(expr, table, catalog, &mut out)?;
    }
    Ok(out)
}

fn flatten_and_lower(
    expr: &Expr,
    table: &str,
    catalog: &WcolCatalog,
    out: &mut Vec<IngestFilter>,
) -> Result<(), LowerError> {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        if *op == Operator::And {
            flatten_and_lower(left, table, catalog, out)?;
            flatten_and_lower(right, table, catalog, out)?;
            return Ok(());
        }
    }
    out.push(lower_filter_expr(expr, table, catalog)?);
    Ok(())
}

fn compare_parts<'a>(
    left: &'a Expr,
    right: &'a Expr,
    op: Operator,
) -> Result<(String, &'a Expr, CmpOp), LowerError> {
    if let (Some(col), lit) = (column_name(left), right) {
        return Ok((col, lit, df_op(op, false)?));
    }
    if let (Some(col), lit) = (column_name(right), left) {
        return Ok((col, lit, df_op(op, true)?));
    }
    Err(LowerError::UnsupportedExpression(
        "compare needs column and literal".into(),
    ))
}

fn df_op(op: Operator, swap: bool) -> Result<CmpOp, LowerError> {
    Ok(match (op, swap) {
        (Operator::Eq, false) | (Operator::Eq, true) => CmpOp::Eq,
        (Operator::NotEq, false) | (Operator::NotEq, true) => CmpOp::Ne,
        (Operator::Lt, false) => CmpOp::Lt,
        (Operator::Lt, true) => CmpOp::Gt,
        (Operator::LtEq, false) => CmpOp::Le,
        (Operator::LtEq, true) => CmpOp::Ge,
        (Operator::Gt, false) => CmpOp::Gt,
        (Operator::Gt, true) => CmpOp::Lt,
        (Operator::GtEq, false) => CmpOp::Ge,
        (Operator::GtEq, true) => CmpOp::Le,
        _ => {
            return Err(LowerError::UnsupportedExpression(format!(
                "comparison operator {op:?}"
            )));
        }
    })
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(
            c.name
                .rsplit('.')
                .next()
                .unwrap_or(c.name.as_str())
                .to_string(),
        ),
        Expr::Alias(a) => column_name(&a.expr),
        Expr::Cast(datafusion_expr::Cast { expr, .. }) => column_name(expr),
        _ => None,
    }
}

fn lower_literal(expr: &Expr) -> Result<IngestScalar, LowerError> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(IngestScalar::Text(s.clone())),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(IngestScalar::Int(*v)),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(IngestScalar::Int(i64::from(*v))),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(IngestScalar::Int(*v as i64)),
        Expr::Literal(ScalarValue::Float64(Some(v)), _) => Ok(IngestScalar::Number(*v)),
        other => Err(LowerError::UnsupportedExpression(format!(
            "literal: {}",
            crate::required::expr_summary(other)
        ))),
    }
}
