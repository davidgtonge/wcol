use std::collections::HashSet;

use datafusion_expr::expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::LogicalPlan;

pub fn required_source_columns(plan: &LogicalPlan) -> Vec<String> {
    let mut cols = HashSet::new();
    let _ = plan.apply_with_subqueries(|node| {
        for expr in node.expressions() {
            let _ = expr_to_columns(&expr, &mut cols);
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    let mut out: Vec<String> = cols
        .into_iter()
        .map(|c| normalize_column_name(&c.name))
        .collect();
    out.sort();
    out
}

fn normalize_column_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

/// Human-readable note for unsupported expression nodes (spike diagnostics).
pub fn expr_summary(expr: &Expr) -> String {
    match expr {
        Expr::Column(c) => format!("column {}", c.name),
        Expr::Literal(v, _) => format!("literal {v}"),
        Expr::BinaryExpr(b) => format!("binary {:?}", b.op),
        Expr::Cast { .. } => "cast".into(),
        Expr::AggregateFunction(agg) => format!("agg {}", agg.func.name()),
        Expr::Alias(a) => expr_summary(&a.expr),
        Expr::IsNull(_) => "is null".into(),
        Expr::IsNotNull(_) => "is not null".into(),
        Expr::Not(_) => "not".into(),
        Expr::Between { .. } => "between".into(),
        Expr::Like { .. } => "like".into(),
        other => format!("{other:?}"),
    }
}
