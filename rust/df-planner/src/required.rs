use std::collections::HashSet;

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
