//! Output field names for a job sink. `partitionBy` binds to these, not to the source catalog.

use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;

use wcol::plan::sql::{plan_sql, WcolCatalog};

/// Final output column names, in order. Aliases and casts keep the name DataFusion assigned.
pub fn output_field_names(plan: &LogicalPlan) -> Vec<String> {
    plan.schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

/// Tables a plan scans. Subqueries are included.
pub fn scanned_tables(plan: &LogicalPlan) -> Vec<String> {
    let mut names = Vec::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            names.push(scan.table_name.table().to_string());
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    names.sort();
    names.dedup();
    names
}

/// Output fields for a job. No SQL is an identity transform: the table's columns, in catalog order.
/// SQL must read `table` and no other table.
pub fn job_output_fields(
    sql: Option<&str>,
    catalog: &WcolCatalog,
    table: &str,
) -> Result<Vec<String>, String> {
    let Some(sql) = sql.map(str::trim).filter(|sql| !sql.is_empty()) else {
        return catalog
            .table_columns(table)
            .map(|columns| columns.iter().map(|column| column.name.clone()).collect())
            .ok_or_else(|| format!("unknown table {table}"));
    };
    let planned = plan_sql(sql, catalog).map_err(|err| err.to_string())?;
    let tables = scanned_tables(planned.optimized.as_ref());
    if tables != [table.to_string()] {
        return Err(format!(
            "query reads {tables:?}; the job source is {table}"
        ));
    }
    Ok(output_field_names(planned.optimized.as_ref()))
}
