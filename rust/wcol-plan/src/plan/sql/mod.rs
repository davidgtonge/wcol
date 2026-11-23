//! DataFusion parse, analyze, optimize, and lower into a [`WcolLogicalPlan`](crate::plan::WcolLogicalPlan).
//!
//! This is the opt-in DataFusion frontend. `plan_query_dispatch` tries it only when the
//! coordinator's [`Frontend`](crate::plan::Frontend) list says so. The engine does not depend
//! on this module.

mod catalog;
mod error;
mod frontend;
mod required;
mod wcol_logical;

pub use catalog::{parquet_physical_to_arrow, WcolCatalog, WcolColumnMeta};
pub use error::{LowerError, PlanError};
pub use frontend::{plan_sql, statement_relations, PlannedQuery};
pub use required::{expr_summary, required_source_columns};
pub use wcol_logical::lower_logical;

use crate::formats::parquet::Plan;
use crate::plan::sql_parser::{OutputItem, OutputSource};
use crate::plan::WcolLogicalPlan;
use wcol_logical::output_columns;

/// Plan `sql` against one Parquet footer.
///
/// The catalog table is the relation DataFusion parsed out of the statement. These queries read
/// one table; that table's columns are the footer leaves, in leaf order.
pub fn logical_plan_from_footer(sql: &str, footer: &Plan) -> Result<WcolLogicalPlan, String> {
    let relations = statement_relations(sql).map_err(|err| err.to_string())?;
    let table = match relations.as_slice() {
        [table] => table.as_str(),
        [] => return Err("sql does not read a table".to_string()),
        many => return Err(format!("sql reads {many:?}")),
    };
    let columns = footer
        .leaves
        .iter()
        .map(|leaf| WcolColumnMeta {
            name: leaf.name.clone(),
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect::<Vec<_>>();
    let catalog = WcolCatalog::single_table(table, &columns);
    let planned = plan_sql(sql, &catalog).map_err(|err| err.to_string())?;
    lower_logical(planned.optimized.as_ref(), &catalog).map_err(|err| err.to_string())
}

/// [`logical_plan_from_footer`] plus the `SELECT` list, read off the optimized plan's output
/// schema. `None` when an output field does not map onto one key, aggregate or source column.
pub fn logical_plan_and_output_from_footer(
    sql: &str,
    footer: &Plan,
) -> Result<(WcolLogicalPlan, Option<Vec<OutputItem>>), String> {
    let relations = statement_relations(sql).map_err(|err| err.to_string())?;
    let table = match relations.as_slice() {
        [table] => table.as_str(),
        [] => return Err("sql does not read a table".to_string()),
        many => return Err(format!("sql reads {many:?}")),
    };
    let columns = footer
        .leaves
        .iter()
        .map(|leaf| WcolColumnMeta {
            name: leaf.name.clone(),
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect::<Vec<_>>();
    let catalog = WcolCatalog::single_table(table, &columns);
    let planned = plan_sql(sql, &catalog).map_err(|err| err.to_string())?;
    let logical =
        lower_logical(planned.optimized.as_ref(), &catalog).map_err(|err| err.to_string())?;
    let plan = planned.optimized.as_ref();
    let output = output_columns(plan).or_else(|| {
        // A row query: each output field is a source column of the one table.
        plan.schema()
            .fields()
            .iter()
            .map(|field| {
                Some(OutputItem {
                    name: field.name().to_string(),
                    source: OutputSource::Column(catalog.column_index(table, field.name())?),
                })
            })
            .collect()
    });
    Ok((logical, output))
}
