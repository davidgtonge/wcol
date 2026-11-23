//! DataFusion SQL → WCOL execution-graph wire.
//!
//! The coordinator plans here. Node programs in the wire carry no SQL.

use wcol::formats::parquet::plan_footer;
use wcol::plan::{
    catalog_from_footer, plan_execution_wire_from_logical, PhysicalPlannerConfig,
};

use crate::output_schema::scanned_tables;
use wcol::plan::sql::{lower_logical, plan_sql, WcolCatalog, WcolColumnMeta};

/// Plan `sql` against one Parquet footer and return the execution-graph wire.
///
/// `table` is the `sources` key and the SQL `FROM` name. Column order is the footer leaf order.
pub fn plan_df_execution_wire(
    sql: &str,
    table: &str,
    object_id: &str,
    footer_bytes: &[u8],
    node_count: u32,
    batch_rows: u32,
) -> Result<Vec<u8>, String> {
    let footer = plan_footer(footer_bytes).map_err(|err| format!("parquet footer: {err:?}"))?;
    let columns = catalog_from_footer(&footer);
    let metas = columns
        .iter()
        .map(|column| WcolColumnMeta {
            name: column.name.clone(),
            physical: column.physical,
            type_length: column.type_length,
            max_def: column.max_def,
            max_rep: column.max_rep,
        })
        .collect::<Vec<_>>();
    let catalog = WcolCatalog::single_table(table, &metas);
    let planned = plan_sql(sql, &catalog).map_err(|err| err.to_string())?;
    let tables = scanned_tables(planned.optimized.as_ref());
    if tables != [table.to_string()] {
        return Err(format!(
            "query reads {tables:?}; the job source is {table}"
        ));
    }
    let logical =
        lower_logical(planned.optimized.as_ref(), &catalog).map_err(|err| err.to_string())?;
    plan_execution_wire_from_logical(
        &logical,
        object_id,
        &footer,
        node_count,
        batch_rows,
        &PhysicalPlannerConfig::default(),
    )
}
