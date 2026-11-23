#![cfg(feature = "datafusion")]

//! Coordinator planner: DataFusion SQL + a Parquet footer → WCOL execution-graph wire.

use wasm_bindgen::prelude::*;
use wcol_sql::plan_df_execution_wire;

#[wasm_bindgen(start)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// DataFusion plan, then the node graph. Each program in the wire carries no SQL.
#[wasm_bindgen]
pub fn wcol_df_plan_graph(
    sql: &str,
    table: &str,
    object_id: &str,
    footer: &[u8],
    node_count: u32,
    batch_rows: u32,
) -> Result<Vec<u8>, JsValue> {
    plan_df_execution_wire(sql, table, object_id, footer, node_count, batch_rows)
        .map_err(|err| JsValue::from_str(&err))
}
