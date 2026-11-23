//! Wasm export: SQL + catalog JSON → optimized DataFusion logical plan (no wcol).

use df_planner::{plan_sql, CatalogColumnMeta, PlannerCatalog};
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[derive(serde::Deserialize)]
struct CatalogJson {
    table: String,
    columns: Vec<CatalogColumnMeta>,
}

#[derive(Serialize)]
struct PlanOk {
    ok: bool,
    required_columns: Vec<String>,
    optimized_logical: String,
    analyzed_logical: String,
    raw_logical: String,
}

#[derive(Serialize)]
struct PlanErr {
    ok: bool,
    error: String,
}

fn catalog_from_json(catalog_json: &str) -> Result<PlannerCatalog, String> {
    let parsed: CatalogJson = serde_json::from_str(catalog_json).map_err(|e| e.to_string())?;
    Ok(PlannerCatalog::single_table(&parsed.table, &parsed.columns))
}

/// Parse, analyze, and optimize SQL against a single-table catalog. Returns JSON.
#[wasm_bindgen]
pub fn df_plan_sql(sql: &str, catalog_json: &str) -> String {
    match run_plan(sql, catalog_json) {
        Ok(out) => serde_json::to_string(&out).unwrap_or_else(|e| e.to_string()),
        Err(err) => serde_json::to_string(&PlanErr {
            ok: false,
            error: err,
        })
        .unwrap_or_else(|e| e.to_string()),
    }
}

fn run_plan(sql: &str, catalog_json: &str) -> Result<PlanOk, String> {
    let catalog = catalog_from_json(catalog_json)?;
    let planned = plan_sql(sql, &catalog).map_err(|e| e.to_string())?;
    Ok(PlanOk {
        ok: true,
        required_columns: planned.required_columns,
        optimized_logical: planned.optimized_logical,
        analyzed_logical: planned.analyzed_logical,
        raw_logical: planned.raw_logical,
    })
}

#[wasm_bindgen]
pub fn df_planner_wasm_version() -> u32 {
    1
}
