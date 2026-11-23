//! Emit DataFusion catalog JSON for hits subset (planner wasm benchmarks).

use std::io::{self, Write};
use std::path::PathBuf;

use wcol::open_plan;
use wcol_sql::WcolColumnMeta;

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
        });
    if !path.is_file() {
        return Err(format!("missing parquet: {}", path.display()));
    }
    let (_f, plan) = open_plan(&path).map_err(|e| e.to_string())?;
    let columns: Vec<_> = plan
        .leaves
        .iter()
        .map(|leaf| WcolColumnMeta {
            name: leaf.name.clone(),
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();
    let body = serde_json::json!({
        "table": "hits",
        "columns": columns.iter().map(|c| serde_json::json!({
            "name": c.name,
            "physical": c.physical,
            "type_length": c.type_length,
            "max_def": c.max_def,
            "max_rep": c.max_rep,
        })).collect::<Vec<_>>(),
    });
    io::stdout()
        .write_all(body.to_string().as_bytes())
        .map_err(|e| e.to_string())?;
    Ok(())
}
