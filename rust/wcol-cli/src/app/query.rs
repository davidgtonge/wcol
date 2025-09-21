use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde_json::json;
use wcol_decoder::native::{NativeRuntime, QueryResult};

use crate::cli::{NativeExecOpts, QueryFormat};

use super::shared::{agg_value_by_name, apply_native_exec_opts, extract_queries, result_to_json};

pub(crate) fn run_query_cmd(
    file: &Path,
    sql: Option<String>,
    sql_file: Option<PathBuf>,
    workers: usize,
    format: QueryFormat,
    native: NativeExecOpts,
) -> Result<()> {
    apply_native_exec_opts(&native);
    let runtime = NativeRuntime::open(file)
        .with_context(|| format!("opening wcol file {}", file.display()))?;

    if let Some(query) = sql {
        let result = runtime.query_sql_with_workers(&query, workers)?;
        emit_result(&result, format)?;
        return Ok(());
    }

    let sql_path = sql_file.ok_or_else(|| anyhow!("provide --sql or --sql-file"))?;
    let queries = extract_queries(&sql_path)?;
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        match runtime.query_sql_with_workers(&query.sql, workers) {
            Ok(result) => out.push(json!({
                "index": query.index,
                "status": "ok",
                "result": result_to_json(&result),
            })),
            Err(err) => out.push(json!({
                "index": query.index,
                "status": "error",
                "error": err.to_string(),
            })),
        }
    }
    match format {
        QueryFormat::Json => println!("{}", serde_json::to_string_pretty(&out)?),
        QueryFormat::Summary => {
            for item in out {
                let idx = item["index"].as_u64().unwrap_or(0);
                let status = item["status"].as_str().unwrap_or("error");
                if status == "ok" {
                    println!("Q{idx}: ok");
                } else {
                    println!(
                        "Q{idx}: error ({})",
                        item["error"].as_str().unwrap_or("unknown error")
                    );
                }
            }
        }
    }
    Ok(())
}

fn emit_result(result: &QueryResult, format: QueryFormat) -> Result<()> {
    match format {
        QueryFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&result_to_json(result))?);
        }
        QueryFormat::Summary => {
            if let Some(groups) = &result.groups {
                println!(
                    "groups={} aggs_per_group={}",
                    groups.keys.len(),
                    groups.aggs.len()
                );
            } else if !result.aggregates.is_empty() {
                println!("aggregates={}", result.aggregates.len());
                for (name, stats) in &result.aggregates {
                    println!("{name}: {}", agg_value_by_name(name, stats));
                }
            } else {
                let preview = result.rows.iter().take(10).copied().collect::<Vec<_>>();
                println!("rows={} preview={preview:?}", result.rows.len());
            }
        }
    }
    Ok(())
}
