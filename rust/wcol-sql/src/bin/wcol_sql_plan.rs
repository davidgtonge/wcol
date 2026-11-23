//! Plan ClickBench (or ad hoc) SQL with DataFusion and print lowering diagnostics.

use std::env;
use std::fs;
use std::path::PathBuf;

use wcol::open_plan;
use wcol::columns_for_sql;
use wcol::plan::sql_parser::ColumnKind;
use wcol::CatalogColumn;
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_to_wcol, plan_sql, PlanError};

const DEFAULT_SCHEMA: &str = "../../data/hits_subset_500k.parquet";

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_SCHEMA);
    let mut queries: Vec<String> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--schema" if i + 1 < args.len() => {
                schema_path = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--query" if i + 1 < args.len() => {
                queries.push(args[i + 1].clone());
                i += 2;
            }
            "--clickbench" => {
                queries.extend(default_clickbench());
                i += 1;
            }
            flag => return Err(format!("unknown arg {flag}")),
        }
    }
    if queries.is_empty() {
        queries.extend(default_clickbench());
    }
    if !schema_path.is_file() {
        return Err(format!(
            "schema parquet not found: {} (use --schema PATH)",
            schema_path.display()
        ));
    }

    let (_file, plan) = open_plan(&schema_path).map_err(|e| e.to_string())?;
    let catalog = catalog_from_wcol_leaves("hits", &plan.leaves);
    let wcol_catalog: Vec<CatalogColumn> = plan
        .leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();

    println!(
        "schema: {} ({} columns, footer only)\n",
        schema_path.display(),
        plan.leaves.len()
    );

    for sql in queries {
        println!("======== SQL ========");
        println!("{sql}\n");
        match plan_sql(&sql, &catalog) {
            Ok(planned) => {
                println!("--- parsed ---\n{}\n", planned.parsed_statement);
                println!("--- raw logical ---\n{}\n", planned.raw_logical);
                println!("--- analyzed ---\n{}\n", planned.analyzed_logical);
                println!("--- optimized ---\n{}\n", planned.optimized_logical);
                println!(
                    "--- required columns (DataFusion) ---\n{}\n",
                    planned.required_columns.join(", ")
                );
                match lower_to_wcol(planned.optimized.as_ref(), &catalog) {
                    Ok(lowered) => {
                        println!("--- wcol lowered ---\n{lowered:#?}\n");
                        if let Ok(needed) = columns_for_sql(&sql, &wcol_catalog) {
                            let wcol_names: Vec<_> = needed
                                .columns
                                .iter()
                                .map(|id| wcol_catalog[*id as usize].name.as_str())
                                .collect();
                            println!(
                                "--- wcol columns_for_sql ---\n{}\n",
                                wcol_names.join(", ")
                            );
                        }
                    }
                    Err(err) => println!("--- lowering ---\n{err}\n"),
                }
            }
            Err(PlanError::Plan(msg)) if msg.contains("not found") => {
                println!("plan error: {msg}\n");
            }
            Err(err) => println!("plan error: {err}\n"),
        }
    }
    Ok(())
}

fn default_clickbench() -> Vec<String> {
    let tsv = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/clickbench-queries.tsv");
    let body = fs::read_to_string(&tsv).unwrap_or_default();
    let want = ["Q3", "Q8", "Q12", "Q33"];
    body.lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, '\t');
            let id = parts.next()?;
            if !want.contains(&id) {
                return None;
            }
            let sql = parts.next()?.trim_end_matches(';').to_string();
            Some(sql)
        })
        .collect()
}
