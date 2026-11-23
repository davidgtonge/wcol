//! Write DataFusion, WCOL logical, and WCOL physical plans for ClickBench SQL.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;

use wcol::open_plan;
use wcol::plan::{
    lower_to_physical, LogicalOp, NodeId, ParquetMetadataCatalog, PhysicalPlannerConfig,
    Predicate, SortTarget,
};
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_logical, plan_sql};

fn note_columns(op: &LogicalOp, ids: &mut BTreeSet<u32>) {
    match op {
        LogicalOp::Scan { columns, .. } => ids.extend(columns.iter().copied()),
        LogicalOp::Filter { input, predicates } => {
            for predicate in predicates {
                ids.insert(match predicate {
                    Predicate::Compare(compare) => compare.column,
                    Predicate::IsNull { column } | Predicate::IsNotNull { column } => *column,
                    Predicate::Like { column, .. } | Predicate::In { column, .. } => *column,
                });
            }
            note_columns(input, ids);
        }
        LogicalOp::Project { input, columns } => {
            ids.extend(columns.iter().copied());
            note_columns(input, ids);
        }
        LogicalOp::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            ids.extend(group_by.iter().copied());
            ids.extend(aggregates.iter().filter_map(|agg| agg.column));
            note_columns(input, ids);
        }
        LogicalOp::Sort { input, keys } => {
            for key in keys {
                if let SortTarget::Column(column) = key.target {
                    ids.insert(column);
                }
            }
            note_columns(input, ids);
        }
        LogicalOp::Limit { input, .. } => note_columns(input, ids),
        LogicalOp::Join {
            left,
            right,
            left_keys,
            right_keys,
        } => {
            ids.extend(left_keys.iter().copied());
            ids.extend(right_keys.iter().copied());
            note_columns(left, ids);
            note_columns(right, ids);
        }
    }
}

fn one_line(err: &impl std::fmt::Display) -> String {
    err.to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let schema = root.join("data/hits_subset_500k.parquet");
    let tsv = root.join("scripts/clickbench-queries.tsv");
    let mut out_path: Option<PathBuf> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--out" {
            out_path = Some(PathBuf::from(args.next().ok_or("--out needs a path")?));
        } else {
            return Err(format!("unknown argument {arg} (use --out <path>)"));
        }
    }

    let (_file, footer) = open_plan(&schema).map_err(|err| err.to_string())?;
    let catalog = catalog_from_wcol_leaves("hits", &footer.leaves);
    let metadata = ParquetMetadataCatalog::from_footer("hits", "hits_subset_500k.parquet", &footer);
    let nodes = [NodeId(0), NodeId(1)];
    let config = PhysicalPlannerConfig::default();
    let row_groups = footer
        .chunks
        .iter()
        .map(|chunk| chunk.row_group)
        .max()
        .map(|id| id + 1)
        .unwrap_or(0);

    let mut supported = String::new();
    let mut skipped = String::new();
    let mut used_columns = BTreeSet::new();
    let mut supported_count = 0usize;
    let mut skipped_count = 0usize;

    for line in fs::read_to_string(&tsv)
        .map_err(|err| err.to_string())?
        .lines()
    {
        if line.is_empty() {
            continue;
        }
        let Some((id, sql)) = line.split_once('\t') else {
            continue;
        };
        let sql = sql.trim().trim_end_matches(';');
        let planned = match plan_sql(sql, &catalog) {
            Ok(planned) => planned,
            Err(err) => {
                skipped_count += 1;
                skipped.push_str(&format!("| {id} | DataFusion | {} |\n", one_line(&err)));
                continue;
            }
        };
        let logical = match lower_logical(planned.optimized.as_ref(), &catalog) {
            Ok(logical) => logical,
            Err(err) => {
                skipped_count += 1;
                skipped.push_str(&format!("| {id} | WCOL logical | {} |\n", one_line(&err)));
                continue;
            }
        };
        let physical = match lower_to_physical(&logical, &metadata, &nodes, &config) {
            Ok(physical) => physical,
            Err(err) => {
                skipped_count += 1;
                skipped.push_str(&format!("| {id} | WCOL physical | {} |\n", one_line(&err)));
                continue;
            }
        };
        supported_count += 1;
        note_columns(&logical.root, &mut used_columns);
        supported.push_str(&format!("## {id}\n\n```sql\n{sql}\n```\n\n"));
        supported.push_str("### DataFusion\n\n```text\n");
        supported.push_str(planned.optimized_logical.trim_end());
        supported.push_str("\n```\n\n### WCOL logical\n\n```text\n");
        supported.push_str(logical.display().trim_end());
        supported.push_str("\n```\n\n### WCOL physical\n\n```text\n");
        supported.push_str(physical.canonical_text().trim_end());
        supported.push_str("\n```\n\n");
    }

    let mut doc = String::new();
    doc.push_str("# ClickBench plans\n\n");
    doc.push_str(
        "Each supported query from `scripts/clickbench-queries.tsv` is shown three times:\n\n",
    );
    doc.push_str("1. **DataFusion** — the optimized logical plan.\n");
    doc.push_str("2. **WCOL logical** — the storage-free plan (`lower_logical`). No byte offsets and no node ids.\n");
    doc.push_str("3. **WCOL physical** — `lower_to_physical` over the cached footer of `data/hits_subset_500k.parquet`.\n\n");
    doc.push_str("Regenerate with `cargo run -p wcol-sql --features native --bin dump-clickbench-plans -- --out docs/CLICKBENCH_PLANS.md` from `rust/`.\n\n");
    doc.push_str(&format!(
        "The file has {row_groups} row groups and {} columns. Physical planning uses two nodes (`0` and `1`), a max gap of 128 KiB, and a max request of 2 MiB. Dictionary pages that the footer folds into the column-chunk span are marked on the logical read. The planner does not read column bytes.\n\n",
        footer.leaves.len()
    ));
    doc.push_str(&format!(
        "{supported_count} queries produce all three plans. {skipped_count} queries stop earlier; the reason is at the bottom.\n\n"
    ));
    doc.push_str("Numbers in a WCOL plan are footer column indexes.\n\n");
    doc.push_str("| Index | Column |\n| --- | --- |\n");
    for id in &used_columns {
        let name = footer
            .leaves
            .get(*id as usize)
            .map(|leaf| leaf.name.as_str())
            .unwrap_or("?");
        doc.push_str(&format!("| {id} | {name} |\n"));
    }
    doc.push('\n');
    doc.push_str(&supported);
    doc.push_str("## Not lowered\n\n");
    doc.push_str("| Query | Stopped at | Reason |\n| --- | --- | --- |\n");
    doc.push_str(&skipped);
    match out_path {
        Some(path) => {
            fs::write(&path, doc).map_err(|err| err.to_string())?;
            eprintln!("wrote {} ({supported_count} supported)", path.display());
        }
        None => print!("{doc}"),
    }
    Ok(())
}
