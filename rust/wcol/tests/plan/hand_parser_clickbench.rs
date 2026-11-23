//! Hand-parser frontend → WCOL logical → physical plan for ClickBench SQL.

use std::fs;
use std::path::PathBuf;

use wcol::plan::sql_parser::{Column, ColumnKind};
use wcol::plan::{
    lower_sql_v0_to_logical, lower_to_physical, NodeId, ParquetMetadataCatalog,
    PhysicalPlannerConfig,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn hits_columns(footer: &wcol::formats::parquet::Plan) -> Vec<Column<'static>> {
    footer
        .leaves
        .iter()
        .map(|leaf| Column {
            name: Box::leak(leaf.name.clone().into_boxed_str()),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
        })
        .collect()
}

#[test]
fn hand_parser_lowers_clickbench_queries_to_physical_plans() {
    let schema = root().join("data/hits_subset_500k.parquet");
    if !schema.is_file() {
        eprintln!("skip: missing {}", schema.display());
        return;
    }
    let tsv = root().join("scripts/clickbench-queries.tsv");
    let (_file, footer) = wcol::open_plan(&schema).unwrap();
    let columns = hits_columns(&footer);
    let metadata = ParquetMetadataCatalog::from_footer("hits", "hits_subset_500k.parquet", &footer);
    let nodes = [NodeId(0), NodeId(1)];
    let config = PhysicalPlannerConfig::default();

    let mut lowered = 0usize;
    let mut skipped = 0usize;
    for line in fs::read_to_string(&tsv).unwrap().lines() {
        if line.is_empty() {
            continue;
        }
        let Some((_id, sql)) = line.split_once('\t') else {
            continue;
        };
        let sql = sql.trim().trim_end_matches(';');
        let logical = match lower_sql_v0_to_logical(sql, &columns, "hits") {
            Ok(logical) => logical,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        lower_to_physical(&logical, &metadata, &nodes, &config)
            .unwrap_or_else(|err| panic!("physical plan failed for {sql}: {err}"));
        lowered += 1;
    }
    eprintln!("hand parser: {lowered} physical plans, {skipped} skipped by parser");
    assert!(
        lowered >= 40,
        "expected most ClickBench queries to lower via hand parser, got {lowered}"
    );
}
