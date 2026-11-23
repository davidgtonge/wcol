#![cfg(feature = "native")]
//! The two SQL frontends (hand parser and DataFusion) must hand the physical planner the same
//! work for every ClickBench query both accept.

use std::path::PathBuf;

use wcol::formats::parquet::open_plan;
use wcol::plan::sql_parser::{Column, ColumnKind};
use wcol::plan::{
    lower_sql_v0_to_logical, lower_to_physical, LogicalOp, NodeId, ParquetMetadataCatalog,
    PhysicalPlannerConfig, WcolLogicalPlan,
};
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_logical, plan_sql};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn hand_parser_and_datafusion_plan_the_same_physical_work() {
    let schema = root().join("data/hits_subset_500k.parquet");
    if !schema.is_file() {
        return;
    }
    let (_file, footer) = open_plan(&schema).unwrap();
    let df_catalog = catalog_from_wcol_leaves("hits", &footer.leaves);
    let columns: Vec<Column<'static>> = footer
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
        .collect();
    let metadata = ParquetMetadataCatalog::from_footer("hits", "hits_subset_500k.parquet", &footer);
    let nodes = [NodeId(0), NodeId(1)];
    let config = PhysicalPlannerConfig::default();
    let tsv = std::fs::read_to_string(root().join("scripts/clickbench-queries.tsv")).unwrap();

    let mut both = 0usize;
    let mut differ = Vec::new();
    for line in tsv.lines() {
        let Some((label, sql)) = line.split_once('\t') else {
            continue;
        };
        let sql = sql.trim().trim_end_matches(';');
        let Ok(hand) = lower_sql_v0_to_logical(sql, &columns, "hits") else {
            continue;
        };
        let Ok(planned) = plan_sql(sql, &df_catalog) else {
            continue;
        };
        let Ok(df) = lower_logical(planned.optimized.as_ref(), &df_catalog) else {
            continue;
        };
        both += 1;
        // Predicates are ANDed, so their order is an execution choice, not a different query.
        let (hand, df) = (sorted_predicates(hand), sorted_predicates(df));
        let hand_plan = lower_to_physical(&hand, &metadata, &nodes, &config).unwrap();
        let df_plan = lower_to_physical(&df, &metadata, &nodes, &config).unwrap();
        if hand_plan.canonical_text() != df_plan.canonical_text() {
            differ.push(format!(
                "{label}: {sql}\n  hand:\n{}\n  datafusion:\n{}",
                indent(&hand.display()),
                indent(&df.display())
            ));
        }
    }
    eprintln!("frontend parity: {both} queries in both, {} differ", differ.len());
    assert!(both >= 40, "expected most ClickBench queries in both frontends, got {both}");
    assert!(differ.is_empty(), "{}", differ.join("\n\n"));
}

fn indent(text: &str) -> String {
    text.lines().map(|line| format!("    {line}")).collect::<Vec<_>>().join("\n")
}

fn sorted_predicates(mut plan: WcolLogicalPlan) -> WcolLogicalPlan {
    fn walk(op: &mut LogicalOp) {
        match op {
            LogicalOp::Filter { predicates, input } => {
                predicates.sort_by_key(|pred| format!("{pred:?}"));
                walk(input);
            }
            LogicalOp::Scan { .. } => {}
            LogicalOp::Project { input, .. }
            | LogicalOp::Aggregate { input, .. }
            | LogicalOp::Sort { input, .. }
            | LogicalOp::Limit { input, .. } => walk(input),
            LogicalOp::Join { left, right, .. } => {
                walk(left);
                walk(right);
            }
        }
    }
    walk(&mut plan.root);
    plan
}
