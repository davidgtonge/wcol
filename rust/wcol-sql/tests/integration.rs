//! DataFusion → wcol integration (native + local hits subset).

use std::path::PathBuf;

use wcol::open_plan;
use wcol::columns_for_sql;
use wcol::plan::sql_parser::ColumnKind;
use wcol::CatalogColumn;
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_to_wcol, plan_sql};

#[test]
fn plans_clickbench_subset_on_hits_schema() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    if !path.is_file() {
        eprintln!("skip: missing {}", path.display());
        return;
    }
    let (_f, plan) = open_plan(&path).unwrap();
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

    let q3 = "SELECT AVG(UserID) FROM hits";
    let q1 = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0";

    for sql in [q1, q3] {
        let planned = plan_sql(sql, &catalog).expect(sql);
        assert!(
            !planned.optimized_logical.is_empty(),
            "expected optimized plan for {sql}"
        );
        let lowered = lower_to_wcol(planned.optimized.as_ref(), &catalog).expect(sql);
        let wcol_needed = columns_for_sql(sql, &wcol_catalog).expect(sql);
        for id in &lowered.source_column_ids {
            assert!(
                wcol_needed.columns.contains(id),
                "{sql}: lowered column {id} not in wcol needed set"
            );
        }
    }
}
