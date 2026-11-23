use df_planner::{plan_sql, CatalogColumnMeta, PlannerCatalog};

#[test]
fn clickbench_q3_plans() {
    let columns = vec![
        CatalogColumnMeta {
            name: "AdvEngineID".into(),
            physical: 1,
            type_length: -1,
            max_def: 1,
            max_rep: 0,
        },
        CatalogColumnMeta {
            name: "CounterID".into(),
            physical: 1,
            type_length: -1,
            max_def: 1,
            max_rep: 0,
        },
    ];
    let catalog = PlannerCatalog::single_table("hits", &columns);
    let sql = "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC LIMIT 10";
    let planned = plan_sql(sql, &catalog).expect("plan");
    assert!(planned.optimized_logical.contains("Aggregate"));
    assert!(planned.required_columns.contains(&"AdvEngineID".to_string()));
}
