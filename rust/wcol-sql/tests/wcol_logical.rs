#![cfg(feature = "native")]

use wcol::plan::{
    lower_to_physical, ChunkMeta, LogicalOp, NodeId, ParquetMetadataCatalog, ParquetObject,
    PhysicalPlannerConfig, Predicate, TableMeta, WcolLogicalPlan,
};
use wcol_sql::{lower_logical, plan_sql, WcolCatalog, WcolColumnMeta};

fn col(name: &str, physical: i32) -> WcolColumnMeta {
    WcolColumnMeta {
        name: name.into(),
        physical,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }
}

fn respondents() -> WcolCatalog {
    WcolCatalog::single_table(
        "respondents",
        &[col("country", 6), col("income", 2), col("age", 1)],
    )
}

fn has_filter(op: &LogicalOp, column: u32) -> bool {
    match op {
        LogicalOp::Filter { predicates, input } => {
            predicates.iter().any(|pred| match pred {
                Predicate::Compare(compare) => compare.column == column,
                Predicate::IsNull { column: id } | Predicate::IsNotNull { column: id } => {
                    *id == column
                }
                Predicate::Like { column: id, .. } | Predicate::In { column: id, .. } => {
                    *id == column
                }
            }) || has_filter(input, column)
        }
        LogicalOp::Scan { .. } => false,
        LogicalOp::Project { input, .. }
        | LogicalOp::Aggregate { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. } => has_filter(input, column),
        LogicalOp::Join { left, right, .. } => {
            has_filter(left, column) || has_filter(right, column)
        }
    }
}

fn scan_columns(op: &LogicalOp) -> Option<&[u32]> {
    match op {
        LogicalOp::Scan { columns, .. } => Some(columns),
        LogicalOp::Filter { input, .. }
        | LogicalOp::Project { input, .. }
        | LogicalOp::Aggregate { input, .. }
        | LogicalOp::Sort { input, .. }
        | LogicalOp::Limit { input, .. } => scan_columns(input),
        LogicalOp::Join { .. } => None,
    }
}

#[test]
fn datafusion_example_lowers_without_storage() {
    let sql = "SELECT country, AVG(income) FROM respondents WHERE age > 30 GROUP BY country";
    let planned = plan_sql(sql, &respondents()).expect(sql);
    let logical = lower_logical(planned.optimized.as_ref(), &respondents()).expect(sql);
    let text = logical.display();
    assert!(!text.contains("offset"), "{text}");
    assert!(!text.contains("node="), "{text}");
    let LogicalOp::Aggregate {
        group_by,
        aggregates,
        input,
    } = &logical.root
    else {
        panic!("root is not an aggregate:\n{text}");
    };
    assert_eq!(group_by, &vec![0]);
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].column, Some(1));
    assert!(has_filter(input, 2), "{text}");
    assert_eq!(scan_columns(input), Some(&[0, 1, 2][..]), "{text}");

    let plan = lower_to_physical(
        &logical,
        &sample_meta(),
        &[NodeId(0), NodeId(1)],
        &PhysicalPlannerConfig::default(),
    )
    .expect("physical");
    let again = lower_logical(planned.optimized.as_ref(), &respondents()).unwrap();
    assert_eq!(logical, again);
    assert!(plan.canonical_text().contains("exchange"));
    assert!(plan.canonical_text().contains("partial_aggregate"));
    assert_eq!(plan.metrics.request_count, 2);
}

fn sample_meta() -> ParquetMetadataCatalog {
    let mut chunks = Vec::new();
    for row_group in 0..2u32 {
        let base = u64::from(row_group) * 5_000_000;
        for column in 0..3u32 {
            chunks.push(ChunkMeta {
                column,
                row_group,
                offset: base + u64::from(column) * 100,
                length: 64,
                rows: 10,
                dictionary_in_span: false,
                dictionary_page: None,
                codec: 0,
                int_range: None,
            });
        }
    }
    ParquetMetadataCatalog {
        tables: vec![TableMeta {
            table: "respondents".into(),
            objects: vec![ParquetObject {
                object_id: "respondents.parquet".into(),
                chunks,
            }],
        }],
        leaf_physical: Vec::new(),
        leaf_distinct_upper: Vec::new(),
        leaf_min_i64: Vec::new(),
        leaf_max_i64: Vec::new(),
        leaf_min_bytes: Vec::new(),
        leaf_max_bytes: Vec::new(),
    }
}

#[test]
fn datafusion_join_lowers_to_two_scans() {
    let catalog = respondents().with_table("cities", &[col("country", 6), col("name", 6)]);
    let sql = "SELECT respondents.country, cities.name FROM respondents \
               JOIN cities ON respondents.country = cities.country";
    let planned = plan_sql(sql, &catalog).expect(sql);
    let logical: WcolLogicalPlan = lower_logical(planned.optimized.as_ref(), &catalog).expect(sql);
    let text = logical.display();
    assert!(!text.contains("offset"), "{text}");
    let LogicalOp::Join {
        left_keys,
        right_keys,
        ..
    } = &logical.root
    else {
        panic!("root is not a join:\n{text}");
    };
    assert_eq!(left_keys, &vec![0]);
    assert_eq!(right_keys, &vec![0]);
}

fn sort_keys(op: &LogicalOp) -> Option<&[wcol::plan::SortKey]> {
    match op {
        LogicalOp::Sort { keys, .. } => Some(keys),
        LogicalOp::Limit { input, .. } | LogicalOp::Project { input, .. } => sort_keys(input),
        _ => None,
    }
}

#[test]
fn order_by_defaults_to_duckdb_nulls_last() {
    let cases = [
        ("SELECT income FROM respondents ORDER BY income", false, false),
        ("SELECT income FROM respondents ORDER BY income DESC", true, false),
        ("SELECT income FROM respondents ORDER BY income DESC NULLS FIRST", true, true),
        ("SELECT income FROM respondents ORDER BY income ASC NULLS FIRST", false, true),
        ("SELECT country, COUNT(*) AS c FROM respondents GROUP BY country ORDER BY c DESC LIMIT 3", true, false),
    ];
    for (sql, descending, nulls_first) in cases {
        let planned = plan_sql(sql, &respondents()).expect(sql);
        let logical = lower_logical(planned.optimized.as_ref(), &respondents()).expect(sql);
        let keys = sort_keys(&logical.root).unwrap_or_else(|| panic!("{sql}: no sort"));
        assert_eq!(keys.len(), 1, "{sql}");
        assert_eq!(keys[0].descending, descending, "{sql}");
        assert_eq!(keys[0].nulls_first, nulls_first, "{sql}");
    }
}
