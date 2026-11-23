use wcol::plan::bind_partition_columns;
use wcol_sql::{job_output_fields, WcolCatalog, WcolColumnMeta};

fn col(name: &str, physical: i32) -> WcolColumnMeta {
    WcolColumnMeta {
        name: name.into(),
        physical,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }
}

fn hits() -> WcolCatalog {
    WcolCatalog::single_table(
        "hits",
        &[
            col("EventTime", 3),
            col("UserID", 2),
            col("EventDate", 1),
        ],
    )
}

#[test]
fn partition_by_binds_to_the_cast_alias() {
    let fields = job_output_fields(
        Some("SELECT CAST(EventTime AS DATE) AS Day, UserID FROM hits"),
        &hits(),
        "hits",
    )
    .expect("cast alias");
    assert_eq!(fields, ["Day", "UserID"]);
    assert_eq!(
        bind_partition_columns(&["Day".into()], &fields).unwrap(),
        vec![0]
    );
    let err = bind_partition_columns(&["EventTime".into()], &fields).unwrap_err();
    assert!(err.contains("not an output column"), "{err}");
}

#[test]
fn identity_output_is_the_catalog_order() {
    let fields = job_output_fields(None, &hits(), "hits").unwrap();
    assert_eq!(fields, ["EventTime", "UserID", "EventDate"]);
    assert_eq!(
        bind_partition_columns(&["EventDate".into()], &fields).unwrap(),
        vec![2]
    );
}

#[test]
fn a_query_must_read_the_source_table() {
    let err = job_output_fields(Some("SELECT UserID FROM other"), &hits(), "hits").unwrap_err();
    assert!(err.contains("other") || err.contains("hits"), "{err}");
}
