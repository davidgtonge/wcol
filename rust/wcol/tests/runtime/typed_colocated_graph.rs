//! Canonical colocated graph: Parquet → Filter → Aggregate → Reduce → Finalize (no WCS between stages).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use wcol::plan::catalog_from_footer;

use wcol::columns_for_sql;
use wcol::formats::parquet::open_plan;
use wcol::ingest_groups_from_plan;
use wcol::ops::query::{partial_passed_rows, CatalogColumn};
use wcol::ops::{ParquetSourceOp, ReduceOp};
use wcol::plan::IngestMemoryProfile;
use wcol::stream::partials::partial_state_first_sum_i;
use wcol::{GraphPump, LinearColocatedGraph};

fn sql_aggregate_graph(
    batch_rows: usize,
    sql: &str,
    catalog: &[CatalogColumn],
    groups: Vec<wcol::IngestGroup>,
) -> LinearColocatedGraph {
    let source = ParquetSourceOp::new(
        batch_rows,
        catalog.to_vec(),
        groups,
        IngestMemoryProfile::default(),
    );
    LinearColocatedGraph::open_custom(
        Some(Box::new(source)),
        vec![
            Box::new(wcol::plan::bind::open_filter_op(sql, catalog).unwrap()),
            Box::new(wcol::plan::bind::open_aggregate_op(sql, catalog).unwrap()),
        ],
        vec![Box::new(ReduceOp::new())],
    )
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

#[test]
fn typed_graph_sum_a_finalize_row() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let mut graph = sql_aggregate_graph(4, sql, &catalog, groups);
    loop {
        match graph.pump().unwrap() {
            GraphPump::ReadAt {
                ticket,
                offset,
                length,
                source: _,
            } => {
                let mut bytes = vec![0u8; length as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                graph.complete_ingest_read(ticket, bytes).unwrap();
            }
            GraphPump::Progress => {}
            GraphPump::AwaitHost => {}
            GraphPump::RowsReady => panic!("aggregate graph queued rows"),
            GraphPump::Done => break,
        }
    }
    let partial = graph.finish_partial_state().unwrap();
    assert_eq!(partial_state_first_sum_i(&partial, 0).unwrap(), 20);
}

#[test]
fn typed_graph_sum_a_through_finalize() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let mut graph = sql_aggregate_graph(4, sql, &catalog, groups);
    let result = graph
        .run_to_finish(|_ticket, _source, offset, length| {
            let mut bytes = vec![0u8; length as usize];
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.read_exact(&mut bytes).unwrap();
            Ok(bytes)
        })
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn typed_graph_metadata_count() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (_file, plan) = open_plan(&path).unwrap();
    let sql = "SELECT COUNT(*) FROM flat";
    let (dispatch, _) = wcol::plan::plan_query_dispatch(
        sql,
        "flat",
        &plan,
        1,
        4,
        &wcol::plan::PhysicalPlannerConfig::default(),
        &[wcol::plan::Frontend::Sql],
    )
    .unwrap();
    let wcol::plan::QueryDispatch::Partial(pwcl) = dispatch else {
        panic!("COUNT(*) is answered from the footer");
    };
    assert_eq!(partial_passed_rows(&pwcl).unwrap(), plan.rows as u64);
}
