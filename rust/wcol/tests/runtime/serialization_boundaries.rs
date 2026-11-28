//! PWCL serialization only at physical boundaries (not between colocated query stages).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use wcol::plan::catalog_from_footer;

use wcol::columns_for_sql;
use wcol::formats::parquet::open_plan;
use wcol::ingest_groups_from_plan;
use wcol::ops::query::partial_first_sum_i;
use wcol::stream::PWCL_MAGIC;
use wcol::{GraphPump, LinearColocatedGraph};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

#[test]
fn typed_graph_finishes_in_memory_before_pwcl_edge() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let mut graph = LinearColocatedGraph::open_custom(
        Some(Box::new(wcol::ops::ParquetSourceOp::new(
            4,
            catalog.clone(),
            groups,
            wcol::plan::IngestMemoryProfile::default(),
        ))),
        vec![
            Box::new(wcol::plan::bind::open_filter_op(sql, &catalog).unwrap()),
            Box::new(wcol::plan::bind::open_aggregate_op(sql, &catalog).unwrap()),
        ],
        vec![Box::new(wcol::ops::ReduceOp::new())],
    );

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

    let wire = graph.finish_wire_partial_bytes().unwrap();
    assert_eq!(&wire[..4], PWCL_MAGIC);
    assert_eq!(partial_first_sum_i(&wire).unwrap(), 20);
}
