//! I-08: colocated ingest emits multiple batches before EOF (incremental stream).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use wcol::plan::catalog_from_footer;

use wcol::columns_for_sql;
use wcol::formats::parquet::open_plan;
use wcol::ingest_groups_from_plan;
use wcol::stream::partials::partial_state_first_sum_i;
use wcol::{GraphPump, LinearColocatedGraph};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

#[test]
fn typed_graph_emits_multiple_batches_before_finish() {
    let path = fixture_path();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    assert!(plan.rows >= 4, "fixture needs multiple rows for batching");
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let batch_rows = 2usize;
    let mut graph = LinearColocatedGraph::open_custom(
        Some(Box::new(wcol::ops::ParquetSourceOp::new(
            batch_rows,
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

    let mut passed_snapshots = Vec::new();
    let mut progress_events = 0usize;

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
            GraphPump::Progress => {
                progress_events += 1;
                if let Some(passed) = graph.aggregate_passed_rows() {
                    if passed_snapshots.last().copied() != Some(passed) {
                        passed_snapshots.push(passed);
                    }
                }
            }
            GraphPump::AwaitHost => {}
            GraphPump::RowsReady => panic!("aggregate graph queued rows"),
            GraphPump::Done => break,
        }
    }

    assert!(
        progress_events >= 2,
        "expected multiple graph pump progress events, got {progress_events}"
    );
    assert!(
        passed_snapshots.len() >= 2,
        "aggregate should observe rows incrementally: {:?}",
        passed_snapshots
    );
    assert!(
        passed_snapshots.windows(2).any(|w| w[1] > w[0]),
        "passed_rows should increase mid-stream: {:?}",
        passed_snapshots
    );

    let partial = graph.finish_partial_state().unwrap();
    assert_eq!(partial.passed_rows, plan.rows as u64);
    assert_eq!(partial_state_first_sum_i(&partial, 0).unwrap(), 20);
}
