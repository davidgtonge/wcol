//! Phase 3: host ReadAt count and bytes match the physical plan fetch list.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use wcol::plan::catalog_from_footer;

use wcol::columns_for_sql;
use wcol::exec::operator::{Operator, OperatorIo, StreamProgress};
use wcol::formats::parquet::open_plan;
use wcol::ops::parquet_source::ParquetSourceOp;
use wcol::plan::sql_parser::ColumnKind;
use wcol::plan::{
    lower_sql_v0_to_logical, lower_to_physical, node_fragments, NodeId, ParquetMetadataCatalog,
    PhysicalPlannerConfig,
};

fn hits_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
}

fn metadata_catalog(footer: &wcol::formats::parquet::schema::Plan) -> ParquetMetadataCatalog {
    ParquetMetadataCatalog::from_footer("hits", "hits_subset_500k.parquet", footer)
}

fn hits_columns(
    footer: &wcol::formats::parquet::schema::Plan,
) -> Vec<wcol::plan::sql_parser::Column<'static>> {
    footer
        .leaves
        .iter()
        .map(|leaf| wcol::plan::sql_parser::Column {
            name: Box::leak(leaf.name.clone().into_boxed_str()),
            kind: if leaf.physical == 6 || leaf.physical == 7 {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
        })
        .collect()
}

fn pump_source(source: &mut ParquetSourceOp, file: &mut File) {
    loop {
        let mut out = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: None,
            input_eof: false,
            output: &mut out,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match source.poll(&mut io).unwrap() {
            StreamProgress::Finished => break,
            StreamProgress::NeedHostRead {
                ticket,
                offset,
                length,
                ..
            } => {
                let mut bytes = vec![0u8; length as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                source.complete_host_read(ticket, bytes).unwrap();
            }
            StreamProgress::HaveOutput | StreamProgress::Blocked | StreamProgress::NeedInput => {}
        }
    }
}

#[test]
fn fetch_stage_host_reads_match_plan_metrics() {
    let path = hits_path();
    if !path.is_file() {
        eprintln!("skip: missing {}", path.display());
        return;
    }
    let sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0";
    let (mut file, footer) = open_plan(&path).unwrap();
    let columns = hits_columns(&footer);
    let logical = lower_sql_v0_to_logical(sql, &columns, "hits").unwrap();
    let config = PhysicalPlannerConfig::default();
    let physical = lower_to_physical(
        &logical,
        &metadata_catalog(&footer),
        &[NodeId(0), NodeId(1)],
        &config,
    )
    .unwrap();

    let fragments = node_fragments(&physical, config.fetch_depth).unwrap();
    for node in [NodeId(0), NodeId(1)] {
        let fragment = fragments.get(&node).expect("node fragment");
        if fragment.fetches.is_empty() {
            continue;
        }
        let groups = fragment.ingest_groups().unwrap();
        let catalog = catalog_from_footer(&footer);
        let mut source = ParquetSourceOp::with_fetch_plan(
            32_768,
            catalog,
            groups,
            Default::default(),
            fragment.fetches.clone(),
            fragment.logical_reads.clone(),
            config.fetch_depth,
        )
        .unwrap();
        pump_source(&mut source, &mut file);
        let metrics = source.fetch_metrics().expect("fetch metrics");
        let planned = physical
            .metrics
            .nodes
            .iter()
            .find(|m| m.node == node)
            .expect("node metrics");
        assert_eq!(
            metrics.host_requests, planned.request_count,
            "node {} host requests",
            node.0
        );
        assert_eq!(
            metrics.host_bytes, planned.fetched_bytes,
            "node {} host bytes",
            node.0
        );
    }
}

#[test]
fn fetch_plan_row_count_matches_legacy_ingest_on_q1() {
    let path = hits_path();
    if !path.is_file() {
        return;
    }
    let sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0";
    let (mut file, footer) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&footer);
    let columns = hits_columns(&footer);
    let needed = columns_for_sql(sql, &catalog).unwrap().columns;
    let legacy_groups = wcol::ingest_groups_from_plan(&footer, &needed);
    let logical = lower_sql_v0_to_logical(sql, &columns, "hits").unwrap();
    let config = PhysicalPlannerConfig::default();
    let physical =
        lower_to_physical(&logical, &metadata_catalog(&footer), &[NodeId(0)], &config).unwrap();
    let fragment = node_fragments(&physical, config.fetch_depth)
        .unwrap()
        .get(&NodeId(0))
        .unwrap()
        .clone();
    let plan_groups = fragment.ingest_groups().unwrap();

    let row_count = |source: &mut ParquetSourceOp, file: &mut File| {
        let mut rows = 0u64;
        loop {
            let mut out = None;
            let mut io = OperatorIo {
                input: None,
                input_slot: None,
                input_eof: false,
                output: &mut out,
                host_bytes: None,
                output_port: None,
                bytes_written: &mut 0,
            };
            match source.poll(&mut io).unwrap() {
                StreamProgress::Finished => break,
                StreamProgress::HaveOutput => {
                    rows += out.unwrap().into_batch().row_count() as u64;
                }
                StreamProgress::NeedHostRead {
                    ticket,
                    offset,
                    length,
                    ..
                } => {
                    let mut bytes = vec![0u8; length as usize];
                    file.seek(SeekFrom::Start(offset)).unwrap();
                    file.read_exact(&mut bytes).unwrap();
                    source.complete_host_read(ticket, bytes).unwrap();
                }
                StreamProgress::Blocked | StreamProgress::NeedInput => {}
            }
        }
        rows
    };

    let mut legacy =
        ParquetSourceOp::new(32_768, catalog.clone(), legacy_groups, Default::default());
    let legacy_rows = row_count(&mut legacy, &mut file);
    file.seek(SeekFrom::Start(0)).unwrap();

    let mut planned = ParquetSourceOp::with_fetch_plan(
        32_768,
        catalog,
        plan_groups,
        Default::default(),
        fragment.fetches,
        fragment.logical_reads,
        config.fetch_depth,
    )
    .unwrap();
    let planned_rows = row_count(&mut planned, &mut file);
    assert_eq!(legacy_rows, planned_rows);
}
