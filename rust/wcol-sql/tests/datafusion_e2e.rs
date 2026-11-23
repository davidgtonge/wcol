#![cfg(feature = "native")]
//! DataFusion → WCOL logical → physical plan → operators built from that plan (no SQL), run over
//! the same Parquet file as a one-node physical plan. Partials must be equivalent.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use wcol::formats::parquet::open_plan;
use wcol::ops::query::{encoded_partials_equivalent, CatalogColumn, PartialSnapshot, SelectionMask};
use wcol::ops::{AggregateOp, FilterOp};
use wcol::plan::sql_parser::ColumnKind;
use wcol::plan::{
    lower_to_physical, plan_query_dispatch, Frontend, NodeId, OpKind, ParquetMetadataCatalog,
    PhysicalPlannerConfig, QueryDispatch, WcolPhysicalPlan,
};
use wcol::stream::core::Action;
use wcol::stream::partial_wcs::partial_state_to_wcs_payload;
use wcol::stream::partials::partial_snapshot_from_wire;
use wcol::{ingest_groups_from_plan, open_wcol_plan, OwnedWcolBatch, ParquetDecode};
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_logical, plan_sql};

const Q3: &str = "SELECT AVG(UserID) FROM hits";
const Q12: &str =
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10";
const Q8_DISTINCT: &str =
    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10";

fn hits_path() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    path.is_file().then_some(path)
}

fn duckdb_fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures").join(name)
}

fn catalog_of(plan: &wcol::formats::parquet::Plan) -> Vec<CatalogColumn> {
    plan.leaves
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
        .collect()
}

fn metadata_of(table: &str, path: &Path, plan: &wcol::formats::parquet::Plan) -> ParquetMetadataCatalog {
    ParquetMetadataCatalog::from_footer(table, &path.file_name().unwrap().to_string_lossy(), &plan)
}

/// One-node physical plan for `sql`, planned from the footer only.
fn physical_for_sql(sql: &str, table: &str, path: &Path) -> WcolPhysicalPlan {
    let (_file, parquet_plan) = open_plan(path).unwrap();
    let df_catalog = catalog_from_wcol_leaves(table, &parquet_plan.leaves);
    let planned = plan_sql(sql, &df_catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let logical = lower_logical(planned.optimized.as_ref(), &df_catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    lower_to_physical(
        &logical,
        &metadata_of(table, path, &parquet_plan),
        &[NodeId(0)],
        &PhysicalPlannerConfig::default(),
    )
    .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

/// The node's filter and partial aggregate, plus the coordinator's limit and ordering, built
/// with the typed constructors. Also returns the columns the node decodes.
fn operators_from_plan(
    plan: &WcolPhysicalPlan,
    catalog: &[CatalogColumn],
) -> (Option<FilterOp>, AggregateOp, Vec<u32>) {
    let mut filter = None;
    let mut partial = None;
    let mut columns = Vec::new();
    let mut limit = None;
    let mut offset = 0u32;
    let mut sorted = false;
    for op in &plan.operators {
        match &op.kind {
            OpKind::Decode { groups } => {
                for group in groups {
                    for column in &group.columns {
                        if !columns.contains(column) {
                            columns.push(*column);
                        }
                    }
                }
            }
            OpKind::Filter { predicates } => {
                assert!(filter.is_none(), "one filter per node fragment");
                filter = Some(
                    FilterOp::from_predicates(
                        &wcol::plan::to_ir::predicates(predicates).unwrap(),
                        catalog,
                    )
                    .unwrap(),
                );
            }
            OpKind::PartialAggregate { group_by, aggregates } => {
                partial = Some((group_by.clone(), aggregates.clone()));
            }
            OpKind::Limit { skip, fetch } => {
                offset = u32::try_from(*skip).unwrap();
                limit = fetch.map(|n| u32::try_from(n).unwrap());
            }
            OpKind::Sort { .. } => sorted = true,
            _ => {}
        }
    }
    let (group_by, aggregates) = partial.expect("plan has a partial aggregate");
    let order_groups_by_count = sorted && !group_by.is_empty();
    let aggregate =
        AggregateOp::from_aggregate(
            &group_by,
            &wcol::plan::to_ir::aggregates(&aggregates).unwrap(),
            limit,
            offset,
            order_groups_by_count,
            catalog,
        )
            .unwrap();
    columns.sort_unstable();
    (filter, aggregate, columns)
}

/// Run the plan-built operators over every row group of `path`. Returns the PWCL partial.
fn run_plan_operators(path: &Path, sql: &str, table: &str, batch_rows: usize) -> Vec<u8> {
    let (_f, parquet_plan) = open_plan(path).unwrap();
    let catalog = catalog_of(&parquet_plan);
    let physical = physical_for_sql(sql, table, path);
    let (mut filter, mut aggregate, columns) = operators_from_plan(&physical, &catalog);
    let groups = ingest_groups_from_plan(&parquet_plan, &columns);
    let mut decode = ParquetDecode::from_ingest_groups(batch_rows, catalog, &groups);
    let mut file = std::fs::File::open(path).unwrap();
    loop {
        let mut batch = OwnedWcolBatch::empty();
        if decode.fill_batch(&mut batch).unwrap() {
            let mask = match filter.as_mut() {
                Some(filter) => filter.selection_mask_for_batch(&batch).unwrap(),
                None => SelectionMask::all(batch.row_count()),
            };
            aggregate.consume_row_batch(&batch, &mask).unwrap();
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        let read = decode.host_read_request().unwrap().expect("decoder needs bytes");
        let mut bytes = vec![0u8; read.length as usize];
        file.seek(SeekFrom::Start(read.offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        decode.complete_read(read.ticket, &bytes).unwrap();
    }
    partial_state_to_wcs_payload(
        &aggregate
            .finish_partial_state()
            .unwrap()
            .expect_ready("e2e aggregate does not spill"),
    )
    .unwrap()
}

/// Hand-parser physical plan, one node, run by GraphHost. Returns the last PWCL frame.
fn run_planned_sql(path: &Path, batch_rows: u32, sql: &str) -> Vec<u8> {
    use wcol::stream::core::{WCS_HEADER_BYTES, WCS_MAGIC};
    let (_f, parquet_plan) = open_plan(path).unwrap();
    let (dispatch, _) = plan_query_dispatch(
        sql,
        "oracle",
        &parquet_plan,
        1,
        batch_rows,
        &PhysicalPlannerConfig::default(),
        &[Frontend::Sql],
    )
    .unwrap();
    let program = match dispatch {
        QueryDispatch::Partial(partial) => return partial,
        QueryDispatch::Nodes(programs) => {
            assert_eq!(programs.len(), 1, "{sql}");
            programs.into_iter().next().unwrap().program
        }
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => panic!("{sql} is a row query"),
    };
    let mut file = std::fs::File::open(path).unwrap();
    let mut pipe = open_wcol_plan(&program, 512 * 1024).unwrap();
    let mut wire = Vec::new();
    for _ in 0..500_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                let dst = pipe.input();
                dst[..bytes.len()].copy_from_slice(&bytes);
                pipe.commit_input(bytes.len()).unwrap();
                pipe.complete_read(ticket).unwrap();
            }
            Action::NeedInput | Action::Yield => {}
            Action::HaveOutput => {
                wire.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Finished => break,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", pipe.error().unwrap()),
        }
    }
    let mut offset = 0usize;
    let mut last = None;
    while offset + WCS_HEADER_BYTES <= wire.len() {
        let hdr = &wire[offset..offset + WCS_HEADER_BYTES];
        if u32::from_le_bytes(hdr[..4].try_into().unwrap()) != WCS_MAGIC {
            break;
        }
        let frame_type = hdr[5];
        let payload_len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let end = offset + WCS_HEADER_BYTES + payload_len;
        if frame_type == 4 {
            last = Some(wire[offset + WCS_HEADER_BYTES..end].to_vec());
        }
        offset = end;
    }
    last.expect("partial frame")
}

fn assert_plan_matches_sql(path: &Path, table: &str, sql: &str, batch_rows: u32) {
    let expect = run_planned_sql(path, batch_rows, sql);
    let got = run_plan_operators(path, sql, table, batch_rows as usize);
    encoded_partials_equivalent(&got, &expect).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[test]
fn q3_avg_through_the_physical_plan() {
    let Some(path) = hits_path() else {
        return;
    };
    assert_plan_matches_sql(&path, "hits", Q3, 32_768);
}

#[test]
fn q12_filter_group_through_the_physical_plan() {
    let Some(path) = hits_path() else {
        return;
    };
    assert_plan_matches_sql(&path, "hits", Q12, 32_768);
}

#[test]
fn q8_count_distinct_through_the_physical_plan() {
    let Some(path) = hits_path() else {
        return;
    };
    assert_plan_matches_sql(&path, "hits", Q8_DISTINCT, 32_768);
}

fn snapshot_through_plan(path: &Path, table: &str, sql: &str, batch_rows: usize) -> PartialSnapshot {
    partial_snapshot_from_wire(&run_plan_operators(path, sql, table, batch_rows)).unwrap()
}

#[test]
fn duckdb_rle_padding_through_the_physical_plan() {
    let path = duckdb_fixture("duckdb_rle_padding.parquet");
    if !path.is_file() {
        return;
    }
    let sql = "SELECT COUNT(*), COUNT(d), SUM(d), COUNT(p), SUM(p), MIN(p), MAX(p) FROM t";
    for batch_rows in [1024usize, 4096] {
        let snap = snapshot_through_plan(&path, "t", sql, batch_rows);
        assert_eq!(snap.passed_rows, 10_000, "batch {batch_rows}");
        let stats = &snap.groups[0].stats;
        assert_eq!(stats[0].count, 10_000);
        assert_eq!(stats[1].count, 10_000);
        assert_eq!(stats[2].sum_i, 245_000);
        assert_eq!(stats[3].count, 10_000);
        assert_eq!(stats[4].sum_i, 21_471_420_284_490);
        assert_eq!(stats[5].min_i, 0);
        assert_eq!(stats[6].max_i, 4_294_646_785);
    }
}

#[test]
fn duckdb_nulls_through_the_physical_plan() {
    let path = duckdb_fixture("duckdb_nulls.parquet");
    if !path.is_file() {
        return;
    }
    let sql = "SELECT COUNT(*), COUNT(d), SUM(d), COUNT(p), SUM(p), MIN(p), MAX(p) FROM t";
    let snap = snapshot_through_plan(&path, "t", sql, 1024);
    assert_eq!(snap.passed_rows, 10_000);
    let stats = &snap.groups[0].stats;
    assert_eq!(stats[0].count, 10_000);
    assert_eq!(stats[1].count, 9_411);
    assert_eq!(stats[2].sum_i, 230_628);
    assert_eq!(stats[3].count, 9_565);
    assert_eq!(stats[4].sum_i, 20_542_334_111_913);
    assert_eq!(stats[5].min_i, 436_797);
    assert_eq!(stats[6].max_i, 4_294_646_785);
}

#[test]
fn duckdb_group_by_through_the_physical_plan() {
    let path = duckdb_fixture("duckdb_nulls.parquet");
    if !path.is_file() {
        return;
    }
    let snap = snapshot_through_plan(&path, "t", "SELECT d, COUNT(*) FROM t GROUP BY d", 1024);
    assert_eq!(snap.groups.len(), 51);
    let count_of = |key: wcol::ops::query::PartialKeyPart| {
        snap.groups
            .iter()
            .find(|g| g.key == vec![key.clone()])
            .map(|g| g.stats[0].count)
    };
    assert_eq!(count_of(wcol::ops::query::PartialKeyPart::Null), Some(589));
    assert_eq!(count_of(wcol::ops::query::PartialKeyPart::Int(7)), Some(188));
}
