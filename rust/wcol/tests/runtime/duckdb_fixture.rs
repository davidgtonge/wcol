//! A DuckDB-written file through the full ingest graph, checked against DuckDB's answers.
//!
//! `tests/fixtures/duckdb_rle_padding.parquet` has 10 000 rows (DuckDB rounds the requested
//! row groups up to 4 096 rows): `d` is INTEGER (RLE_DICTIONARY, 50 values), `p` BIGINT
//! (PLAIN); both nullable in the schema with no NULLs, like ClickBench `hits`. A page's
//! value stream can end before the decoder sees its last run finish; row groups must
//! still complete once every row has its value.
//!
//! `tests/fixtures/duckdb_nulls.parquet` is the same shape with real NULLs: `d` is NULL on
//! every 17th row and `p` on every 23rd, so definition levels are mixed.

use std::path::PathBuf;

use wcol::formats::parquet::open_plan;
use wcol::ops::query::PartialSnapshot;
use wcol::plan::{plan_query_dispatch, PhysicalPlannerConfig, QueryDispatch};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::stream::partials::partial_snapshot_from_state;
use wcol::{coordinator_reduce_partials, open_wcol_plan};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .join(name)
}

fn run(sql: &str, batch_rows: usize) -> PartialSnapshot {
    run_file("duckdb_rle_padding.parquet", sql, batch_rows)
}

fn run_file(name: &str, sql: &str, batch_rows: usize) -> PartialSnapshot {
    let path = fixture(name);
    let bytes = std::fs::read(&path).unwrap();
    let (_, footer) = open_plan(&path).unwrap();
    let (dispatch, _) = plan_query_dispatch(
        sql,
        name,
        &footer,
        1,
        batch_rows as u32,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let state = match dispatch {
        QueryDispatch::Partial(partial) => {
            wcol::stream::partial_wcs::partial_state_from_wire_payload(&partial).unwrap()
        }
        QueryDispatch::Nodes(programs) => {
            let mut partials = Vec::new();
            for node in &programs {
                partials.push(run_node(&bytes, &node.program));
            }
            coordinator_reduce_partials(partials).unwrap()
        }
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => panic!("{sql} is a row query"),
    };
    partial_snapshot_from_state(&state).unwrap()
}

fn run_node(file: &[u8], program: &[u8]) -> wcol::stream::partials::PartialStateWcol {
    let mut pipe = open_wcol_plan(program, 256 * 1024).unwrap();
    let mut wire = Vec::new();
    loop {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let slice = &file[offset as usize..offset as usize + len as usize];
                for part in slice.chunks(256 * 1024) {
                    pipe.input()[..part.len()].copy_from_slice(part);
                    pipe.commit_input(part.len()).unwrap();
                    pipe.complete_read(ticket).unwrap();
                }
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
        let header = &wire[offset..offset + WCS_HEADER_BYTES];
        if u32::from_le_bytes(header[..4].try_into().unwrap()) != WCS_MAGIC {
            break;
        }
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        if header[5] == 4 {
            last = Some(&wire[offset + WCS_HEADER_BYTES..offset + WCS_HEADER_BYTES + len]);
        }
        offset += WCS_HEADER_BYTES + len;
    }
    wcol::stream::partial_wcs::partial_state_from_wire_payload(last.unwrap()).unwrap()
}

#[test]
fn aggregates_match_duckdb_across_row_groups() {
    // DuckDB: count(*)=10000 count(d)=10000 sum(d)=245000 count(p)=10000
    //         sum(p)=21471420284490 min(p)=0 max(p)=4294646785
    let sql = "SELECT COUNT(*), COUNT(d), SUM(d), COUNT(p), SUM(p), MIN(p), MAX(p) FROM t";
    for batch_rows in [7, 1024, 4096] {
        let snap = run(sql, batch_rows);
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
fn dictionary_group_by_matches_duckdb() {
    // DuckDB: 50 groups; rows with d = 7: 200.
    let snap = run("SELECT d, COUNT(*) FROM t GROUP BY d", 1024);
    assert_eq!(snap.groups.len(), 50);
    let count_of = |key: wcol::ops::query::PartialKeyPart| {
        snap.groups
            .iter()
            .find(|g| g.key == vec![key.clone()])
            .map(|g| g.stats[0].count)
    };
    assert_eq!(count_of(wcol::ops::query::PartialKeyPart::Null), None);
    assert_eq!(
        count_of(wcol::ops::query::PartialKeyPart::Int(7)),
        Some(200)
    );
}

#[test]
fn nullable_columns_with_nulls_match_duckdb() {
    // DuckDB: count(*)=10000 count(d)=9411 sum(d)=230628 count(p)=9565
    //         sum(p)=20542334111913 min(p)=436797 max(p)=4294646785
    let sql = "SELECT COUNT(*), COUNT(d), SUM(d), COUNT(p), SUM(p), MIN(p), MAX(p) FROM t";
    for batch_rows in [7, 1024, 4096] {
        let snap = run_file("duckdb_nulls.parquet", sql, batch_rows);
        assert_eq!(snap.passed_rows, 10_000, "batch {batch_rows}");
        let stats = &snap.groups[0].stats;
        assert_eq!(stats[0].count, 10_000);
        assert_eq!(stats[1].count, 9_411);
        assert_eq!(stats[2].sum_i, 230_628);
        assert_eq!(stats[3].count, 9_565);
        assert_eq!(stats[4].sum_i, 20_542_334_111_913);
        assert_eq!(stats[5].min_i, 436_797);
        assert_eq!(stats[6].max_i, 4_294_646_785);
    }
}

#[test]
fn null_group_key_matches_duckdb() {
    // DuckDB: 51 groups (50 values + NULL); d IS NULL: 589 rows, d = 7: 188 rows.
    let snap = run_file(
        "duckdb_nulls.parquet",
        "SELECT d, COUNT(*) FROM t GROUP BY d",
        1024,
    );
    assert_eq!(snap.groups.len(), 51);
    let count_of = |key: wcol::ops::query::PartialKeyPart| {
        snap.groups
            .iter()
            .find(|g| g.key == vec![key.clone()])
            .map(|g| g.stats[0].count)
    };
    assert_eq!(count_of(wcol::ops::query::PartialKeyPart::Null), Some(589));
    assert_eq!(
        count_of(wcol::ops::query::PartialKeyPart::Int(7)),
        Some(188)
    );
}
