//! ClickBench Q8 / Q3 / Q1 through the ingest graph, checked against DuckDB.
//!
//! Set `HITS_PARQUET` to any local hits parquet (e.g. `wcol/data/hits_subset_500k.parquet`).
//! Full ClickHouse `hits.parquet` is optional; tests are skipped when the file is absent.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use wcol::plan::catalog_from_footer;

use wcol::ops::query::{finalize_groups, PartialKeyPart};
use wcol::plan::{plan_query_dispatch, Frontend, PhysicalPlannerConfig, QueryDispatch};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC};
use wcol::stream::partials::partial_snapshot_from_wire;
use wcol::{open_plan, open_wcol_plan, CatalogColumn, Plan};

const Q8: &str = "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC";
const Q3: &str = "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits";
const Q1: &str = "SELECT COUNT(*) FROM hits";

fn hits_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("HITS_PARQUET") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    None
}

/// DuckDB on the PATH, or where its installer puts it.
fn duckdb_bins() -> [String; 2] {
    let home = std::env::var("HOME").unwrap_or_default();
    ["duckdb".into(), format!("{home}/.duckdb/cli/latest/duckdb")]
}

fn duckdb_csv(sql: &str) -> Option<String> {
    for bin in duckdb_bins() {
        let Ok(output) = std::process::Command::new(bin)
            .args(["-csv", "-c", sql])
            .output()
        else {
            continue;
        };
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .ok()
                .map(|text| text.trim().to_string());
        }
    }
    None
}

fn last_partial_payload(wire: &[u8]) -> Option<Vec<u8>> {
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
        if end > wire.len() {
            break;
        }
        if frame_type == 4 {
            last = Some(wire[offset + WCS_HEADER_BYTES..end].to_vec());
        }
        offset = end;
    }
    last
}

fn run_execution_group(path: &Path, program: &[u8], _feed: &[(u64, u32)]) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut pipe = open_wcol_plan(program, 1024 * 1024).unwrap();
    let mut out = Vec::new();
    for _ in 0..10_000_000 {
        match pipe.poll() {
            Action::ReadAt => {
                let (ticket, _source, offset, len) = pipe.read_at();
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                let dst = pipe.input();
                assert!(dst.len() >= bytes.len());
                dst[..bytes.len()].copy_from_slice(&bytes);
                pipe.commit_input(bytes.len()).unwrap();
                pipe.complete_read(ticket).unwrap();
            }
            Action::NeedInput => {}
            Action::HaveOutput => {
                out.extend_from_slice(pipe.output());
                pipe.consume_output(pipe.output().len()).unwrap();
            }
            Action::Yield => {}
            Action::Finished => break,
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", pipe.error().unwrap()),
        }
    }
    out
}

fn assert_query_matches(
    path: &Path,
    plan: &Plan,
    _catalog: &[CatalogColumn],
    sql: &str,
    batch_rows: u32,
) {
    let (dispatch, _) = plan_query_dispatch(
        sql,
        "hits",
        plan,
        1,
        batch_rows,
        &PhysicalPlannerConfig::default(),
        &[Frontend::Sql],
    )
    .unwrap();
    let got = match dispatch {
        QueryDispatch::Partial(partial) => partial,
        QueryDispatch::Nodes(programs) => {
            assert_eq!(programs.len(), 1, "{sql}");
            let wire = run_execution_group(path, &programs[0].program, &[]);
            last_partial_payload(&wire).expect("WCS partial").to_vec()
        }
        QueryDispatch::Rows { .. } | QueryDispatch::Parquet(_) => panic!("{sql} is a row query"),
    };
    let snap = partial_snapshot_from_wire(&got).unwrap();
    let path_sql = path.display().to_string().replace('\'', "''");
    if sql == Q1 {
        assert_eq!(snap.groups[0].stats[0].count, plan.rows as u64);
        return;
    }
    let statement = if sql == Q3 {
        format!(
            "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM read_parquet('{path_sql}')"
        )
    } else {
        format!(
            "SELECT AdvEngineID, COUNT(*) FROM read_parquet('{path_sql}') WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"
        )
    };
    let Some(csv) = duckdb_csv(&statement) else {
        eprintln!("skip: duckdb is not available to check {sql}");
        return;
    };
    let rows: Vec<Vec<&str>> = csv
        .lines()
        .skip(1)
        .map(|line| line.split(',').collect())
        .collect();
    if sql == Q3 {
        let row = &rows[0];
        let stats = &snap.groups[0].stats;
        assert_eq!(stats[0].sum_i, row[0].parse::<i128>().unwrap(), "{sql}");
        assert_eq!(stats[1].count, row[1].parse::<u64>().unwrap(), "{sql}");
        let avg = row[2].parse::<f64>().unwrap();
        let mean = stats[2].sum_i as f64 / stats[2].count as f64;
        assert!((mean - avg).abs() < 1e-6, "{sql}: {mean} vs {avg}");
        return;
    }
    let groups = finalize_groups(&snap).unwrap();
    let mut got = std::collections::BTreeMap::new();
    for group in &groups {
        if group.stats[0].count == 0 {
            continue;
        }
        let PartialKeyPart::Int(key) = group.key[0] else {
            panic!("expected int group key");
        };
        got.insert(key, group.stats[0].count);
    }
    assert_eq!(got.len(), rows.len(), "{sql}");
    for row in &rows {
        let key = row[0].parse::<i128>().unwrap();
        assert_eq!(
            got.get(&key).copied(),
            Some(row[1].parse::<u64>().unwrap()),
            "{key}"
        );
    }
}

#[test]
fn clickbench_q8_matches_oracle() {
    let Some(path) = hits_path() else {
        eprintln!("skip: set HITS_PARQUET to run ClickBench Q8");
        return;
    };
    let (file, plan) = open_plan(&path).unwrap();
    drop(file);
    let catalog = catalog_from_footer(&plan);
    assert_query_matches(&path, &plan, &catalog, Q8, 32_768);
}

#[test]
fn clickbench_q3_matches_oracle() {
    let Some(path) = hits_path() else {
        eprintln!("skip: set HITS_PARQUET to run ClickBench Q3");
        return;
    };
    let (_file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    assert_query_matches(&path, &plan, &catalog, Q3, 32_768);
}

#[test]
fn clickbench_q1_metadata_count() {
    let Some(path) = hits_path() else {
        eprintln!("skip: set HITS_PARQUET to run ClickBench Q1");
        return;
    };
    let (_file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    assert_query_matches(&path, &plan, &catalog, Q1, 32_768);
}
