//! ClickBench row queries on the physical graph: Q19 and Q23–Q26 against the hits subset,
//! plus the row-graph regressions those queries are meant to catch.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use wcol::exec::operators::{RowBatchWcsEgress, RowBatchWcsIngress};
use wcol::formats::parquet::{open_plan, ParquetEncode, ParquetEncodeConfig, ParquetSinkColumn};
use wcol::open_wcol_plan;
use wcol::plan::{
    encode_coordinator_program, plan_query_dispatch, OpKind, PhysicalPlannerConfig, QueryDispatch,
    SortTarget, WcolPhysicalPlan,
};
use wcol::stream::core::{Action, WCS_HEADER_BYTES, WCS_MAGIC, WCS_MAX_PAYLOAD};
use wcol::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, WcolColumn};
use wcol::Operator;

const BATCH_ROWS: u32 = 8_192;
const RING: usize = 256 * 1024;

fn hits_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
}

fn parquet_sql(path: &Path, sql: &str) -> String {
    let file = path.display().to_string().replace('\'', "''");
    sql.replace("FROM hits", &format!("FROM '{file}'"))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Null,
    Int(i64),
    Text(String),
}

impl Cell {
    fn csv(&self) -> String {
        match self {
            Cell::Null => "\\N".into(),
            Cell::Int(v) => v.to_string(),
            Cell::Text(v) => v.clone(),
        }
    }
}

fn cell_of(col: &WcolColumn<'_>, row: usize) -> Cell {
    let present = |nulls: &[u8]| nulls.get(row).copied().unwrap_or(1) != 0;
    match col {
        WcolColumn::I64 { values, nulls, .. } => {
            if present(nulls) {
                Cell::Int(values[row])
            } else {
                Cell::Null
            }
        }
        WcolColumn::F64 { values, nulls, .. } => {
            if present(nulls) {
                Cell::Text(format!("{}", values[row]))
            } else {
                Cell::Null
            }
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            if present(nulls) {
                let text =
                    std::str::from_utf8(&bytes[offsets[row] as usize..offsets[row + 1] as usize])
                        .unwrap()
                        .to_string();
                Cell::Text(text)
            } else {
                Cell::Null
            }
        }
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            if !present(nulls) {
                return Cell::Null;
            }
            let id = ids[row] as usize;
            match &dictionary.entries {
                DictEntries::Int(entries) => Cell::Int(entries[id]),
                DictEntries::Float(entries) => Cell::Text(format!("{}", entries[id])),
                DictEntries::Bytes { offsets, bytes } => {
                    let text =
                        std::str::from_utf8(&bytes[offsets[id] as usize..offsets[id + 1] as usize])
                            .unwrap()
                            .to_string();
                    Cell::Text(text)
                }
            }
        }
    }
}

fn rows_of(wire: &[u8]) -> Vec<Vec<Cell>> {
    assert_frames_fit(wire);
    let mut ingress = RowBatchWcsIngress::new();
    let mut at = 0usize;
    while at < wire.len() {
        let n = (at + 1).min(wire.len());
        at += ingress.feed(&wire[at..n]).unwrap();
    }
    ingress.finish_feed().unwrap();
    let mut rows = Vec::new();
    for batch in ingress.take_batches() {
        let mut cols = Vec::new();
        let view = batch.view_with(&mut cols);
        for row in 0..view.row_count {
            rows.push(view.columns.iter().map(|col| cell_of(col, row)).collect());
        }
    }
    rows
}

fn assert_frames_fit(wire: &[u8]) {
    let mut at = 0usize;
    while at + WCS_HEADER_BYTES <= wire.len() {
        let hdr = &wire[at..at + WCS_HEADER_BYTES];
        assert_eq!(u32::from_le_bytes(hdr[..4].try_into().unwrap()), WCS_MAGIC);
        let len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        assert!(
            len <= WCS_MAX_PAYLOAD,
            "frame payload {len} exceeds WCS_MAX_PAYLOAD"
        );
        at += WCS_HEADER_BYTES + len;
    }
    assert_eq!(at, wire.len(), "trailing bytes after WCS frames");
}

fn run_node(path: &Path, program: &[u8]) -> (Vec<u8>, u32, u64) {
    let mut file = File::open(path).unwrap();
    let mut host = open_wcol_plan(program, RING).unwrap();
    let mut wire = Vec::new();
    let (mut reads, mut bytes) = (0u32, 0u64);
    for _ in 0..5_000_000 {
        match host.poll() {
            Action::ReadAt => {
                let (ticket, _, offset, len) = host.read_at();
                reads += 1;
                bytes += u64::from(len);
                let mut buf = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut buf).unwrap();
                for part in buf.chunks(RING) {
                    let dst = host.input();
                    dst[..part.len()].copy_from_slice(part);
                    host.commit_input(part.len()).unwrap();
                    host.complete_read(ticket).unwrap();
                }
            }
            Action::HaveOutput => {
                wire.extend_from_slice(host.output());
                let n = host.output().len();
                host.consume_output(n).unwrap();
            }
            Action::NeedInput | Action::Yield => {}
            Action::Finished => return (wire, reads, bytes),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", host.error().unwrap()),
        }
    }
    panic!("node did not finish");
}

fn run_coordinator(program: &[u8], wires: &[Vec<u8>]) -> Vec<u8> {
    let mut host = open_wcol_plan(program, RING).unwrap();
    let mut at = vec![0usize; wires.len()];
    let mut eof = vec![false; wires.len()];
    let mut rr = 0usize;
    let mut out = Vec::new();
    for _ in 0..5_000_000 {
        match host.poll() {
            Action::NeedInput => {
                let nports = wires.len();
                let mut fed = false;
                for step in 0..nports {
                    let port = (rr + step) % nports;
                    if at[port] < wires[port].len() {
                        let dst = host.input();
                        if dst.is_empty() {
                            break;
                        }
                        let n = dst.len().min(wires[port].len() - at[port]).min(1);
                        dst[..n].copy_from_slice(&wires[port][at[port]..at[port] + n]);
                        host.commit_input_port(port as u32, n).unwrap();
                        at[port] += n;
                        rr = port + 1;
                        fed = true;
                        break;
                    }
                }
                if !fed {
                    if let Some(port) = eof.iter().position(|done| !done) {
                        host.end_input_port(port as u32).unwrap();
                        eof[port] = true;
                    }
                }
            }
            Action::Yield => {}
            Action::HaveOutput => {
                out.extend_from_slice(host.output());
                let n = host.output().len();
                host.consume_output(n).unwrap();
            }
            Action::Finished => {
                assert_frames_fit(&out);
                return out;
            }
            Action::ReadAt => panic!("coordinator requested a host read"),
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("{}", host.error().unwrap()),
        }
    }
    panic!("coordinator did not finish");
}

fn no_sql(program: &[u8]) {
    assert!(
        !program
            .windows(6)
            .any(|window| window.eq_ignore_ascii_case(b"select")),
        "program contains SQL"
    );
}

struct Ran {
    rows: Vec<Vec<Cell>>,
}

fn execute(path: &Path, sql: &str, nodes: u32, reverse_nodes: bool) -> Ran {
    let (_file, footer) = open_plan(path).unwrap();
    let (dispatch, plan) = plan_query_dispatch(
        sql,
        "hits_subset_500k.parquet",
        &footer,
        nodes,
        BATCH_ROWS,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let QueryDispatch::Rows {
        nodes: programs,
        coordinator,
    } = dispatch
    else {
        panic!("{sql} did not plan as a row query");
    };
    if sql.contains("ORDER BY EventTime LIMIT 10") && sql.contains("SearchPhrase FROM") {
        assert_sort_key_survives_until_project(&plan, "EventTime");
    }
    no_sql(&coordinator);
    let mut wires = Vec::new();
    for node in &programs {
        no_sql(&node.program);
        let (wire, reads, bytes) = run_node(path, &node.program);
        assert_eq!(reads, node.request_count, "node {} reads", node.node.0);
        assert_eq!(bytes, node.fetched_bytes, "node {} bytes", node.node.0);
        wires.push(wire);
    }
    if reverse_nodes {
        wires.reverse();
    }
    Ran {
        rows: rows_of(&run_coordinator(&coordinator, &wires)),
    }
}

fn assert_sort_key_survives_until_project(plan: &WcolPhysicalPlan, key: &str) {
    let (_file_ignored, footer) = open_plan(&hits_path()).unwrap();
    let key_id = footer
        .leaves
        .iter()
        .position(|leaf| leaf.name == key)
        .unwrap() as u32;
    assert!(
        plan.operators.iter().any(|op| {
            op.node.is_some()
                && matches!(&op.kind, OpKind::TopK { keys, .. } if keys.iter().any(|k| k.target == SortTarget::Column(key_id)))
        }),
        "node top-k dropped {key}"
    );
    assert!(
        !plan
            .operators
            .iter()
            .any(|op| op.node.is_some() && matches!(op.kind, OpKind::Project { .. })),
        "node projected before the exchange"
    );
    let project = plan
        .operators
        .iter()
        .find(|op| op.node.is_none() && matches!(op.kind, OpKind::Project { .. }))
        .unwrap();
    let OpKind::Project { columns } = &project.kind else {
        unreachable!()
    };
    assert!(
        !columns.contains(&key_id),
        "{key} leaked into the final projection"
    );
    let limit = &plan.operators[project.inputs[0] as usize];
    assert!(
        matches!(limit.kind, OpKind::Limit { .. }),
        "project must follow limit"
    );
    let sort = &plan.operators[limit.inputs[0] as usize];
    assert!(
        matches!(&sort.kind, OpKind::Sort { keys } if keys.iter().any(|k| k.target == SortTarget::Column(key_id))),
        "coordinator sort lost {key}"
    );
}

fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                }
            } else {
                field.push(ch);
            }
        } else if ch == '"' {
            quoted = true;
        } else if ch == ',' {
            row.push(std::mem::take(&mut field));
        } else if ch == '\n' {
            row.push(std::mem::take(&mut field));
            if row.iter().any(|cell| !cell.is_empty()) {
                rows.push(std::mem::take(&mut row));
            } else {
                row.clear();
            }
        } else if ch != '\r' {
            field.push(ch);
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        if row.iter().any(|cell| !cell.is_empty()) {
            rows.push(row);
        }
    }
    rows
}

fn show_row(row: &[Cell]) -> Vec<String> {
    row.iter().map(Cell::csv).collect()
}

/// `ranked` rows are projected columns followed by `key_width` sort-key columns, in order.
/// `full_last` is every projected row whose sort key equals the cutoff (not truncated by LIMIT).
fn assert_topk(
    wcol: &[Vec<Cell>],
    ranked: &[Vec<Cell>],
    key_width: usize,
    full_last: &[Vec<Cell>],
) {
    assert_eq!(wcol.len(), ranked.len(), "row count");
    if wcol.is_empty() {
        return;
    }
    let mut groups: Vec<(Vec<String>, Vec<Vec<String>>)> = Vec::new();
    for row in ranked {
        let key = show_row(&row[row.len() - key_width..]);
        let projected = show_row(&row[..row.len() - key_width]);
        if groups.last().is_some_and(|(prev, _)| prev == &key) {
            groups.last_mut().unwrap().1.push(projected);
        } else {
            groups.push((key, vec![projected]));
        }
    }
    let wcol: Vec<Vec<String>> = wcol.iter().map(|row| show_row(row)).collect();
    let mut at = 0usize;
    for (index, (_key, rows)) in groups.iter().enumerate() {
        let last = index + 1 == groups.len();
        let class: Vec<Vec<String>> = if last {
            full_last.iter().map(|row| show_row(row)).collect()
        } else {
            rows.clone()
        };
        let take = if last { wcol.len() - at } else { rows.len() };
        assert!(at + take <= wcol.len(), "sort groups ran past the result");
        let slice = &wcol[at..at + take];
        let mut bag = class;
        for row in slice {
            let pos = bag
                .iter()
                .position(|cand| cand == row)
                .unwrap_or_else(|| panic!("row {row:?} is not in its sort-key class"));
            bag.remove(pos);
        }
        if !last {
            assert_eq!(
                slice.len(),
                rows.len(),
                "a strictly better sort key was dropped"
            );
        }
        at += take;
    }
    assert_eq!(at, wcol.len());
}

fn multiset_eq(mut left: Vec<Vec<Cell>>, mut right: Vec<Vec<Cell>>) {
    left.sort();
    right.sort();
    assert_eq!(left, right);
}

fn sample_two_column_file() -> Vec<u8> {
    let mut batch = OwnedWcolBatch::empty();
    // a is the output, b is the sort key. Ties on b sit on different row groups.
    batch.push_i64_column(0, vec![10, 11, 20, 21, 30, 31], vec![1; 6]);
    batch.push_i64_column(1, vec![1, 1, 2, 2, 3, 3], vec![1; 6]);
    batch.set_row_count(6).unwrap();
    let mut enc = ParquetEncode::open_typed(
        vec![
            ParquetSinkColumn {
                name: "a".into(),
                optional: false,
                physical: 2,
                type_length: 0,
                date: false,
            },
            ParquetSinkColumn {
                name: "b".into(),
                optional: false,
                physical: 2,
                type_length: 0,
                date: false,
            },
        ],
        ParquetEncodeConfig {
            max_row_group_rows: 2,
            max_row_groups: 8,
        },
    )
    .unwrap();
    let mut file = Vec::new();
    let mut ring = vec![0u8; 64 * 1024];
    let mut pending = Some(wcol::WcolStreamItem::from_row_batch(batch));
    let mut eof = false;
    for _ in 0..100_000 {
        if pending.is_none() {
            eof = true;
        }
        let mut written = 0usize;
        let mut unused = None;
        let mut io = wcol::exec::operator::OperatorIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof: eof,
            output: &mut unused,
            host_bytes: None,
            output_port: Some(&mut ring),
            bytes_written: &mut written,
        };
        match enc.poll(&mut io).unwrap() {
            wcol::exec::operator::StreamProgress::Finished => {
                file.extend_from_slice(&ring[..written]);
                return file;
            }
            wcol::exec::operator::StreamProgress::HaveOutput => {
                file.extend_from_slice(&ring[..written])
            }
            wcol::exec::operator::StreamProgress::NeedInput
            | wcol::exec::operator::StreamProgress::Blocked => {}
            wcol::exec::operator::StreamProgress::NeedHostRead { .. } => panic!("host read"),
        }
    }
    panic!("encode");
}

fn dict_stream(text: &[u8]) -> Vec<u8> {
    let mut batch = OwnedWcolBatch::empty();
    batch.set_scope(1);
    batch.push_dict_id_column(
        0,
        vec![0],
        vec![1],
        DictionaryValues {
            scope: 1,
            column: 0,
            entries: DictEntries::Bytes {
                offsets: Arc::from(vec![0u32, text.len() as u32]),
                bytes: Arc::from(text.to_vec()),
            },
        },
    );
    batch.set_row_count(1).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(&batch).unwrap();
    egress.push_end().unwrap();
    egress.drain_to_vec().unwrap()
}

#[test]
fn conflicting_dictionary_scopes_stay_with_their_producer() {
    let program = encode_coordinator_program(2, &[]);
    let left = dict_stream(b"left");
    let right = dict_stream(b"right");
    let rows = rows_of(&run_coordinator(&program, &[left, right]));
    let mut texts: Vec<String> = rows
        .into_iter()
        .map(|row| match &row[0] {
            Cell::Text(text) => text.clone(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect();
    texts.sort();
    assert_eq!(texts, vec!["left".to_string(), "right".to_string()]);
}

#[test]
fn order_by_a_non_output_column_keeps_the_key_through_topk() {
    let bytes = sample_two_column_file();
    let path = std::env::temp_dir().join(format!("wcol-row-order-{}.parquet", std::process::id()));
    std::fs::write(&path, &bytes).unwrap();
    let got = execute(&path, "SELECT a FROM t ORDER BY b LIMIT 3", 2, false);
    let _ = std::fs::remove_file(&path);
    assert_eq!(got.rows.len(), 3);
    let mut values: Vec<i64> = got
        .rows
        .iter()
        .map(|row| match row[0] {
            Cell::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    // b=1 rows (10, 11) are strictly better than the cutoff b=2 and must both be present.
    assert!(values.contains(&10) && values.contains(&11), "{values:?}");
    values.retain(|v| *v != 10 && *v != 11);
    assert_eq!(values.len(), 1);
    assert!(values[0] == 20 || values[0] == 21, "{values:?}");
}

#[test]
fn clickbench_row_queries_match_duckdb_on_one_two_and_four_nodes() {
    let path = hits_path();
    if !path.is_file() {
        eprintln!("skip: missing {}", path.display());
        return;
    }
    let q19 = "SELECT UserID FROM hits WHERE UserID = 435090932899640449";
    let expected_q19 = golden_cells(
        &path,
        "q19",
        "SELECT CAST(UserID AS VARCHAR) FROM hits WHERE UserID = 435090932899640449",
    );
    for nodes in [1u32, 2, 4] {
        for reverse in [false, true] {
            let got = execute(&path, q19, nodes, reverse);
            multiset_eq(got.rows, expected_q19.clone());
        }
    }

    check_ordered(
        &path,
        "q23_google_by_time",
        "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
        "SELECT *, CAST(EventTime AS VARCHAR) FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
        1,
        "SELECT * FROM hits WHERE URL LIKE '%google%' AND EventTime = (SELECT EventTime FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 1 OFFSET 9)",
    );
    check_ordered(
        &path,
        "q24_phrase_by_time",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        "SELECT CAST(SearchPhrase AS VARCHAR), CAST(EventTime AS VARCHAR) FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        1,
        "SELECT CAST(SearchPhrase AS VARCHAR) FROM hits WHERE SearchPhrase <> '' AND EventTime = (SELECT EventTime FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 1 OFFSET 9)",
    );
    check_ordered(
        &path,
        "q25_phrase_by_phrase",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
        "SELECT CAST(SearchPhrase AS VARCHAR), CAST(SearchPhrase AS VARCHAR) FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
        1,
        "SELECT CAST(SearchPhrase AS VARCHAR) FROM hits WHERE SearchPhrase <> '' AND SearchPhrase = (SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 1 OFFSET 9)",
    );
    check_ordered(
        &path,
        "q26_phrase_by_time_and_phrase",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
        "SELECT CAST(SearchPhrase AS VARCHAR), CAST(EventTime AS VARCHAR), CAST(SearchPhrase AS VARCHAR) FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
        2,
        "SELECT CAST(SearchPhrase AS VARCHAR) FROM hits WHERE SearchPhrase <> '' AND EventTime = (SELECT EventTime FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 1 OFFSET 9) AND SearchPhrase = (SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 1 OFFSET 9)",
    );
}

/// DuckDB's answer to `sql` on the hits subset, read from `tests/fixtures/golden/<name>.csv`.
/// `WCOL_UPDATE_GOLDEN=1` rewrites the file with the `duckdb` CLI first; the SQL stays here
/// so the oracle can be regenerated.
fn golden_cells(path: &Path, name: &str, sql: &str) -> Vec<Vec<Cell>> {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/golden")
        .join(format!("{name}.csv"));
    if std::env::var_os("WCOL_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let copy = format!(
            "COPY ({}) TO '{}' (FORMAT CSV, HEADER false, NULLSTR '\\N')",
            parquet_sql(path, sql),
            file.display()
        );
        let status = Command::new("duckdb").args(["-c", &copy]).status().unwrap();
        assert!(status.success(), "duckdb copy failed");
    }
    let text = std::fs::read_to_string(&file).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; regenerate with WCOL_UPDATE_GOLDEN=1",
            file.display()
        )
    });
    parse_csv(&text)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| {
                    if cell == "\\N" {
                        Cell::Null
                    } else {
                        Cell::Text(cell)
                    }
                })
                .collect()
        })
        .collect()
}

fn check_ordered(
    path: &Path,
    name: &str,
    wcol_sql: &str,
    ranked_sql: &str,
    key_width: usize,
    tie_sql: &str,
) {
    let ranked = golden_cells(path, name, ranked_sql);
    let ties = golden_cells(path, &format!("{name}_ties"), tie_sql);
    for nodes in [1u32, 2, 4] {
        for reverse in [false, true] {
            let got = execute(path, wcol_sql, nodes, reverse);
            assert_topk(&got.rows, &ranked, key_width, &ties);
        }
    }
}
