//! Native-typed Parquet sink: every physical type, dictionary pages, and row-group rotation.

use std::collections::HashSet;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use wcol::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use wcol::formats::parquet::{
    open_plan, ParquetDecode, ParquetEncode, ParquetEncodeConfig, ParquetSinkColumn, RowGroupSpec,
};
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, WcolColumn};

fn drive_encode(enc: &mut ParquetEncode, batches: Vec<OwnedWcolBatch>) -> Result<Vec<u8>, String> {
    let mut file = Vec::new();
    let mut ring = vec![0u8; 64 * 1024];
    let mut pending: Option<WcolStreamItem> = None;
    let mut batch_iter = batches.into_iter();
    let mut eof = false;
    for _ in 0..500_000 {
        if pending.is_none() && !eof {
            if let Some(batch) = batch_iter.next() {
                pending = Some(WcolStreamItem::from_row_batch(batch));
            } else {
                eof = true;
            }
        }
        let mut written = 0usize;
        let mut unused = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof: eof,
            output: &mut unused,
            host_bytes: None,
            output_port: Some(&mut ring),
            bytes_written: &mut written,
        };
        match enc.poll(&mut io)? {
            StreamProgress::Finished => {
                file.extend_from_slice(&ring[..written]);
                return Ok(file);
            }
            StreamProgress::Blocked => continue,
            StreamProgress::HaveOutput => file.extend_from_slice(&ring[..written]),
            StreamProgress::NeedInput => {}
            StreamProgress::NeedHostRead { .. } => return Err("unexpected host read".to_string()),
        }
    }
    Err("encode driver exceeded iteration budget".to_string())
}

fn col(
    name: &str,
    optional: bool,
    physical: i32,
    type_length: i32,
    date: bool,
) -> ParquetSinkColumn {
    ParquetSinkColumn {
        name: name.to_string(),
        optional,
        physical,
        type_length,
        date,
    }
}

fn dict_int(column: u32, entries: &[i64]) -> DictionaryValues {
    DictionaryValues {
        scope: 0,
        column,
        entries: DictEntries::Int(Arc::new(entries.to_vec())),
    }
}

fn dict_float(column: u32, entries: &[f64]) -> DictionaryValues {
    DictionaryValues {
        scope: 0,
        column,
        entries: DictEntries::Float(Arc::new(entries.to_vec())),
    }
}

fn dict_bytes(column: u32, entries: &[&[u8]]) -> DictionaryValues {
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for entry in entries {
        bytes.extend_from_slice(entry);
        offsets.push(bytes.len() as u32);
    }
    DictionaryValues {
        scope: 0,
        column,
        entries: DictEntries::Bytes {
            offsets: offsets.into(),
            bytes: bytes.into(),
        },
    }
}

fn sample_batch() -> OwnedWcolBatch {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 0, 0, 0], vec![1, 1, 0, 1]);
    batch.push_i64_column(1, vec![1, -2, 3, 4], vec![1, 1, 1, 1]);
    batch.push_i64_column(2, vec![100, 200, 300, 400], vec![1, 1, 1, 1]);
    batch.push_i64_column(3, vec![0, 1, 2, 3], vec![1, 1, 1, 1]);
    batch.push_f64_column(4, vec![1.5, -0.0, f64::INFINITY, 0.5], vec![1, 1, 1, 1]);
    batch.push_f64_column(5, vec![1.25, 0.0, f64::NAN, 2.5], vec![1, 0, 1, 1]);
    let hello = "héllo".as_bytes();
    let nihao = "你好".as_bytes();
    let mut text = Vec::new();
    let mut offsets = vec![0u32];
    text.extend_from_slice(b"");
    offsets.push(text.len() as u32);
    text.extend_from_slice(hello);
    offsets.push(text.len() as u32);
    offsets.push(text.len() as u32);
    text.extend_from_slice(nihao);
    offsets.push(text.len() as u32);
    batch.push_utf8_column(6, offsets, text, vec![1, 1, 0, 1]);
    batch.push_utf8_column(
        7,
        vec![0, 2, 4, 6, 8],
        b"abcdefgh".to_vec(),
        vec![1, 1, 1, 1],
    );
    batch.push_dict_id_column(
        8,
        vec![0, 1, 0, 1],
        vec![1, 1, 1, 1],
        dict_bytes(8, &[b"aa", b"bb"]),
    );
    batch.push_dict_id_column(9, vec![0, 1, 0, 1], vec![1, 1, 1, 1], dict_int(9, &[7, 8]));
    batch.push_dict_id_column(
        10,
        vec![0, 1, 0, 1],
        vec![1, 1, 1, 1],
        dict_float(10, &[0.5, 1.5]),
    );
    batch.set_row_count(4).unwrap();
    batch
}

fn schema() -> Vec<ParquetSinkColumn> {
    vec![
        col("flag", true, 0, 0, false),
        col("i32", false, 1, 0, false),
        col("i64", false, 2, 0, false),
        col("day", false, 1, 0, true),
        col("f32", false, 4, 0, false),
        col("f64", true, 5, 0, false),
        col("text", true, 6, 0, false),
        col("fixed", false, 7, 2, false),
        col("dbytes", false, 6, 0, false),
        col("di64", false, 2, 0, false),
        col("df64", false, 5, 0, false),
    ]
}

#[derive(Debug, PartialEq)]
enum Cell {
    Null,
    Int(i64),
    Float(u64),
    Bytes(Vec<u8>),
}

fn cell_at(col: &WcolColumn<'_>, row: usize) -> Cell {
    let present = match col {
        WcolColumn::I64 { nulls, .. }
        | WcolColumn::F64 { nulls, .. }
        | WcolColumn::Utf8 { nulls, .. }
        | WcolColumn::DictId { nulls, .. } => nulls.get(row).copied().unwrap_or(1),
    };
    if present == 0 {
        return Cell::Null;
    }
    match col {
        WcolColumn::I64 { values, .. } => Cell::Int(values[row]),
        WcolColumn::F64 { values, .. } => Cell::Float(values[row].to_bits()),
        WcolColumn::Utf8 { offsets, bytes, .. } => {
            Cell::Bytes(bytes[offsets[row] as usize..offsets[row + 1] as usize].to_vec())
        }
        WcolColumn::DictId {
            ids, dictionary, ..
        } => {
            let id = ids[row] as usize;
            match &dictionary.entries {
                DictEntries::Int(values) => Cell::Int(values[id]),
                DictEntries::Float(values) => Cell::Float(values[id].to_bits()),
                DictEntries::Bytes { offsets, bytes } => {
                    Cell::Bytes(bytes[offsets[id] as usize..offsets[id + 1] as usize].to_vec())
                }
            }
        }
    }
}

fn read_back(path: &Path, dict_columns: &HashSet<u32>) -> Vec<Vec<Cell>> {
    let (mut file, plan) = open_plan(path).unwrap();
    let catalog: Vec<CatalogColumn> = plan
        .leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: if matches!(leaf.physical, 6 | 7) {
                ColumnKind::Text
            } else {
                ColumnKind::Number
            },
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();
    let mut by_group: Vec<RowGroupSpec> = Vec::new();
    for chunk in &plan.chunks {
        if by_group
            .last()
            .is_none_or(|group| group.0 != chunk.row_group)
        {
            by_group.push((chunk.row_group, chunk.rows as u64, Vec::new()));
        }
        by_group.last_mut().unwrap().2.push((
            chunk.leaf,
            chunk.start,
            chunk.codec,
            chunk.compressed_size as u32,
        ));
    }
    let mut decode = ParquetDecode::new(8, catalog, by_group, dict_columns.clone());
    let mut columns = Vec::with_capacity(plan.leaves.len());
    for _ in 0..plan.leaves.len() {
        columns.push(Vec::new());
    }
    let mut batch = OwnedWcolBatch::empty();
    loop {
        if decode.fill_batch(&mut batch).unwrap() {
            let mut view_buf = Vec::new();
            let view = batch.view_with(&mut view_buf);
            for col in view.columns {
                let column = match col {
                    WcolColumn::I64 { column, .. }
                    | WcolColumn::F64 { column, .. }
                    | WcolColumn::Utf8 { column, .. }
                    | WcolColumn::DictId { column, .. } => *column,
                };
                if dict_columns.contains(&column) {
                    assert!(
                        matches!(col, WcolColumn::DictId { .. }),
                        "column {column} was not dictionary-decoded"
                    );
                }
                for row in 0..view.row_count {
                    columns[column as usize].push(cell_at(col, row));
                }
            }
            continue;
        }
        if decode.ingest_complete() {
            break;
        }
        let read = decode.host_read_request().unwrap().expect("host read");
        let mut bytes = vec![0u8; read.length as usize];
        file.seek(SeekFrom::Start(read.offset)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        decode.complete_read(read.ticket, &bytes).unwrap();
    }
    columns
}

fn f32_bits(value: f64) -> u64 {
    (value as f32 as f64).to_bits()
}

fn csv_field(line: &str) -> String {
    if line.len() >= 2 && line.starts_with('"') && line.ends_with('"') {
        line[1..line.len() - 1].replace("\"\"", "\"")
    } else {
        line.to_string()
    }
}

/// DuckDB on the PATH, or where its installer puts it.
fn duckdb_bins() -> [String; 2] {
    let home = std::env::var("HOME").unwrap_or_default();
    ["duckdb".into(), format!("{home}/.duckdb/cli/latest/duckdb")]
}

fn duckdb_csv(sql: &str) -> Option<String> {
    let wrapped = format!("COPY ({sql}) TO STDOUT (HEADER false)");
    let mut launched = false;
    let mut last_err = String::new();
    for bin in duckdb_bins() {
        let Ok(output) = Command::new(bin).args(["-c", &wrapped]).output() else {
            continue;
        };
        launched = true;
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .ok()
                .map(|text| text.trim().to_string());
        }
        last_err = String::from_utf8_lossy(&output.stderr).into_owned();
    }
    if launched {
        panic!("duckdb query failed: {last_err}\n{sql}");
    }
    None
}

fn duckdb_column(path: &Path, name: &str, float: bool) -> Option<Vec<String>> {
    let body = if float {
        format!(
            "CASE \
                WHEN \"{name}\" IS NULL THEN 'NULL' \
                WHEN isnan(\"{name}\") THEN 'NAN' \
                WHEN isinf(\"{name}\") AND \"{name}\" > 0 THEN 'INF' \
                WHEN \"{name}\" = 0 AND signbit(\"{name}\") THEN '-0' \
                ELSE CAST(\"{name}\" AS VARCHAR) \
             END"
        )
    } else {
        format!("CASE WHEN \"{name}\" IS NULL THEN 'NULL' ELSE CAST(\"{name}\" AS VARCHAR) END")
    };
    let sql = format!("SELECT {body} FROM read_parquet('{}')", path.display());
    duckdb_csv(&sql).map(|text| {
        if text.is_empty() {
            Vec::new()
        } else {
            text.lines().map(csv_field).collect()
        }
    })
}

#[test]
fn typed_sink_round_trips_every_physical_type() {
    let config = ParquetEncodeConfig {
        max_row_group_rows: 2,
        max_row_groups: 8,
    };
    let mut enc = ParquetEncode::open_typed(schema(), config).unwrap();
    let bytes = drive_encode(&mut enc, vec![sample_batch()]).unwrap();
    let path = std::env::temp_dir().join(format!("wcol-typed-sink-{}.parquet", std::process::id()));
    fs::write(&path, &bytes).unwrap();

    let (_, plan) = open_plan(&path).unwrap();
    assert_eq!(plan.rows, 4);
    assert_eq!(
        plan.leaves
            .iter()
            .map(|leaf| leaf.physical)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 1, 4, 5, 6, 7, 6, 2, 5]
    );
    assert_eq!(plan.leaves[7].type_length, 2);
    assert_eq!(
        plan.leaves
            .iter()
            .map(|leaf| leaf.max_def)
            .collect::<Vec<_>>(),
        vec![1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]
    );
    let groups: Vec<u32> = plan.chunks.iter().map(|chunk| chunk.row_group).collect();
    assert_eq!(groups.iter().filter(|group| **group == 0).count(), 11);
    assert_eq!(groups.iter().filter(|group| **group == 1).count(), 11);
    assert!(plan.chunks.iter().all(|chunk| chunk.rows == 2));
    for chunk in &plan.chunks {
        let dictionary = matches!(chunk.leaf, 8..=10);
        assert_eq!(chunk.has_dictionary, dictionary, "leaf {}", chunk.leaf);
    }

    let mut dict_columns = HashSet::new();
    dict_columns.extend([8, 9, 10]);
    let got = read_back(&path, &dict_columns);
    let expected = vec![
        vec![Cell::Int(1), Cell::Int(0), Cell::Null, Cell::Int(0)],
        vec![Cell::Int(1), Cell::Int(-2), Cell::Int(3), Cell::Int(4)],
        vec![
            Cell::Int(100),
            Cell::Int(200),
            Cell::Int(300),
            Cell::Int(400),
        ],
        vec![Cell::Int(0), Cell::Int(1), Cell::Int(2), Cell::Int(3)],
        vec![
            Cell::Float(f32_bits(1.5)),
            Cell::Float(f32_bits(-0.0)),
            Cell::Float(f32_bits(f64::INFINITY)),
            Cell::Float(f32_bits(0.5)),
        ],
        vec![
            Cell::Float(1.25f64.to_bits()),
            Cell::Null,
            Cell::Float(f64::NAN.to_bits()),
            Cell::Float(2.5f64.to_bits()),
        ],
        vec![
            Cell::Bytes(b"".to_vec()),
            Cell::Bytes("héllo".as_bytes().to_vec()),
            Cell::Null,
            Cell::Bytes("你好".as_bytes().to_vec()),
        ],
        vec![
            Cell::Bytes(b"ab".to_vec()),
            Cell::Bytes(b"cd".to_vec()),
            Cell::Bytes(b"ef".to_vec()),
            Cell::Bytes(b"gh".to_vec()),
        ],
        vec![
            Cell::Bytes(b"aa".to_vec()),
            Cell::Bytes(b"bb".to_vec()),
            Cell::Bytes(b"aa".to_vec()),
            Cell::Bytes(b"bb".to_vec()),
        ],
        vec![Cell::Int(7), Cell::Int(8), Cell::Int(7), Cell::Int(8)],
        vec![
            Cell::Float(0.5f64.to_bits()),
            Cell::Float(1.5f64.to_bits()),
            Cell::Float(0.5f64.to_bits()),
            Cell::Float(1.5f64.to_bits()),
        ],
    ];
    assert_eq!(got, expected);

    if duckdb_csv("SELECT 1").is_some() {
        let checks = [
            ("flag", false, vec!["true", "false", "NULL", "false"]),
            ("i32", false, vec!["1", "-2", "3", "4"]),
            ("i64", false, vec!["100", "200", "300", "400"]),
            (
                "day",
                false,
                vec!["1970-01-01", "1970-01-02", "1970-01-03", "1970-01-04"],
            ),
            ("f32", true, vec!["1.5", "-0", "INF", "0.5"]),
            ("f64", true, vec!["1.25", "NULL", "NAN", "2.5"]),
            ("text", false, vec!["", "héllo", "NULL", "你好"]),
            ("fixed", false, vec!["ab", "cd", "ef", "gh"]),
            ("dbytes", false, vec!["aa", "bb", "aa", "bb"]),
            ("di64", false, vec!["7", "8", "7", "8"]),
            ("df64", true, vec!["0.5", "1.5", "0.5", "1.5"]),
        ];
        for (name, float, rows) in checks {
            let got = duckdb_column(&path, name, float).unwrap();
            assert_eq!(got, rows, "{name}");
        }
        let types = duckdb_csv(&format!(
            "SELECT typeof(flag), typeof(i32), typeof(day), typeof(f32), typeof(f64), typeof(text), typeof(fixed) \
             FROM read_parquet('{}') LIMIT 1",
            path.display()
        ))
        .unwrap();
        assert_eq!(types, "BOOLEAN,INTEGER,DATE,FLOAT,DOUBLE,VARCHAR,BLOB");
    } else {
        eprintln!("skip: duckdb is not available for the typed parquet sink");
    }
    let _ = fs::remove_file(path);
}

#[test]
fn required_null_is_rejected() {
    let mut enc = ParquetEncode::open_typed(
        vec![col("n", false, 2, 0, false)],
        ParquetEncodeConfig {
            max_row_group_rows: 8,
            max_row_groups: 2,
        },
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 0], vec![1, 0]);
    batch.set_row_count(2).unwrap();
    let err = drive_encode(&mut enc, vec![batch]).unwrap_err();
    assert!(err.contains("null in required column"), "{err}");
}

#[test]
fn a_changed_dictionary_starts_a_new_row_group() {
    let config = ParquetEncodeConfig {
        max_row_group_rows: 100,
        max_row_groups: 4,
    };
    let mut enc = ParquetEncode::open_typed(vec![col("s", false, 6, 0, false)], config).unwrap();
    let mut first = OwnedWcolBatch::empty();
    first.push_dict_id_column(0, vec![0], vec![1], dict_bytes(0, &[b"one"]));
    first.set_row_count(1).unwrap();
    let mut second = OwnedWcolBatch::empty();
    second.push_dict_id_column(0, vec![0], vec![1], dict_bytes(0, &[b"two"]));
    second.set_row_count(1).unwrap();
    let bytes = drive_encode(&mut enc, vec![first, second]).unwrap();
    let path = std::env::temp_dir().join(format!(
        "wcol-typed-dict-flush-{}.parquet",
        std::process::id()
    ));
    fs::write(&path, &bytes).unwrap();
    let (_, plan) = open_plan(&path).unwrap();
    assert_eq!(plan.rows, 2);
    assert_eq!(plan.chunks.len(), 2);
    assert!(plan
        .chunks
        .iter()
        .all(|chunk| chunk.has_dictionary && chunk.rows == 1));
    let mut dict_columns = HashSet::new();
    dict_columns.insert(0);
    let got = read_back(&path, &dict_columns);
    assert_eq!(
        got,
        vec![vec![
            Cell::Bytes(b"one".to_vec()),
            Cell::Bytes(b"two".to_vec())
        ]]
    );
    let _ = fs::remove_file(path);
}

/// Long strings end a row group by bytes before its row limit: encoding a group holds its
/// values, page scratch and output at once.
#[test]
fn long_strings_cut_row_groups_by_bytes() {
    const ROWS: usize = 20_000;
    let config = ParquetEncodeConfig {
        max_row_group_rows: 65_536,
        max_row_groups: 64,
    };
    let mut enc = ParquetEncode::open_typed(vec![col("s", false, 6, 0, false)], config).unwrap();
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for row in 0..ROWS {
        bytes.extend(format!("{row:08}").bytes().cycle().take(1024));
        offsets.push(bytes.len() as u32);
    }
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(0, offsets, bytes, vec![1; ROWS]);
    batch.set_row_count(ROWS).unwrap();
    let file = drive_encode(&mut enc, vec![batch]).unwrap();
    let path = std::env::temp_dir().join(format!(
        "wcol-typed-byte-cut-{}.parquet",
        std::process::id()
    ));
    fs::write(&path, &file).unwrap();
    let (_, plan) = open_plan(&path).unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(plan.rows, ROWS as i64);
    // 20,000 KiB of values in groups of at most 2 MiB.
    assert!(plan.chunks.len() >= 10, "{} row groups", plan.chunks.len());
    assert!(plan.chunks.iter().all(|chunk| chunk.rows < ROWS as i64));
}
