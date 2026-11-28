//! Page-copy compaction: merge row groups without decode.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use compact_parquet_thrift::encode::{
    write_footer, write_magic, write_row_group, ChunkPlace, Column, ColumnValues, RowGroupFooter,
    UNCOMPRESSED,
};
use compact_parquet_thrift::parquet::footer_range;
use wcol::exec::wcol_session::open_wcol_plan;
use wcol::formats::parquet::{open_plan, read_footer};
use wcol::plan::distributed::SourceVersion;
use wcol::plan::page_compaction::{
    encode_page_compact_wire, plan_page_compaction, CompactionInputFile, PageCompactionPolicy,
};
use wcol::stream::core::Action;

fn write_three_row_group_fixture(path: &PathBuf) {
    write_fixture(
        path,
        &[Column {
            name: "v",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::I64(&[1, 2, 3, 4]),
        }],
    );
}

/// Three identical row groups of `columns`, uncompressed and without dictionaries.
fn write_fixture(path: &PathBuf, columns: &[Column<'_>]) {
    let mut scratch = vec![0u8; 4096];
    let mut snappy = [0u32; 4096];
    let mut out = vec![0u8; 64 * 1024];
    let mut at = write_magic(&mut out).unwrap();
    let mut all_places: Vec<Vec<ChunkPlace>> = Vec::new();
    for _ in 0..3 {
        let mut places = vec![ChunkPlace::default(); columns.len()];
        let rg_len = write_row_group(
            &mut out[at..],
            4,
            columns,
            &mut places,
            &mut scratch,
            &mut snappy,
        )
        .unwrap();
        for place in &mut places {
            place.offset = place.offset.saturating_add(at as u64);
            place.data_page_offset = place.data_page_offset.saturating_add(at as u64);
            if let Some(dict) = place.dictionary_page_offset {
                place.dictionary_page_offset = Some(dict.saturating_add(at as u64));
            }
        }
        all_places.push(places);
        at += rg_len;
    }
    let footers: Vec<RowGroupFooter<'_>> = all_places
        .iter()
        .map(|places| RowGroupFooter {
            num_rows: 4,
            columns,
            places,
        })
        .collect();
    let footer_len = write_footer(&mut out[at..], columns, &footers).unwrap();
    out.truncate(at + footer_len);
    std::fs::write(path, &out).unwrap();
}

/// Compact `path`'s row groups into one part; the output file and the reads it took.
fn compact_file(path: &std::path::Path) -> (Vec<u8>, usize) {
    let (mut file, plan) = open_plan(path).unwrap();
    let file_size = file.metadata().unwrap().len();
    read_footer(&mut file, file_size).unwrap();
    let inputs = vec![CompactionInputFile {
        object_key: path.to_string_lossy().into_owned(),
        file_size,
        plan,
    }];
    let compact_plan = plan_page_compaction(
        SourceVersion {
            object_key_hash: 42,
        },
        "page-compact-test",
        inputs,
        PageCompactionPolicy {
            target_output_bytes: 1024 * 1024,
            target_row_group_bytes: 1024 * 1024,
            output_parts: 1,
        },
    )
    .unwrap();
    assert_eq!(compact_plan.parts.len(), 1);
    assert_eq!(compact_plan.parts[0].merged_row_groups.len(), 1);
    assert_eq!(compact_plan.parts[0].rows, 12);

    let wire =
        encode_page_compact_wire(&compact_plan.parts[0], &compact_plan.inputs[0].plan.leaves)
            .unwrap();
    let mut host = open_wcol_plan(&wire, 256 * 1024).unwrap();
    let mut output = Vec::new();
    let mut reads: HashMap<u32, (u16, u64, u32)> = HashMap::new();
    for _ in 0..500_000 {
        match host.poll() {
            Action::HaveOutput => {
                let chunk = host.output();
                output.extend_from_slice(chunk);
                host.consume_output(chunk.len()).unwrap();
            }
            Action::ReadAt => {
                let (ticket, source, offset, len) = host.read_at();
                reads.insert(ticket, (source, offset, len));
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                {
                    let dst = host.input();
                    dst[..len as usize].copy_from_slice(&bytes);
                }
                host.commit_input(len as usize).unwrap();
                host.complete_read(ticket).unwrap();
            }
            Action::Finished => {
                let chunk = host.output();
                if !chunk.is_empty() {
                    output.extend_from_slice(chunk);
                    host.consume_output(chunk.len()).unwrap();
                }
                break;
            }
            Action::NeedInput | Action::Yield => {}
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!("compact host failed: {:?}", host.error()),
        }
    }
    assert!(output.starts_with(b"PAR1"), "output len {}", output.len());
    (output, reads.len())
}

fn footer_plan(output: &[u8]) -> wcol::formats::parquet::Plan {
    let tail: [u8; 8] = output[output.len() - 8..].try_into().unwrap();
    let range = footer_range(output.len() as u64, &tail).unwrap();
    wcol::formats::parquet::plan_footer(&output[range.start as usize..range.end as usize])
        .expect("plan footer")
}

#[test]
fn page_compact_merges_three_row_groups_into_one() {
    let path = std::env::temp_dir().join("wcol-page-compact-src.parquet");
    write_three_row_group_fixture(&path);
    let (output, reads) = compact_file(&path);
    let out_plan = footer_plan(&output);
    assert_eq!(out_plan.rows, 12);
    assert_eq!(out_plan.chunks.len(), 1);
    assert_eq!(reads, 3);
}

#[test]
fn page_compact_keeps_each_column_type_in_the_built_footer() {
    let path = std::env::temp_dir().join("wcol-page-compact-typed.parquet");
    let words: [&[u8]; 4] = [b"a", b"bb", b"ccc", b"dddd"];
    write_fixture(
        &path,
        &[
            Column {
                name: "n",
                rows: 4,
                codec: UNCOMPRESSED,
                present: None,
                values: ColumnValues::I32(&[1, 2, 3, 4]),
            },
            Column {
                name: "s",
                rows: 4,
                codec: UNCOMPRESSED,
                present: None,
                values: ColumnValues::Bytes(&words),
            },
            Column {
                name: "f",
                rows: 4,
                codec: UNCOMPRESSED,
                present: None,
                values: ColumnValues::F64(&[0.5, 1.5, 2.5, 3.5]),
            },
        ],
    );
    let (output, _) = compact_file(&path);
    let out_plan = footer_plan(&output);
    let types: Vec<(String, i32)> = out_plan
        .leaves
        .iter()
        .map(|leaf| (leaf.name.clone(), leaf.physical))
        .collect();
    assert_eq!(
        types,
        [
            ("n".to_string(), 1),
            ("s".to_string(), 6),
            ("f".to_string(), 5)
        ]
    );
    assert_eq!(out_plan.rows, 12);
}

/// Real ClickBench row groups: the committed sample's groups merge into one file that keeps
/// every row, read range by range.
#[test]
#[ignore = "page copy needs uncompressed, dictionary-free pages; the committed sample is Snappy-compressed"]
fn page_compact_merges_hits_sample_row_groups() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet");
    let file_size = std::fs::metadata(&path).unwrap().len();
    let (file, plan) = open_plan(&path).unwrap();
    let input_rows = plan.rows;
    let input_chunks = plan.chunks.len();
    assert!(input_chunks > plan.leaves.len(), "one row group");
    let inputs = vec![CompactionInputFile {
        object_key: "hits_subset_500k.parquet".to_string(),
        file_size,
        plan,
    }];
    let mut files = [(file, path)];
    let compact_plan = plan_page_compaction(
        SourceVersion {
            object_key_hash: 42,
        },
        "hits-sample",
        inputs,
        PageCompactionPolicy {
            target_output_bytes: 600 * 1024 * 1024,
            target_row_group_bytes: 128 * 1024 * 1024,
            output_parts: 1,
        },
    )
    .unwrap();
    let wire =
        encode_page_compact_wire(&compact_plan.parts[0], &compact_plan.inputs[0].plan.leaves)
            .unwrap();
    let mut host = open_wcol_plan(&wire, 1024 * 1024).unwrap();
    let mut output = Vec::new();
    let mut reads: HashMap<u32, (u16, u64, u32)> = HashMap::new();
    let mut actions: HashMap<String, u32> = HashMap::new();
    for _ in 0..500_000 {
        let action = host.poll();
        *actions.entry(format!("{action:?}")).or_insert(0) += 1;
        match action {
            Action::HaveOutput => {
                let chunk = host.output();
                output.extend_from_slice(chunk);
                host.consume_output(chunk.len()).unwrap();
            }
            Action::ReadAt => {
                let (ticket, source, offset, len) = host.read_at();
                reads.insert(ticket, (source, offset, len));
                let (file, _) = &mut files[source as usize];
                let mut bytes = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                {
                    let dst = host.input();
                    dst[..len as usize].copy_from_slice(&bytes);
                }
                host.commit_input(len as usize).unwrap();
                host.complete_read(ticket).unwrap();
            }
            Action::Finished => {
                let chunk = host.output();
                if !chunk.is_empty() {
                    output.extend_from_slice(chunk);
                    host.consume_output(chunk.len()).unwrap();
                }
                break;
            }
            Action::NeedInput | Action::Yield => {}
            Action::SpillExec => panic!("{}", wcol::stream::native_poll::SPILL_EXEC_NATIVE),
            Action::Failed => panic!(
                "compact host failed: {:?} actions {:?}",
                host.error(),
                actions
            ),
        }
    }
    assert!(
        reads.len() > 10,
        "reads {} actions {:?}",
        reads.len(),
        actions
    );
    let out_plan = footer_plan(&output);
    assert_eq!(out_plan.rows, input_rows);
    assert!(
        out_plan.chunks.len() < input_chunks,
        "{} of {input_chunks} column chunks",
        out_plan.chunks.len()
    );
}
