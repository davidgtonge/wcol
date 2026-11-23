//! Streaming ParquetEncode sink acceptance (DoD Parquet output).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use compact_parquet_thrift::encode::ChunkPlace;
use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Identity, PageAssembler, PageEvent, Physical, Value,
};
use compact_parquet_thrift::parquet::{footer_range, page_header, scan_footer};
use compact_parquet_thrift::values::DeltaByteParts;
use wcol::columns_for_sql;
use wcol::exec::graph_scheduler::{PollSinkOutcome, SinkPoll};
use wcol::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use wcol::formats::parquet::{open_plan, plan_footer, ParquetEncode, ParquetEncodeConfig};
use wcol::ops::query::CatalogColumn;
use wcol::plan::sql_parser::ColumnKind;
use wcol::stream::{DictionaryValues, SelectionMask};
use wcol::{LinearColocatedGraph, OwnedWcolBatch};

fn drive_encode(
    enc: &mut ParquetEncode,
    batches: Vec<OwnedWcolBatch>,
    ring_size: usize,
) -> Result<Vec<u8>, String> {
    let mut file = Vec::new();
    let mut ring = vec![0u8; ring_size.max(1)];
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
            StreamProgress::HaveOutput => {
                file.extend_from_slice(&ring[..written]);
            }
            StreamProgress::NeedInput => {}
            StreamProgress::NeedHostRead { .. } => {
                return Err("unexpected host read".to_string());
            }
        }
    }
    Err("encode driver exceeded iteration budget".to_string())
}

fn drive_encode_graph(
    graph: &mut LinearColocatedGraph,
    batches: Vec<OwnedWcolBatch>,
    ring_size: usize,
) -> Result<Vec<u8>, String> {
    let mut file = Vec::new();
    let mut ring = vec![0u8; ring_size.max(1)];
    for batch in batches {
        graph.feed_sink_row_batch(batch)?;
        loop {
            match graph.poll_sink(&mut ring)? {
                PollSinkOutcome::ReadAt { .. } => {
                    return Err("unexpected host read from encode sink".to_string());
                }
                PollSinkOutcome::Done(poll, n) => {
                    if n > 0 {
                        file.extend_from_slice(&ring[..n]);
                    }
                    match poll {
                        SinkPoll::Blocked => break,
                        SinkPoll::Progress if n == 0 => break,
                        SinkPoll::Progress => continue,
                        SinkPoll::Finished => return Ok(file),
                    }
                }
            }
        }
    }
    graph.mark_sink_eof();
    for _ in 0..500_000 {
        match graph.poll_sink(&mut ring)? {
            PollSinkOutcome::ReadAt { .. } => {
                return Err("unexpected host read from encode sink".to_string());
            }
            PollSinkOutcome::Done(poll, n) => {
                if n > 0 {
                    file.extend_from_slice(&ring[..n]);
                }
                if poll == SinkPoll::Finished {
                    return Ok(file);
                }
            }
        }
    }
    Err("graph sink driver exceeded iteration budget".to_string())
}

fn decode_i64_column(file: &[u8], place: &ChunkPlace) -> Vec<i64> {
    let chunk =
        &file[place.offset as usize..place.offset as usize + place.compressed_size as usize];
    let mut at = 0;
    let mut rows = Vec::new();
    while at < chunk.len() {
        let (header, header_len) = page_header(&chunk[at..]).unwrap();
        let compressed = header.compressed_size.unwrap() as usize;
        let body = &chunk[at + header_len..at + header_len + compressed];
        let spec = ColumnSpec::flat(Physical::Int64, 0);
        let count = header.data_v1_num_values.unwrap() as usize;
        let mut present = vec![0u8; count.div_ceil(8).max(1)];
        let mut widths = [0u8; 32];
        let mut lengths = vec![0u32; count];
        let mut parts = vec![DeltaByteParts::default(); count];
        let mut previous = vec![0u8; 4096];
        let workspace = DecodeWorkspace {
            present: &mut present,
            widths: &mut widths,
            lengths: &mut lengths,
            parts: &mut parts,
            previous: &mut previous,
        };
        let mut page_values = Vec::new();
        let mut sink = |event: PageEvent<'_>| {
            if let PageEvent::Value(_, Value::I64(v)) = event {
                page_values.push(v);
            }
        };
        let mut identity = Identity::default();
        let mut page = PageAssembler::new(header, spec, workspace, &mut identity).unwrap();
        assert_eq!(page.feed(body, &mut sink).unwrap(), body.len());
        page.finish(&mut sink).unwrap();
        rows.extend(page_values);
        at += header_len + compressed;
    }
    rows
}

fn footer_row_groups(file: &[u8]) -> Vec<i64> {
    let tail: [u8; 8] = file[file.len() - 8..].try_into().unwrap();
    let range = footer_range(file.len() as u64, &tail).unwrap();
    let footer = &file[range.start as usize..range.end as usize];
    let mut rg_rows = std::collections::HashMap::new();
    scan_footer(footer, |chunk| {
        let rg = chunk.row_group;
        let n = chunk
            .row_group_rows
            .unwrap_or(chunk.num_values.unwrap_or(0));
        rg_rows.insert(rg, n);
        Ok(())
    })
    .unwrap();
    let mut keys: Vec<_> = rg_rows.keys().copied().collect();
    keys.sort();
    keys.into_iter().map(|k| rg_rows[&k]).collect()
}

fn sum_i64_values(file: &[u8]) -> i64 {
    let tail: [u8; 8] = file[file.len() - 8..].try_into().unwrap();
    let range = footer_range(file.len() as u64, &tail).unwrap();
    let footer = &file[range.start as usize..range.end as usize];
    let mut place = ChunkPlace::default();
    scan_footer(footer, |chunk| {
        if place.offset == 0 {
            place = ChunkPlace {
                offset: chunk.file_offset.unwrap_or(0) as u64,
                data_page_offset: chunk.data_page_offset.unwrap_or(0) as u64,
                dictionary_page_offset: chunk.dictionary_page_offset.map(|v| v as u64),
                compressed_size: chunk.compressed_size.unwrap_or(0) as u64,
                uncompressed_size: chunk.compressed_size.unwrap_or(0) as u64,
                ..ChunkPlace::default()
            };
        }
        Ok(())
    })
    .unwrap();
    decode_i64_column(file, &place).into_iter().sum()
}

fn batch_i64(values: &[i64]) -> OwnedWcolBatch {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, values.to_vec(), vec![1; values.len()]);
    batch.set_row_count(values.len()).unwrap();
    batch
}

#[test]
fn parquet_encode_sum_roundtrip() {
    let mut enc = ParquetEncode::open(
        vec!["sum_a".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 8192,
            max_row_groups: 4,
        },
    )
    .unwrap();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![20], vec![1]);
    batch.set_row_count(1).unwrap();
    let file = drive_encode(&mut enc, vec![batch], 4096).unwrap();
    assert_eq!(sum_i64_values(&file), 20);
}

#[test]
fn parquet_encode_writes_an_empty_file_when_no_rows_match() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 1024,
            max_row_groups: 4,
        },
    )
    .unwrap();
    let mut batch = batch_i64(&[1, 2, 3]);
    batch.set_selection_mask(SelectionMask::with_rows(3));
    let file = drive_encode(&mut enc, vec![batch], 4096).unwrap();
    assert!(file.starts_with(b"PAR1"));
    assert!(file.ends_with(b"PAR1"));
    assert!(footer_row_groups(&file).is_empty());
}

#[test]
fn parquet_encode_honours_selection_mask_and_dict_ids() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 100,
            max_row_groups: 4,
        },
    )
    .unwrap();
    let mut masked = batch_i64(&[1, 2, 3, 4]);
    let mut mask = SelectionMask::with_rows(4);
    mask.set_selected(0, true);
    mask.set_selected(2, true);
    masked.set_selection_mask(mask);

    let mut dict_batch = OwnedWcolBatch::empty();
    dict_batch.push_dict_id_column(
        0,
        vec![1, 0],
        vec![1, 1],
        DictionaryValues {
            scope: 0,
            column: 0,
            entries: wcol::stream::DictEntries::Int(std::sync::Arc::new(vec![10_i64, 20])),
        },
    );
    dict_batch.set_row_count(2).unwrap();

    let file = drive_encode(&mut enc, vec![masked, dict_batch], 4096).unwrap();
    assert_eq!(sum_i64_values(&file), 1 + 3 + 20 + 10);
    assert_eq!(footer_row_groups(&file), vec![4]);
}

#[test]
fn parquet_encode_multiple_batches_one_row_group() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 100,
            max_row_groups: 4,
        },
    )
    .unwrap();
    let file = drive_encode(&mut enc, vec![batch_i64(&[1, 2]), batch_i64(&[3, 4])], 4096).unwrap();
    assert_eq!(sum_i64_values(&file), 10);
    let groups = footer_row_groups(&file);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0], 4);
}

#[test]
fn parquet_encode_multiple_row_groups() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 2,
            max_row_groups: 8,
        },
    )
    .unwrap();
    let file = drive_encode(&mut enc, vec![batch_i64(&[1, 2, 3, 4, 5])], 4096).unwrap();
    let groups = footer_row_groups(&file);
    assert_eq!(groups.iter().sum::<i64>(), 5);
    assert_eq!(groups.len(), 3);
}

/// Range-sorted exports and row-group pruning read chunk min/max from the footer, so
/// files WCOL writes must carry them: per row group, over present rows only.
#[test]
fn parquet_encode_writes_chunk_min_max() {
    let values: Vec<i64> = (0..25).map(|i| 40 - i * 3).collect();
    let present: Vec<u8> = (0..25).map(|i| u8::from(i % 4 != 0)).collect();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, values.clone(), present.clone());
    batch.set_row_count(values.len()).unwrap();
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![true],
        ParquetEncodeConfig {
            max_row_group_rows: 10,
            max_row_groups: 8,
        },
    )
    .unwrap();
    let file = drive_encode(&mut enc, vec![batch], 4096).unwrap();
    let tail: [u8; 8] = file[file.len() - 8..].try_into().unwrap();
    let range = footer_range(file.len() as u64, &tail).unwrap();
    let plan = plan_footer(&file[range.start as usize..range.end as usize]).unwrap();
    let got: Vec<(Option<i64>, Option<i64>)> = plan
        .chunks
        .iter()
        .map(|chunk| (chunk.min_i64, chunk.max_i64))
        .collect();
    let expect: Vec<(Option<i64>, Option<i64>)> = values
        .chunks(10)
        .zip(present.chunks(10))
        .map(|(group, present)| {
            let kept = || {
                group
                    .iter()
                    .zip(present)
                    .filter(|(_, p)| **p != 0)
                    .map(|(v, _)| *v)
            };
            (kept().min(), kept().max())
        })
        .collect();
    assert_eq!(
        expect,
        vec![
            (Some(13), Some(37)),
            (Some(-17), Some(10)),
            (Some(-29), Some(-23))
        ]
    );
    assert_eq!(got, expect);
}

#[test]
fn parquet_encode_small_output_ring_completes() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig::default(),
    )
    .unwrap();
    let file = drive_encode(&mut enc, vec![batch_i64(&[7])], 16).unwrap();
    assert_eq!(sum_i64_values(&file), 7);
}

#[test]
fn parquet_encode_slow_sink_backpressure() {
    let mut enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig::default(),
    )
    .unwrap();
    let mut ring = [0u8; 64];
    let mut batch_slot = Some(WcolStreamItem::from_row_batch(batch_i64(&[9])));
    let mut blocked_while_pending = false;
    let mut file = Vec::new();
    let mut eof = false;
    let mut deny_writes = true;
    for _ in 0..10_000 {
        deny_writes = !deny_writes;
        let mut port: &mut [u8] = if deny_writes { &mut [] } else { &mut ring };
        let mut written = 0usize;
        let mut unused = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut batch_slot),
            input_eof: eof,
            output: &mut unused,
            host_bytes: None,
            output_port: Some(&mut port),
            bytes_written: &mut written,
        };
        match enc.poll(&mut io) {
            Ok(StreamProgress::Blocked) => {
                blocked_while_pending = true;
                continue;
            }
            Ok(StreamProgress::HaveOutput) => {
                if written > 0 {
                    file.extend_from_slice(&ring[..written]);
                }
                continue;
            }
            Ok(StreamProgress::NeedInput) => {
                if batch_slot.is_none() && !eof {
                    eof = true;
                }
            }
            Ok(StreamProgress::Finished) => break,
            Err(e) => panic!("{e}"),
            Ok(StreamProgress::NeedHostRead { .. }) => panic!("host read"),
        }
    }
    assert!(blocked_while_pending);
    assert_eq!(sum_i64_values(&file), 9);
}

#[test]
fn parquet_encode_graph_sink_matches_direct() {
    let batches = vec![batch_i64(&[2, 3])];
    let mut direct = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig::default(),
    )
    .unwrap();
    let direct_bytes = drive_encode(&mut direct, batches.clone(), 256).unwrap();

    let mut graph = LinearColocatedGraph::open_custom(None, vec![], vec![]);
    graph.attach_sink(Box::new(
        ParquetEncode::open(
            vec!["v".to_string()],
            vec![false],
            ParquetEncodeConfig::default(),
        )
        .unwrap(),
    ));
    let graph_bytes = drive_encode_graph(&mut graph, batches, 256).unwrap();
    assert_eq!(direct_bytes, graph_bytes);
}

#[test]
fn parquet_pipeline_sum_a_matches_json() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
    if !path.exists() {
        return;
    }
    let sql = "SELECT SUM(a) FROM flat";
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog: Vec<CatalogColumn> = plan
        .leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: ColumnKind::Number,
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect();
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = wcol::ingest_groups_from_plan(&plan, &columns);
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
            wcol::GraphPump::ReadAt {
                ticket,
                source: _,
                offset,
                length,
            } => {
                let mut bytes = vec![0u8; length as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                graph.complete_ingest_read(ticket, bytes).unwrap();
            }
            wcol::GraphPump::Progress => {}
            wcol::GraphPump::AwaitHost => {
                panic!("test harness must complete reads before AwaitHost");
            }
            wcol::GraphPump::RowsReady => panic!("aggregate graph queued rows"),
            wcol::GraphPump::Done => break,
        }
    }
    let result = graph.finish().unwrap().clone();
    let mut enc = ParquetEncode::open(
        vec!["sum_a".to_string()],
        vec![false],
        ParquetEncodeConfig::default(),
    )
    .unwrap();
    let parquet = drive_encode(&mut enc, vec![result], 512).unwrap();
    assert_eq!(sum_i64_values(&parquet), 20);
}

#[test]
fn parquet_encode_memory_budget_fixed_at_open() {
    let enc = ParquetEncode::open(
        vec!["v".to_string()],
        vec![false],
        ParquetEncodeConfig {
            max_row_group_rows: 4,
            max_row_groups: 2,
        },
    )
    .unwrap();
    assert_eq!(enc.max_row_group_rows(), 4);
    assert_eq!(enc.max_row_groups(), 2);
}

#[test]
fn no_batch_only_parquet_export_api() {
    let src = include_str!("../../../wcol-engine/src/formats/parquet/encode.rs");
    assert!(
        !src.contains("write_batch"),
        "batch-only export API must not exist"
    );
}
