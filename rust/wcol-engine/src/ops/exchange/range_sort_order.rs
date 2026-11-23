//! Concatenated range shards stay in global sort order across Parquet row groups.

use std::cmp::Ordering;

use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Identity, PageAssembler, PageEvent, Physical, Value,
};
use compact_parquet_thrift::parquet::{footer_range, page_header, scan_footer};

use super::{compare_composite, row_composite, split_owned_batch, RangePartitionPlan};
use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::formats::parquet::{ParquetEncode, ParquetEncodeConfig};
use crate::ir::{Scalar, SortKey as RowSortKey};
use crate::ops::sort::sort_rows;
use crate::program::types::{SortKey, SortTarget};
use crate::stream::{OwnedWcolBatch, WcolColumn};

type Cell = Option<i64>;
type Row = Vec<Cell>;

struct SortCase {
    name: &'static str,
    keys: Vec<SortKey>,
    row_keys: Vec<RowSortKey>,
    rows: Vec<(Cell, Cell)>,
}

#[test]
fn range_shards_match_global_order_across_row_groups() {
    let mut crossed_a_boundary = false;
    for case in cases() {
        let batch = batch_of(&case.rows);
        let sorted = sort_rows(std::slice::from_ref(&batch), &case.row_keys).unwrap();
        let expected = read_i64(&sorted);
        let plan = plan_for(&sorted, &case.keys);
        let parts = split_owned_batch(&plan, &batch).unwrap();
        let mut concatenated = Vec::new();
        let mut previous_shard_end: Option<Row> = None;
        for (part_index, part) in parts.iter().enumerate() {
            if part.row_count() == 0 {
                continue;
            }
            let local = sort_rows(std::slice::from_ref(part), &case.row_keys).unwrap();
            let decoded = decode_shard(&encode_shard(&local));
            assert!(
                !decoded.group_rows.is_empty(),
                "{} shard {part_index} wrote no row groups",
                case.name
            );
            if decoded.group_rows.len() > 1 {
                crossed_a_boundary = true;
            }
            assert_eq!(
                decoded.columns,
                read_i64(&local),
                "{} shard {part_index} changed order inside its parquet file",
                case.name
            );
            assert_boundaries(&case, &decoded, part_index);
            if let Some(previous) = previous_shard_end.as_ref() {
                let next = row_at(&decoded.columns, 0);
                assert_ne!(
                    compare_rows(&case.keys, previous, &next),
                    Ordering::Greater,
                    "{} shard {part_index} starts before the previous shard ends",
                    case.name
                );
            }
            if !decoded.columns[0].is_empty() {
                previous_shard_end = Some(row_at(&decoded.columns, decoded.columns[0].len() - 1));
            }
            for row in 0..decoded.columns[0].len() {
                concatenated.push(row_at(&decoded.columns, row));
            }
        }
        let expected_rows: Vec<Row> = (0..expected[0].len())
            .map(|row| row_at(&expected, row))
            .collect();
        assert_eq!(
            concatenated, expected_rows,
            "{} concatenated shards are not the global sort",
            case.name
        );
    }
    assert!(crossed_a_boundary, "no shard wrote more than one row group");
}

fn cases() -> Vec<SortCase> {
    let mut rows = Vec::new();
    for value in [1_i64, 1, 1, 2, 2, 5, 5, 9, -4, -4, 0, 7, 8, 3, 40, -20] {
        rows.push((Some(value), Some(rows.len() as i64)));
    }
    for _ in 0..6 {
        rows.push((None, Some(rows.len() as i64)));
    }
    // Enough rows that a partition crosses several row groups of 8.
    for index in 0..220 {
        let key = (index % 17) as i64 - 3;
        rows.push((Some(key), Some(1_000 + index as i64)));
    }
    let uniform = shuffle(rows);

    let mut skewed = Vec::new();
    for index in 0..180 {
        skewed.push((Some(0), Some(index as i64)));
    }
    for key in 1..40 {
        skewed.push((Some(key), Some(10_000 + key as i64)));
        skewed.push((Some(key), Some(20_000 + key as i64)));
    }
    skewed.push((None, Some(30_000)));
    skewed.push((None, Some(30_001)));
    let skewed = shuffle(skewed);

    let mut composite = Vec::new();
    for left in [None, Some(1), Some(1), Some(2), Some(4), Some(4), Some(9)] {
        for right in [None, Some(1), Some(1), Some(5), Some(8)] {
            composite.push((left, right));
        }
    }
    for index in 0..160 {
        composite.push((Some((index % 11) as i64), Some((index % 5) as i64)));
    }
    let composite = shuffle(composite);

    vec![
        SortCase {
            name: "asc nulls last duplicates",
            keys: vec![range_key(0, false, false)],
            row_keys: vec![row_key(0, false, false)],
            rows: uniform.clone(),
        },
        SortCase {
            name: "desc nulls last",
            keys: vec![range_key(0, true, false)],
            row_keys: vec![row_key(0, true, false)],
            rows: uniform.clone(),
        },
        SortCase {
            name: "desc nulls first",
            keys: vec![range_key(0, true, true)],
            row_keys: vec![row_key(0, true, true)],
            rows: uniform,
        },
        SortCase {
            name: "composite asc then desc",
            keys: vec![range_key(0, false, false), range_key(1, true, false)],
            row_keys: vec![row_key(0, false, false), row_key(1, true, false)],
            rows: composite,
        },
        SortCase {
            name: "skewed asc nulls last",
            keys: vec![range_key(0, false, false)],
            row_keys: vec![row_key(0, false, false)],
            rows: skewed,
        },
    ]
}

fn range_key(column: u32, descending: bool, nulls_first: bool) -> SortKey {
    SortKey {
        target: SortTarget::Column(column),
        descending,
        nulls_first,
    }
}

fn row_key(column: u32, descending: bool, nulls_first: bool) -> RowSortKey {
    RowSortKey {
        column,
        descending,
        nulls_first,
    }
}

fn shuffle(mut rows: Vec<(Cell, Cell)>) -> Vec<(Cell, Cell)> {
    let mut state = 0x1234_5678_u64;
    for index in (1..rows.len()).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let swap = (state as usize) % (index + 1);
        rows.swap(index, swap);
    }
    rows
}

fn batch_of(rows: &[(Cell, Cell)]) -> OwnedWcolBatch {
    let mut left = Vec::with_capacity(rows.len());
    let mut left_nulls = Vec::with_capacity(rows.len());
    let mut right = Vec::with_capacity(rows.len());
    let mut right_nulls = Vec::with_capacity(rows.len());
    for (a, b) in rows {
        match a {
            Some(value) => {
                left.push(*value);
                left_nulls.push(1);
            }
            None => {
                left.push(0);
                left_nulls.push(0);
            }
        }
        match b {
            Some(value) => {
                right.push(*value);
                right_nulls.push(1);
            }
            None => {
                right.push(0);
                right_nulls.push(0);
            }
        }
    }
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, left, left_nulls);
    batch.push_i64_column(1, right, right_nulls);
    batch.set_row_count(rows.len()).unwrap();
    batch
}

fn read_i64(batch: &OwnedWcolBatch) -> Vec<Vec<Cell>> {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    view.columns
        .iter()
        .map(|col| match col {
            WcolColumn::I64 { values, nulls, .. } => values
                .iter()
                .zip(nulls.iter())
                .map(|(value, present)| if *present == 0 { None } else { Some(*value) })
                .collect(),
            _ => panic!("expected i64 columns"),
        })
        .collect()
}

fn row_at(columns: &[Vec<Cell>], row: usize) -> Row {
    columns.iter().map(|column| column[row]).collect()
}

fn plan_for(sorted: &OwnedWcolBatch, keys: &[SortKey]) -> RangePartitionPlan {
    let mut partitions = 4u32;
    loop {
        let splits = quantile_splits(sorted, keys, partitions);
        if splits.len() == (partitions - 1) as usize {
            return RangePartitionPlan::new(keys.to_vec(), partitions, splits).unwrap();
        }
        partitions -= 1;
        assert!(
            partitions >= 2,
            "could not place strictly increasing splits"
        );
    }
}

fn quantile_splits(
    sorted: &OwnedWcolBatch,
    keys: &[SortKey],
    partitions: u32,
) -> Vec<Vec<Option<Scalar>>> {
    let rows = sorted.row_count();
    let mut splits = Vec::new();
    for index in 1..partitions {
        let mut row = rows * index as usize / partitions as usize;
        loop {
            if row >= rows {
                return splits;
            }
            let key = row_composite(sorted, row, keys).unwrap();
            let increases = match splits.last() {
                None => true,
                Some(previous) => compare_composite(keys, previous, &key) == Ordering::Less,
            };
            if increases {
                splits.push(key);
                break;
            }
            row += 1;
        }
    }
    splits
}

fn compare_rows(keys: &[SortKey], left: &Row, right: &Row) -> Ordering {
    let left_opts: Vec<Option<Scalar>> = left.iter().copied().map(cell_scalar).collect();
    let right_opts: Vec<Option<Scalar>> = right.iter().copied().map(cell_scalar).collect();
    compare_composite(keys, &left_opts, &right_opts)
}

fn cell_scalar(cell: Cell) -> Option<Scalar> {
    cell.map(Scalar::Int)
}

struct DecodedShard {
    columns: Vec<Vec<Cell>>,
    group_rows: Vec<usize>,
}

fn encode_shard(batch: &OwnedWcolBatch) -> Vec<u8> {
    let mut enc = ParquetEncode::open(
        vec!["a".to_string(), "b".to_string()],
        vec![true, true],
        ParquetEncodeConfig {
            max_row_group_rows: 8,
            max_row_groups: 64,
        },
    )
    .unwrap();
    drive_encode(&mut enc, vec![batch.clone()]).unwrap()
}

fn drive_encode(enc: &mut ParquetEncode, batches: Vec<OwnedWcolBatch>) -> Result<Vec<u8>, String> {
    let mut file = Vec::new();
    let mut ring = vec![0u8; 64 * 1024];
    let mut pending: Option<WcolStreamItem> = None;
    let mut batch_iter = batches.into_iter();
    let mut eof = false;
    for _ in 0..100_000 {
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
            StreamProgress::HaveOutput => file.extend_from_slice(&ring[..written]),
            StreamProgress::Blocked | StreamProgress::NeedInput => {}
            StreamProgress::NeedHostRead { .. } => {
                return Err("unexpected host read".to_string());
            }
        }
    }
    Err("encode driver exceeded iteration budget".to_string())
}

fn decode_shard(file: &[u8]) -> DecodedShard {
    let tail: [u8; 8] = file[file.len() - 8..].try_into().unwrap();
    let range = footer_range(file.len() as u64, &tail).unwrap();
    let footer = &file[range.start as usize..range.end as usize];
    let mut chunks = Vec::new();
    scan_footer(footer, |chunk| {
        chunks.push((
            chunk.row_group,
            chunk.index,
            chunk.file_offset.unwrap_or(0) as usize,
            chunk.compressed_size.unwrap_or(0) as usize,
            chunk.codec.unwrap_or(0),
            chunk.row_group_rows.unwrap_or(0) as usize,
        ));
        Ok(())
    })
    .unwrap();
    chunks.sort_by_key(|chunk| (chunk.0, chunk.1));
    let mut columns: Vec<Vec<Cell>> = vec![Vec::new(), Vec::new()];
    let mut group_rows = Vec::new();
    let mut seen_group = None;
    for (row_group, index, offset, len, codec, rows) in chunks {
        if seen_group != Some(row_group) {
            seen_group = Some(row_group);
            if index == 0 {
                group_rows.push(rows);
            }
        }
        let body = &file[offset..offset + len];
        columns[index as usize].extend(decode_i64_chunk(body, codec));
    }
    DecodedShard {
        columns,
        group_rows,
    }
}

fn decode_i64_chunk(chunk: &[u8], codec: i32) -> Vec<Cell> {
    let mut at = 0;
    let mut rows = Vec::new();
    while at < chunk.len() {
        let (header, header_len) = page_header(&chunk[at..]).unwrap();
        let compressed = header.compressed_size.unwrap() as usize;
        let body = &chunk[at + header_len..at + header_len + compressed];
        let spec = ColumnSpec::flat(Physical::Int64, codec).optional();
        let count = header.data_v1_num_values.unwrap_or(0) as usize;
        let mut present = vec![0u8; count.div_ceil(8).max(1)];
        let mut widths = [0u8; 32];
        let mut lengths = vec![0u32; count.max(1)];
        let mut parts =
            vec![compact_parquet_thrift::values::DeltaByteParts::default(); count.max(1)];
        let mut previous = vec![0u8; 4096];
        let workspace = DecodeWorkspace {
            present: &mut present,
            widths: &mut widths,
            lengths: &mut lengths,
            parts: &mut parts,
            previous: &mut previous,
        };
        let mut identity = Identity::default();
        let mut page = PageAssembler::new(header, spec, workspace, &mut identity).unwrap();
        let mut sink = |event: PageEvent<'_>| match event {
            PageEvent::Null(_) => rows.push(None),
            PageEvent::Value(_, Value::I64(value)) => rows.push(Some(value)),
            PageEvent::Value(_, Value::DictionaryId(_)) => {
                panic!("plain int column was dictionary encoded");
            }
            other => panic!("unexpected parquet value {other:?}"),
        };
        assert_eq!(page.feed(body, &mut sink).unwrap(), body.len());
        page.finish(&mut sink).unwrap();
        at += header_len + compressed;
    }
    rows
}

fn assert_boundaries(case: &SortCase, decoded: &DecodedShard, part_index: usize) {
    let mut at = 0usize;
    for (group_index, rows) in decoded.group_rows.iter().copied().enumerate() {
        if group_index > 0 && rows > 0 && at > 0 {
            let previous = row_at(&decoded.columns, at - 1);
            let next = row_at(&decoded.columns, at);
            assert_ne!(
                compare_rows(&case.keys, &previous, &next),
                Ordering::Greater,
                "{} shard {part_index} row group {group_index} breaks sort order",
                case.name
            );
        }
        at += rows;
    }
}
