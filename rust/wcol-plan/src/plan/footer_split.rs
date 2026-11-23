//! Split a Parquet object by row group without decoding it.
//!
//! The part copies each selected column chunk byte for byte (dictionary and data pages, any
//! codec) and carries the source's own footer, rewritten: only the selected row groups, with
//! every offset moved to where its chunk now sits. Schema, encodings, statistics and
//! key-value metadata are copied unchanged. Page indexes and bloom filters live outside the
//! copied chunks, so their offsets are dropped.

use std::ops::Range;

use compact_parquet_thrift::{Reader, Result as ThriftResult, Sink, WireType, Writer};

use crate::formats::parquet::schema::Plan;
use crate::plan::page_compaction::{CopySegment, MergedColumnPlan, MergedRowGroupPlan};

/// Parquet's leading magic: the first chunk of a part starts after it.
const MAGIC_LEN: u64 = 4;

// Field ids from parquet.thrift.
const FILE_NUM_ROWS: i16 = 3;
const FILE_ROW_GROUPS: i16 = 4;
const GROUP_COLUMNS: i16 = 1;
const GROUP_FILE_OFFSET: i16 = 5;
const GROUP_ORDINAL: i16 = 7;
const CHUNK_FILE_OFFSET: i16 = 2;
const CHUNK_META_DATA: i16 = 3;
/// `offset_index_offset`, `offset_index_length`, `column_index_offset`, `column_index_length`.
const CHUNK_PAGE_INDEX: Range<i16> = 4..8;
const META_DATA_PAGE_OFFSET: i16 = 9;
const META_INDEX_PAGE_OFFSET: i16 = 10;
const META_DICTIONARY_PAGE_OFFSET: i16 = 11;
/// `bloom_filter_offset`, `bloom_filter_length`.
const META_BLOOM_FILTER: Range<i16> = 14..16;

/// What a part copies, in order, and the footer it ends with.
#[derive(Debug)]
pub struct RowGroupSplit {
    pub row_groups: Vec<MergedRowGroupPlan>,
    /// `FileMetaData` thrift, without the trailing length and magic.
    pub footer: Vec<u8>,
    pub rows: i64,
}

/// The part holding `range` of `plan`'s row groups. `footer` is the source's `FileMetaData`.
pub fn split_row_groups(
    footer: &[u8],
    plan: &Plan,
    range: Range<u32>,
) -> Result<RowGroupSplit, String> {
    let groups = plan
        .chunks
        .iter()
        .map(|chunk| chunk.row_group + 1)
        .max()
        .unwrap_or(0);
    if range.is_empty() || range.end > groups {
        return Err(format!(
            "rowGroups {}..{} is not within the input's {groups} row groups",
            range.start, range.end
        ));
    }
    let leaves = plan.leaves.len();
    // `shifts[group][leaf]`: how far that chunk moves, new offset minus old.
    let mut shifts = Vec::with_capacity(range.len());
    let mut row_groups = Vec::with_capacity(range.len());
    let mut at = MAGIC_LEN;
    let mut rows = 0i64;
    for group in range.clone() {
        let mut columns = Vec::with_capacity(leaves);
        let mut moved = Vec::with_capacity(leaves);
        for leaf in 0..leaves as u32 {
            let chunk = plan
                .chunks
                .iter()
                .find(|chunk| chunk.row_group == group && chunk.leaf == leaf)
                .ok_or_else(|| format!("row group {group} has no chunk for column {leaf}"))?;
            let length = u32::try_from(chunk.compressed_size)
                .map_err(|_| format!("row group {group} column {leaf} chunk exceeds 4 GiB"))?;
            moved.push(signed(at)? - signed(chunk.start)?);
            columns.push(MergedColumnPlan {
                leaf,
                num_values: chunk.num_values,
                uncompressed_size: chunk.uncompressed_size,
                compressed_size: chunk.compressed_size,
                segments: vec![CopySegment {
                    input_index: 0,
                    offset: chunk.start,
                    length,
                }],
            });
            at += chunk.compressed_size;
        }
        let num_rows = plan
            .chunks
            .iter()
            .find(|chunk| chunk.row_group == group)
            .map(|chunk| chunk.rows)
            .unwrap_or(0);
        rows += num_rows;
        shifts.push(moved);
        row_groups.push(MergedRowGroupPlan { num_rows, columns });
    }
    let footer = rewrite_footer(footer, &range, &shifts, rows)
        .map_err(|err| format!("parquet footer: {err:?}"))?;
    Ok(RowGroupSplit {
        row_groups,
        footer,
        rows,
    })
}

fn signed(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "file offset exceeds i64".to_string())
}

struct VecSink(Vec<u8>);

impl Sink for VecSink {
    fn write(&mut self, bytes: &[u8]) -> ThriftResult<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

fn rewrite_footer(
    footer: &[u8],
    range: &Range<u32>,
    shifts: &[Vec<i64>],
    rows: i64,
) -> ThriftResult<Vec<u8>> {
    let mut reader = Reader::new(footer);
    let mut writer = Writer::new(VecSink(Vec::with_capacity(footer.len())));
    let mut file = reader.structure()?;
    writer.structure(|w| {
        while let Some(field) = file.next_field()? {
            match field.id {
                FILE_NUM_ROWS => {
                    file.skip(field.kind)?;
                    w.field_i64(FILE_NUM_ROWS, rows)?;
                }
                FILE_ROW_GROUPS => {
                    let list = file.list_header()?;
                    w.field_header(FILE_ROW_GROUPS, WireType::List)?;
                    w.list_header(WireType::Struct, range.len() as u32)?;
                    for index in 0..list.len {
                        if range.contains(&index) {
                            let local = (index - range.start) as usize;
                            rewrite_row_group(&mut file, w, &shifts[local], local as i16)?;
                        } else {
                            file.skip(list.element)?;
                        }
                    }
                }
                id => w.raw_field(id, field.kind, file.raw_value(field.kind)?)?,
            }
        }
        Ok(())
    })?;
    drop(file);
    Ok(writer.into_inner().0)
}

fn rewrite_row_group(
    reader: &mut Reader<'_>,
    w: &mut Writer<VecSink>,
    shifts: &[i64],
    ordinal: i16,
) -> ThriftResult<()> {
    let mut group = reader.structure()?;
    w.structure(|w| {
        while let Some(field) = group.next_field()? {
            match field.id {
                GROUP_COLUMNS => {
                    let list = group.list_header()?;
                    w.field_header(GROUP_COLUMNS, WireType::List)?;
                    w.list_header(WireType::Struct, list.len)?;
                    for leaf in 0..list.len as usize {
                        // A footer with more columns than the plan has leaves is malformed.
                        let shift = *shifts
                            .get(leaf)
                            .ok_or(compact_parquet_thrift::Error::UnexpectedWireType)?;
                        rewrite_column_chunk(&mut group, w, shift)?;
                    }
                }
                GROUP_FILE_OFFSET => {
                    let offset = group.i64()?;
                    w.field_i64(GROUP_FILE_OFFSET, offset + shifts[0])?;
                }
                GROUP_ORDINAL => {
                    group.skip(field.kind)?;
                    w.field_i16(GROUP_ORDINAL, ordinal)?;
                }
                id => w.raw_field(id, field.kind, group.raw_value(field.kind)?)?,
            }
        }
        Ok(())
    })
}

fn rewrite_column_chunk(
    reader: &mut Reader<'_>,
    w: &mut Writer<VecSink>,
    shift: i64,
) -> ThriftResult<()> {
    let mut chunk = reader.structure()?;
    w.structure(|w| {
        while let Some(field) = chunk.next_field()? {
            match field.id {
                CHUNK_FILE_OFFSET => {
                    let offset = chunk.i64()?;
                    w.field_i64(CHUNK_FILE_OFFSET, offset + shift)?;
                }
                CHUNK_META_DATA => {
                    w.field_header(CHUNK_META_DATA, WireType::Struct)?;
                    rewrite_column_meta(&mut chunk, w, shift)?;
                }
                id if CHUNK_PAGE_INDEX.contains(&id) => chunk.skip(field.kind)?,
                id => w.raw_field(id, field.kind, chunk.raw_value(field.kind)?)?,
            }
        }
        Ok(())
    })
}

fn rewrite_column_meta(
    reader: &mut Reader<'_>,
    w: &mut Writer<VecSink>,
    shift: i64,
) -> ThriftResult<()> {
    let mut meta = reader.structure()?;
    w.structure(|w| {
        while let Some(field) = meta.next_field()? {
            match field.id {
                id @ (META_DATA_PAGE_OFFSET
                | META_INDEX_PAGE_OFFSET
                | META_DICTIONARY_PAGE_OFFSET) => {
                    let offset = meta.i64()?;
                    w.field_i64(id, offset + shift)?;
                }
                id if META_BLOOM_FILTER.contains(&id) => meta.skip(field.kind)?,
                id => w.raw_field(id, field.kind, meta.raw_value(field.kind)?)?,
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::parquet::plan_footer;

    fn fixture() -> Vec<u8> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/duckdb_nulls.parquet"
        );
        std::fs::read(path).unwrap()
    }

    fn footer_of(file: &[u8]) -> &[u8] {
        let len =
            u32::from_le_bytes(file[file.len() - 8..file.len() - 4].try_into().unwrap()) as usize;
        &file[file.len() - 8 - len..file.len() - 8]
    }

    /// The bytes a page-copy program writes for `split`.
    fn assemble(source: &[u8], split: &RowGroupSplit) -> Vec<u8> {
        let mut out = b"PAR1".to_vec();
        for group in &split.row_groups {
            for column in &group.columns {
                for segment in &column.segments {
                    let start = segment.offset as usize;
                    out.extend_from_slice(&source[start..start + segment.length as usize]);
                }
            }
        }
        out.extend_from_slice(&split.footer);
        out.extend_from_slice(&(split.footer.len() as u32).to_le_bytes());
        out.extend_from_slice(b"PAR1");
        out
    }

    #[test]
    fn a_part_reads_back_as_the_selected_row_groups_byte_for_byte() {
        let source = fixture();
        let plan = plan_footer(footer_of(&source)).unwrap();
        // Snappy and dictionary pages: chunks a merge could not copy.
        assert!(plan
            .chunks
            .iter()
            .any(|chunk| chunk.codec != 0 && chunk.has_dictionary));
        let split = split_row_groups(footer_of(&source), &plan, 1..3).unwrap();
        let part = assemble(&source, &split);
        let copied = plan_footer(footer_of(&part)).unwrap();
        assert_eq!(copied.leaves, plan.leaves);
        let expected_rows: i64 = (1..3)
            .map(|group| {
                plan.chunks
                    .iter()
                    .find(|chunk| chunk.row_group == group)
                    .unwrap()
                    .rows
            })
            .sum();
        assert_eq!((copied.rows, split.rows), (expected_rows, expected_rows));
        assert_eq!(copied.chunks.len(), 2 * plan.leaves.len());
        for chunk in &copied.chunks {
            let original = plan
                .chunks
                .iter()
                .find(|old| old.row_group == chunk.row_group + 1 && old.leaf == chunk.leaf)
                .unwrap();
            assert_eq!(
                (chunk.codec, chunk.has_dictionary, chunk.num_values),
                (original.codec, original.has_dictionary, original.num_values)
            );
            let at = |c: &crate::formats::parquet::Chunk, file: &[u8]| {
                file[c.start as usize..(c.start + c.compressed_size) as usize].to_vec()
            };
            assert_eq!(
                at(chunk, &part),
                at(original, &source),
                "row group {} column {}",
                chunk.row_group,
                chunk.leaf
            );
        }
    }

    #[test]
    fn a_range_outside_the_file_is_refused() {
        let source = fixture();
        let plan = plan_footer(footer_of(&source)).unwrap();
        assert!(split_row_groups(footer_of(&source), &plan, 2..4).is_err());
        assert!(split_row_groups(footer_of(&source), &plan, 1..1).is_err());
    }
}
