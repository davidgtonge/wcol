use compact_parquet_thrift::parquet::{footer_range, scan_footer, ColumnChunkInfo};
use compact_parquet_thrift::{Reader, WireType};

use super::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leaf {
    pub name: String,
    pub physical: i32,
    pub type_length: i32,
    pub max_def: u8,
    pub max_rep: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub leaf: u32,
    pub row_group: u32,
    pub codec: i32,
    pub num_values: i64,
    /// Absolute file offset of the first page in the column chunk.
    pub start: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    /// Footer reported a dictionary page for this column chunk.
    pub has_dictionary: bool,
    /// Compressed bytes of the dictionary page (header included), from the footer's page
    /// offsets. A cardinality signal for keys without statistics: a few hundred distinct
    /// strings fit a small page, millions fill the writer's dictionary limit.
    pub dictionary_bytes: Option<u64>,
    /// Footer points at a column index, offset index or bloom filter. Readers ignore them;
    /// page-copy compaction rejects them because copied pages would make them stale.
    pub has_page_index: bool,
    /// Row-group `num_rows` from the footer, shared by every column in the group.
    pub rows: i64,
    /// Integer min from column chunk statistics, when the physical type is BOOLEAN/INT32/INT64.
    pub min_i64: Option<i64>,
    /// Integer max from column chunk statistics, when the physical type is BOOLEAN/INT32/INT64.
    pub max_i64: Option<i64>,
    /// Raw min/max statistic bytes (FLOAT, DOUBLE, BYTE_ARRAY, …) when the footer provides them.
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
    /// Footer `distinct_count` for this chunk, when present and non-negative.
    pub distinct_count: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct Plan {
    pub rows: i64,
    pub leaves: Vec<Leaf>,
    pub chunks: Vec<Chunk>,
}

pub fn footer_span(file_size: u64, tail: &[u8; 8]) -> Result<(u64, u64), Error> {
    let range = footer_range(file_size, tail)?;
    let len = range.end.checked_sub(range.start).ok_or(Error::BadFooter)?;
    Ok((range.start, len))
}

pub fn plan_footer(footer: &[u8]) -> Result<Plan, Error> {
    let leaves = schema_leaves(footer)?;
    if leaves.is_empty() {
        return Err(Error::Message("parquet file has no columns"));
    }
    let mut chunks = Vec::new();
    let mut failure = None;
    let summary = scan_footer(footer, |column| {
        match owned_chunk(&column) {
            Ok(chunk) => chunks.push(chunk),
            Err(err) => failure = Some(err),
        }
        Ok(())
    })?;
    if let Some(err) = failure {
        return Err(err);
    }
    if chunks.len() % leaves.len() != 0 {
        return Err(Error::Message(
            "column chunks are not a multiple of schema leaves",
        ));
    }
    for (index, chunk) in chunks.iter_mut().enumerate() {
        chunk.leaf = (index % leaves.len()) as u32;
    }
    Ok(Plan {
        rows: summary.num_rows.unwrap_or(0),
        leaves,
        chunks,
    })
}

fn owned_chunk(column: &ColumnChunkInfo<'_>) -> Result<Chunk, Error> {
    if column.external_file.is_some() {
        return Err(Error::Message(
            "external parquet column files are not supported",
        ));
    }
    let data = column
        .data_page_offset
        .ok_or(Error::Message("column has no data page"))?;
    let start = match column.dictionary_page_offset {
        Some(dict) => dict.min(data),
        None => data,
    };
    if start < 0 || column.compressed_size.unwrap_or(-1) < 0 {
        return Err(Error::Message("column chunk offset is negative"));
    }
    let rows = column
        .row_group_rows
        .ok_or(Error::Message("row group has no num_rows"))?;
    if rows < 0 {
        return Err(Error::Message("row group num_rows is negative"));
    }
    Ok(Chunk {
        leaf: column.index,
        row_group: column.row_group,
        codec: column.codec.unwrap_or(0),
        num_values: column.num_values.unwrap_or(0),
        start: start as u64,
        compressed_size: column.compressed_size.unwrap_or(0) as u64,
        uncompressed_size: column.uncompressed_size.unwrap_or(0) as u64,
        has_dictionary: column.dictionary_page_offset.is_some(),
        dictionary_bytes: column
            .dictionary_page_offset
            .filter(|dict| *dict < data)
            .and_then(|dict| u64::try_from(data - dict).ok()),
        has_page_index: column.offset_index_offset.is_some()
            || column.column_index_offset.is_some()
            || column.bloom_filter_offset.is_some(),
        rows,
        min_i64: int_stat(column.physical_type, column.min_value.or(column.min)),
        max_i64: int_stat(column.physical_type, column.max_value.or(column.max)),
        min_bytes: owned_stat_bytes(column.min_value.or(column.min)),
        max_bytes: owned_stat_bytes(column.max_value.or(column.max)),
        distinct_count: column
            .distinct_count
            .and_then(|count| u64::try_from(count).ok()),
    })
}

fn owned_stat_bytes(bytes: Option<&[u8]>) -> Option<Vec<u8>> {
    bytes
        .filter(|value| !value.is_empty())
        .map(|value| value.to_vec())
}

fn int_stat(physical: Option<i32>, bytes: Option<&[u8]>) -> Option<i64> {
    let bytes = bytes?;
    match physical? {
        0 if bytes.len() == 1 => Some(i64::from(bytes[0])),
        1 if bytes.len() == 4 => Some(i32::from_le_bytes(bytes.try_into().ok()?) as i64),
        2 if bytes.len() == 8 => Some(i64::from_le_bytes(bytes.try_into().ok()?)),
        _ => None,
    }
}

/// Tightest safe distinct-count upper bound per leaf, from footer statistics.
/// `None` means that leaf has no bound the planner should trust.
/// Global integer min/max per leaf from footer chunk statistics, when present.
pub fn leaf_int_bounds(plan: &Plan) -> (Vec<Option<i64>>, Vec<Option<i64>>) {
    let count = plan.leaves.len();
    let mut mins = vec![None; count];
    let mut maxs = vec![None; count];
    for chunk in &plan.chunks {
        let index = chunk.leaf as usize;
        if index >= count {
            continue;
        }
        if let Some(value) = chunk.min_i64 {
            mins[index] = Some(mins[index].map_or(value, |current: i64| current.min(value)));
        }
        if let Some(value) = chunk.max_i64 {
            maxs[index] = Some(maxs[index].map_or(value, |current: i64| current.max(value)));
        }
    }
    (mins, maxs)
}

/// Global lexicographic min/max per leaf from footer chunk statistics, when present.
pub fn leaf_byte_bounds(plan: &Plan) -> (Vec<Option<Vec<u8>>>, Vec<Option<Vec<u8>>>) {
    let count = plan.leaves.len();
    let mut mins: Vec<Option<Vec<u8>>> = vec![None; count];
    let mut maxs: Vec<Option<Vec<u8>>> = vec![None; count];
    for chunk in &plan.chunks {
        let index = chunk.leaf as usize;
        if index >= count {
            continue;
        }
        if let Some(value) = &chunk.min_bytes {
            mins[index] = Some(match mins[index].take() {
                None => value.clone(),
                Some(current) => {
                    if value < &current {
                        value.clone()
                    } else {
                        current
                    }
                }
            });
        }
        if let Some(value) = &chunk.max_bytes {
            maxs[index] = Some(match maxs[index].take() {
                None => value.clone(),
                Some(current) => {
                    if value > &current {
                        value.clone()
                    } else {
                        current
                    }
                }
            });
        }
    }
    (mins, maxs)
}

/// Largest dictionary page of each leaf, or `None` when a chunk has no dictionary page.
pub fn leaf_dictionary_bytes(plan: &Plan) -> Vec<Option<u64>> {
    (0..plan.leaves.len())
        .map(|index| {
            let mut largest = None;
            for chunk in plan
                .chunks
                .iter()
                .filter(|chunk| chunk.leaf as usize == index)
            {
                largest = Some(largest.unwrap_or(0).max(chunk.dictionary_bytes?));
            }
            largest
        })
        .collect()
}

pub fn leaf_distinct_upper(plan: &Plan) -> Vec<Option<u64>> {
    plan.leaves
        .iter()
        .enumerate()
        .map(|(index, leaf)| {
            let chunks: Vec<&Chunk> = plan
                .chunks
                .iter()
                .filter(|chunk| chunk.leaf as usize == index)
                .collect();
            if chunks.is_empty() {
                return None;
            }
            let mut bound: Option<u64> = None;
            if leaf.physical == 0 {
                bound = Some(2);
            }
            if chunks.iter().all(|chunk| chunk.distinct_count.is_some()) {
                let sum = chunks.iter().fold(0u64, |total, chunk| {
                    total.saturating_add(chunk.distinct_count.unwrap_or(0))
                });
                bound = Some(bound.map_or(sum, |known| known.min(sum)));
            }
            if matches!(leaf.physical, 1 | 2)
                && chunks
                    .iter()
                    .all(|chunk| chunk.min_i64.is_some() && chunk.max_i64.is_some())
            {
                let min = chunks.iter().filter_map(|chunk| chunk.min_i64).min();
                let max = chunks.iter().filter_map(|chunk| chunk.max_i64).max();
                if let (Some(min), Some(max)) = (min, max) {
                    if max >= min {
                        let domain = (i128::from(max) - i128::from(min) + 1)
                            .clamp(0, i128::from(u64::MAX))
                            as u64;
                        bound = Some(bound.map_or(domain, |known| known.min(domain)));
                    }
                }
            }
            bound
        })
        .collect()
}

struct Element {
    name: String,
    physical: Option<i32>,
    type_length: Option<i32>,
    repetition: Option<i32>,
    num_children: Option<i32>,
}

fn schema_leaves(footer: &[u8]) -> Result<Vec<Leaf>, Error> {
    let mut reader = Reader::new(footer);
    let mut root = reader.structure()?;
    let mut elements = Vec::new();
    while let Some(field) = root.next_field()? {
        if field.id != 2 {
            root.skip(field.kind)?;
            continue;
        }
        field.expect(WireType::List)?;
        let list = root.list_header()?;
        if list.element != WireType::Struct {
            return Err(Error::Message("schema list is not structs"));
        }
        for _ in 0..list.len {
            elements.push(read_element(&mut root)?);
        }
    }
    if elements.is_empty() {
        return Err(Error::Message("parquet file has no schema"));
    }
    let mut leaves = Vec::new();
    let mut index = 0;
    walk(&elements, &mut index, 0, 0, true, &mut leaves)?;
    if index != elements.len() {
        return Err(Error::Message("schema walk did not consume every element"));
    }
    Ok(leaves)
}

fn read_element(reader: &mut Reader<'_>) -> Result<Element, Error> {
    let mut element = reader.structure()?;
    let mut out = Element {
        name: String::new(),
        physical: None,
        type_length: None,
        repetition: None,
        num_children: None,
    };
    while let Some(field) = element.next_field()? {
        match field.id {
            1 => {
                field.expect(WireType::I32)?;
                out.physical = Some(element.i32()?);
            }
            2 => {
                field.expect(WireType::I32)?;
                out.type_length = Some(element.i32()?);
            }
            3 => {
                field.expect(WireType::I32)?;
                out.repetition = Some(element.i32()?);
            }
            4 => {
                field.expect(WireType::Binary)?;
                let name = element.binary()?;
                out.name = std::str::from_utf8(name)
                    .map_err(|_| Error::Message("schema name is not utf-8"))?
                    .to_string();
            }
            5 => {
                field.expect(WireType::I32)?;
                out.num_children = Some(element.i32()?);
            }
            _ => element.skip(field.kind)?,
        }
    }
    Ok(out)
}

fn walk(
    elements: &[Element],
    index: &mut usize,
    def: u8,
    rep: u8,
    is_root: bool,
    leaves: &mut Vec<Leaf>,
) -> Result<(), Error> {
    let element = elements
        .get(*index)
        .ok_or(Error::Message("schema ended early"))?;
    *index += 1;
    let mut def = def;
    let mut rep = rep;
    if !is_root {
        match element.repetition {
            Some(1) => {
                def = def
                    .checked_add(1)
                    .ok_or(Error::Message("definition level overflow"))?
            }
            Some(2) => {
                def = def
                    .checked_add(1)
                    .ok_or(Error::Message("definition level overflow"))?;
                rep = rep
                    .checked_add(1)
                    .ok_or(Error::Message("repetition level overflow"))?;
            }
            Some(0) | None => {}
            Some(_) => return Err(Error::Message("unsupported repetition type")),
        }
    }
    let children = element.num_children.unwrap_or(0);
    if children == 0 {
        leaves.push(Leaf {
            name: element.name.clone(),
            physical: element.physical.unwrap_or(-1),
            type_length: element.type_length.unwrap_or(-1),
            max_def: def,
            max_rep: rep,
        });
        return Ok(());
    }
    for _ in 0..children {
        walk(elements, index, def, rep, false, leaves)?;
    }
    Ok(())
}

pub fn encode_plan(plan: &Plan) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    out.extend_from_slice(b"WPQ1");
    out.extend_from_slice(&plan.rows.to_le_bytes());
    out.extend_from_slice(&(plan.leaves.len() as u32).to_le_bytes());
    for leaf in &plan.leaves {
        let name = leaf.name.as_bytes();
        let name_len =
            u16::try_from(name.len()).map_err(|_| Error::Message("schema name is too long"))?;
        out.extend_from_slice(&name_len.to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&leaf.physical.to_le_bytes());
        out.extend_from_slice(&leaf.type_length.to_le_bytes());
        out.push(leaf.max_def);
        out.push(leaf.max_rep);
    }
    out.extend_from_slice(&(plan.chunks.len() as u32).to_le_bytes());
    for chunk in &plan.chunks {
        out.extend_from_slice(&chunk.leaf.to_le_bytes());
        out.extend_from_slice(&chunk.codec.to_le_bytes());
        out.extend_from_slice(&chunk.num_values.to_le_bytes());
        out.extend_from_slice(&chunk.start.to_le_bytes());
        out.extend_from_slice(&chunk.compressed_size.to_le_bytes());
    }
    let groups = plan
        .chunks
        .iter()
        .map(|chunk| chunk.row_group as usize + 1)
        .max()
        .unwrap_or(0);
    let mut group_rows = vec![None; groups];
    for chunk in &plan.chunks {
        let slot = &mut group_rows[chunk.row_group as usize];
        if let Some(existing) = *slot {
            if existing != chunk.rows {
                return Err(Error::Message("row group num_rows disagree"));
            }
        } else {
            *slot = Some(chunk.rows);
        }
    }
    out.extend_from_slice(&(groups as u32).to_le_bytes());
    for rows in group_rows {
        let rows = rows.ok_or(Error::Message("row group has no num_rows"))?;
        out.extend_from_slice(&rows.to_le_bytes());
    }
    Ok(out)
}
