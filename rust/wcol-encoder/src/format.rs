use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::constants::{
    HEADER_BYTES, HEADER_FLAG_DICT_COMPRESSED, INDEX_ENTRY_BYTES, NULL_SENTINEL_U64,
    ROWS_PER_CHUNK, WCOL_VERSION,
};
use crate::types::{ChunkPages, ColumnSpec, IndexLayout};

pub(crate) fn write_wcol(
    output: &Path,
    columns: &[ColumnSpec],
    chunks: &mut [ChunkPages],
    total_rows: u64,
) -> Result<()> {
    let schema_bytes = build_schema_bytes(columns)?;
    let dict_raw_bytes = build_dict_bytes(columns)?;
    let dict_raw_len = dict_raw_bytes.len();
    let (dict_bytes, header_flags) = if dict_raw_len == 0 {
        (dict_raw_bytes, 0)
    } else {
        let compressed = lz4_flex::block::compress(&dict_raw_bytes);
        if compressed.len() < dict_raw_len {
            (compressed, HEADER_FLAG_DICT_COMPRESSED)
        } else {
            (dict_raw_bytes, 0)
        }
    };

    let schema_off = HEADER_BYTES as u64;
    let schema_len = schema_bytes.len() as u64;
    let index_off = schema_off + schema_len;

    let layout = compute_index_layout(
        columns,
        chunks,
        schema_bytes.len(),
        dict_bytes.len(),
        index_off,
    )?;
    let dict_len = dict_bytes.len() as u64;
    let dict_raw_len_u64 = dict_raw_len as u64;
    let header = build_header(
        columns.len() as u32,
        chunks.len() as u32,
        schema_off,
        schema_len,
        index_off,
        layout.index_len,
        index_off + layout.index_len,
        dict_len,
        layout.data_off,
        total_rows,
        header_flags,
        dict_raw_len_u64,
    )?;

    let mut file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    file.write_all(&header)?;
    file.write_all(&schema_bytes)?;
    file.write_all(&build_toc(&layout.toc))?;
    for block in &layout.index_blocks {
        file.write_all(block)?;
    }
    if !dict_bytes.is_empty() {
        file.write_all(&dict_bytes)?;
    }
    let index_end = index_off + layout.index_len;
    let dict_end = index_end + dict_bytes.len() as u64;
    if layout.data_off < dict_end {
        bail!("data offset underflow");
    }
    let padding_len = layout.data_off - dict_end;
    if padding_len > 0 {
        let padding = vec![0u8; padding_len as usize];
        file.write_all(&padding)?;
    }
    for chunk in chunks.iter() {
        for page in &chunk.columns {
            file.write_all(&page.data_comp)?;
            if let Some(null_comp) = &page.null_comp {
                file.write_all(null_comp)?;
            }
            if let Some(empty_comp) = &page.empty_comp {
                file.write_all(empty_comp)?;
            }
        }
    }

    Ok(())
}

fn build_schema_bytes(columns: &[ColumnSpec]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for col in columns {
        let name_bytes = col.name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            bail!("Column name too long: {}", col.name);
        }
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.push(col.logical_type);
        out.push(col.physical_type);
        out.push(col.flags);
        out.push(col.encoding);
        out.extend_from_slice(&col.dict_id.to_le_bytes());
        out.push(col.dict_index_width);
        out.extend_from_slice(&col.scale.to_le_bytes());
        out.push(0);
    }
    Ok(out)
}

fn build_dict_bytes(columns: &[ColumnSpec]) -> Result<Vec<u8>> {
    let dict_cols: Vec<_> = columns
        .iter()
        .filter(|col| (col.flags & crate::constants::FLAG_DICT) != 0)
        .collect();
    if dict_cols.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    out.extend_from_slice(&(dict_cols.len() as u32).to_le_bytes());
    for col in dict_cols {
        out.extend_from_slice(&col.dict_id.to_le_bytes());
        out.extend_from_slice(&(col.dict_values.len() as u32).to_le_bytes());

        let mut max_len = 0usize;
        for value in &col.dict_values {
            max_len = max_len.max(value.len());
        }
        let len_width = if max_len <= u16::MAX as usize {
            2u8
        } else {
            4u8
        };
        out.push(len_width);
        out.push(0);
        out.extend_from_slice(&0u16.to_le_bytes());

        let mut total_len: u64 = 0;
        for value in &col.dict_values {
            let bytes = value.as_bytes();
            total_len = total_len
                .checked_add(bytes.len() as u64)
                .context("dictionary blob length overflow")?;
            if len_width == 2 {
                if bytes.len() > u16::MAX as usize {
                    bail!("Dictionary value too long in {}", col.name);
                }
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            } else {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            }
        }
        if total_len > u32::MAX as u64 {
            bail!("Dictionary blob length exceeds u32 range in {}", col.name);
        }
        for value in &col.dict_values {
            out.extend_from_slice(value.as_bytes());
        }
    }
    Ok(out)
}

fn compute_index_layout(
    columns: &[ColumnSpec],
    chunks: &mut [ChunkPages],
    schema_len: usize,
    dict_len: usize,
    index_off: u64,
) -> Result<IndexLayout> {
    let toc_len = chunks.len() * 8;
    let raw_len_per_chunk = columns.len() * INDEX_ENTRY_BYTES;
    let max_index_len = toc_len + raw_len_per_chunk * chunks.len();
    let data_off = HEADER_BYTES + schema_len + dict_len + max_index_len;
    let data_off = data_off as u64;

    let build = build_index_blocks(columns, chunks, index_off, data_off)?;
    Ok(IndexLayout {
        data_off,
        toc: build.toc,
        index_blocks: build.index_blocks,
        index_len: build.index_len,
    })
}

fn build_index_blocks(
    columns: &[ColumnSpec],
    chunks: &mut [ChunkPages],
    index_off: u64,
    data_off: u64,
) -> Result<IndexLayout> {
    let mut cursor = data_off;
    for chunk in chunks.iter_mut() {
        for page in &mut chunk.columns {
            page.data_off = cursor;
            cursor += page.data_comp.len() as u64;
            if let Some(null_comp) = &page.null_comp {
                page.null_off = cursor;
                cursor += null_comp.len() as u64;
            } else {
                page.null_off = NULL_SENTINEL_U64;
            }
            if let Some(empty_comp) = &page.empty_comp {
                page.empty_off = cursor;
                cursor += empty_comp.len() as u64;
            } else {
                page.empty_off = NULL_SENTINEL_U64;
            }
        }
    }

    let mut index_blocks = Vec::with_capacity(chunks.len());
    for chunk in chunks.iter() {
        let mut raw = Vec::with_capacity(columns.len() * INDEX_ENTRY_BYTES);
        for page in &chunk.columns {
            raw.extend_from_slice(&page.data_off.to_le_bytes());
            raw.extend_from_slice(&(page.data_comp.len() as u32).to_le_bytes());
            raw.extend_from_slice(&page.data_raw_len.to_le_bytes());
            raw.extend_from_slice(&page.null_off.to_le_bytes());
            raw.extend_from_slice(
                &(page.null_comp.as_ref().map(|v| v.len()).unwrap_or(0) as u32).to_le_bytes(),
            );
            raw.extend_from_slice(&page.null_raw_len.to_le_bytes());
            raw.extend_from_slice(&(page.empty_mode as u32).to_le_bytes());
            raw.extend_from_slice(&page.empty_count.to_le_bytes());
            raw.extend_from_slice(&page.empty_off.to_le_bytes());
            raw.extend_from_slice(
                &(page.empty_comp.as_ref().map(|v| v.len()).unwrap_or(0) as u32).to_le_bytes(),
            );
            raw.extend_from_slice(&page.empty_raw_len.to_le_bytes());
            raw.extend_from_slice(&page.min.to_le_bytes());
            raw.extend_from_slice(&page.max.to_le_bytes());
            raw.extend_from_slice(&page.presence.to_le_bytes());
        }
        index_blocks.push(lz4_flex::block::compress(&raw));
    }

    let mut toc = Vec::with_capacity(chunks.len());
    let toc_len = chunks.len() as u64 * 8;
    let mut offset = index_off + toc_len;
    for block in &index_blocks {
        toc.push(offset);
        offset = offset
            .checked_add(block.len() as u64)
            .context("index length overflow")?;
    }

    let index_len = toc_len + index_blocks.iter().map(|b| b.len() as u64).sum::<u64>();
    Ok(IndexLayout {
        data_off,
        toc,
        index_blocks,
        index_len,
    })
}

fn build_toc(toc: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(toc.len() * 8);
    for offset in toc {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn build_header(
    ncols: u32,
    nchunks: u32,
    schema_off: u64,
    schema_len: u64,
    index_off: u64,
    index_len: u64,
    dict_off: u64,
    dict_len: u64,
    data_off: u64,
    total_rows: u64,
    flags: u16,
    dict_raw_len: u64,
) -> Result<Vec<u8>> {
    let mut out = vec![0u8; HEADER_BYTES];
    out[0..4].copy_from_slice(b"WCOL");
    out[4..6].copy_from_slice(&WCOL_VERSION.to_le_bytes());
    out[6..8].copy_from_slice(&flags.to_le_bytes());
    out[8..12].copy_from_slice(&ncols.to_le_bytes());
    out[12..16].copy_from_slice(&nchunks.to_le_bytes());
    out[16..20].copy_from_slice(&(ROWS_PER_CHUNK as u32).to_le_bytes());
    out[20..28].copy_from_slice(&total_rows.to_le_bytes());
    out[28..36].copy_from_slice(&schema_off.to_le_bytes());
    out[36..44].copy_from_slice(&schema_len.to_le_bytes());
    out[44..52].copy_from_slice(&index_off.to_le_bytes());
    out[52..60].copy_from_slice(&index_len.to_le_bytes());
    out[60..68].copy_from_slice(&dict_off.to_le_bytes());
    out[68..76].copy_from_slice(&dict_len.to_le_bytes());
    out[76..84].copy_from_slice(&data_off.to_le_bytes());
    out[84..92].copy_from_slice(&dict_raw_len.to_le_bytes());
    Ok(out)
}
