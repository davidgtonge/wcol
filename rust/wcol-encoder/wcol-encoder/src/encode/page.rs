use anyhow::{bail, Result};
use rayon::prelude::*;

use crate::constants::{
    EMPTY_MODE_ALL_ONE, EMPTY_MODE_ALL_ZERO, EMPTY_MODE_MIXED, ENCODING_NUM_DICT, FLAG_NULLABLE,
    NULL_SENTINEL_U64, TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16, TYPE_I32, TYPE_I64, TYPE_I8,
    TYPE_STRING, TYPE_U16, TYPE_U32, TYPE_U8,
};
use crate::types::{ChunkPages, ColumnBuffer, ColumnPage, ColumnSpec, ColumnValues};
use crate::utils::{is_valid, set_bit};

use super::stats::compute_page_stats;

const STRING_LAYOUT_OPTION_A_FLAG: u8 = 0x80;

pub(crate) fn finalize_chunk(
    rows: usize,
    columns: &[ColumnSpec],
    buffers: &mut [ColumnBuffer],
) -> Result<ChunkPages> {
    let page_results: Vec<Result<ColumnPage>> = columns
        .par_iter()
        .zip(buffers.par_iter())
        .map(|(col, buffer)| encode_column_page(col, buffer, rows))
        .collect();
    let pages: Vec<ColumnPage> = page_results.into_iter().collect::<Result<Vec<_>>>()?;
    for buffer in buffers.iter_mut() {
        buffer.reset();
    }
    Ok(ChunkPages { columns: pages })
}

fn encode_column_page(col: &ColumnSpec, buffer: &ColumnBuffer, rows: usize) -> Result<ColumnPage> {
    let (min, max, presence) = compute_page_stats(col, buffer, rows);
    let data_raw = if col.encoding == ENCODING_NUM_DICT {
        encode_numeric_dict(col, buffer, rows)?
    } else {
        match col.physical_type {
            TYPE_STRING => encode_string(buffer, rows)?,
            TYPE_BOOL => encode_bool(buffer, rows)?,
            TYPE_U8 => encode_u8(buffer, rows)?,
            TYPE_U16 => encode_u16(buffer, rows)?,
            TYPE_U32 => encode_u32(buffer, rows)?,
            TYPE_I8 => encode_i8(buffer, rows)?,
            TYPE_I16 => encode_i16(buffer, rows)?,
            TYPE_I32 => encode_i32(buffer, rows)?,
            TYPE_I64 => encode_i64(buffer, rows)?,
            TYPE_F32 => encode_f32(buffer, rows)?,
            TYPE_F64 => encode_f64(buffer, rows)?,
            _ => bail!("Unsupported physical type for {}", col.name),
        }
    };

    let data_compressed = lz4_flex::block::compress(&data_raw);
    let data_comp = if data_compressed.len() >= data_raw.len() {
        data_raw.clone()
    } else {
        data_compressed
    };

    let mut null_raw_len = 0u32;
    let mut null_comp = None;
    if (col.flags & FLAG_NULLABLE) != 0 && buffer.has_nulls {
        let bytes = rows.div_ceil(8);
        let null_raw = buffer.nulls[..bytes].to_vec();
        null_raw_len = null_raw.len() as u32;
        let null_compressed = lz4_flex::block::compress(&null_raw);
        null_comp = if null_compressed.len() >= null_raw.len() {
            Some(null_raw)
        } else {
            Some(null_compressed)
        };
    }

    let mut empty_mode = EMPTY_MODE_ALL_ZERO;
    let mut empty_count = 0u32;
    let mut empty_raw_len = 0u32;
    let mut empty_comp = None;
    if col.physical_type == TYPE_STRING {
        empty_count = buffer.empty_count;
        if empty_count == 0 {
            empty_mode = EMPTY_MODE_ALL_ZERO;
        } else if buffer.null_count == 0 && empty_count == rows as u32 {
            empty_mode = EMPTY_MODE_ALL_ONE;
        } else {
            empty_mode = EMPTY_MODE_MIXED;
            let bytes = rows.div_ceil(8);
            let empty_raw = buffer.empties[..bytes].to_vec();
            empty_raw_len = empty_raw.len() as u32;
            let empty_compressed = lz4_flex::block::compress(&empty_raw);
            empty_comp = if empty_compressed.len() >= empty_raw.len() {
                Some(empty_raw)
            } else {
                Some(empty_compressed)
            };
        }
    }

    Ok(ColumnPage {
        data_raw_len: data_raw.len() as u32,
        data_comp,
        null_raw_len,
        null_comp,
        empty_mode,
        empty_count,
        empty_raw_len,
        empty_comp,
        data_off: 0,
        null_off: NULL_SENTINEL_U64,
        empty_off: NULL_SENTINEL_U64,
        min,
        max,
        presence,
    })
}

fn encode_bool(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values = match &buffer.values {
        ColumnValues::Bool(values) => values,
        _ => bail!("Expected bool buffer"),
    };
    let mut out = vec![0u8; rows.div_ceil(8)];
    for (i, &val) in values.iter().enumerate().take(rows) {
        if val {
            set_bit(&mut out, i);
        }
    }
    Ok(out)
}

#[allow(clippy::needless_range_loop)]
fn encode_numeric_dict(col: &ColumnSpec, buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let (value_width, min_value, max_value, is_unsigned) = match col.physical_type {
        TYPE_U8 => (1u8, 0i64, u8::MAX as i64, true),
        TYPE_U16 => (2u8, 0i64, u16::MAX as i64, true),
        TYPE_U32 => (4u8, 0i64, u32::MAX as i64, true),
        TYPE_I8 => (1u8, i8::MIN as i64, i8::MAX as i64, false),
        TYPE_I16 => (2u8, i16::MIN as i64, i16::MAX as i64, false),
        TYPE_I32 => (4u8, i32::MIN as i64, i32::MAX as i64, false),
        TYPE_I64 => (8u8, i64::MIN, i64::MAX, false),
        _ => bail!("Unsupported dict physical type for {}", col.name),
    };

    let mut dict_map: std::collections::HashMap<i64, u32> = std::collections::HashMap::new();
    let mut dict_values: Vec<i64> = Vec::new();
    let mut ids: Vec<u32> = Vec::with_capacity(rows);

    for idx in 0..rows {
        let value = if is_valid(&buffer.nulls, idx) {
            match &buffer.values {
                ColumnValues::Int(values) => values[idx],
                ColumnValues::Float(values) => values[idx] as i64,
                _ => bail!("Expected numeric buffer"),
            }
        } else {
            0
        };
        if value > max_value || value < min_value || (is_unsigned && value < 0) {
            bail!("Numeric dict value out of range for {}", col.name);
        }
        let id = if let Some(id) = dict_map.get(&value) {
            *id
        } else {
            let id = dict_values.len() as u32;
            dict_values.push(value);
            dict_map.insert(value, id);
            id
        };
        ids.push(id);
    }

    let dict_len = dict_values.len();
    if dict_len > u16::MAX as usize {
        bail!("Numeric dict too large for {}", col.name);
    }
    let id_width = if dict_len <= u8::MAX as usize {
        1u8
    } else if dict_len <= u16::MAX as usize {
        2u8
    } else {
        4u8
    };

    let raw = encode_numeric_raw(col, buffer, rows)?;
    let mut dict_values_bytes = Vec::with_capacity(dict_len * value_width as usize);
    for value in dict_values {
        match col.physical_type {
            TYPE_U8 => dict_values_bytes.push(value as u8),
            TYPE_I8 => dict_values_bytes.push(value as i8 as u8),
            TYPE_U16 => dict_values_bytes.extend_from_slice(&(value as u16).to_le_bytes()),
            TYPE_I16 => dict_values_bytes.extend_from_slice(&(value as i16).to_le_bytes()),
            TYPE_U32 => dict_values_bytes.extend_from_slice(&(value as u32).to_le_bytes()),
            TYPE_I32 => dict_values_bytes.extend_from_slice(&(value as i32).to_le_bytes()),
            TYPE_I64 => dict_values_bytes.extend_from_slice(&(value as i64).to_le_bytes()),
            _ => bail!("Unsupported dict physical type for {}", col.name),
        }
    }

    let dict_payload_len = 4 + dict_values_bytes.len() + rows * id_width as usize;
    let mut dict = Vec::with_capacity(dict_payload_len);
    dict.extend_from_slice(&(dict_len as u16).to_le_bytes());
    dict.push(id_width);
    dict.push(value_width);
    dict.extend_from_slice(&dict_values_bytes);

    match id_width {
        1 => {
            for &id in &ids {
                dict.push(id as u8);
            }
        }
        2 => {
            for &id in &ids {
                dict.extend_from_slice(&(id as u16).to_le_bytes());
            }
        }
        _ => {
            for &id in &ids {
                dict.extend_from_slice(&id.to_le_bytes());
            }
        }
    }

    // Choose based on compressed size to avoid regressions.
    let raw_comp = lz4_flex::block::compress(&raw);
    let dict_comp = lz4_flex::block::compress(&dict);
    let (bit_width, bitpacked_ids) = encode_bitpacked_ids(&ids, dict_len as u32);
    let mut bitpacked = Vec::with_capacity(4 + dict_values_bytes.len() + bitpacked_ids.len());
    bitpacked.extend_from_slice(&(dict_len as u16).to_le_bytes());
    bitpacked.push(bit_width);
    bitpacked.push(value_width);
    bitpacked.extend_from_slice(&dict_values_bytes);
    bitpacked.extend_from_slice(&bitpacked_ids);
    let bitpacked_comp = lz4_flex::block::compress(&bitpacked);

    let (mode, payload) =
        if bitpacked_comp.len() < dict_comp.len() && bitpacked_comp.len() < raw_comp.len() {
            (2u8, &bitpacked)
        } else if dict_comp.len() < raw_comp.len() {
            (1u8, &dict)
        } else {
            (0u8, &raw)
        };

    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(mode);
    out.extend_from_slice(payload);
    Ok(out)
}

fn encode_numeric_raw(col: &ColumnSpec, buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    match col.physical_type {
        TYPE_U8 => encode_u8(buffer, rows),
        TYPE_U16 => encode_u16(buffer, rows),
        TYPE_U32 => encode_u32(buffer, rows),
        TYPE_I8 => encode_i8(buffer, rows),
        TYPE_I16 => encode_i16(buffer, rows),
        TYPE_I32 => encode_i32(buffer, rows),
        TYPE_I64 => encode_i64(buffer, rows),
        TYPE_F32 => encode_f32(buffer, rows),
        TYPE_F64 => encode_f64(buffer, rows),
        _ => bail!("Unsupported numeric raw type for {}", col.name),
    }
}

fn encode_bitpacked_ids(ids: &[u32], dict_len: u32) -> (u8, Vec<u8>) {
    let bit_width = bit_width_for_len(dict_len);
    let total_bits = ids.len() * bit_width as usize;
    let byte_len = total_bits.div_ceil(8);
    let mut out = Vec::with_capacity(byte_len);
    if bit_width == 0 {
        return (bit_width, out);
    }
    let mut bit_pos = 0usize;
    out.resize(byte_len, 0u8);
    for &id in ids {
        let mut value = id;
        for _ in 0..bit_width {
            if value & 1 != 0 {
                let byte_idx = bit_pos >> 3;
                let bit_idx = bit_pos & 7;
                out[byte_idx] |= 1u8 << bit_idx;
            }
            value >>= 1;
            bit_pos += 1;
        }
    }
    (bit_width, out)
}

fn bit_width_for_len(len: u32) -> u8 {
    if len <= 1 {
        return 0;
    }
    32 - (len - 1).leading_zeros() as u8
}

fn encode_string(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values = match &buffer.values {
        ColumnValues::String(values) => values,
        _ => bail!("Expected string buffer"),
    };
    if rows > u16::MAX as usize {
        bail!("Block-local string rows exceed u16 max");
    }

    let mut sorted: Vec<usize> = (0..rows).collect();
    sorted.sort_by(|a, b| values[*a].as_bytes().cmp(values[*b].as_bytes()));

    // Option A: map each row to a unique sorted value id and store value payload for uniques only.
    let mut row_to_unique = vec![0u32; rows];
    let mut unique_values: Vec<String> = Vec::new();
    let mut lcps: Vec<u16> = Vec::new();
    let mut suffix_lens: Vec<u32> = Vec::new();
    let mut data_blob: Vec<u8> = Vec::new();
    let mut prev_unique: Vec<u8> = Vec::new();

    for &orig_idx in &sorted {
        let bytes = values[orig_idx].as_bytes();
        if !unique_values.is_empty() && bytes == prev_unique.as_slice() {
            row_to_unique[orig_idx] = (unique_values.len() - 1) as u32;
            continue;
        }
        let lcp = if unique_values.is_empty() {
            0
        } else {
            common_prefix_len(&prev_unique, bytes)
        };
        if lcp > u16::MAX as usize {
            bail!("Block-local string prefix exceeds u16 max");
        }
        let suffix = &bytes[lcp..];
        let suffix_len = u32::try_from(suffix.len())
            .map_err(|_| anyhow::anyhow!("String too large to encode"))?;
        lcps.push(lcp as u16);
        suffix_lens.push(suffix_len);
        data_blob.extend_from_slice(suffix);
        unique_values.push(values[orig_idx].clone());
        prev_unique.clear();
        prev_unique.extend_from_slice(bytes);
        row_to_unique[orig_idx] = (unique_values.len() - 1) as u32;
    }

    let v1 = build_block_option_a_v1(rows, &row_to_unique, &lcps, &suffix_lens, &data_blob, None)?;
    let v2 = build_block_option_a_v2(
        rows,
        &unique_values,
        &row_to_unique,
        &lcps,
        &suffix_lens,
        &data_blob,
    )?;

    if let Some(v2) = v2 {
        if v2.len() < v1.len() {
            return Ok(v2);
        }
    }
    Ok(v1)
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    for i in 0..max {
        if a[i] != b[i] {
            return i;
        }
    }
    max
}

fn build_block_option_a_v1(
    rows: usize,
    row_to_unique: &[u32],
    lcps: &[u16],
    suffix_lens: &[u32],
    data_blob: &[u8],
    dict_bytes: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let unique_count = lcps.len();
    let max_unique = unique_count.saturating_sub(1) as u32;
    let row_id_width = if max_unique <= u16::MAX as u32 {
        2u8
    } else {
        4u8
    };
    let max_suffix = suffix_lens.iter().copied().max().unwrap_or(0);
    let suffix_len_width = if max_suffix <= u16::MAX as u32 {
        2u8
    } else {
        4u8
    };
    let dict_len = dict_bytes.map(|d| d.len()).unwrap_or(0);
    let header_size = 32usize;
    let perm_off = header_size as u32;
    let lcp_off = perm_off + (rows as u32) * row_id_width as u32;
    let len_off = lcp_off + (unique_count as u32) * 2;
    let dict_off = len_off + (unique_count as u32) * suffix_len_width as u32;
    let data_off = dict_off + dict_len as u32;
    let data_len = u32::try_from(data_blob.len())
        .map_err(|_| anyhow::anyhow!("Block-local data blob too large"))?;

    let mut out = vec![0u8; header_size];
    out[0..2].copy_from_slice(&(rows as u16).to_le_bytes());
    out[2] = row_id_width;
    out[3] = suffix_len_width | STRING_LAYOUT_OPTION_A_FLAG;
    out[4..8].copy_from_slice(&perm_off.to_le_bytes());
    out[8..12].copy_from_slice(&lcp_off.to_le_bytes());
    out[12..16].copy_from_slice(&len_off.to_le_bytes());
    out[16..20].copy_from_slice(&data_off.to_le_bytes());
    out[20..24].copy_from_slice(&data_len.to_le_bytes());
    out[24..28].copy_from_slice(&dict_off.to_le_bytes());
    out[28..32].copy_from_slice(&(dict_len as u32).to_le_bytes());

    for &value in row_to_unique {
        if value >= unique_count as u32 {
            bail!("row_to_unique id out of range");
        }
        if row_id_width == 2 {
            out.extend_from_slice(&(value as u16).to_le_bytes());
        } else {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    for value in lcps {
        out.extend_from_slice(&value.to_le_bytes());
    }
    for value in suffix_lens {
        if suffix_len_width == 2 {
            let short = u16::try_from(*value)
                .map_err(|_| anyhow::anyhow!("Suffix length exceeds u16 max"))?;
            out.extend_from_slice(&short.to_le_bytes());
        } else {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    if let Some(dict) = dict_bytes {
        out.extend_from_slice(dict);
    }
    out.extend_from_slice(data_blob);
    Ok(out)
}

fn build_block_option_a_v2(
    rows: usize,
    unique_values: &[String],
    row_to_unique: &[u32],
    lcps: &[u16],
    suffix_lens: &[u32],
    data_blob: &[u8],
) -> Result<Option<Vec<u8>>> {
    let (dict, dict_bytes) = build_token_dict(unique_values)?;
    if dict.is_empty() {
        return Ok(None);
    }
    let encoded = encode_token_stream(data_blob, &dict);
    if encoded.len() >= data_blob.len() {
        return Ok(None);
    }
    let out = build_block_option_a_v1(
        rows,
        row_to_unique,
        lcps,
        suffix_lens,
        &encoded,
        Some(&dict_bytes),
    )?;
    Ok(Some(out))
}

fn build_token_dict(values: &[String]) -> Result<(Vec<Vec<u8>>, Vec<u8>)> {
    let mut idxs: Vec<usize> = (0..values.len()).collect();
    idxs.sort_by_key(|idx| std::cmp::Reverse(values[*idx].len()));
    let take = idxs.len().min(200);
    let delimiters: &[u8] = b"/.?=&:\" ";
    let mut counts: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();

    for &idx in idxs.iter().take(take) {
        let bytes = values[idx].as_bytes();
        let mut start = 0usize;
        for (pos, b) in bytes.iter().enumerate() {
            if delimiters.contains(b) {
                count_segment(bytes, start, pos, &mut counts);
                start = pos + 1;
            }
        }
        count_segment(bytes, start, bytes.len(), &mut counts);
    }

    let mut candidates: Vec<(Vec<u8>, u32)> = counts.into_iter().collect();
    candidates.retain(|(token, _)| token.len() >= 3 && token.len() <= 8);
    candidates.sort_by(|(a_tok, a_count), (b_tok, b_count)| {
        let a_score = (*a_count as usize) * (a_tok.len().saturating_sub(1));
        let b_score = (*b_count as usize) * (b_tok.len().saturating_sub(1));
        b_score
            .cmp(&a_score)
            .then_with(|| b_tok.len().cmp(&a_tok.len()))
            .then_with(|| b_tok.cmp(a_tok))
    });

    let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(128);
    for (token, _) in candidates.into_iter().take(128) {
        tokens.push(token);
    }
    if tokens.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    while tokens.len() < 128 {
        tokens.push(Vec::new());
    }

    let mut dict_blob: Vec<u8> = Vec::new();
    let mut offsets: Vec<u16> = Vec::with_capacity(129);
    offsets.push(0);
    for token in &tokens {
        dict_blob.extend_from_slice(token);
        let next = dict_blob.len();
        if next > u16::MAX as usize {
            return Ok((Vec::new(), Vec::new()));
        }
        offsets.push(next as u16);
    }

    let mut dict_bytes = Vec::with_capacity(offsets.len() * 2 + dict_blob.len());
    for off in offsets {
        dict_bytes.extend_from_slice(&off.to_le_bytes());
    }
    dict_bytes.extend_from_slice(&dict_blob);
    Ok((tokens, dict_bytes))
}

fn count_segment(
    bytes: &[u8],
    start: usize,
    end: usize,
    counts: &mut std::collections::HashMap<Vec<u8>, u32>,
) {
    if end <= start {
        return;
    }
    let segment = &bytes[start..end];
    for len in 3..=8 {
        if segment.len() < len {
            break;
        }
        for idx in 0..=segment.len() - len {
            let token = segment[idx..idx + len].to_vec();
            *counts.entry(token).or_insert(0) += 1;
        }
    }
}

fn encode_token_stream(suffix_blob: &[u8], tokens: &[Vec<u8>]) -> Vec<u8> {
    let mut by_len: Vec<std::collections::HashMap<Vec<u8>, u8>> =
        vec![std::collections::HashMap::new(); 9];
    for (id, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            continue;
        }
        let len = token.len();
        if (3..=8).contains(&len) {
            by_len[len].insert(token.clone(), id as u8);
        }
    }

    let mut out: Vec<u8> = Vec::new();
    let mut i = 0usize;
    while i < suffix_blob.len() {
        let mut matched = None;
        for len in (3..=8).rev() {
            if i + len > suffix_blob.len() {
                continue;
            }
            if let Some(id) = by_len[len].get(&suffix_blob[i..i + len]) {
                matched = Some((*id, len));
                break;
            }
        }
        if let Some((id, len)) = matched {
            out.push(id);
            i += len;
            continue;
        }
        let literal_start = i;
        let mut literal_len = 0usize;
        while i < suffix_blob.len() {
            let mut has_match = false;
            for len in (3..=8).rev() {
                if i + len > suffix_blob.len() {
                    continue;
                }
                if by_len[len].contains_key(&suffix_blob[i..i + len]) {
                    has_match = true;
                    break;
                }
            }
            if has_match || literal_len == 127 {
                break;
            }
            literal_len += 1;
            i += 1;
        }
        out.push(128u8 + (literal_len as u8));
        out.extend_from_slice(&suffix_blob[literal_start..literal_start + literal_len]);
    }
    out
}

fn encode_u8(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values = match &buffer.values {
        ColumnValues::Dict(values) => values.iter().map(|v| *v as u8).collect(),
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as u8).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as u8).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    Ok(values)
}

fn encode_u16(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<u16> = match &buffer.values {
        ColumnValues::Dict(values) => values.iter().take(rows).map(|v| *v as u16).collect(),
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as u16).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as u16).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 2);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_u32(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<u32> = match &buffer.values {
        ColumnValues::Dict(values) => values.iter().take(rows).copied().collect(),
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as u32).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as u32).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_i8(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<i8> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as i8).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as i8).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    Ok(values.iter().map(|&v| v as u8).collect())
}

fn encode_i16(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<i16> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as i16).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as i16).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 2);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_i32(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<i32> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as i32).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as i32).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_i64(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<i64> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).copied().collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as i64).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 8);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_f32(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<f32> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as f32).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).map(|v| *v as f32).collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_f64(buffer: &ColumnBuffer, rows: usize) -> Result<Vec<u8>> {
    let values: Vec<f64> = match &buffer.values {
        ColumnValues::Int(values) => values.iter().take(rows).map(|v| *v as f64).collect(),
        ColumnValues::Float(values) => values.iter().take(rows).copied().collect(),
        _ => bail!("Expected numeric buffer"),
    };
    let mut out = Vec::with_capacity(values.len() * 8);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}
