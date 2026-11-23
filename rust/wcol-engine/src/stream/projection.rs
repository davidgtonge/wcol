//! `WCOLpjv1` columnar projection blobs (WCOL wire / batch columns).

use crate::error::{WcolError, WcolResult};
use crate::stream::batch::{I64Column, WcolBatch, WcolColumn};

/// Version 1 projection bodies (`WCOLpjv1`): i64 and dictionary ids. Still decoded.
pub const PROJ_MAGIC: &[u8; 8] = b"WCOLpjv1";
/// Version 2 bodies add f64 and utf8. The last magic byte is the body version.
pub const PROJ_MAGIC_V2: &[u8; 8] = b"WCOLpjv2";
pub const PROJ_KIND_DICT_ID: u8 = 1;
pub const PROJ_KIND_I64: u8 = 3;
pub const PROJ_KIND_F64: u8 = 4;
pub const PROJ_KIND_UTF8: u8 = 5;

/// LSB-first validity bitmap (`row & 7` bit index).
pub const PROJ_NULL_BITMAP: u8 = 1;
/// All rows valid; no null bytes in section.
pub const PROJ_NULL_ALL_VALID: u8 = 2;

/// Default encoded WCS1 payload budget (excluding 32-byte frame header).
pub const DEFAULT_STREAM_PAYLOAD_BUDGET: usize = 256 * 1024;

pub enum ProjectionColumn<'a> {
    I64 {
        id: u32,
        values: &'a [i64],
        nulls: &'a [u8],
    },
    F64 {
        id: u32,
        values: &'a [f64],
        nulls: &'a [u8],
    },
    DictId {
        id: u32,
        ids: &'a [u32],
        nulls: &'a [u8],
    },
    /// `offsets.len() == nulls.len() + 1`. Values are `bytes[offsets[i]..offsets[i + 1]]`.
    Utf8 {
        id: u32,
        offsets: &'a [u32],
        bytes: &'a [u8],
        nulls: &'a [u8],
    },
}

pub enum DecodedProjectionColumn {
    I64 {
        id: u32,
        values: Vec<i64>,
        nulls: Vec<u8>,
    },
    F64 {
        id: u32,
        values: Vec<f64>,
        nulls: Vec<u8>,
    },
    DictId {
        id: u32,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    Utf8 {
        id: u32,
        offsets: Vec<u32>,
        bytes: Vec<u8>,
        nulls: Vec<u8>,
    },
}

fn legacy_value_width(kind: u8) -> usize {
    match kind {
        PROJ_KIND_DICT_ID => 4,
        PROJ_KIND_I64 => 8,
        _ => 8,
    }
}

fn dict_id_value_width(ids: &[u32]) -> u8 {
    let max = ids.iter().copied().max().unwrap_or(0);
    if max <= u8::MAX as u32 {
        1
    } else if max <= u16::MAX as u32 {
        2
    } else {
        4
    }
}

fn i64_value_width(values: &[i64]) -> u8 {
    let mut width = 1u8;
    for &v in values {
        if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
            continue;
        }
        width = width.max(2);
        if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
            continue;
        }
        width = width.max(4);
        if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            continue;
        }
        return 8;
    }
    width
}

fn nulls_all_present(nulls: &[u8]) -> bool {
    nulls.iter().all(|&b| b != 0)
}

fn null_bytes_for_encoding(nulls: &[u8], null_encoding: u8) -> usize {
    match null_encoding {
        PROJ_NULL_ALL_VALID => 0,
        PROJ_NULL_BITMAP => nulls.len().div_ceil(8),
        _ => nulls.len(),
    }
}

fn choose_null_encoding(nulls: &[u8]) -> u8 {
    if nulls_all_present(nulls) {
        PROJ_NULL_ALL_VALID
    } else {
        PROJ_NULL_BITMAP
    }
}

struct ColumnPlan {
    kind: u8,
    value_width: u8,
    null_encoding: u8,
    values_bytes: usize,
    section_len: usize,
}

fn plan_column_i64(values: &[i64], nulls: &[u8]) -> ColumnPlan {
    let value_width = i64_value_width(values);
    let null_encoding = choose_null_encoding(nulls);
    let values_bytes = values.len() * value_width as usize;
    ColumnPlan {
        kind: PROJ_KIND_I64,
        value_width,
        null_encoding,
        values_bytes,
        section_len: values_bytes + null_bytes_for_encoding(nulls, null_encoding),
    }
}

fn plan_column_f64(values: &[f64], nulls: &[u8]) -> ColumnPlan {
    let null_encoding = choose_null_encoding(nulls);
    let values_bytes = values.len() * 8;
    ColumnPlan {
        kind: PROJ_KIND_F64,
        value_width: 8,
        null_encoding,
        values_bytes,
        section_len: values_bytes + null_bytes_for_encoding(nulls, null_encoding),
    }
}

fn plan_column_dict(ids: &[u32], nulls: &[u8]) -> ColumnPlan {
    let value_width = dict_id_value_width(ids);
    let null_encoding = choose_null_encoding(nulls);
    let values_bytes = ids.len() * value_width as usize;
    ColumnPlan {
        kind: PROJ_KIND_DICT_ID,
        value_width,
        null_encoding,
        values_bytes,
        section_len: values_bytes + null_bytes_for_encoding(nulls, null_encoding),
    }
}

fn plan_column_utf8(offsets: &[u32], bytes: &[u8], nulls: &[u8]) -> WcolResult<ColumnPlan> {
    let rows = nulls.len();
    if offsets.len() != rows + 1 {
        return Err(WcolError::from("utf8 offsets do not match row count"));
    }
    let base = offsets[0] as usize;
    let end = offsets[rows] as usize;
    if end < base || end > bytes.len() {
        return Err(WcolError::from("utf8 offsets out of range"));
    }
    for pair in offsets.windows(2) {
        if pair[1] < pair[0] {
            return Err(WcolError::from("utf8 offsets are not monotonic"));
        }
    }
    let null_encoding = choose_null_encoding(nulls);
    let values_bytes = 4 + (rows + 1) * 4 + (end - base);
    Ok(ColumnPlan {
        kind: PROJ_KIND_UTF8,
        value_width: 4,
        null_encoding,
        values_bytes,
        section_len: values_bytes + null_bytes_for_encoding(nulls, null_encoding),
    })
}

fn projection_rows(col: &ProjectionColumn<'_>) -> usize {
    match col {
        ProjectionColumn::I64 { values, .. } => values.len(),
        ProjectionColumn::F64 { values, .. } => values.len(),
        ProjectionColumn::DictId { ids, .. } => ids.len(),
        ProjectionColumn::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
    }
}

fn plan_projection_columns(
    columns: &[ProjectionColumn<'_>],
) -> WcolResult<(usize, Vec<ColumnPlan>)> {
    let rows = columns.first().map(projection_rows).unwrap_or(0);
    let mut plans = Vec::with_capacity(columns.len());
    for col in columns {
        match col {
            ProjectionColumn::I64 { values, nulls, .. } => {
                if values.len() != rows || nulls.len() != rows {
                    return Err(WcolError::from("projection columns differ in length"));
                }
                plans.push(plan_column_i64(values, nulls));
            }
            ProjectionColumn::F64 { values, nulls, .. } => {
                if values.len() != rows || nulls.len() != rows {
                    return Err(WcolError::from("projection columns differ in length"));
                }
                plans.push(plan_column_f64(values, nulls));
            }
            ProjectionColumn::DictId { ids, nulls, .. } => {
                if ids.len() != rows || nulls.len() != rows {
                    return Err(WcolError::from("projection columns differ in length"));
                }
                plans.push(plan_column_dict(ids, nulls));
            }
            ProjectionColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => {
                if nulls.len() != rows {
                    return Err(WcolError::from("projection columns differ in length"));
                }
                plans.push(plan_column_utf8(offsets, bytes, nulls)?);
            }
        }
    }
    Ok((rows, plans))
}

pub fn projection_body_byte_len(columns: &[ProjectionColumn<'_>]) -> WcolResult<usize> {
    let (_rows, plans) = plan_projection_columns(columns)?;
    let header = 16 + columns.len() * 16;
    let body: usize = plans.iter().map(|p| p.section_len).sum();
    Ok(header + body)
}

/// Max row count from `row_start` that fits in `body_budget` bytes.
pub fn max_rows_for_body_budget(
    view: WcolBatch<'_>,
    row_start: usize,
    body_budget: usize,
) -> WcolResult<usize> {
    let available = view.row_count.saturating_sub(row_start);
    if available == 0 {
        return Ok(0);
    }
    let header = 16 + view.columns.len() * 16;
    if body_budget <= header {
        return Err(WcolError::from("projection body budget too small"));
    }
    let mut lo = 1usize;
    let mut hi = available;
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if body_len_for_row_slice(view, row_start, mid)? <= body_budget {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    if body_len_for_row_slice(view, row_start, lo)? > body_budget {
        return Err(WcolError::from("single projection row exceeds body budget"));
    }
    Ok(lo)
}

fn body_len_for_row_slice(view: WcolBatch<'_>, row_start: usize, rows: usize) -> WcolResult<usize> {
    let cols = projection_columns_for_range(view, row_start, row_start + rows)?;
    projection_body_byte_len(&cols)
}

pub fn projection_columns_for_range<'a>(
    view: WcolBatch<'a>,
    row_start: usize,
    row_end: usize,
) -> WcolResult<Vec<ProjectionColumn<'a>>> {
    if row_end > view.row_count || row_start > row_end {
        return Err(WcolError::from("projection row range out of bounds"));
    }
    let mut out = Vec::with_capacity(view.columns.len());
    for col in view.columns {
        match col {
            WcolColumn::I64 {
                column,
                values,
                nulls,
            } => out.push(ProjectionColumn::I64 {
                id: *column,
                values: &values[row_start..row_end],
                nulls: &nulls[row_start..row_end],
            }),
            WcolColumn::F64 {
                column,
                values,
                nulls,
            } => out.push(ProjectionColumn::F64 {
                id: *column,
                values: &values[row_start..row_end],
                nulls: &nulls[row_start..row_end],
            }),
            WcolColumn::DictId {
                column, ids, nulls, ..
            } => out.push(ProjectionColumn::DictId {
                id: *column,
                ids: &ids[row_start..row_end],
                nulls: &nulls[row_start..row_end],
            }),
            WcolColumn::Utf8 {
                column,
                offsets,
                bytes,
                nulls,
            } => out.push(ProjectionColumn::Utf8 {
                id: *column,
                offsets: &offsets[row_start..row_end + 1],
                bytes,
                nulls: &nulls[row_start..row_end],
            }),
        }
    }
    Ok(out)
}

fn write_null_bitmap(out: &mut [u8], nulls: &[u8]) {
    let byte_len = nulls.len().div_ceil(8);
    out[..byte_len].fill(0);
    for (row, &present) in nulls.iter().enumerate() {
        if present != 0 {
            let byte = row >> 3;
            let bit = row & 7;
            out[byte] |= 1 << bit;
        }
    }
}

fn write_dict_id(out: &mut [u8], at: usize, width: u8, id: u32) {
    match width {
        1 => out[at] = id as u8,
        2 => out[at..at + 2].copy_from_slice(&(id as u16).to_le_bytes()),
        _ => out[at..at + 4].copy_from_slice(&id.to_le_bytes()),
    }
}

fn write_i64(out: &mut [u8], at: usize, width: u8, value: i64) {
    match width {
        1 => out[at] = value as i8 as u8,
        2 => out[at..at + 2].copy_from_slice(&(value as i16).to_le_bytes()),
        4 => out[at..at + 4].copy_from_slice(&(value as i32).to_le_bytes()),
        _ => out[at..at + 8].copy_from_slice(&value.to_le_bytes()),
    }
}

fn read_dict_id(data: &[u8], at: usize, width: u8) -> u32 {
    match width {
        1 => data[at] as u32,
        2 => u16::from_le_bytes(data[at..at + 2].try_into().unwrap()) as u32,
        _ => u32::from_le_bytes(data[at..at + 4].try_into().unwrap()),
    }
}

fn read_i64_sign_ext(data: &[u8], at: usize, width: u8) -> i64 {
    match width {
        1 => data[at] as i8 as i64,
        2 => i16::from_le_bytes(data[at..at + 2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as i64,
        _ => i64::from_le_bytes(data[at..at + 8].try_into().unwrap()),
    }
}

fn read_null_byte(nulls: &[u8], row: usize) -> u8 {
    if is_valid_bitmap(nulls, row) {
        1
    } else {
        0
    }
}

pub fn is_valid_bitmap(bitmap: &[u8], row: usize) -> bool {
    let byte = bitmap[row >> 3];
    let bit = row & 7;
    (byte & (1 << bit)) != 0
}

/// Compact stream encoding (explicit value width + null encoding).
pub fn encode_projection_columns(columns: &[ProjectionColumn<'_>]) -> WcolResult<Vec<u8>> {
    let (rows, plans) = plan_projection_columns(columns)?;
    let header = 16 + columns.len() * 16;
    let body: usize = plans.iter().map(|p| p.section_len).sum();
    let mut out = vec![0u8; header + body];
    encode_projection_columns_into(columns, &plans, rows, &mut out)?;
    Ok(out)
}

fn encode_projection_columns_into(
    columns: &[ProjectionColumn<'_>],
    plans: &[ColumnPlan],
    rows: usize,
    out: &mut [u8],
) -> WcolResult<usize> {
    let header = 16 + columns.len() * 16;
    let body: usize = plans.iter().map(|p| p.section_len).sum();
    let total = header + body;
    if out.len() < total {
        return Err(WcolError::from("projection encode buffer too small"));
    }
    let magic = if plans
        .iter()
        .any(|plan| matches!(plan.kind, PROJ_KIND_F64 | PROJ_KIND_UTF8))
    {
        PROJ_MAGIC_V2
    } else {
        PROJ_MAGIC
    };
    out[..8].copy_from_slice(magic);
    out[8..12].copy_from_slice(&(rows as u32).to_le_bytes());
    out[12..16].copy_from_slice(&(columns.len() as u32).to_le_bytes());

    let mut section_offset = header;
    for (index, (col, plan)) in columns.iter().zip(plans.iter()).enumerate() {
        let cursor = 16 + index * 16;
        let col_id = match col {
            ProjectionColumn::I64 { id, .. }
            | ProjectionColumn::F64 { id, .. }
            | ProjectionColumn::DictId { id, .. }
            | ProjectionColumn::Utf8 { id, .. } => *id,
        };
        out[cursor..cursor + 4].copy_from_slice(&col_id.to_le_bytes());
        out[cursor + 4] = plan.kind;
        out[cursor + 5] = plan.value_width;
        out[cursor + 6] = plan.null_encoding;
        out[cursor + 7] = 0;
        out[cursor + 8..cursor + 12].copy_from_slice(&(section_offset as u32).to_le_bytes());
        out[cursor + 12..cursor + 16].copy_from_slice(&(plan.section_len as u32).to_le_bytes());

        let values_bytes = plan.values_bytes;
        match col {
            ProjectionColumn::I64 { values, nulls, .. } => {
                for (row, &value) in values.iter().enumerate() {
                    write_i64(
                        out,
                        section_offset + row * plan.value_width as usize,
                        plan.value_width,
                        value,
                    );
                }
                write_nulls_into(
                    &mut out[section_offset + values_bytes..section_offset + plan.section_len],
                    nulls,
                    plan.null_encoding,
                );
            }
            ProjectionColumn::F64 { values, nulls, .. } => {
                for (row, &value) in values.iter().enumerate() {
                    let at = section_offset + row * 8;
                    out[at..at + 8].copy_from_slice(&value.to_le_bytes());
                }
                write_nulls_into(
                    &mut out[section_offset + values_bytes..section_offset + plan.section_len],
                    nulls,
                    plan.null_encoding,
                );
            }
            ProjectionColumn::DictId { ids, nulls, .. } => {
                for (row, &id) in ids.iter().enumerate() {
                    write_dict_id(
                        out,
                        section_offset + row * plan.value_width as usize,
                        plan.value_width,
                        id,
                    );
                }
                write_nulls_into(
                    &mut out[section_offset + values_bytes..section_offset + plan.section_len],
                    nulls,
                    plan.null_encoding,
                );
            }
            ProjectionColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => {
                let base = offsets[0];
                let end = offsets[rows];
                let byte_len = end - base;
                out[section_offset..section_offset + 4].copy_from_slice(&byte_len.to_le_bytes());
                for (row, &offset) in offsets.iter().enumerate() {
                    let at = section_offset + 4 + row * 4;
                    out[at..at + 4].copy_from_slice(&(offset - base).to_le_bytes());
                }
                let bytes_at = section_offset + 4 + offsets.len() * 4;
                out[bytes_at..bytes_at + byte_len as usize]
                    .copy_from_slice(&bytes[base as usize..end as usize]);
                write_nulls_into(
                    &mut out[section_offset + values_bytes..section_offset + plan.section_len],
                    nulls,
                    plan.null_encoding,
                );
            }
        }
        section_offset += plan.section_len;
    }
    Ok(total)
}

fn write_nulls_into(out: &mut [u8], nulls: &[u8], null_encoding: u8) {
    match null_encoding {
        PROJ_NULL_ALL_VALID => {}
        PROJ_NULL_BITMAP => write_null_bitmap(out, nulls),
        _ => out[..nulls.len()].copy_from_slice(nulls),
    }
}

/// Legacy layout for callers that expect widened i64 + byte nulls per row.
pub fn encode_i64_columns(columns: &[(u32, &[i64], &[u8])]) -> WcolResult<Vec<u8>> {
    let rows = columns.first().map(|(_, v, _)| v.len()).unwrap_or(0);
    let header = 16 + columns.len() * 16;
    let body = columns.len() * (rows * 8 + rows);
    let mut out = vec![0u8; header + body];
    out[..8].copy_from_slice(PROJ_MAGIC);
    out[8..12].copy_from_slice(&(rows as u32).to_le_bytes());
    out[12..16].copy_from_slice(&(columns.len() as u32).to_le_bytes());
    let mut section_offset = header;
    for (index, (column, values, nulls)) in columns.iter().enumerate() {
        if values.len() != rows || nulls.len() != rows {
            return Err(WcolError::from("projection columns differ in length"));
        }
        let section_len = rows * 8 + rows;
        let cursor = 16 + index * 16;
        out[cursor..cursor + 4].copy_from_slice(&column.to_le_bytes());
        out[cursor + 4] = PROJ_KIND_I64;
        out[cursor + 5] = 0;
        out[cursor + 6] = 0;
        out[cursor + 7] = 0;
        out[cursor + 8..cursor + 12].copy_from_slice(&(section_offset as u32).to_le_bytes());
        out[cursor + 12..cursor + 16].copy_from_slice(&(section_len as u32).to_le_bytes());
        for (row, value) in values.iter().enumerate() {
            let at = section_offset + row * 8;
            out[at..at + 8].copy_from_slice(&value.to_le_bytes());
        }
        out[section_offset + rows * 8..section_offset + section_len].copy_from_slice(nulls);
        section_offset += section_len;
    }
    Ok(out)
}

pub fn decode_projection_columns(body: &[u8]) -> WcolResult<Vec<DecodedProjectionColumn>> {
    if body.len() < 16 || (&body[..8] != PROJ_MAGIC && &body[..8] != PROJ_MAGIC_V2) {
        return Err(WcolError::from("invalid projection magic"));
    }
    // Bodies arrive from other workers: every length is checked without overflow (wasm32
    // `usize` is 32 bits) and before any allocation sized from the header.
    let rows = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    let col_count = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    if col_count > (body.len() - 16) / 16 {
        return Err(WcolError::from("truncated projection header"));
    }
    let mut out = Vec::with_capacity(col_count);
    for index in 0..col_count {
        let cursor = 16 + index * 16;
        let col_id = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap());
        let kind = body[cursor + 4];
        let value_width = body[cursor + 5];
        let null_encoding = body[cursor + 6];
        let offset = u32::from_le_bytes(body[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        let section_len =
            u32::from_le_bytes(body[cursor + 12..cursor + 16].try_into().unwrap()) as usize;
        if offset
            .checked_add(section_len)
            .is_none_or(|end| end > body.len())
        {
            return Err(WcolError::from("truncated projection column"));
        }

        if kind == PROJ_KIND_UTF8 {
            out.push(decode_utf8_column(
                body,
                col_id,
                offset,
                section_len,
                null_encoding,
                rows,
            )?);
            continue;
        }
        let (vw, legacy) = if value_width == 0 && null_encoding == 0 {
            (legacy_value_width(kind) as u8, true)
        } else {
            (value_width, false)
        };
        let width_ok = match kind {
            PROJ_KIND_I64 => matches!(vw, 1 | 2 | 4 | 8),
            PROJ_KIND_F64 => vw == 8,
            PROJ_KIND_DICT_ID => matches!(vw, 1 | 2 | 4),
            _ => return Err(WcolError::from("unsupported projection column kind")),
        };
        if !width_ok {
            return Err(WcolError::from("invalid projection value width"));
        }

        let values_bytes = rows
            .checked_mul(vw as usize)
            .ok_or("projection section too small")?;
        if section_len < values_bytes {
            return Err(WcolError::from("projection section too small"));
        }
        let null_raw = &body[offset + values_bytes..offset + section_len];

        match kind {
            PROJ_KIND_I64 => {
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    if legacy {
                        let at = offset + row * 8;
                        values.push(i64::from_le_bytes(body[at..at + 8].try_into().unwrap()));
                    } else {
                        values.push(read_i64_sign_ext(body, offset + row * vw as usize, vw));
                    }
                }
                let nulls = decode_nulls(null_raw, null_encoding, rows, legacy)?;
                out.push(DecodedProjectionColumn::I64 {
                    id: col_id,
                    values,
                    nulls,
                });
            }
            PROJ_KIND_DICT_ID => {
                let mut ids = Vec::with_capacity(rows);
                for row in 0..rows {
                    if legacy {
                        let at = offset + row * 4;
                        ids.push(u32::from_le_bytes(body[at..at + 4].try_into().unwrap()));
                    } else {
                        ids.push(read_dict_id(body, offset + row * vw as usize, vw));
                    }
                }
                let nulls = decode_nulls(null_raw, null_encoding, rows, legacy)?;
                out.push(DecodedProjectionColumn::DictId {
                    id: col_id,
                    ids,
                    nulls,
                });
            }
            PROJ_KIND_F64 => {
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    let at = offset + row * 8;
                    values.push(f64::from_le_bytes(body[at..at + 8].try_into().unwrap()));
                }
                let nulls = decode_nulls(null_raw, null_encoding, rows, legacy)?;
                out.push(DecodedProjectionColumn::F64 {
                    id: col_id,
                    values,
                    nulls,
                });
            }
            _ => return Err(WcolError::from("unsupported projection column kind")),
        }
    }
    Ok(out)
}

fn decode_utf8_column(
    body: &[u8],
    col_id: u32,
    offset: usize,
    section_len: usize,
    null_encoding: u8,
    rows: usize,
) -> WcolResult<DecodedProjectionColumn> {
    let offsets_bytes = rows
        .checked_add(1)
        .and_then(|n| n.checked_mul(4))
        .ok_or("utf8 offsets overflow")?;
    let header = 4usize
        .checked_add(offsets_bytes)
        .ok_or("utf8 section overflow")?;
    if section_len < header {
        return Err(WcolError::from("utf8 section too small"));
    }
    let byte_len = u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
    let values_bytes = header
        .checked_add(byte_len)
        .ok_or("utf8 section overflow")?;
    if section_len < values_bytes {
        return Err(WcolError::from("utf8 section too small"));
    }
    let mut offsets = Vec::with_capacity(rows + 1);
    for row in 0..=rows {
        let at = offset + 4 + row * 4;
        offsets.push(u32::from_le_bytes(body[at..at + 4].try_into().unwrap()));
    }
    if offsets[0] != 0 || offsets[rows] as usize != byte_len {
        return Err(WcolError::from(
            "utf8 offsets do not cover the byte section",
        ));
    }
    for pair in offsets.windows(2) {
        if pair[1] < pair[0] {
            return Err(WcolError::from("utf8 offsets are not monotonic"));
        }
    }
    let bytes_at = offset + header;
    let bytes = body[bytes_at..bytes_at + byte_len].to_vec();
    let null_raw = &body[offset + values_bytes..offset + section_len];
    let nulls = decode_nulls(null_raw, null_encoding, rows, false)?;
    Ok(DecodedProjectionColumn::Utf8 {
        id: col_id,
        offsets,
        bytes,
        nulls,
    })
}

fn decode_nulls(
    null_raw: &[u8],
    null_encoding: u8,
    rows: usize,
    legacy: bool,
) -> WcolResult<Vec<u8>> {
    if legacy {
        if null_raw.len() < rows {
            return Err(WcolError::from("projection nulls truncated"));
        }
        return Ok(null_raw[..rows].to_vec());
    }
    match null_encoding {
        PROJ_NULL_ALL_VALID => Ok(vec![1u8; rows]),
        PROJ_NULL_BITMAP => {
            let need = rows.div_ceil(8);
            if null_raw.len() < need {
                return Err(WcolError::from("projection null bitmap truncated"));
            }
            let mut out = Vec::with_capacity(rows);
            for row in 0..rows {
                out.push(read_null_byte(null_raw, row));
            }
            Ok(out)
        }
        0 => {
            if null_raw.len() < rows {
                return Err(WcolError::from("projection nulls truncated"));
            }
            Ok(null_raw[..rows].to_vec())
        }
        _ => Err(WcolError::from("unsupported null encoding")),
    }
}

pub fn decode_projection_i64_columns(body: &[u8]) -> WcolResult<Vec<I64Column>> {
    let cols = decode_projection_columns(body)?;
    let mut out = Vec::new();
    for col in cols {
        match col {
            DecodedProjectionColumn::I64 { id, values, nulls } => out.push((id, values, nulls)),
            DecodedProjectionColumn::F64 { .. }
            | DecodedProjectionColumn::DictId { .. }
            | DecodedProjectionColumn::Utf8 { .. } => {
                return Err(WcolError::from("projection column is not i64"));
            }
        }
    }
    Ok(out)
}

/// Incremental flush of a frame already written into `scratch`.
pub struct ScratchFrameOut {
    pub scratch: Vec<u8>,
    flush_at: usize,
    frame_len: usize,
}

impl ScratchFrameOut {
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            scratch: Vec::with_capacity(initial_capacity),
            flush_at: 0,
            frame_len: 0,
        }
    }

    pub fn has_pending(&self) -> bool {
        self.frame_len > 0 && self.flush_at < self.frame_len
    }

    pub fn queue_frame(&mut self, frame_len: usize) {
        self.frame_len = frame_len;
        self.flush_at = 0;
    }

    pub fn flush(&mut self, output: &mut [u8]) -> usize {
        if !self.has_pending() {
            return 0;
        }
        let n = (self.frame_len - self.flush_at).min(output.len());
        output[..n].copy_from_slice(&self.scratch[self.flush_at..self.flush_at + n]);
        self.flush_at += n;
        if self.flush_at >= self.frame_len {
            self.frame_len = 0;
            self.flush_at = 0;
        }
        n
    }
}

use crate::stream::core::{WCS_HEADER_BYTES, WCS_MAGIC};

const DATA_BATCH_PREFIX: usize = 13;

/// Write WCS1 header + DATA_BATCH prefix; projection body must already sit at `WCS_HEADER_BYTES + 13`.
pub fn write_data_batch_frame_header(
    scratch: &mut [u8],
    sequence: u32,
    dict_scope: u32,
    chunk_id: u32,
    proj_len: usize,
) -> WcolResult<usize> {
    let payload_len = DATA_BATCH_PREFIX + proj_len;
    if payload_len > crate::stream::wcs::MAX_WCS_PAYLOAD {
        return Err(WcolError::from("WCS frame exceeds maximum size"));
    }
    let frame_len = WCS_HEADER_BYTES + payload_len;
    if scratch.len() < frame_len {
        return Err(WcolError::from("scratch too small for data batch frame"));
    }
    scratch[..4].copy_from_slice(&WCS_MAGIC.to_le_bytes());
    scratch[4] = 1;
    scratch[5] = crate::stream::wcs::FRAME_DATA_BATCH;
    scratch[6..8].copy_from_slice(&0u16.to_le_bytes());
    scratch[8..12].copy_from_slice(&(payload_len as u32).to_le_bytes());
    scratch[12..32].fill(0);
    scratch[24..28].copy_from_slice(&sequence.to_le_bytes());
    let payload = &mut scratch[WCS_HEADER_BYTES..frame_len];
    payload[0] = 2;
    payload[1..5].copy_from_slice(&dict_scope.to_le_bytes());
    payload[5..9].copy_from_slice(&chunk_id.to_le_bytes());
    payload[9..13].copy_from_slice(&(proj_len as u32).to_le_bytes());
    Ok(frame_len)
}

/// Write a full WCS1 DATA_BATCH frame into `scratch`. Returns total frame length.
pub fn encode_data_batch_frame_into(
    scratch: &mut Vec<u8>,
    sequence: u32,
    dict_scope: u32,
    chunk_id: u32,
    projection_body: &[u8],
) -> WcolResult<usize> {
    let reserve = WCS_HEADER_BYTES + DATA_BATCH_PREFIX;
    let need = reserve + projection_body.len();
    if scratch.len() < need {
        scratch.resize(need, 0);
    }
    scratch[reserve..need].copy_from_slice(projection_body);
    write_data_batch_frame_header(
        scratch,
        sequence,
        dict_scope,
        chunk_id,
        projection_body.len(),
    )
}

/// Encode projection body at end of scratch after reserving space for WCS header + batch prefix.
pub fn encode_projection_into_scratch(
    scratch: &mut Vec<u8>,
    frame_header_reserve: usize,
    columns: &[ProjectionColumn<'_>],
) -> WcolResult<(usize, usize)> {
    let (rows, plans) = plan_projection_columns(columns)?;
    let header = 16 + columns.len() * 16;
    let body: usize = plans.iter().map(|p| p.section_len).sum();
    let proj_len = header + body;
    let need = frame_header_reserve + proj_len;
    if scratch.len() < need {
        scratch.resize(need, 0);
    }
    let proj_start = frame_header_reserve;
    let written =
        encode_projection_columns_into(columns, &plans, rows, &mut scratch[proj_start..])?;
    debug_assert_eq!(written, proj_len);
    Ok((proj_start, written))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dict_id_projection_layout() {
        let ids = [0u32, 1, 2, 3];
        let nulls = [1u8; 4];
        let body = encode_projection_columns(&[ProjectionColumn::DictId {
            id: 7,
            ids: &ids,
            nulls: &nulls,
        }])
        .unwrap();
        assert_eq!(&body[..8], PROJ_MAGIC);
        assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 4);
        assert_eq!(body[20], PROJ_KIND_DICT_ID);
        assert_eq!(body[21], 1);
        assert_eq!(body[22], PROJ_NULL_ALL_VALID);
    }

    #[test]
    fn dict_null_bitmap_size_when_sparse() {
        let rows = 32_768usize;
        let mut ids = Vec::with_capacity(rows);
        let mut nulls = Vec::with_capacity(rows);
        for row in 0..rows {
            ids.push((row % 18) as u32);
            nulls.push(if row % 3 == 0 { 0 } else { 1 });
        }
        let body = encode_projection_columns(&[ProjectionColumn::DictId {
            id: 1,
            ids: &ids,
            nulls: &nulls,
        }])
        .unwrap();
        let section_len = u32::from_le_bytes(body[28..32].try_into().unwrap()) as usize;
        assert_eq!(body[22], PROJ_NULL_BITMAP);
        assert_eq!(section_len, 36_864);
    }

    #[test]
    fn adv_engine_id_synthetic_size() {
        let rows = 32_768usize;
        let mut ids = Vec::with_capacity(rows);
        for row in 0..rows {
            ids.push((row % 18) as u32);
        }
        let nulls = vec![1u8; rows];
        let body = encode_projection_columns(&[ProjectionColumn::DictId {
            id: 1,
            ids: &ids,
            nulls: &nulls,
        }])
        .unwrap();
        let col_id = u32::from_le_bytes(body[16..20].try_into().unwrap());
        assert_eq!(col_id, 1);
        let offset = u32::from_le_bytes(body[24..28].try_into().unwrap()) as usize;
        let section_len = u32::from_le_bytes(body[28..32].try_into().unwrap()) as usize;
        assert_eq!(body[21], 1, "dict id width");
        assert_eq!(body[22], PROJ_NULL_ALL_VALID);
        assert_eq!(section_len, 32_768);
        assert_eq!(offset + section_len, body.len());
        let decoded = decode_projection_columns(&body).unwrap();
        match &decoded[0] {
            DecodedProjectionColumn::DictId { ids: back, .. } => {
                assert_eq!(back.len(), rows);
                assert_eq!(back[0], 0);
                assert_eq!(back[17], 17);
                assert_eq!(back[18], 0);
            }
            _ => panic!("expected dict column"),
        }
    }

    #[test]
    fn dict_id_width_edges() {
        for (max_id, expected_w) in [(255u32, 1u8), (256, 2), (65535, 2), (65536, 4)] {
            let ids = [0, max_id];
            let nulls = [1u8, 1];
            let body = encode_projection_columns(&[ProjectionColumn::DictId {
                id: 0,
                ids: &ids,
                nulls: &nulls,
            }])
            .unwrap();
            assert_eq!(body[21], expected_w);
            let decoded = decode_projection_columns(&body).unwrap();
            match &decoded[0] {
                DecodedProjectionColumn::DictId { ids: back, .. } => {
                    assert_eq!(back[1], max_id);
                }
                _ => panic!("dict"),
            }
        }
    }

    #[test]
    fn i64_width_and_legacy_roundtrip() {
        let cases: &[(i64, u8)] = &[
            (42, 1),
            (-128, 1),
            (127, 1),
            (-129, 2),
            (30_000, 2),
            (i32::MAX as i64, 4),
            (i64::MIN, 8),
        ];
        for &(value, expected_w) in cases {
            let values = [value];
            let nulls = [1u8];
            let body = encode_projection_columns(&[ProjectionColumn::I64 {
                id: 0,
                values: &values,
                nulls: &nulls,
            }])
            .unwrap();
            assert_eq!(body[21], expected_w, "value {value}");
            let decoded = decode_projection_columns(&body).unwrap();
            match &decoded[0] {
                DecodedProjectionColumn::I64 { values: back, .. } => assert_eq!(back[0], value),
                _ => panic!("i64"),
            }
        }

        let legacy = encode_i64_columns(&[(0, &[9i64, -1][..], &[1, 0][..])]).unwrap();
        assert_eq!(legacy[21], 0);
        let decoded = decode_projection_i64_columns(&legacy).unwrap();
        assert_eq!(decoded[0].1, vec![9, -1]);
        assert_eq!(decoded[0].2, vec![1, 0]);
    }

    #[test]
    fn scratch_frame_short_write() {
        let ids = [0u32, 1, 2];
        let nulls = [1u8; 3];
        let cols = [ProjectionColumn::DictId {
            id: 1,
            ids: &ids,
            nulls: &nulls,
        }];
        let mut out = ScratchFrameOut::new(4096);
        let reserve = WCS_HEADER_BYTES + 13;
        let (proj_off, proj_len) =
            encode_projection_into_scratch(&mut out.scratch, reserve, &cols).unwrap();
        let body = out.scratch[proj_off..proj_off + proj_len].to_vec();
        let frame_len = encode_data_batch_frame_into(&mut out.scratch, 0, 0, 0, &body).unwrap();
        out.queue_frame(frame_len);
        let cap_before = out.scratch.capacity();
        let mut buf = [0u8; 10];
        let mut total = 0usize;
        for _ in 0..100 {
            let n = out.flush(&mut buf);
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(total, frame_len);
        assert!(!out.has_pending());
        assert_eq!(out.scratch.capacity(), cap_before);
    }
}
