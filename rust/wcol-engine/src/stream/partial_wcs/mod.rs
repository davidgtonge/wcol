//! WCS1 `FRAME_PARTIAL` payload: typed partial-state WCOL (`PWCL`).

use crate::ops::query::PartialAggSpec;

use super::partials::{
    PartialColumn, PartialSketchColumn, PartialStateSchemaV1, PartialStateWcol,
    PartialStringKeyColumn, PARTIAL_SCHEMA_VERSION,
};

pub const PWCL_MAGIC: &[u8; 4] = b"PWCL";
/// v5 stores each column in its smallest form (see [`encode_columns`]) and always carries the
/// key masks, sketches and string keys. Producer and consumer are the same Wasm build, so
/// older versions are not read.
const PWCL_VERSION: u8 = 5;

/// Column values: one value for every row, zigzag varints, fixed 8-byte integers, a copy of
/// an earlier column, or f64 bits that are all whole numbers (stored as varint integers).
const VALUES_CONSTANT: u8 = 0;
const VALUES_VARINT: u8 = 1;
const VALUES_FIXED: u8 = 2;
const VALUES_SAME_AS: u8 = 3;
const VALUES_WHOLE_F64: u8 = 4;
/// Null bytes: one byte for every row, or one per row.
const NULLS_CONSTANT: u8 = 0;
const NULLS_RAW: u8 = 1;

/// Smallest encoded column: id, value tag, no value bytes (zero rows), null tag.
const MIN_COLUMN_BYTES: usize = 6;
/// Encoded aggregate spec: kind byte and column id.
const AGG_BYTES: usize = 5;

/// Encode logical partial state for a WCS partial frame.
pub fn partial_state_to_wcs_payload(state: &PartialStateWcol) -> Result<Vec<u8>, String> {
    if state.schema.version != PARTIAL_SCHEMA_VERSION {
        return Err("unsupported partial schema version".to_string());
    }
    let columns = encode_columns(&state.columns, state.row_count)?;
    let mut out = Vec::with_capacity(32 + columns.len());
    out.extend_from_slice(PWCL_MAGIC);
    out.push(PWCL_VERSION);
    let mut flags = 0u8;
    if state.schema.grouped {
        flags |= 1;
    }
    if state.schema.order_groups_by_count {
        flags |= 2;
    }
    out.push(flags);
    out.extend_from_slice(&state.schema.limit.to_le_bytes());
    out.extend_from_slice(&state.schema.offset.to_le_bytes());
    out.push(state.schema.group_key_len);
    out.extend_from_slice(&state.schema.group_key_float_mask.to_le_bytes());
    out.extend_from_slice(&state.schema.group_key_string_mask.to_le_bytes());
    out.extend_from_slice(&(state.schema.aggs.len() as u32).to_le_bytes());
    for agg in &state.schema.aggs {
        out.push(agg.kind);
        out.extend_from_slice(&agg.column.to_le_bytes());
    }
    out.extend_from_slice(&state.passed_rows.to_le_bytes());
    out.extend_from_slice(&(state.row_count as u32).to_le_bytes());
    out.extend_from_slice(&columns);
    {
        out.extend_from_slice(&(state.sketches.len() as u32).to_le_bytes());
        for sketch in &state.sketches {
            if sketch.offsets.len() != state.row_count + 1 {
                return Err("sketch column row count mismatch".to_string());
            }
            out.extend_from_slice(&sketch.agg.to_le_bytes());
            for offset in &sketch.offsets {
                out.extend_from_slice(&offset.to_le_bytes());
            }
            out.extend_from_slice(&(sketch.bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&sketch.bytes);
        }
    }
    {
        out.extend_from_slice(&(state.string_keys.len() as u32).to_le_bytes());
        for sk in &state.string_keys {
            out.push(sk.key_index);
            for offset in &sk.offsets {
                out.extend_from_slice(&offset.to_le_bytes());
            }
            out.extend_from_slice(&(sk.bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&sk.bytes);
            out.extend_from_slice(&(sk.nulls.len() as u32).to_le_bytes());
            out.extend_from_slice(&sk.nulls);
        }
    }
    Ok(out)
}

/// Decode a WCS partial payload (`PWCL`).
pub fn partial_state_from_wire_payload(bytes: &[u8]) -> Result<PartialStateWcol, String> {
    if bytes.len() >= 4 && &bytes[..4] == PWCL_MAGIC {
        decode_pwcl(bytes)
    } else {
        Err("partial wire format is not PWCL".to_string())
    }
}

pub fn partial_first_sum_from_wire(bytes: &[u8]) -> Result<i128, String> {
    let state = partial_state_from_wire_payload(bytes)?;
    super::partials::partial_state_first_sum_i(&state, 0)
}

fn decode_pwcl(bytes: &[u8]) -> Result<PartialStateWcol, String> {
    let mut at = 4usize;
    let version = take_u8(bytes, &mut at)?;
    if version != PWCL_VERSION {
        return Err(format!("unsupported PWCL version {version}"));
    }
    let flags = take_u8(bytes, &mut at)?;
    let limit = take_u32(bytes, &mut at)?;
    let offset = take_u32(bytes, &mut at)?;
    let group_key_len = take_u8(bytes, &mut at)?;
    let group_key_float_mask = take_u32(bytes, &mut at)?;
    let group_key_string_mask = take_u32(bytes, &mut at)?;
    let agg_count = take_u32(bytes, &mut at)? as usize;
    // Payloads arrive from other workers: bound every count by the bytes it needs before
    // allocating for it.
    if agg_count > remaining(bytes, at) / AGG_BYTES {
        return Err("PWCL aggregate count exceeds payload".to_string());
    }
    let mut aggs = Vec::with_capacity(agg_count);
    for _ in 0..agg_count {
        aggs.push(PartialAggSpec {
            kind: take_u8(bytes, &mut at)?,
            column: take_u32(bytes, &mut at)?,
        });
    }
    let passed_rows = take_u64(bytes, &mut at)?;
    let row_count = take_u32(bytes, &mut at)? as usize;
    // Every row past the first costs at least a byte (a varying key or a string offset), so
    // a corrupt count cannot make a constant column allocate past what the payload holds.
    if row_count > bytes.len() {
        return Err("PWCL row count exceeds payload".to_string());
    }
    let columns = decode_columns(bytes, &mut at, row_count)?;
    let mut sketches = Vec::new();
    {
        let count = take_u32(bytes, &mut at)? as usize;
        for _ in 0..count {
            let agg = take_u32(bytes, &mut at)?;
            if agg as usize >= aggs.len() {
                return Err("PWCL sketch for a missing aggregate".to_string());
            }
            let offsets = take_offsets(bytes, &mut at, row_count)?;
            let len = take_u32(bytes, &mut at)? as usize;
            let body = bytes
                .get(at..at.checked_add(len).ok_or("truncated PWCL sketches")?)
                .ok_or("truncated PWCL sketches")?;
            at += len;
            if offsets.windows(2).any(|w| w[0] > w[1])
                || offsets.last().copied() != Some(len as u32)
            {
                return Err("PWCL sketch offsets are invalid".to_string());
            }
            sketches.push(PartialSketchColumn {
                agg,
                offsets,
                bytes: body.to_vec(),
            });
        }
    }
    let mut string_keys = Vec::new();
    {
        let count = take_u32(bytes, &mut at)? as usize;
        for _ in 0..count {
            let key_index = take_u8(bytes, &mut at)?;
            let offsets = take_offsets(bytes, &mut at, row_count)?;
            let len = take_u32(bytes, &mut at)? as usize;
            let body = bytes
                .get(at..at.checked_add(len).ok_or("truncated PWCL string keys")?)
                .ok_or("truncated PWCL string keys")?;
            at += len;
            let null_len = take_u32(bytes, &mut at)? as usize;
            let nulls = bytes
                .get(
                    at..at
                        .checked_add(null_len)
                        .ok_or("truncated PWCL string nulls")?,
                )
                .ok_or("truncated PWCL string nulls")?
                .to_vec();
            at += null_len;
            if nulls.len() != row_count {
                return Err("PWCL string null count mismatch".to_string());
            }
            string_keys.push(PartialStringKeyColumn {
                key_index,
                offsets,
                bytes: body.to_vec(),
                nulls,
            });
        }
    }
    if at != bytes.len() {
        return Err("PWCL has trailing bytes".to_string());
    }
    Ok(PartialStateWcol {
        schema: PartialStateSchemaV1 {
            version: PARTIAL_SCHEMA_VERSION,
            grouped: flags & 1 != 0,
            order_groups_by_count: flags & 2 != 0,
            limit,
            offset,
            group_key_len,
            group_key_float_mask,
            group_key_string_mask,
            aggs,
        },
        passed_rows,
        row_count,
        columns,
        sketches,
        string_keys,
    })
}

/// Partial columns in their smallest form. Every aggregate fills 11 stat columns: unused ones
/// are constant, and COUNT copies its count into the sum, min and max columns and again as
/// f64 bits. At fixed 8-byte cells a partial row was wider than the raw row it summarised.
fn encode_columns(columns: &[PartialColumn], rows: usize) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for column in columns {
        if column.values.len() != rows || column.nulls.len() != rows {
            return Err("partial column row count mismatch".to_string());
        }
        out.extend_from_slice(&column.id.to_le_bytes());
        let earlier = columns
            .iter()
            .take_while(|other| !std::ptr::eq(*other, column))
            .position(|other| other.values == column.values);
        match earlier {
            Some(index) if rows > 0 => {
                out.push(VALUES_SAME_AS);
                out.extend_from_slice(&(index as u32).to_le_bytes());
            }
            _ => encode_values(&column.values, &mut out),
        }
        match column.nulls.first() {
            Some(first) if column.nulls.iter().all(|byte| byte == first) => {
                out.push(NULLS_CONSTANT);
                out.push(*first);
            }
            _ => {
                out.push(NULLS_RAW);
                out.extend_from_slice(&column.nulls);
            }
        }
    }
    Ok(out)
}

/// The smallest form, by exact size, so a split frame is never larger than its whole.
fn encode_values(values: &[i64], out: &mut Vec<u8>) {
    let varint_bytes: usize = values.iter().map(|v| varint_len(zigzag(*v))).sum();
    let whole_bytes: Option<usize> = values
        .iter()
        .map(|v| whole_f64(*v).map(|whole| varint_len(zigzag(whole))))
        .sum();
    let fixed = values.len() * 8;
    let constant = values
        .first()
        .filter(|first| values.iter().all(|v| v == *first));
    let candidates = [
        (VALUES_CONSTANT, constant.map(|_| 8)),
        (VALUES_VARINT, Some(4 + varint_bytes)),
        (VALUES_WHOLE_F64, whole_bytes.map(|len| 4 + len)),
        (VALUES_FIXED, Some(fixed)),
    ];
    let (tag, _) = candidates
        .iter()
        .filter_map(|(tag, size)| size.map(|size| (*tag, size)))
        .min_by_key(|(_, size)| *size)
        .unwrap_or((VALUES_FIXED, fixed));
    out.push(tag);
    match tag {
        VALUES_CONSTANT => out.extend_from_slice(&values[0].to_le_bytes()),
        VALUES_VARINT | VALUES_WHOLE_F64 => {
            let len = if tag == VALUES_VARINT {
                varint_bytes
            } else {
                whole_bytes.unwrap_or(0)
            };
            out.extend_from_slice(&(len as u32).to_le_bytes());
            for value in values {
                let value = if tag == VALUES_VARINT {
                    *value
                } else {
                    whole_f64(*value).unwrap_or(0)
                };
                put_varint(zigzag(value), out);
            }
        }
        _ => {
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

/// The integer whose f64 has exactly these bits, if there is one (so `-0.0` and NaNs are not).
fn whole_f64(bits: i64) -> Option<i64> {
    let float = f64::from_bits(bits as u64);
    let whole = float as i64;
    ((whole as f64).to_bits() == bits as u64).then_some(whole)
}

fn decode_columns(bytes: &[u8], at: &mut usize, rows: usize) -> Result<Vec<PartialColumn>, String> {
    let count = take_u32(bytes, at)? as usize;
    if count > remaining(bytes, *at) / MIN_COLUMN_BYTES {
        return Err("PWCL column count exceeds payload".to_string());
    }
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let id = take_u32(bytes, at)?;
        let values = match take_u8(bytes, at)? {
            VALUES_CONSTANT => {
                let value = take_u64(bytes, at)? as i64;
                vec![value; rows]
            }
            tag @ (VALUES_VARINT | VALUES_WHOLE_F64) => {
                let len = take_u32(bytes, at)? as usize;
                let end = at.checked_add(len).filter(|end| *end <= bytes.len());
                let end = end.ok_or("truncated PWCL varint column")?;
                let mut values = Vec::with_capacity(rows.min(len));
                while *at < end {
                    let value = unzigzag(take_varint(&bytes[..end], at)?);
                    values.push(if tag == VALUES_VARINT {
                        value
                    } else {
                        (value as f64).to_bits() as i64
                    });
                }
                values
            }
            VALUES_SAME_AS => {
                let index = take_u32(bytes, at)? as usize;
                let earlier: &PartialColumn = columns
                    .get(index)
                    .ok_or("PWCL column copies a later column")?;
                earlier.values.clone()
            }
            VALUES_FIXED => {
                let body = take_bytes(bytes, at, rows.checked_mul(8).ok_or("PWCL rows overflow")?)?;
                body.chunks_exact(8)
                    .map(|cell| i64::from_le_bytes(cell.try_into().unwrap()))
                    .collect()
            }
            _ => return Err("unknown PWCL value encoding".to_string()),
        };
        if values.len() != rows {
            return Err("PWCL column row count mismatch".to_string());
        }
        let nulls = match take_u8(bytes, at)? {
            NULLS_CONSTANT => vec![take_u8(bytes, at)?; rows],
            NULLS_RAW => take_bytes(bytes, at, rows)?.to_vec(),
            _ => return Err("unknown PWCL null encoding".to_string()),
        };
        columns.push(PartialColumn { id, values, nulls });
    }
    Ok(columns)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn put_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(bytes: &[u8], at: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = take_u8(bytes, at)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("PWCL varint is too long".to_string())
}

fn remaining(bytes: &[u8], at: usize) -> usize {
    bytes.len().saturating_sub(at)
}

/// `rows + 1` little-endian `u32` offsets.
fn take_offsets(bytes: &[u8], at: &mut usize, rows: usize) -> Result<Vec<u32>, String> {
    let raw = take_bytes(
        bytes,
        at,
        rows.checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or("PWCL rows overflow")?,
    )?;
    Ok(raw
        .chunks_exact(4)
        .map(|cell| u32::from_le_bytes(cell.try_into().unwrap()))
        .collect())
}

fn take_bytes<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let end = at.checked_add(len).filter(|end| *end <= bytes.len());
    let body = bytes[*at..end.ok_or("truncated PWCL body")?].as_ref();
    *at += len;
    Ok(body)
}

fn take_u8(bytes: &[u8], at: &mut usize) -> Result<u8, String> {
    bytes
        .get(*at)
        .copied()
        .ok_or_else(|| "truncated PWCL header".to_string())
        .inspect(|_b| {
            *at += 1;
        })
}

fn take_u32(bytes: &[u8], at: &mut usize) -> Result<u32, String> {
    if bytes.len() < *at + 4 {
        return Err("truncated PWCL header".to_string());
    }
    let v = u32::from_le_bytes(bytes[*at..*at + 4].try_into().unwrap());
    *at += 4;
    Ok(v)
}

fn take_u64(bytes: &[u8], at: &mut usize) -> Result<u64, String> {
    if bytes.len() < *at + 8 {
        return Err("truncated PWCL header".to_string());
    }
    let v = u64::from_le_bytes(bytes[*at..*at + 8].try_into().unwrap());
    *at += 8;
    Ok(v)
}

#[cfg(test)]
mod tests;
