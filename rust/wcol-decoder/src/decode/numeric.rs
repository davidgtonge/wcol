
use std::convert::TryInto;

use crate::constants::{
    TYPE_I16, TYPE_I32, TYPE_I64, TYPE_I8, TYPE_U16, TYPE_U32, TYPE_U8,
};
use crate::types::ColumnData;

use super::{decode_physical_column, UnknownPhysical, E_DECODE};

struct NumericDictLayout<'a> {
    dict_raw: &'a [u8],
    ids_raw: &'a [u8],
    value_width: usize,
}

fn parse_numeric_dict_layout(raw: &[u8], ids_byte_len: usize) -> Result<NumericDictLayout<'_>, i32> {
    if raw.len() < 4 {
        return Err(E_DECODE);
    }
    let dict_len = u16::from_le_bytes([raw[0], raw[1]]) as usize;
    let value_width = raw[3] as usize;
    let dict_bytes = dict_len * value_width;
    let ids_off = 4 + dict_bytes;
    if ids_off > raw.len() || ids_off + ids_byte_len > raw.len() {
        return Err(E_DECODE);
    }
    Ok(NumericDictLayout {
        dict_raw: &raw[4..ids_off],
        ids_raw: &raw[ids_off..ids_off + ids_byte_len],
        value_width,
    })
}

pub(super) fn decode_numeric_raw(ty: u8, raw: &[u8]) -> Result<ColumnData, i32> {
    decode_physical_column(ty, raw, UnknownPhysical::Err)
}

pub(super) fn decode_numeric_bitpacked(ty: u8, raw: &[u8], rows: usize) -> Result<ColumnData, i32> {
    let bit_width = if raw.len() >= 3 {
        raw[2] as usize
    } else {
        0
    };
    let expected_ids = (rows * bit_width).div_ceil(8);
    let layout = parse_numeric_dict_layout(raw, expected_ids)?;
    let ids = decode_bitpacked_ids(bit_width, layout.ids_raw, rows)?;
    decode_dict_ids_to_column(ty, layout.dict_raw, layout.value_width, &ids)
}

fn decode_bitpacked_ids(bit_width: usize, data: &[u8], rows: usize) -> Result<Vec<u32>, i32> {
    if bit_width == 0 {
        return Ok(vec![0u32; rows]);
    }
    let total_bits = rows * bit_width;
    let byte_len = total_bits.div_ceil(8);
    if data.len() < byte_len {
        return Err(E_DECODE);
    }
    let mut out = Vec::with_capacity(rows);
    let mut bit_pos = 0usize;
    for _ in 0..rows {
        let mut value: u32 = 0;
        for bit in 0..bit_width {
            let byte_idx = bit_pos >> 3;
            let bit_idx = bit_pos & 7;
            let bit_val = (data[byte_idx] >> bit_idx) & 1;
            value |= (bit_val as u32) << bit;
            bit_pos += 1;
        }
        out.push(value);
    }
    Ok(out)
}

pub(super) fn decode_numeric_dict(ty: u8, raw: &[u8], rows: usize) -> Result<ColumnData, i32> {
    let id_width = if raw.len() >= 3 {
        raw[2] as usize
    } else {
        0
    };
    if id_width != 1 && id_width != 2 && id_width != 4 {
        return Err(E_DECODE);
    }
    let expected_ids = rows * id_width;
    let layout = parse_numeric_dict_layout(raw, expected_ids)?;
    let ids: Result<Vec<u32>, i32> = (0..rows)
        .map(|idx| read_id(layout.ids_raw, idx, id_width))
        .collect();
    let ids = ids?;
    decode_dict_ids_to_column(ty, layout.dict_raw, layout.value_width, &ids)
}

fn gather<T: Copy>(dict: &[T], ids: &[u32]) -> Result<Vec<T>, i32> {
    ids.iter()
        .map(|&id| dict.get(id as usize).copied().ok_or(E_DECODE))
        .collect()
}

macro_rules! dict_col {
    ($ty:ty, $variant:ident, $width:expr, $value_width:expr, $dict_raw:expr, $ids:expr) => {{
        if $value_width != $width || $dict_raw.len() % $width != 0 {
            return Err(E_DECODE);
        }

        let dict: Vec<$ty> = $dict_raw
            .chunks_exact($width)
            .map(|c| <$ty>::from_le_bytes(c.try_into().unwrap()))
            .collect();

        Ok(ColumnData::$variant(gather(&dict, $ids)?))
    }};
}

macro_rules! dict_col_i8 {
    ($value_width:expr, $dict_raw:expr, $ids:expr) => {{
        if $value_width != 1 {
            return Err(E_DECODE);
        }
        let dict: Vec<i8> = $dict_raw.iter().map(|&b| b as i8).collect();
        Ok(ColumnData::I8(gather(&dict, $ids)?))
    }};
}

fn read_id(bytes: &[u8], idx: usize, width: usize) -> Result<u32, i32> {
    let off = idx * width;
    let s = bytes.get(off..off + width).ok_or(E_DECODE)?;

    match s {
        [a] => Ok(*a as u32),
        [a, b] => Ok(u16::from_le_bytes([*a, *b]) as u32),
        [a, b, c, d] => Ok(u32::from_le_bytes([*a, *b, *c, *d])),
        _ => Err(E_DECODE),
    }
}

fn decode_dict_ids_to_column(
    ty: u8,
    dict_raw: &[u8],
    value_width: usize,
    ids: &[u32],
) -> Result<ColumnData, i32> {
    match ty {
        TYPE_U8 => {
            if value_width != 1 {
                return Err(E_DECODE);
            }
            Ok(ColumnData::U8(gather(dict_raw, ids)?))
        }
        TYPE_I8 => dict_col_i8!(value_width, dict_raw, ids),
        TYPE_U16 => dict_col!(u16, U16, 2, value_width, dict_raw, ids),
        TYPE_I16 => dict_col!(i16, I16, 2, value_width, dict_raw, ids),
        TYPE_U32 => dict_col!(u32, U32, 4, value_width, dict_raw, ids),
        TYPE_I32 => dict_col!(i32, I32, 4, value_width, dict_raw, ids),
        TYPE_I64 => dict_col!(i64, I64, 8, value_width, dict_raw, ids),
        _ => Err(E_DECODE),
    }
}
