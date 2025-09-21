mod mask;
mod numeric;
mod primitive;
mod simd;
mod string;

pub(crate) use simd::find_subslice;

use rustc_hash::FxHashMap;

use crate::constants::{
    ENCODING_NUM_DICT, FLAG_DICT, TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16, TYPE_I32, TYPE_I64,
    TYPE_I8, TYPE_STRING, TYPE_U16, TYPE_U32, TYPE_U8,
};
use crate::types::{Column, ColumnData, Dictionary, PlanTiming, Runtime};

pub(crate) const E_DECODE: i32 = -17;

pub(super) enum UnknownPhysical {
    Err,
    FallbackU32,
}

pub(crate) fn dict_index_at(data: &ColumnData, row: usize) -> Option<usize> {
    match data {
        ColumnData::U8(values) => values.get(row).map(|&v| v as usize),
        ColumnData::U16(values) => values.get(row).map(|&v| v as usize),
        ColumnData::U32(values) => values.get(row).map(|&v| v as usize),
        _ => None,
    }
}

pub(super) fn decode_physical_column(
    ty: u8,
    raw: &[u8],
    unknown: UnknownPhysical,
) -> Result<ColumnData, i32> {
    match ty {
        TYPE_I8 => Ok(ColumnData::I8(primitive::decode_i8(raw)?)),
        TYPE_I16 => Ok(ColumnData::I16(primitive::decode_i16(raw)?)),
        TYPE_I32 => Ok(ColumnData::I32(primitive::decode_i32(raw)?)),
        TYPE_I64 => Ok(ColumnData::I64(primitive::decode_i64(raw)?)),
        TYPE_U8 => Ok(ColumnData::U8(primitive::decode_u8(raw))),
        TYPE_U16 => Ok(ColumnData::U16(primitive::decode_u16(raw)?)),
        TYPE_U32 => Ok(ColumnData::U32(primitive::decode_u32(raw)?)),
        TYPE_F32 => Ok(ColumnData::F64(primitive::decode_f32(raw)?)),
        TYPE_F64 => Ok(ColumnData::F64(primitive::decode_f64(raw)?)),
        TYPE_BOOL => Ok(ColumnData::Bool(raw.to_vec())),
        _ => match unknown {
            UnknownPhysical::Err => Err(E_DECODE),
            UnknownPhysical::FallbackU32 => Ok(ColumnData::U32(primitive::decode_u32(raw)?)),
        },
    }
}

pub(crate) use string::{decode_raw_string_ids, decode_raw_string_like_mask};

pub(crate) fn ensure_decoded(
    col: &Column,
    raw: &[u8],
    cache: &mut FxHashMap<u32, ColumnData>,
    rows: usize,
    runtime: &mut Runtime,
    timing: Option<&mut PlanTiming>,
) -> Result<(), i32> {
    if cache.contains_key(&col.id) {
        return Ok(());
    }
    if col.logical_type == TYPE_STRING
        && (col.flags & FLAG_DICT) != 0
        && col.physical_type != TYPE_STRING
    {
        let ids = primitive::decode_index_ids_as_u32(col.physical_type, raw)?;
        cache.insert(col.id, ColumnData::U32(ids));
        return Ok(());
    }
    let data = if col.encoding == ENCODING_NUM_DICT {
        if raw.is_empty() {
            return Err(E_DECODE);
        }
        match raw[0] {
            0 => numeric::decode_numeric_raw(col.physical_type, &raw[1..]),
            1 => numeric::decode_numeric_dict(col.physical_type, &raw[1..], rows),
            2 => numeric::decode_numeric_bitpacked(col.physical_type, &raw[1..], rows),
            _ => Err(E_DECODE),
        }?
    } else if col.physical_type == TYPE_STRING {
        let dict = runtime
            .dicts
            .entry(col.dict_id)
            .or_insert_with(Dictionary::new);
        ColumnData::U32(decode_raw_string_ids(raw, rows, dict, timing)?)
    } else {
        decode_physical_column(col.physical_type, raw, UnknownPhysical::FallbackU32)?
    };
    cache.insert(col.id, data);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::constants::TYPE_U32;
    use crate::types::ColumnData;

    use super::numeric::decode_numeric_bitpacked;

    fn pack_bitpacked_ids(ids: &[u32], bit_width: u8) -> Vec<u8> {
        if bit_width == 0 {
            return Vec::new();
        }
        let total_bits = ids.len() * bit_width as usize;
        let byte_len = total_bits.div_ceil(8);
        let mut out = vec![0u8; byte_len];
        let mut bit_pos = 0usize;
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
        out
    }

    #[test]
    fn numeric_bitpacked_decodes_dict_values() {
        let values = [10u32, 20u32, 10u32, 30u32];
        let dict = [10u32, 20u32, 30u32];
        let ids = [0u32, 1u32, 0u32, 2u32];
        let dict_len = dict.len() as u16;
        let bit_width = 2u8;
        let value_width = 4u8;

        let mut raw = Vec::new();
        raw.extend_from_slice(&dict_len.to_le_bytes());
        raw.push(bit_width);
        raw.push(value_width);
        for &v in &dict {
            raw.extend_from_slice(&v.to_le_bytes());
        }
        raw.extend_from_slice(&pack_bitpacked_ids(&ids, bit_width));

        let decoded = decode_numeric_bitpacked(TYPE_U32, &raw, values.len()).expect("decode");
        match decoded {
            ColumnData::U32(out) => assert_eq!(out, values),
            _ => panic!("unexpected column data"),
        }
    }
}
