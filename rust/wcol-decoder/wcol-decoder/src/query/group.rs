use crate::constants::{FLAG_DICT, TYPE_STRING};
use crate::decode::dict_index_at;
use crate::query::mask::is_valid;
use crate::query::scale::scale_int_value;
use crate::runtime::dict_value_bytes;
use crate::{Column, ColumnData, Runtime};
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

#[derive(Clone, Debug)]
pub(crate) struct MaterializedGroupKey {
    pub(crate) key: crate::types::GroupKey,
    pub(crate) repr: Vec<u8>,
}

pub(crate) fn build_group_key(
    keys: &[(&Column, &ColumnData)],
    row: usize,
) -> crate::types::GroupKey {
    build_group_key_with_runtime(keys, row, None)
}

pub(crate) fn build_group_key_with_runtime(
    keys: &[(&Column, &ColumnData)],
    row: usize,
    runtime: Option<&Runtime>,
) -> crate::types::GroupKey {
    if keys.len() == 1 {
        crate::types::GroupKey {
            a: read_key_value_with_runtime(keys[0].0, keys[0].1, row, runtime),
            b: 0,
        }
    } else {
        let a = read_key_value_with_runtime(keys[0].0, keys[0].1, row, runtime);
        let b = read_key_value_with_runtime(keys[1].0, keys[1].1, row, runtime);
        crate::types::GroupKey { a, b }
    }
}

pub(crate) fn build_group_key_materialized_with_runtime(
    keys: &[(&Column, &ColumnData)],
    row: usize,
    runtime: &Runtime,
) -> MaterializedGroupKey {
    let mut repr = Vec::new();
    for (idx, (col, data)) in keys.iter().enumerate() {
        repr.push(idx as u8);
        repr.push(col.logical_type);
        repr.push(col.physical_type);
        if let Some(bytes) = read_key_bytes_with_runtime(col, data, row, runtime) {
            let len = (bytes.len() as u32).to_le_bytes();
            repr.extend_from_slice(&len);
            repr.extend_from_slice(bytes);
        } else {
            let value = read_key_value_with_runtime(col, data, row, Some(runtime));
            repr.extend_from_slice(&8u32.to_le_bytes());
            repr.extend_from_slice(&value.to_le_bytes());
        }
    }
    let h1 = xxh3_64(&repr);
    let h2 = xxh3_64_with_seed(&repr, 0x9e37_79b9_7f4a_7c15);
    MaterializedGroupKey {
        key: crate::types::GroupKey { a: h1, b: h2 },
        repr,
    }
}

pub(crate) fn read_key_value(_col: &Column, data: &ColumnData, row: usize) -> u64 {
    read_key_value_with_runtime(_col, data, row, None)
}

pub(crate) fn read_key_value_with_runtime(
    col: &Column,
    data: &ColumnData,
    row: usize,
    runtime: Option<&Runtime>,
) -> u64 {
    // Raw string pages are decoded to worker-local IDs. Hash string bytes into a stable key
    // so group keys merge correctly across workers.
    if col.logical_type == TYPE_STRING && (col.flags & FLAG_DICT) == 0 {
        if let Some(stable) = stable_raw_string_key(col, data, row, runtime) {
            return stable;
        }
    }
    match data {
        ColumnData::U8(values) => values[row] as u64,
        ColumnData::U16(values) => values[row] as u64,
        ColumnData::I8(values) => values[row] as i32 as u32 as u64,
        ColumnData::I16(values) => values[row] as i32 as u32 as u64,
        ColumnData::I32(values) => values[row] as u32 as u64,
        ColumnData::I64(values) => values[row] as u64,
        ColumnData::U32(values) => values[row] as u64,
        ColumnData::F64(values) => values[row] as u64,
        ColumnData::Bool(values) => {
            if is_valid(values, row) {
                1
            } else {
                0
            }
        }
    }
}

fn stable_raw_string_key(
    col: &Column,
    data: &ColumnData,
    row: usize,
    runtime: Option<&Runtime>,
) -> Option<u64> {
    let id = dict_index_at(data, row)?;
    let runtime = runtime?;
    let dict = runtime.dicts.get(&col.dict_id)?;
    if let Some(&cached) = dict.hash_cache.get(id) {
        return Some(cached);
    }
    let bytes = dict_value_bytes(dict, id)?;
    Some(xxh3_64(bytes))
}

fn read_key_bytes_with_runtime<'a>(
    col: &Column,
    data: &'a ColumnData,
    row: usize,
    runtime: &'a Runtime,
) -> Option<&'a [u8]> {
    if col.logical_type != TYPE_STRING {
        return None;
    }
    let id = dict_index_at(data, row)?;
    let dict = runtime.dicts.get(&col.dict_id)?;
    dict_value_bytes(dict, id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Dictionary;
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    #[test]
    fn materialized_group_key_is_stable_and_byte_based_for_strings() {
        let col = Column {
            id: 0,
            name: "s".to_string(),
            logical_type: TYPE_STRING,
            physical_type: TYPE_STRING,
            flags: 0,
            encoding: 0,
            dict_id: 1,
            dict_index_width: 2,
            scale: 0,
        };
        let data = ColumnData::U16(vec![0, 1, 0]);
        let dict = Dictionary {
            offsets: vec![0, 1, 2],
            blob: b"ab".to_vec(),
            values: Vec::new(),
            lookup: FxHashMap::default(),
            hash_cache: vec![xxh3_64(b"a"), xxh3_64(b"b")],
        };
        let mut dicts = FxHashMap::default();
        dicts.insert(1, dict);
        let runtime = Runtime {
            header: None,
            schema: Arc::from(vec![col.clone()]),
            toc: Vec::new(),
            dicts,
            index_cache: FxHashMap::default(),
        };
        let keys = vec![(&col, &data)];

        let k0 = build_group_key_materialized_with_runtime(&keys, 0, &runtime);
        let k1 = build_group_key_materialized_with_runtime(&keys, 1, &runtime);
        let k2 = build_group_key_materialized_with_runtime(&keys, 2, &runtime);

        assert_ne!(k0.repr, k1.repr);
        assert_eq!(k0.repr, k2.repr);
        assert_ne!(k0.key, k1.key);
        assert_eq!(k0.key, k2.key);
    }
}

#[inline]
fn read_scaled_f64<T: Copy>(values: &[T], row: usize, scale: i32, to_f64: fn(T) -> f64) -> f64 {
    scale_int_value(to_f64(values[row]), scale)
}

pub(crate) fn read_value_f64(col: &Column, data: &ColumnData, row: usize) -> f64 {
    match data {
        ColumnData::U8(values) => read_scaled_f64(values, row, col.scale, |v: u8| v as f64),
        ColumnData::U16(values) => read_scaled_f64(values, row, col.scale, |v: u16| v as f64),
        ColumnData::I8(values) => read_scaled_f64(values, row, col.scale, |v: i8| v as f64),
        ColumnData::I16(values) => read_scaled_f64(values, row, col.scale, |v: i16| v as f64),
        ColumnData::I32(values) => read_scaled_f64(values, row, col.scale, |v: i32| v as f64),
        ColumnData::I64(values) => read_scaled_f64(values, row, col.scale, |v: i64| v as f64),
        ColumnData::U32(values) => read_scaled_f64(values, row, col.scale, |v: u32| v as f64),
        ColumnData::F64(values) => values[row],
        ColumnData::Bool(values) => {
            if is_valid(values, row) {
                1.0
            } else {
                0.0
            }
        }
    }
}

pub(crate) fn read_distinct_key(_col: &Column, data: &ColumnData, row: usize) -> Option<u64> {
    match data {
        ColumnData::U8(values) => Some(values[row] as u64),
        ColumnData::U16(values) => Some(values[row] as u64),
        ColumnData::U32(values) => Some(values[row] as u64),
        ColumnData::I8(values) => Some(values[row] as i64 as u64),
        ColumnData::I16(values) => Some(values[row] as i64 as u64),
        ColumnData::I32(values) => Some(values[row] as i64 as u64),
        ColumnData::I64(values) => Some(values[row] as u64),
        ColumnData::F64(values) => {
            let value = values[row];
            if value.is_nan() {
                return None;
            }
            let canonical = if value == 0.0 { 0.0 } else { value };
            Some(canonical.to_bits())
        }
        ColumnData::Bool(values) => {
            if is_valid(values, row) {
                Some(1)
            } else {
                Some(0)
            }
        }
    }
}
