use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;

use anyhow::{bail, Context, Result};
use arrow2::array::{Array, BinaryArray, DictionaryArray, PrimitiveArray, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, IntegerType, Schema};
use arrow2::io::parquet::read::FileReader;
use arrow2::types::Index;
use parquet2::metadata::FileMetaData;
use rayon::prelude::*;
#[cfg(feature = "sav")]
use sav_to_cbor::parser::streaming_nom::{ColumnRows, ColumnarColumn};

use crate::constants::{
    ENCODING_NONE, ENCODING_NUM_DICT, FLAG_DICT, FLAG_NULLABLE, OTHER_DICT_VALUE, SCALE_CANDIDATES,
    SCALE_CANDIDATE_LEN, SCALE_EPS, SCALE_MASK_ALL, TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16,
    TYPE_I32, TYPE_I64, TYPE_I8, TYPE_STRING, TYPE_U16, TYPE_U32, TYPE_U8,
};
use crate::dict_limits::{
    default_max_dict_values, dict_value_limit_for_column, is_large_dict_column,
};
use crate::types::{ColumnKind, ColumnSpec};
use crate::utils::decode_binary_value;

pub(crate) fn init_columns(schema: &Schema) -> Result<Vec<ColumnSpec>> {
    let mut columns = Vec::with_capacity(schema.fields.len());
    for (id, field) in schema.fields.iter().enumerate() {
        let kind = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => ColumnKind::String,
            DataType::Binary | DataType::LargeBinary => ColumnKind::String,
            DataType::Boolean => ColumnKind::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => ColumnKind::Int,
            DataType::Float32 | DataType::Float64 => ColumnKind::Float,
            DataType::Dictionary(_, value, _) => match value.as_ref() {
                DataType::Utf8 | DataType::LargeUtf8 => ColumnKind::String,
                DataType::Binary | DataType::LargeBinary => ColumnKind::String,
                _ => bail!(
                    "Unsupported dictionary value type for column {}",
                    field.name
                ),
            },
            _ => bail!("Unsupported parquet type for column {}", field.name),
        };

        columns.push(ColumnSpec {
            id,
            name: field.name.clone(),
            kind,
            nullable: field.is_nullable,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            f32_ok: true,
            scale_candidates: if kind == ColumnKind::Float {
                SCALE_MASK_ALL
            } else {
                0
            },
            scaled_min: [i64::MAX; SCALE_CANDIDATE_LEN],
            scaled_max: [i64::MIN; SCALE_CANDIDATE_LEN],
            unsafe_int: false,
            dict_map: if kind == ColumnKind::String {
                Some(HashMap::new())
            } else {
                None
            },
            dict_values: Vec::new(),
            num_dict_values: if kind == ColumnKind::Int {
                Some(HashSet::new())
            } else {
                None
            },
            num_dict: false,
            float_int_ok: kind == ColumnKind::Float,
            float_int_min: i64::MAX,
            float_int_max: i64::MIN,
            logical_type: 0,
            physical_type: 0,
            flags: 0,
            encoding: ENCODING_NONE,
            dict_id: 0,
            dict_index_width: 0,
            scale: 0,
            other_dict_id: None,
        });
    }
    Ok(columns)
}

#[cfg(feature = "sav")]
pub(crate) fn sav_row_count(columns: &[ColumnarColumn]) -> Result<usize> {
    let mut count: Option<usize> = None;
    for col in columns {
        let len = match &col.rows {
            ColumnRows::Numeric(rows) => rows.len(),
            ColumnRows::Indexed(rows) => rows.len(),
        };
        if let Some(existing) = count {
            if existing != len {
                bail!(
                    "SAV column length mismatch: expected {}, got {}",
                    existing,
                    len
                );
            }
        } else {
            count = Some(len);
        }
    }
    Ok(count.unwrap_or(0))
}

#[cfg(feature = "sav")]
pub(crate) fn init_sav_columns(columns: &[ColumnarColumn]) -> Result<Vec<ColumnSpec>> {
    let mut specs = Vec::with_capacity(columns.len());
    for (id, col) in columns.iter().enumerate() {
        let (kind, dict_values, mut dict_map) = match &col.rows {
            ColumnRows::Numeric(_) => (ColumnKind::Float, Vec::new(), None),
            ColumnRows::Indexed(_) => {
                let values = col.values.clone().unwrap_or_default();
                let mut map = HashMap::with_capacity(values.len());
                for (idx, value) in values.iter().enumerate() {
                    map.insert(value.clone(), idx as u32);
                }
                (ColumnKind::String, values, Some(map))
            }
        };
        if dict_values.len() > default_max_dict_values() {
            dict_map = None;
        }

        specs.push(ColumnSpec {
            id,
            name: col.id.clone(),
            kind,
            nullable: false,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            f32_ok: true,
            scale_candidates: if kind == ColumnKind::Float {
                SCALE_MASK_ALL
            } else {
                0
            },
            scaled_min: [i64::MAX; SCALE_CANDIDATE_LEN],
            scaled_max: [i64::MIN; SCALE_CANDIDATE_LEN],
            unsafe_int: false,
            dict_map,
            dict_values,
            num_dict_values: None,
            num_dict: false,
            float_int_ok: kind == ColumnKind::Float,
            float_int_min: i64::MAX,
            float_int_max: i64::MIN,
            logical_type: 0,
            physical_type: 0,
            flags: 0,
            encoding: ENCODING_NONE,
            dict_id: 0,
            dict_index_width: 0,
            scale: 0,
            other_dict_id: None,
        });
    }
    Ok(specs)
}

#[cfg(feature = "sav")]
pub(crate) fn scan_sav_columns(columns: &[ColumnarColumn], specs: &mut [ColumnSpec]) -> Result<()> {
    for (idx, col) in columns.iter().enumerate() {
        let spec = &mut specs[idx];
        match &col.rows {
            ColumnRows::Numeric(rows) => {
                if spec.kind != ColumnKind::Float {
                    bail!("Unexpected numeric column kind for {}", spec.name);
                }
                for value in rows {
                    match value {
                        Some(v) => update_float_stats(spec, *v),
                        None => spec.nullable = true,
                    }
                }
            }
            ColumnRows::Indexed(rows) => {
                if spec.kind != ColumnKind::String {
                    bail!("Unexpected indexed column kind for {}", spec.name);
                }
                if spec.dict_map.is_some() {
                    let dict_len = spec.dict_values.len();
                    for value in rows {
                        match value {
                            Some(idx) => {
                                if *idx >= dict_len {
                                    bail!(
                                        "Dictionary index {} out of range for column {}",
                                        idx,
                                        spec.name
                                    );
                                }
                            }
                            None => spec.nullable = true,
                        }
                    }
                } else {
                    for value in rows {
                        if value.is_none() {
                            spec.nullable = true;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn scan_columns(
    input: &Path,
    metadata: &parquet2::metadata::FileMetaData,
    schema: &Schema,
    columns: &mut [ColumnSpec],
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let row_groups = metadata.row_groups.clone();
    let mut reader = FileReader::new(file, row_groups, schema.clone(), None, None, None);

    for maybe_chunk in &mut reader {
        let chunk = maybe_chunk.context("read parquet row group")?;
        scan_chunk(columns, &chunk)?;
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn read_parquet_chunks_parallel(
    input: &Path,
    metadata: &FileMetaData,
    schema: &Schema,
) -> Result<Vec<Chunk<Box<dyn Array>>>> {
    let row_groups = metadata.row_groups.clone();
    eprintln!("Parquet row groups: {}", row_groups.len());
    let chunks: Vec<Result<Chunk<Box<dyn Array>>>> = row_groups
        .par_iter()
        .map(|row_group| {
            let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
            let mut reader = FileReader::new(
                file,
                vec![row_group.clone()],
                schema.clone(),
                None,
                None,
                None,
            );
            let chunk = reader
                .next()
                .transpose()
                .context("read parquet row group")?
                .unwrap_or_else(|| Chunk::new(vec![]));
            Ok(chunk)
        })
        .collect();
    chunks.into_iter().collect()
}

#[allow(dead_code)]
pub(crate) fn scan_chunks(
    columns: &mut [ColumnSpec],
    chunks: &[Chunk<Box<dyn Array>>],
) -> Result<()> {
    for chunk in chunks {
        scan_chunk(columns, chunk)?;
    }
    Ok(())
}

fn scan_chunk(columns: &mut [ColumnSpec], chunk: &Chunk<Box<dyn Array>>) -> Result<()> {
    for (idx, array) in chunk.arrays().iter().enumerate() {
        let col = &mut columns[idx];
        if array.null_count() > 0 {
            col.nullable = true;
        }
        scan_array(col, array.as_ref())?;
    }
    Ok(())
}

fn scan_array(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    match col.kind {
        ColumnKind::String => scan_string_array(col, array),
        ColumnKind::Boolean => Ok(()),
        ColumnKind::Int => scan_int_array(col, array),
        ColumnKind::Float => scan_float_array(col, array),
    }
}

fn scan_string_array(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .context("utf8 downcast")?;
            for value in arr.iter().flatten() {
                add_dict_value(col, value);
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .context("large utf8 downcast")?;
            for value in arr.iter().flatten() {
                add_dict_value(col, value);
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .context("binary downcast")?;
            scan_binary_array(col, arr)?;
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray<i64>>()
                .context("large binary downcast")?;
            scan_binary_array(col, arr)?;
        }
        DataType::Dictionary(key_type, value_type, _) => {
            if matches!(value_type.as_ref(), DataType::Utf8) {
                scan_dict_utf8::<i32>(col, array, *key_type)?;
            } else if matches!(value_type.as_ref(), DataType::LargeUtf8) {
                scan_dict_utf8::<i64>(col, array, *key_type)?;
            } else if matches!(value_type.as_ref(), DataType::Binary) {
                scan_dict_binary::<i32>(col, array, *key_type)?;
            } else if matches!(value_type.as_ref(), DataType::LargeBinary) {
                scan_dict_binary::<i64>(col, array, *key_type)?;
            } else {
                bail!("Unsupported dictionary value type for {}", col.name);
            }
        }
        _ => bail!("Unsupported string array for {}", col.name),
    }
    Ok(())
}

fn scan_dict_utf8<O: arrow2::types::Offset>(
    col: &mut ColumnSpec,
    array: &dyn Array,
    key_type: IntegerType,
) -> Result<()> {
    macro_rules! scan_keys {
        ($key_ty:ty) => {{
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<$key_ty>>()
                .context("dict downcast")?;
            let keys = dict.keys();
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<Utf8Array<O>>()
                .context("dict values downcast")?;
            for (idx, key) in keys.iter().enumerate() {
                if dict.is_valid(idx) {
                    let key = key.context("dict key")?;
                    let value = values.value(key.to_usize());
                    add_dict_value(col, value);
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => scan_keys!(u8),
        IntegerType::UInt16 => scan_keys!(u16),
        IntegerType::UInt32 => scan_keys!(u32),
        IntegerType::UInt64 => scan_keys!(u64),
        IntegerType::Int8 => scan_keys!(i8),
        IntegerType::Int16 => scan_keys!(i16),
        IntegerType::Int32 => scan_keys!(i32),
        IntegerType::Int64 => scan_keys!(i64),
    }
}

fn scan_binary_array<O: arrow2::types::Offset>(
    col: &mut ColumnSpec,
    array: &BinaryArray<O>,
) -> Result<()> {
    for value in array.iter().flatten() {
        add_dict_value_bytes(col, value)?;
    }
    Ok(())
}

fn scan_dict_binary<O: arrow2::types::Offset>(
    col: &mut ColumnSpec,
    array: &dyn Array,
    key_type: IntegerType,
) -> Result<()> {
    macro_rules! scan_keys {
        ($key_ty:ty) => {{
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<$key_ty>>()
                .context("dict downcast")?;
            let keys = dict.keys();
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<BinaryArray<O>>()
                .context("dict values downcast")?;
            for (idx, key) in keys.iter().enumerate() {
                if dict.is_valid(idx) {
                    let key = key.context("dict key")?;
                    let value = values.value(key.to_usize());
                    add_dict_value_bytes(col, value)?;
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => scan_keys!(u8),
        IntegerType::UInt16 => scan_keys!(u16),
        IntegerType::UInt32 => scan_keys!(u32),
        IntegerType::UInt64 => scan_keys!(u64),
        IntegerType::Int8 => scan_keys!(i8),
        IntegerType::Int16 => scan_keys!(i16),
        IntegerType::Int32 => scan_keys!(i32),
        IntegerType::Int64 => scan_keys!(i64),
    }
}

fn scan_int_array(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::Int8 => scan_primitive_i64(col, array),
        DataType::Int16 => scan_primitive_i64(col, array),
        DataType::Int32 => scan_primitive_i64(col, array),
        DataType::Int64 => scan_primitive_i64(col, array),
        DataType::UInt8 => scan_primitive_u64(col, array),
        DataType::UInt16 => scan_primitive_u64(col, array),
        DataType::UInt32 => scan_primitive_u64(col, array),
        DataType::UInt64 => scan_primitive_u64(col, array),
        DataType::Date32 => scan_primitive_i64(col, array),
        DataType::Date64 => scan_primitive_i64(col, array),
        DataType::Time32(_) => scan_primitive_i64(col, array),
        DataType::Time64(_) => scan_primitive_i64(col, array),
        DataType::Timestamp(_, _) => scan_primitive_i64(col, array),
        DataType::Duration(_) => scan_primitive_i64(col, array),
        _ => bail!("Unsupported integer array for {}", col.name),
    }
}

fn scan_float_array(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    match array.data_type() {
        DataType::Float32 => scan_primitive_f64(col, array),
        DataType::Float64 => scan_primitive_f64(col, array),
        _ => bail!("Unsupported float array for {}", col.name),
    }
}

fn scan_primitive_i64(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    macro_rules! scan {
        ($ty:ty) => {{
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .context("primitive downcast")?;
            for value in arr.iter().flatten() {
                update_int_stats(col, *value as i64);
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Int8 => scan!(i8),
        DataType::Int16 => scan!(i16),
        DataType::Int32 => scan!(i32),
        DataType::Int64 => scan!(i64),
        DataType::Date32 => scan!(i32),
        DataType::Date64 => scan!(i64),
        DataType::Time32(_) => scan!(i32),
        DataType::Time64(_) => scan!(i64),
        DataType::Timestamp(_, _) => scan!(i64),
        DataType::Duration(_) => scan!(i64),
        _ => bail!("Unexpected signed int array for {}", col.name),
    }
}

fn scan_primitive_u64(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    macro_rules! scan {
        ($ty:ty) => {{
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .context("primitive downcast")?;
            for value in arr.iter().flatten() {
                update_uint_stats(col, *value as u64);
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::UInt8 => scan!(u8),
        DataType::UInt16 => scan!(u16),
        DataType::UInt32 => scan!(u32),
        DataType::UInt64 => scan!(u64),
        _ => bail!("Unexpected unsigned int array for {}", col.name),
    }
}

fn scan_primitive_f64(col: &mut ColumnSpec, array: &dyn Array) -> Result<()> {
    macro_rules! scan {
        ($ty:ty) => {{
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .context("primitive downcast")?;
            for value in arr.iter().flatten() {
                update_float_stats(col, *value as f64);
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Float32 => scan!(f32),
        DataType::Float64 => scan!(f64),
        _ => bail!("Unexpected float array for {}", col.name),
    }
}

fn update_int_stats(col: &mut ColumnSpec, value: i64) {
    record_num_dict_value(col, value);
    let v = value as f64;
    if v < col.min {
        col.min = v;
    }
    if v > col.max {
        col.max = v;
    }
}

fn update_uint_stats(col: &mut ColumnSpec, value: u64) {
    if value > i64::MAX as u64 {
        col.unsafe_int = true;
    }
    if value <= i64::MAX as u64 {
        record_num_dict_value(col, value as i64);
    } else {
        col.num_dict_values = None;
    }
    let v = value as f64;
    if v < col.min {
        col.min = v;
    }
    if v > col.max {
        col.max = v;
    }
}

fn update_float_stats(col: &mut ColumnSpec, value: f64) {
    if !value.is_finite() {
        col.scale_candidates = 0;
        col.float_int_ok = false;
        return;
    }
    if col.float_int_ok {
        let rounded = value.round();
        let err = (value - rounded).abs();
        if err > SCALE_EPS || rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
            col.float_int_ok = false;
        } else {
            let rounded = rounded as i64;
            if rounded < col.float_int_min {
                col.float_int_min = rounded;
            }
            if rounded > col.float_int_max {
                col.float_int_max = rounded;
            }
        }
    }
    if value < col.min {
        col.min = value;
    }
    if value > col.max {
        col.max = value;
    }
    if col.f32_ok && (value as f32 as f64) != value {
        col.f32_ok = false;
    }
    if col.scale_candidates == 0 {
        return;
    }
    let mut mask = col.scale_candidates;
    for (idx, scale) in SCALE_CANDIDATES.iter().enumerate() {
        let bit = 1u32 << idx;
        if (mask & bit) == 0 {
            continue;
        }
        let scaled = value * (*scale as f64);
        if !scaled.is_finite() {
            mask &= !bit;
            continue;
        }
        let rounded = scaled.round();
        let err = (scaled - rounded).abs();
        if err > SCALE_EPS || rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
            mask &= !bit;
            continue;
        }
        let rounded = rounded as i64;
        if rounded < col.scaled_min[idx] {
            col.scaled_min[idx] = rounded;
        }
        if rounded > col.scaled_max[idx] {
            col.scaled_max[idx] = rounded;
        }
    }
    col.scale_candidates = mask;
}

fn record_num_dict_value(col: &mut ColumnSpec, value: i64) {
    let Some(values) = col.num_dict_values.as_mut() else {
        return;
    };
    values.insert(value);
    if values.len() > default_max_dict_values() {
        col.num_dict_values = None;
    }
}

fn ensure_other_dict_bucket(col: &mut ColumnSpec) {
    let Some(map) = col.dict_map.as_mut() else {
        return;
    };
    if map.contains_key(OTHER_DICT_VALUE) {
        col.other_dict_id = map.get(OTHER_DICT_VALUE).copied();
        return;
    }
    let id = col.dict_values.len() as u32;
    col.dict_values.push(OTHER_DICT_VALUE.to_string());
    map.insert(OTHER_DICT_VALUE.to_string(), id);
    col.other_dict_id = Some(id);
}

fn add_dict_value(col: &mut ColumnSpec, value: &str) {
    if value == OTHER_DICT_VALUE {
        return;
    }
    let Some(map) = col.dict_map.as_mut() else {
        return;
    };
    if map.contains_key(value) {
        return;
    }
    let limit = dict_value_limit_for_column(&col.name);
    if col.dict_values.len() >= limit {
        if is_large_dict_column(&col.name) {
            ensure_other_dict_bucket(col);
        } else {
            col.dict_map = None;
        }
        return;
    }
    let id = col.dict_values.len() as u32;
    col.dict_values.push(value.to_string());
    map.insert(value.to_string(), id);
    if col.dict_values.len() >= limit {
        if is_large_dict_column(&col.name) {
            ensure_other_dict_bucket(col);
        } else {
            col.dict_map = None;
        }
    }
}

pub(crate) fn dict_id_for_value(col: &ColumnSpec, value: &str) -> u32 {
    let map = col
        .dict_map
        .as_ref()
        .unwrap_or_else(|| panic!("dict map missing for column {}", col.name));
    if let Some(&id) = map.get(value) {
        return id;
    }
    col.other_dict_id.unwrap_or_else(|| {
        panic!(
            "dictionary id missing for value in column {} (no overflow bucket)",
            col.name
        )
    })
}

fn add_dict_value_bytes(col: &mut ColumnSpec, value: &[u8]) -> Result<()> {
    let value = decode_binary_value(value, &col.name)?;
    add_dict_value(col, value);
    Ok(())
}

pub(crate) fn finalize_columns(columns: &mut [ColumnSpec]) -> Result<()> {
    for col in columns.iter_mut() {
        if !col.min.is_finite() || !col.max.is_finite() {
            col.min = 0.0;
            col.max = 0.0;
            col.f32_ok = true;
            col.scale_candidates = 0;
        }

        match col.kind {
            ColumnKind::String => {
                col.dict_id = col.id as u32;
                if col.dict_map.is_some() {
                    let width = dict_index_width(col.dict_values.len());
                    col.dict_index_width = width;
                    col.physical_type = width_to_type(width);
                    col.logical_type = TYPE_STRING;
                    col.flags = FLAG_DICT | if col.nullable { FLAG_NULLABLE } else { 0 };
                    col.encoding = ENCODING_NONE;
                } else {
                    col.dict_index_width = 0;
                    col.physical_type = TYPE_STRING;
                    col.logical_type = TYPE_STRING;
                    col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                    col.encoding = ENCODING_NONE;
                }
            }
            ColumnKind::Boolean => {
                col.physical_type = TYPE_BOOL;
                col.logical_type = TYPE_BOOL;
                col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                col.encoding = ENCODING_NONE;
            }
            ColumnKind::Float => {
                col.scale = 0;
                if let Some((idx, physical)) = choose_scale_for_range(col) {
                    let _min = col.scaled_min[idx];
                    let _max = col.scaled_max[idx];
                    col.scale = SCALE_CANDIDATES[idx];
                    col.physical_type = physical;
                    col.logical_type = if col.f32_ok { TYPE_F32 } else { TYPE_F64 };
                    col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                    col.encoding = ENCODING_NUM_DICT;
                    continue;
                }
                if col.float_int_ok {
                    let min = col.float_int_min;
                    let max = col.float_int_max;
                    if let Some(physical) = int_type_for_range(min, max) {
                        col.scale = 1;
                        col.physical_type = physical;
                        col.logical_type = if col.f32_ok { TYPE_F32 } else { TYPE_F64 };
                        col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                        col.encoding = ENCODING_NUM_DICT;
                        continue;
                    }
                }
                col.physical_type = if col.f32_ok { TYPE_F32 } else { TYPE_F64 };
                col.logical_type = col.physical_type;
                col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                col.encoding = ENCODING_NONE;
            }
            ColumnKind::Int => {
                if col.unsafe_int {
                    col.physical_type = TYPE_F64;
                    col.logical_type = TYPE_F64;
                    col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                    col.kind = ColumnKind::Float;
                    col.encoding = ENCODING_NONE;
                    col.num_dict = false;
                    col.num_dict_values = None;
                    continue;
                }
                let min = col.min;
                let max = col.max;
                if min >= 0.0 && max <= u8::MAX as f64 {
                    col.physical_type = TYPE_U8;
                } else if min >= 0.0 && max <= u16::MAX as f64 {
                    col.physical_type = TYPE_U16;
                } else if min >= 0.0 && max <= u32::MAX as f64 {
                    col.physical_type = TYPE_U32;
                } else if min >= i8::MIN as f64 && max <= i8::MAX as f64 {
                    col.physical_type = TYPE_I8;
                } else if min >= i16::MIN as f64 && max <= i16::MAX as f64 {
                    col.physical_type = TYPE_I16;
                } else if min >= i32::MIN as f64 && max <= i32::MAX as f64 {
                    col.physical_type = TYPE_I32;
                } else {
                    col.physical_type = TYPE_I64;
                }
                col.logical_type = col.physical_type;
                col.flags = if col.nullable { FLAG_NULLABLE } else { 0 };
                col.num_dict = false;
                col.encoding = ENCODING_NUM_DICT;
                col.num_dict_values = None;
            }
        }
    }
    Ok(())
}

fn choose_scale_for_range(col: &ColumnSpec) -> Option<(usize, u8)> {
    let mask = col.scale_candidates;
    if mask == 0 {
        return None;
    }
    // Prefer scale=60 when it fits, otherwise fall back to any candidate that fits.
    for (idx, scale) in SCALE_CANDIDATES.iter().enumerate() {
        if *scale != 60 {
            continue;
        }
        if (mask & (1u32 << idx)) == 0 {
            continue;
        }
        let min = col.scaled_min[idx];
        let max = col.scaled_max[idx];
        if min == i64::MAX || max == i64::MIN {
            continue;
        }
        if let Some(physical) = int_type_for_range(min, max) {
            return Some((idx, physical));
        }
    }
    for (idx, _) in SCALE_CANDIDATES.iter().enumerate() {
        if (mask & (1u32 << idx)) == 0 {
            continue;
        }
        let min = col.scaled_min[idx];
        let max = col.scaled_max[idx];
        if min == i64::MAX || max == i64::MIN {
            continue;
        }
        if let Some(physical) = int_type_for_range(min, max) {
            return Some((idx, physical));
        }
    }
    None
}

fn int_type_for_range(min: i64, max: i64) -> Option<u8> {
    if min >= 0 {
        if max <= u8::MAX as i64 {
            Some(TYPE_U8)
        } else if max <= u16::MAX as i64 {
            Some(TYPE_U16)
        } else if max <= u32::MAX as i64 {
            Some(TYPE_U32)
        } else {
            Some(TYPE_I64)
        }
    } else if min >= i8::MIN as i64 && max <= i8::MAX as i64 {
        Some(TYPE_I8)
    } else if min >= i16::MIN as i64 && max <= i16::MAX as i64 {
        Some(TYPE_I16)
    } else if min >= i32::MIN as i64 && max <= i32::MAX as i64 {
        Some(TYPE_I32)
    } else {
        Some(TYPE_I64)
    }
}

fn dict_index_width(size: usize) -> u8 {
    if size <= u8::MAX as usize {
        1
    } else if size <= u16::MAX as usize {
        2
    } else {
        4
    }
}

fn width_to_type(width: u8) -> u8 {
    match width {
        1 => TYPE_U8,
        2 => TYPE_U16,
        _ => TYPE_U32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_col() -> ColumnSpec {
        ColumnSpec {
            id: 0,
            name: "col".to_string(),
            kind: ColumnKind::String,
            nullable: false,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            f32_ok: true,
            scale_candidates: 0,
            scaled_min: [i64::MAX; SCALE_CANDIDATE_LEN],
            scaled_max: [i64::MIN; SCALE_CANDIDATE_LEN],
            unsafe_int: false,
            dict_map: Some(HashMap::new()),
            dict_values: Vec::new(),
            num_dict_values: None,
            num_dict: false,
            float_int_ok: true,
            float_int_min: i64::MAX,
            float_int_max: i64::MIN,
            logical_type: 0,
            physical_type: 0,
            flags: 0,
            encoding: ENCODING_NONE,
            dict_id: 0,
            dict_index_width: 0,
            scale: 0,
            other_dict_id: None,
        }
    }

    #[test]
    fn low_cardinality_keeps_dict() {
        let mut col = make_string_col();
        for idx in 0..100 {
            add_dict_value(&mut col, &format!("v{}", idx));
        }
        finalize_columns(std::slice::from_mut(&mut col)).expect("finalize");
        assert!((col.flags & FLAG_DICT) != 0);
        assert_eq!(col.encoding, 0);
        assert_eq!(col.physical_type, TYPE_U8);
    }

    #[test]
    fn high_cardinality_disables_dict() {
        let mut col = make_string_col();
        for idx in 0..5000 {
            add_dict_value(&mut col, &format!("v{}", idx));
        }
        assert!(col.dict_map.is_none());
        finalize_columns(std::slice::from_mut(&mut col)).expect("finalize");
        assert!((col.flags & FLAG_DICT) == 0);
        assert_eq!(col.encoding, 0);
        assert_eq!(col.physical_type, TYPE_STRING);
    }

    #[test]
    fn dict_boundary_respected() {
        let mut col = make_string_col();
        let limit = default_max_dict_values();
        for idx in 0..limit.saturating_sub(1) {
            add_dict_value(&mut col, &format!("v{}", idx));
        }
        assert!(col.dict_map.is_some());
        add_dict_value(&mut col, &format!("v{}", limit.saturating_sub(1)));
        assert!(col.dict_map.is_none());
    }

    #[test]
    fn large_dict_column_keeps_overflow_bucket() {
        let mut col = make_string_col();
        col.name = "crate_name".to_string();
        let limit = dict_value_limit_for_column(&col.name);
        for idx in 0..limit {
            add_dict_value(&mut col, &format!("v{}", idx));
        }
        assert!(col.dict_map.is_some());
        add_dict_value(&mut col, "overflow");
        assert!(col.dict_map.is_some());
        assert!(col.other_dict_id.is_some());
    }
}
