use anyhow::{bail, Context, Result};
use arrow2::array::{Array, BinaryArray, BooleanArray, DictionaryArray, PrimitiveArray, Utf8Array};
use arrow2::datatypes::{DataType, IntegerType};
use arrow2::types::Index;

use crate::scan::dict_id_for_value;
use crate::types::{ColumnBuffer, ColumnKind, ColumnSpec, ColumnValues};
use crate::utils::{decode_binary_value, scaled_f64, set_bit};

#[inline]
fn mark_empty(empties: &mut [u8], empty_count: &mut u32, idx: usize) {
    set_bit(empties, idx);
    *empty_count += 1;
}

#[inline]
fn mark_null(has_nulls: &mut bool, null_count: &mut u32) {
    *has_nulls = true;
    *null_count += 1;
}

pub(crate) fn append_array_slice(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    start: usize,
    len: usize,
) -> Result<()> {
    match col.kind {
        ColumnKind::String => append_string_slice(col, buffer, array, start, len),
        ColumnKind::Boolean => append_bool_slice(buffer, array, start, len),
        ColumnKind::Int | ColumnKind::Float => append_numeric_slice(col, buffer, array, start, len),
    }
}

fn append_string_slice(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    start: usize,
    len: usize,
) -> Result<()> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .context("utf8 downcast")?;
            if col.dict_map.is_none() {
                append_utf8_raw_values(buffer, arr, start, len);
            } else {
                append_utf8_values(col, buffer, arr, start, len);
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .context("large utf8 downcast")?;
            if col.dict_map.is_none() {
                append_utf8_raw_values(buffer, arr, start, len);
            } else {
                append_utf8_values(col, buffer, arr, start, len);
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .context("binary downcast")?;
            if col.dict_map.is_none() {
                append_binary_raw_values(col, buffer, arr, start, len)?;
            } else {
                append_binary_values(col, buffer, arr, start, len)?;
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray<i64>>()
                .context("large binary downcast")?;
            if col.dict_map.is_none() {
                append_binary_raw_values(col, buffer, arr, start, len)?;
            } else {
                append_binary_values(col, buffer, arr, start, len)?;
            }
        }
        DataType::Dictionary(key_type, value_type, _) => {
            if matches!(value_type.as_ref(), DataType::Utf8) {
                if col.dict_map.is_none() {
                    append_dict_utf8_raw::<i32>(col, buffer, array, *key_type, start, len)?;
                } else {
                    append_dict_utf8::<i32>(col, buffer, array, *key_type, start, len)?;
                }
            } else if matches!(value_type.as_ref(), DataType::LargeUtf8) {
                if col.dict_map.is_none() {
                    append_dict_utf8_raw::<i64>(col, buffer, array, *key_type, start, len)?;
                } else {
                    append_dict_utf8::<i64>(col, buffer, array, *key_type, start, len)?;
                }
            } else if matches!(value_type.as_ref(), DataType::Binary) {
                if col.dict_map.is_none() {
                    append_dict_binary_raw::<i32>(col, buffer, array, *key_type, start, len)?;
                } else {
                    append_dict_binary::<i32>(col, buffer, array, *key_type, start, len)?;
                }
            } else if matches!(value_type.as_ref(), DataType::LargeBinary) {
                if col.dict_map.is_none() {
                    append_dict_binary_raw::<i64>(col, buffer, array, *key_type, start, len)?;
                } else {
                    append_dict_binary::<i64>(col, buffer, array, *key_type, start, len)?;
                }
            } else {
                bail!("Unsupported dictionary value type for {}", col.name);
            }
        }
        _ => bail!("Unsupported string array for {}", col.name),
    }
    Ok(())
}

fn append_utf8_values<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    arr: &Utf8Array<O>,
    start: usize,
    len: usize,
) {
    let base = buffer.len();
    let ColumnBuffer {
        values: buffer_values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    let dict = col.dict_map.as_ref().expect("dict map");
    let values = match buffer_values {
        ColumnValues::Dict(values) => values,
        _ => unreachable!("dict buffer mismatch"),
    };

    for idx in 0..len {
        let row = start + idx;
        if arr.is_valid(row) {
            let value = arr.value(row);
            let id = dict_id_for_value(col, value);
            values.push(id);
            set_bit(nulls, base + idx);
            if value.is_empty() {
                mark_empty(empties, empty_count, base + idx);
            }
        } else {
            values.push(0);
            mark_null(has_nulls, null_count);
        }
    }
}

fn append_utf8_raw_values<O: arrow2::types::Offset>(
    buffer: &mut ColumnBuffer,
    arr: &Utf8Array<O>,
    start: usize,
    len: usize,
) {
    let base = buffer.len();
    let ColumnBuffer {
        values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    let values = match values {
        ColumnValues::String(values) => values,
        _ => unreachable!("string buffer mismatch"),
    };
    for idx in 0..len {
        let row = start + idx;
        if arr.is_valid(row) {
            let value = arr.value(row);
            values.push(value.to_string());
            set_bit(nulls, base + idx);
            if value.is_empty() {
                mark_empty(empties, empty_count, base + idx);
            }
        } else {
            values.push(String::new());
            mark_null(has_nulls, null_count);
        }
    }
}

fn append_binary_values<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    arr: &BinaryArray<O>,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let dict = col.dict_map.as_ref().expect("dict map");
    let ColumnBuffer {
        values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    let values = match values {
        ColumnValues::Dict(values) => values,
        _ => unreachable!("dict buffer mismatch"),
    };

    for idx in 0..len {
        let row = start + idx;
        if arr.is_valid(row) {
            let bytes = arr.value(row);
            let value = decode_binary_value(bytes, &col.name)?;
            let id = dict_id_for_value(col, value);
            values.push(id);
            set_bit(nulls, base + idx);
            if value.is_empty() {
                mark_empty(empties, empty_count, base + idx);
            }
        } else {
            values.push(0);
            mark_null(has_nulls, null_count);
        }
    }
    Ok(())
}

fn append_binary_raw_values<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    arr: &BinaryArray<O>,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let ColumnBuffer {
        values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    let values = match values {
        ColumnValues::String(values) => values,
        _ => unreachable!("string buffer mismatch"),
    };
    for idx in 0..len {
        let row = start + idx;
        if arr.is_valid(row) {
            let bytes = arr.value(row);
            let value = decode_binary_value(bytes, &col.name)?;
            values.push(value.to_string());
            set_bit(nulls, base + idx);
            if value.is_empty() {
                mark_empty(empties, empty_count, base + idx);
            }
        } else {
            values.push(String::new());
            mark_null(has_nulls, null_count);
        }
    }
    Ok(())
}

fn append_dict_utf8<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    key_type: IntegerType,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let ColumnBuffer {
        values: buffer_values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    macro_rules! append_keys {
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
            let dict_map = col.dict_map.as_ref().expect("dict map");
            let out = match buffer_values {
                ColumnValues::Dict(values) => values,
                _ => unreachable!("dict buffer mismatch"),
            };
            for idx in 0..len {
                let row = start + idx;
                if dict.is_valid(row) {
                    let key = keys.value(row).to_usize();
                    let value = values.value(key);
                    let id = dict_id_for_value(col, value);
                    out.push(id);
                    set_bit(nulls, base + idx);
                    if value.is_empty() {
                        mark_empty(empties, empty_count, base + idx);
                    }
                } else {
                    out.push(0);
                    mark_null(has_nulls, null_count);
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => append_keys!(u8),
        IntegerType::UInt16 => append_keys!(u16),
        IntegerType::UInt32 => append_keys!(u32),
        IntegerType::UInt64 => append_keys!(u64),
        IntegerType::Int8 => append_keys!(i8),
        IntegerType::Int16 => append_keys!(i16),
        IntegerType::Int32 => append_keys!(i32),
        IntegerType::Int64 => append_keys!(i64),
    }
}

fn append_dict_utf8_raw<O: arrow2::types::Offset>(
    _col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    key_type: IntegerType,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let ColumnBuffer {
        values: buffer_values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    macro_rules! append_keys {
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
            let out = match buffer_values {
                ColumnValues::String(values) => values,
                _ => unreachable!("string buffer mismatch"),
            };
            for idx in 0..len {
                let row = start + idx;
                if dict.is_valid(row) {
                    let key = keys.value(row).to_usize();
                    let value = values.value(key);
                    out.push(value.to_string());
                    set_bit(nulls, base + idx);
                    if value.is_empty() {
                        mark_empty(empties, empty_count, base + idx);
                    }
                } else {
                    out.push(String::new());
                    mark_null(has_nulls, null_count);
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => append_keys!(u8),
        IntegerType::UInt16 => append_keys!(u16),
        IntegerType::UInt32 => append_keys!(u32),
        IntegerType::UInt64 => append_keys!(u64),
        IntegerType::Int8 => append_keys!(i8),
        IntegerType::Int16 => append_keys!(i16),
        IntegerType::Int32 => append_keys!(i32),
        IntegerType::Int64 => append_keys!(i64),
    }
}

fn append_dict_binary<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    key_type: IntegerType,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let ColumnBuffer {
        values: buffer_values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    macro_rules! append_keys {
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
            let dict_map = col.dict_map.as_ref().expect("dict map");
            let out = match buffer_values {
                ColumnValues::Dict(values) => values,
                _ => unreachable!("dict buffer mismatch"),
            };
            for idx in 0..len {
                let row = start + idx;
                if dict.is_valid(row) {
                    let key = keys.value(row).to_usize();
                    let bytes = values.value(key);
                    let value = decode_binary_value(bytes, &col.name)?;
                    let id = dict_id_for_value(col, value);
                    out.push(id);
                    set_bit(nulls, base + idx);
                    if value.is_empty() {
                        mark_empty(empties, empty_count, base + idx);
                    }
                } else {
                    out.push(0);
                    mark_null(has_nulls, null_count);
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => append_keys!(u8),
        IntegerType::UInt16 => append_keys!(u16),
        IntegerType::UInt32 => append_keys!(u32),
        IntegerType::UInt64 => append_keys!(u64),
        IntegerType::Int8 => append_keys!(i8),
        IntegerType::Int16 => append_keys!(i16),
        IntegerType::Int32 => append_keys!(i32),
        IntegerType::Int64 => append_keys!(i64),
    }
}

fn append_dict_binary_raw<O: arrow2::types::Offset>(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    key_type: IntegerType,
    start: usize,
    len: usize,
) -> Result<()> {
    let base = buffer.len();
    let ColumnBuffer {
        values: buffer_values,
        nulls,
        has_nulls,
        null_count,
        empties,
        empty_count,
    } = buffer;
    macro_rules! append_keys {
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
            let out = match buffer_values {
                ColumnValues::String(values) => values,
                _ => unreachable!("string buffer mismatch"),
            };
            for idx in 0..len {
                let row = start + idx;
                if dict.is_valid(row) {
                    let key = keys.value(row).to_usize();
                    let bytes = values.value(key);
                    let value = decode_binary_value(bytes, &col.name)?;
                    out.push(value.to_string());
                    set_bit(nulls, base + idx);
                    if value.is_empty() {
                        mark_empty(empties, empty_count, base + idx);
                    }
                } else {
                    out.push(String::new());
                    mark_null(has_nulls, null_count);
                }
            }
            Ok(())
        }};
    }

    match key_type {
        IntegerType::UInt8 => append_keys!(u8),
        IntegerType::UInt16 => append_keys!(u16),
        IntegerType::UInt32 => append_keys!(u32),
        IntegerType::UInt64 => append_keys!(u64),
        IntegerType::Int8 => append_keys!(i8),
        IntegerType::Int16 => append_keys!(i16),
        IntegerType::Int32 => append_keys!(i32),
        IntegerType::Int64 => append_keys!(i64),
    }
}

fn append_bool_slice(
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    start: usize,
    len: usize,
) -> Result<()> {
    let arr = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("bool downcast")?;
    let base = buffer.len();
    let ColumnBuffer {
        values,
        nulls,
        has_nulls,
        null_count,
        ..
    } = buffer;
    let out = match values {
        ColumnValues::Bool(values) => values,
        _ => unreachable!("bool buffer mismatch"),
    };
    for idx in 0..len {
        let row = start + idx;
        if arr.is_valid(row) {
            out.push(arr.value(row));
            set_bit(nulls, base + idx);
        } else {
            out.push(false);
            mark_null(has_nulls, null_count);
        }
    }
    Ok(())
}

macro_rules! append_int_values {
    ($ty:ty, $values:expr, $nulls:expr, $has_nulls:expr, $null_count:expr, $array:expr, $start:expr, $len:expr, $base:expr, $conv:expr) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .context("numeric downcast")?;
        let out = match $values {
            ColumnValues::Int(values) => values,
            _ => unreachable!("int buffer mismatch"),
        };
        for idx in 0..$len {
            let row = $start + idx;
            if arr.is_valid(row) {
                let value = $conv(arr.value(row));
                out.push(value);
                set_bit($nulls, $base + idx);
            } else {
                out.push(0);
                mark_null($has_nulls, $null_count);
            }
        }
        Ok(())
    }};
}

macro_rules! append_float_values {
    ($ty:ty, $values:expr, $nulls:expr, $has_nulls:expr, $null_count:expr, $array:expr, $start:expr, $len:expr, $base:expr, $conv:expr) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .context("numeric downcast")?;
        let out = match $values {
            ColumnValues::Float(values) => values,
            _ => unreachable!("float buffer mismatch"),
        };
        for idx in 0..$len {
            let row = $start + idx;
            if arr.is_valid(row) {
                let value = $conv(arr.value(row));
                out.push(value);
                set_bit($nulls, $base + idx);
            } else {
                out.push(0.0);
                mark_null($has_nulls, $null_count);
            }
        }
        Ok(())
    }};
}

fn append_numeric_slice(
    col: &ColumnSpec,
    buffer: &mut ColumnBuffer,
    array: &dyn Array,
    start: usize,
    len: usize,
) -> Result<()> {
    let is_int = matches!(col.kind, crate::types::ColumnKind::Int);
    let base = buffer.len();
    let ColumnBuffer {
        values,
        nulls,
        has_nulls,
        null_count,
        ..
    } = buffer;
    match array.data_type() {
        DataType::Int8 => {
            if is_int {
                append_int_values!(
                    i8,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    i8,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Int16 => {
            if is_int {
                append_int_values!(
                    i16,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    i16,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Int32 => {
            if is_int {
                append_int_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Int64 => {
            if is_int {
                append_int_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            } else {
                append_float_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::UInt8 => {
            if is_int {
                append_int_values!(
                    u8,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    u8,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::UInt16 => {
            if is_int {
                append_int_values!(
                    u16,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    u16,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::UInt32 => {
            if is_int {
                append_int_values!(
                    u32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    u32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::UInt64 => {
            if is_int {
                append_int_values!(
                    u64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    u64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Float32 => {
            if col.scale != 0 {
                append_float_values!(
                    f32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| { scaled_f64(v as f64, col.scale) }
                )
            } else {
                append_float_values!(
                    f32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Float64 => {
            if col.scale != 0 {
                append_float_values!(
                    f64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| { scaled_f64(v, col.scale) }
                )
            } else {
                append_float_values!(
                    f64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            }
        }
        DataType::Date32 => {
            if is_int {
                append_int_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Date64 => {
            if is_int {
                append_int_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            } else {
                append_float_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Time32(_) => {
            if is_int {
                append_int_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as i64
                )
            } else {
                append_float_values!(
                    i32,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Time64(_) => {
            if is_int {
                append_int_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            } else {
                append_float_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Timestamp(_, _) => {
            if is_int {
                append_int_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            } else {
                append_float_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        DataType::Duration(_) => {
            if is_int {
                append_int_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v
                )
            } else {
                append_float_values!(
                    i64,
                    values,
                    nulls,
                    has_nulls,
                    null_count,
                    array,
                    start,
                    len,
                    base,
                    |v| v as f64
                )
            }
        }
        _ => bail!("Unsupported numeric array for {}", col.name),
    }
}
