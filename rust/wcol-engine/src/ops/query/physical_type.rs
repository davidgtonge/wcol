//! Parquet physical types (`Type` in the Parquet Thrift schema) as the footer reports them.

pub const INT32: i32 = 1;
pub const INT64: i32 = 2;
pub const FLOAT: i32 = 4;
pub const DOUBLE: i32 = 5;
pub const BYTE_ARRAY: i32 = 6;
pub const FIXED_LEN_BYTE_ARRAY: i32 = 7;

pub fn is_float(physical: i32) -> bool {
    matches!(physical, FLOAT | DOUBLE)
}

pub fn is_integer(physical: i32) -> bool {
    matches!(physical, INT32 | INT64)
}

/// Byte-string columns: text to SQL.
pub fn is_bytes(physical: i32) -> bool {
    matches!(physical, BYTE_ARRAY | FIXED_LEN_BYTE_ARRAY)
}
