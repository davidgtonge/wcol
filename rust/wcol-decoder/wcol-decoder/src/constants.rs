#[allow(unused_imports)]
pub(crate) use wcol_format::{
    EMPTY_MODE_ALL_ONE, EMPTY_MODE_ALL_ZERO, EMPTY_MODE_MIXED, ENCODING_NONE, ENCODING_NUM_DICT,
    FLAG_DICT, FLAG_NULLABLE, HEADER_BYTES, INDEX_ENTRY_BYTES, NULL_SENTINEL, ROWS_PER_CHUNK,
    TOC_ENTRY_BYTES, TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16, TYPE_I32, TYPE_I64, TYPE_I8,
    TYPE_STRING, TYPE_U16, TYPE_U32, TYPE_U8, WCOL_VERSION,
};

pub(crate) const MASK_WORDS: usize = ROWS_PER_CHUNK / 32;

pub(crate) const OP_EQ: u8 = 0;
pub(crate) const OP_NEQ: u8 = 1;
pub(crate) const OP_LT: u8 = 2;
pub(crate) const OP_LTE: u8 = 3;
pub(crate) const OP_GT: u8 = 4;
pub(crate) const OP_GTE: u8 = 5;
pub(crate) const OP_BETWEEN: u8 = 6;
pub(crate) const OP_LIKE: u8 = 7;
pub(crate) const OP_NOT_LIKE: u8 = 8;

#[allow(dead_code)]
pub(crate) const COMB_AND: i32 = -1;
#[allow(dead_code)]
pub(crate) const COMB_OR: i32 = -2;
#[allow(dead_code)]
pub(crate) const COMB_NOT: i32 = -3;

#[allow(dead_code)]
pub(crate) const PAGE_KIND_DATA: u32 = 0;
#[allow(dead_code)]
pub(crate) const PAGE_KIND_NULL: u32 = 1;
#[allow(dead_code)]
pub(crate) const PAGE_KIND_EMPTY: u32 = 2;
#[allow(dead_code)]
pub(crate) const PAGE_REQ_WORDS: usize = 6;
#[allow(dead_code)]
pub(crate) const PAGE_EXEC_WORDS: usize = 5;

#[allow(dead_code)]
pub(crate) const ERR_UNSUPPORTED: i32 = -1000;
#[allow(dead_code)]
pub(crate) const ERR_NON_NUMERIC_AGG: i32 = -1100;

/// Sentinel col_id for COUNT(*) — no real column; executor adds popcount(mask) to agg count.
pub(crate) const ROW_COUNT_COL_ID: u32 = u32::MAX;

/// Aggregate kind stored in low byte of agg_key.
///
/// Current agg_key encoding (u32):
/// bits: [ offset:i8 | col_id:u16 | kind:u8 ]
pub(crate) const AGG_KIND_COUNT_STAR: u8 = 0;
pub(crate) const AGG_KIND_SUM: u8 = 1;
pub(crate) const AGG_KIND_AVG: u8 = 2;
pub(crate) const AGG_KIND_MIN: u8 = 3;
pub(crate) const AGG_KIND_MAX: u8 = 4;
pub(crate) const AGG_KIND_COUNT: u8 = 5;
pub(crate) const AGG_KIND_APPROX_DISTINCT: u8 = 6;

/// Default HLL precision (p=14 => 16384 registers, ~0.8% error).
pub(crate) const HLL_DEFAULT_PRECISION: u8 = 14;
