pub(crate) use wcol_format::{
    EMPTY_MODE_ALL_ONE, EMPTY_MODE_ALL_ZERO, EMPTY_MODE_MIXED, ENCODING_NONE, ENCODING_NUM_DICT,
    FLAG_DICT, FLAG_NULLABLE, HEADER_BYTES, INDEX_ENTRY_BYTES, NULL_SENTINEL_U64, ROWS_PER_CHUNK,
    TYPE_BOOL, TYPE_F32, TYPE_F64, TYPE_I16, TYPE_I32, TYPE_I64, TYPE_I8, TYPE_STRING, TYPE_U16,
    TYPE_U32, TYPE_U8, WCOL_VERSION,
};
pub(crate) const MAX_SAFE_INT: i64 = 9_007_199_254_740_991;
pub(crate) const MIN_SAFE_INT: i64 = -9_007_199_254_740_991;
pub(crate) const SCALE_CANDIDATE_LEN: usize = 10;
pub(crate) const SCALE_CANDIDATES: [i32; SCALE_CANDIDATE_LEN] = [
    1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 60, 600, 3_600,
];
pub(crate) const SCALE_MASK_ALL: u32 = (1u32 << SCALE_CANDIDATE_LEN) - 1;
pub(crate) const SCALE_EPS: f64 = 1e-4;
pub(crate) const HEADER_FLAG_DICT_COMPRESSED: u16 = 1;
pub(crate) const PRESENCE_MAX_VALUES: usize = 64;
pub(crate) const MAX_DICT_VALUES: usize = 2000;
/// Reserved dictionary entry for values beyond the per-column cap (large-dict columns only).
pub(crate) const OTHER_DICT_VALUE: &str = "__wcol_other__";
