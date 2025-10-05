#![allow(dead_code)]

use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Header {
    pub(crate) version: u16,
    pub(crate) flags: u16,
    pub(crate) ncols: u32,
    pub(crate) nchunks: u32,
    pub(crate) rows_per_chunk: u32,
    pub(crate) total_rows: u64,
    pub(crate) schema_off: u64,
    pub(crate) schema_len: u64,
    pub(crate) index_off: u64,
    pub(crate) index_len: u64,
    pub(crate) dict_off: u64,
    pub(crate) dict_len: u64,
    pub(crate) data_off: u64,
    pub(crate) dict_raw_len: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) logical_type: u8,
    pub(crate) physical_type: u8,
    pub(crate) flags: u8,
    pub(crate) encoding: u8,
    pub(crate) dict_id: u32,
    #[allow(dead_code)]
    pub(crate) dict_index_width: u8,
    pub(crate) scale: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct Dictionary {
    pub(crate) offsets: Vec<u32>,
    pub(crate) blob: Vec<u8>,
    pub(crate) values: Vec<String>,
    pub(crate) lookup: FxHashMap<String, u32>,
    pub(crate) hash_cache: Vec<u64>,
}

impl Dictionary {
    pub(crate) fn new() -> Self {
        Self {
            offsets: Vec::new(),
            blob: Vec::new(),
            values: Vec::new(),
            lookup: FxHashMap::default(),
            hash_cache: Vec::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        if !self.offsets.is_empty() {
            self.offsets.len().saturating_sub(1)
        } else {
            self.values.len()
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Runtime {
    pub(crate) header: Option<Header>,
    pub(crate) schema: Arc<[Column]>,
    pub(crate) toc: Vec<u64>,
    pub(crate) dicts: FxHashMap<u32, Dictionary>,
    pub(crate) index_cache: FxHashMap<u32, Vec<IndexEntry>>,
}

#[derive(Clone, Debug)]
pub(crate) struct Filter {
    pub(crate) col_id: u32,
    pub(crate) op: u8,
    pub(crate) value: f64,
    pub(crate) value2: f64,
    pub(crate) in_list: Option<Vec<f64>>,
    /// When Some, resolve at execution time using chunk's dict (block-local strings)
    pub(crate) value_str: Option<String>,
    /// When Some, resolve at execution time (IN with string literals, block-local)
    pub(crate) in_list_str: Option<Vec<String>>,
    /// Precomputed LIKE match set for dict ids (1 = match), shared across chunks.
    pub(crate) like_ids: Option<Arc<Vec<u8>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct GroupBy {
    pub(crate) keys: Vec<u32>,
    pub(crate) value_col: Option<u32>,
    pub(crate) value_kind: u8,
    pub(crate) count_kind: u8,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GroupAgg {
    pub(crate) col_id: u32,
    pub(crate) kind: u8,
}

#[derive(Clone, Debug)]
pub(crate) struct AggState {
    pub(crate) sum: f64,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) count: u32,
}

/// HyperLogLog state for approx_count_distinct.
/// Uses p=14 by default (16384 registers, ~0.8% error).
#[derive(Clone, Debug)]
pub(crate) struct HllState {
    /// Precision parameter (number of bits for register index).
    pub(crate) p: u8,
    /// Registers: m = 2^p registers, each storing max leading zeros + 1.
    pub(crate) registers: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct GroupState {
    pub(crate) aggs: Vec<GroupAggState>,
}

#[derive(Clone, Debug)]
pub(crate) enum GroupAggState {
    Numeric(AggState),
    Distinct(FxHashSet<u64>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct GroupKey {
    pub(crate) a: u64,
    pub(crate) b: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Plan {
    pub(crate) runtime: u32,
    pub(crate) filters: Vec<Filter>,
    pub(crate) combine: Vec<i32>,
    pub(crate) group_by: Option<GroupBy>,
    /// Aggregate key encoding (u32):
    /// bits: [ offset:i8 | col_id:u16 | kind:u8 ]
    pub(crate) aggregates: Vec<u32>,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
    pub(crate) rows: Vec<u64>,
    pub(crate) agg_state: FxHashMap<u32, AggState>,
    pub(crate) group_state: FxHashMap<GroupKey, GroupState>,
    pub(crate) group_keys: Vec<GroupKey>,
    pub(crate) group_key_repr: FxHashMap<GroupKey, Vec<u8>>,
    pub(crate) group_order_by_count: bool,
    pub(crate) group_aggs: Vec<GroupAgg>,
    pub(crate) row_order_by: Vec<u32>,
    pub(crate) row_heap: BinaryHeap<RowCandidate>,
    pub(crate) row_order_lex_ranks: FxHashMap<u32, Vec<u32>>,
    /// HLL states for approx_count_distinct aggregates, keyed by agg_key.
    pub(crate) hll_state: FxHashMap<u32, HllState>,
    pub(crate) group_emit_raw: bool,
    pub(crate) group_rows_raw_with_keys: Vec<u8>,
    /// File dict length for dense `GROUP BY dict` + `COUNT(*)` histogram (0 = disabled).
    pub(crate) group_dict_hist_dict_len: u32,
    pub(crate) group_dict_hist_counts: Option<Vec<u32>>,
    /// Parallel sum bucket for `GROUP BY` one dict key + `SUM(f64)` fast path.
    pub(crate) group_dict_hist_sums: Option<Vec<f64>>,
    pub(crate) select_cols: Vec<u32>,
    pub(crate) row_projection: RowProjectionBuf,
    pub(crate) timing: PlanTiming,
    pub(crate) filter_timing: FilterTiming,
}

pub(crate) const PROJ_KIND_F64: u8 = 0;
pub(crate) const PROJ_KIND_DICT_ID: u8 = 1;
pub(crate) const PROJ_KIND_BOOL: u8 = 2;

pub(crate) const PROJ_MAGIC: &[u8; 8] = b"WCOLpjv1";

#[derive(Clone, Debug)]
pub(crate) enum ProjectionColumnBuf {
    F64 {
        values: Vec<f64>,
        nulls: Vec<u8>,
    },
    DictId {
        values: Vec<u32>,
        nulls: Vec<u8>,
    },
    Bool {
        values: Vec<u8>,
        nulls: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RowProjectionBuf {
    pub(crate) row_count: usize,
    pub(crate) col_ids: Vec<u32>,
    pub(crate) kinds: Vec<u8>,
    pub(crate) columns: Vec<ProjectionColumnBuf>,
}

impl RowProjectionBuf {
    pub(crate) fn clear(&mut self) {
        self.row_count = 0;
        self.col_ids.clear();
        self.kinds.clear();
        self.columns.clear();
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PlanTiming {
    #[cfg(feature = "timing")]
    pub(crate) chunks: u32,
    #[cfg(feature = "timing")]
    pub(crate) ms_decode: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_filters: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_filters_decode: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_filters_build: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_filters_nulls: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_filters_combine: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_aggs: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_group: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_rows: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_str_perm: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_str_token: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_str_reconstruct: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_str_dict: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FilterTiming {
    #[cfg(feature = "timing")]
    pub(crate) cols: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) ops: Vec<u8>,
    #[cfg(feature = "timing")]
    pub(crate) ms_decode: Vec<f64>,
    #[cfg(feature = "timing")]
    pub(crate) ms_build: Vec<f64>,
    #[cfg(feature = "timing")]
    pub(crate) ms_nulls: Vec<f64>,
    #[cfg(feature = "timing")]
    pub(crate) like_blocks_total: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) like_blocks_skipped: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) like_blocks_passed: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) like_blocks_matched: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) like_rows_verified: Vec<u32>,
    #[cfg(feature = "timing")]
    pub(crate) like_ms_mask: Vec<f64>,
    #[cfg(feature = "timing")]
    pub(crate) like_ms_verify: Vec<f64>,
    #[cfg(feature = "timing")]
    pub(crate) like_ms_other: Vec<f64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LikeMaskStats {
    #[cfg(feature = "timing")]
    pub(crate) blocks_total: u32,
    #[cfg(feature = "timing")]
    pub(crate) blocks_skipped: u32,
    #[cfg(feature = "timing")]
    pub(crate) blocks_passed: u32,
    #[cfg(feature = "timing")]
    pub(crate) blocks_matched: u32,
    #[cfg(feature = "timing")]
    pub(crate) rows_verified: u32,
    #[cfg(feature = "timing")]
    pub(crate) ms_mask: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_verify: f64,
    #[cfg(feature = "timing")]
    pub(crate) ms_other: f64,
}

#[derive(Clone, Debug)]
pub(crate) enum RowKey {
    Null,
    Num(f64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug)]
pub(crate) struct RowCandidate {
    pub(crate) k1: RowKey,
    pub(crate) k2: Option<RowKey>,
    pub(crate) row_id: u64,
}

fn cmp_row_key(a: &RowKey, b: &RowKey) -> Ordering {
    match (a, b) {
        (RowKey::Null, RowKey::Null) => Ordering::Equal,
        (RowKey::Null, _) => Ordering::Greater,
        (_, RowKey::Null) => Ordering::Less,
        (RowKey::Num(x), RowKey::Num(y)) => x.total_cmp(y),
        (RowKey::Bytes(x), RowKey::Bytes(y)) => x.cmp(y),
        (RowKey::Num(_), RowKey::Bytes(_)) => Ordering::Less,
        (RowKey::Bytes(_), RowKey::Num(_)) => Ordering::Greater,
    }
}

impl PartialEq for RowCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RowCandidate {}

impl PartialOrd for RowCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_row_key(&self.k1, &other.k1)
            .then_with(|| match (&self.k2, &other.k2) {
                (Some(a), Some(b)) => cmp_row_key(a, b),
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
            })
            .then_with(|| self.row_id.cmp(&other.row_id))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IndexEntry {
    pub(crate) data_off: u64,
    pub(crate) data_comp_len: u32,
    pub(crate) data_raw_len: u32,
    pub(crate) null_off: u64,
    pub(crate) null_comp_len: u32,
    pub(crate) null_raw_len: u32,
    pub(crate) empty_mode: u8,
    pub(crate) empty_count: u32,
    pub(crate) empty_off: u64,
    pub(crate) empty_comp_len: u32,
    pub(crate) empty_raw_len: u32,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) presence: u64,
}

#[derive(Clone)]
pub(crate) struct PageDesc {
    pub(crate) kind: u32,
    pub(crate) col_id: u32,
    pub(crate) offset: u64,
    pub(crate) comp_len: u32,
    pub(crate) raw_len: u32,
}

#[derive(Clone)]
pub(crate) enum ColumnData {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Bool(Vec<u8>),
}
