use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub struct HeaderInfo {
    pub version: u32,
    pub flags: u32,
    pub ncols: u32,
    pub nchunks: u32,
    pub rows_per_chunk: u32,
    pub total_rows: u64,
    pub schema_off: u64,
    pub schema_len: u64,
    pub index_off: u64,
    pub index_len: u64,
    pub dict_off: u64,
    pub dict_len: u64,
    pub data_off: u64,
    pub dict_raw_len: u64,
}

#[derive(Clone, Debug)]
pub struct AggregateStats {
    pub count: u32,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
}

#[derive(Clone, Debug)]
pub struct GroupKeyInfo {
    pub col_id: u32,
    pub physical_type: u8,
    pub flags: u8,
}

#[derive(Clone, Debug)]
pub struct GroupAggInfo {
    pub col_id: u32,
    pub kind: u8,
}

#[derive(Clone, Debug)]
pub struct GroupResult {
    pub keys: Vec<u64>,
    pub keys2: Option<Vec<u64>>,
    pub key_info: Vec<GroupKeyInfo>,
    pub aggs: Vec<GroupAggInfo>,
    pub values: Vec<Vec<AggregateStats>>,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub rows: Vec<u64>,
    pub aggregates: BTreeMap<String, AggregateStats>,
    pub groups: Option<GroupResult>,
}

#[derive(Clone)]
pub(crate) struct RuntimeInit {
    pub(crate) header: Vec<u8>,
    pub(crate) schema: Vec<u8>,
    pub(crate) toc: Vec<u8>,
    pub(crate) dicts: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PageRequest {
    pub(crate) kind: u32,
    pub(crate) col_id: u32,
    pub(crate) offset: u64,
    pub(crate) comp_len: u32,
    pub(crate) raw_len: u32,
}

pub(crate) enum RequiredPages {
    Skip,
    Requests(Vec<PageRequest>),
}

pub(crate) struct ChunkPartial {
    pub(crate) chunk_id: u32,
    pub(crate) rows: Vec<u8>,
    pub(crate) row_candidates: Vec<u8>,
    pub(crate) aggs: Vec<u8>,
    pub(crate) groups: Vec<u8>,
    pub(crate) partitioned_groups: Vec<Vec<u8>>,
    pub(crate) work_bytes_est: u64,
}

impl ChunkPartial {
    pub(crate) fn empty(chunk_id: u32) -> Self {
        Self {
            chunk_id,
            rows: Vec::new(),
            row_candidates: Vec::new(),
            aggs: Vec::new(),
            groups: Vec::new(),
            partitioned_groups: Vec::new(),
            work_bytes_est: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkerDebugStats {
    pub runtime_index_chunks: u64,
    pub runtime_index_entries_len: u64,
    pub runtime_index_entries_cap: u64,
    pub runtime_index_bytes_est: u64,
    pub plan_rows_len: u64,
    pub plan_rows_cap: u64,
    pub plan_group_state_len: u64,
    pub plan_group_state_cap: u64,
    pub plan_group_keys_len: u64,
    pub plan_group_keys_cap: u64,
    pub plan_row_heap_len: u64,
    pub plan_row_heap_cap: u64,
    pub plan_row_order_ranks_len: u64,
    pub plan_row_order_ranks_cap: u64,
    pub arena_reserved_bytes: u64,
    pub arena_used_bytes: u64,
    pub arena_peak_bytes: u64,
    pub perf_available: u8,
    pub perf_cycles: u64,
    pub perf_instructions: u64,
    pub perf_cache_refs: u64,
    pub perf_cache_misses: u64,
    pub perf_llc_refs: u64,
    pub perf_llc_misses: u64,
    pub perf_l1d_misses: u64,
    pub perf_l2_misses: u64,
    pub plan_timing_chunks: u64,
    pub plan_ms_decode: f64,
    pub plan_ms_filters: f64,
    pub plan_ms_aggs: f64,
    pub plan_ms_group: f64,
    pub plan_ms_rows: f64,
}

#[derive(Clone, Debug, Default)]
pub struct GlobalDebugStats {
    pub plan_count: u64,
    pub plan_rows_cap_total: u64,
    pub plan_group_state_cap_total: u64,
    pub plan_group_keys_cap_total: u64,
    pub plan_row_heap_cap_total: u64,
    pub plan_row_order_ranks_cap_total: u64,
    pub runtime_count: u64,
    pub runtime_index_chunks_total: u64,
    pub runtime_index_entries_len_total: u64,
    pub runtime_index_entries_cap_total: u64,
    pub runtime_index_bytes_est_total: u64,
    pub ffi_plan_lock_count: u64,
    pub ffi_plan_lock_wait_ns: u64,
    pub ffi_runtime_lock_count: u64,
    pub ffi_runtime_lock_wait_ns: u64,
}

pub(crate) struct PlanGuard {
    pub(crate) handle: u32,
}

impl Drop for PlanGuard {
    fn drop(&mut self) {
        unsafe {
            crate::ffi::destroy_plan(self.handle);
        }
    }
}

pub(crate) struct RuntimeGuard {
    pub(crate) handle: u32,
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        unsafe {
            crate::ffi::destroy_runtime(self.handle);
        }
    }
}
