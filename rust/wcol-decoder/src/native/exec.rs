use std::fs::File;
use std::sync::Arc;

use crate::constants::{INDEX_ENTRY_BYTES, PAGE_REQ_WORDS};
use crate::ffi;

use super::cache::ReadCache;
use super::config::{GroupEngineMode, MergeKeysMode};
use super::error::{NativeError, NativeResult};
use super::helpers::{call_status, checked_count, read_out_bytes, read_u32};
use super::helpers::{read_f64, read_u64};
use super::types::{
    ChunkPartial, HeaderInfo, PageRequest, PlanGuard, RequiredPages, RuntimeGuard, RuntimeInit,
    WorkerDebugStats,
};
use super::{AGG_RECORD_SIZE, GROUP_AGG_RECORD_SIZE};

pub(crate) struct WorkerContext {
    _runtime_guard: RuntimeGuard,
    _plan_guard: PlanGuard,
    runtime: u32,
    plan: u32,
    header: HeaderInfo,
    file: Arc<File>,
    read_cache: Arc<ReadCache>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PlanTimingSnapshot {
    pub(crate) chunks: u64,
    pub(crate) ms_decode: f64,
    pub(crate) ms_filters: f64,
    pub(crate) ms_aggs: f64,
    pub(crate) ms_group: f64,
    pub(crate) ms_rows: f64,
}

impl WorkerContext {
    pub(crate) fn new(
        file: Arc<File>,
        read_cache: Arc<ReadCache>,
        header: HeaderInfo,
        init: &RuntimeInit,
    ) -> NativeResult<Self> {
        let runtime = unsafe { ffi::create_runtime() };
        if runtime == 0 {
            return Err(NativeError::Invalid("create_runtime returned 0"));
        }
        let runtime_guard = RuntimeGuard { handle: runtime };
        initialize_runtime(runtime, init)?;

        let plan = unsafe { ffi::create_plan(runtime) };
        if plan == 0 {
            return Err(NativeError::Invalid("create_plan returned 0"));
        }
        let plan_guard = PlanGuard { handle: plan };

        Ok(Self {
            _runtime_guard: runtime_guard,
            _plan_guard: plan_guard,
            runtime,
            plan,
            header,
            file,
            read_cache,
        })
    }

    pub(crate) fn prepare_sql(&self, sql: &str) -> NativeResult<()> {
        call_status("plan_clear", unsafe { ffi::plan_clear(self.plan) })?;
        call_status("plan_reset_results", unsafe {
            ffi::plan_reset_results(self.plan)
        })?;
        apply_sql(self.plan, sql)
    }

    pub(crate) fn execute_chunk(
        &self,
        chunk_id: u32,
        has_filters: bool,
        merge_keys_mode: MergeKeysMode,
        group_engine_mode: GroupEngineMode,
    ) -> NativeResult<ChunkPartial> {
        call_status("plan_reset_results", unsafe {
            ffi::plan_reset_results(self.plan)
        })?;
        call_status("plan_set_group_emit_raw", unsafe {
            ffi::plan_set_group_emit_raw(
                self.plan,
                if group_engine_mode == GroupEngineMode::PartitionSortV2 {
                    1
                } else {
                    0
                },
            )
        })?;

        let row_order_by_len = checked_count("plan_row_order_by_len", unsafe {
            ffi::plan_row_order_by_len(self.plan)
        })?;
        let group_key_count = checked_count("plan_group_key_count", unsafe {
            ffi::plan_group_key_count(self.plan)
        })?;
        if group_engine_mode == GroupEngineMode::PartitionSortV2
            && std::env::var("WCOL_DEBUG_V2")
                .map(|v| v != "0")
                .unwrap_or(false)
        {
            eprintln!(
                "WCOL_V2_EXEC chunk={} group_key_count={} row_order_by_len={}",
                chunk_id, group_key_count, row_order_by_len
            );
        }
        let plan_limit = checked_count("plan_limit", unsafe { ffi::plan_limit(self.plan) })? as u32;
        let plan_offset =
            checked_count("plan_offset", unsafe { ffi::plan_offset(self.plan) })? as u32;
        let disable_group_window =
            group_key_count > 0 && row_order_by_len == 0 && (plan_limit > 0 || plan_offset > 0);
        if disable_group_window {
            call_status("plan_set_limit", unsafe {
                ffi::plan_set_limit(self.plan, 0)
            })?;
            call_status("plan_set_offset", unsafe {
                ffi::plan_set_offset(self.plan, 0)
            })?;
        }

        let span = read_out_bytes(12, |ptr, len| unsafe {
            ffi::runtime_chunk_index_span(self.runtime, chunk_id, ptr, len)
        })?;
        if span.len() != 8 && span.len() != 12 {
            return Err(NativeError::Invalid(
                "runtime_chunk_index_span returned invalid size",
            ));
        }
        let (index_offset, index_comp_len) = if span.len() == 12 {
            (read_u64(&span, 0), read_u32(&span, 8))
        } else {
            (read_u32(&span, 0) as u64, read_u32(&span, 4))
        };
        let index_bytes =
            self.read_cache
                .read_exact(&self.file, index_offset, index_comp_len as usize)?;

        let decode_bytes_est = match required_pages(
            self.runtime,
            self.plan,
            self.header,
            chunk_id,
            index_bytes.as_ref(),
            has_filters,
        )? {
            RequiredPages::Skip => return Ok(ChunkPartial::empty(chunk_id)),
            RequiredPages::Requests(requests) => {
                let estimate = requests.iter().map(|req| req.raw_len as u64).sum::<u64>();
                exec_chunk_payload(
                    self.runtime,
                    self.plan,
                    chunk_id,
                    &self.file,
                    &self.read_cache,
                    &requests,
                )?;
                estimate
            }
        };
        if row_order_by_len == 0 {
            call_status("plan_finalize_rows", unsafe {
                ffi::plan_finalize_rows(self.plan)
            })?;
        }

        let rows = if row_order_by_len == 0 {
            copy_rows_bytes(self.plan)?
        } else {
            Vec::new()
        };
        let row_candidates = if row_order_by_len > 0 {
            copy_row_candidates_bytes(self.plan)?
        } else {
            Vec::new()
        };
        let aggs = copy_aggs_bytes(self.plan)?;
        let hist_active = checked_count("plan_group_dict_hist_active", unsafe {
            ffi::plan_group_dict_hist_active(self.plan)
        })? > 0;
        let groups = if hist_active {
            copy_group_hist_partial_bytes(self.plan)?
        } else {
            match merge_keys_mode {
                MergeKeysMode::Hash => copy_groups_bytes(self.plan)?,
                MergeKeysMode::Bytes => copy_groups_bytes_with_keys(self.plan)?,
            }
        };
        if group_engine_mode == GroupEngineMode::PartitionSortV2
            && std::env::var("WCOL_DEBUG_V2")
                .map(|v| v != "0")
                .unwrap_or(false)
        {
            eprintln!(
                "WCOL_V2_EXEC chunk={} groups_bytes={}",
                chunk_id,
                groups.len()
            );
        }
        let state_bytes_est = rows.len() as u64
            + row_candidates.len() as u64
            + aggs.len() as u64
            + groups.len() as u64;

        if disable_group_window {
            call_status("plan_set_limit", unsafe {
                ffi::plan_set_limit(self.plan, plan_limit)
            })?;
            call_status("plan_set_offset", unsafe {
                ffi::plan_set_offset(self.plan, plan_offset)
            })?;
        }

        Ok(ChunkPartial {
            chunk_id,
            rows,
            row_candidates,
            aggs,
            groups,
            partitioned_groups: Vec::new(),
            work_bytes_est: decode_bytes_est.saturating_add(state_bytes_est),
        })
    }

    pub(crate) fn debug_stats(&self) -> NativeResult<WorkerDebugStats> {
        let runtime_stats = read_out_bytes(32, |ptr, len| unsafe {
            ffi::runtime_index_cache_stats(self.runtime, ptr, len)
        })?;
        let plan_stats = read_out_bytes(80, |ptr, len| unsafe {
            ffi::plan_memory_stats(self.plan, ptr, len)
        })?;
        let timing = self.plan_timing_snapshot().unwrap_or_default();
        Ok(WorkerDebugStats {
            runtime_index_chunks: read_u64(&runtime_stats, 0),
            runtime_index_entries_len: read_u64(&runtime_stats, 8),
            runtime_index_entries_cap: read_u64(&runtime_stats, 16),
            runtime_index_bytes_est: read_u64(&runtime_stats, 24),
            plan_rows_len: read_u64(&plan_stats, 0),
            plan_rows_cap: read_u64(&plan_stats, 8),
            plan_group_state_len: read_u64(&plan_stats, 16),
            plan_group_state_cap: read_u64(&plan_stats, 24),
            plan_group_keys_len: read_u64(&plan_stats, 32),
            plan_group_keys_cap: read_u64(&plan_stats, 40),
            plan_row_heap_len: read_u64(&plan_stats, 48),
            plan_row_heap_cap: read_u64(&plan_stats, 56),
            plan_row_order_ranks_len: read_u64(&plan_stats, 64),
            plan_row_order_ranks_cap: read_u64(&plan_stats, 72),
            arena_reserved_bytes: 0,
            arena_used_bytes: 0,
            arena_peak_bytes: 0,
            perf_available: 0,
            perf_cycles: 0,
            perf_instructions: 0,
            perf_cache_refs: 0,
            perf_cache_misses: 0,
            perf_llc_refs: 0,
            perf_llc_misses: 0,
            perf_l1d_misses: 0,
            perf_l2_misses: 0,
            plan_timing_chunks: timing.chunks,
            plan_ms_decode: timing.ms_decode,
            plan_ms_filters: timing.ms_filters,
            plan_ms_aggs: timing.ms_aggs,
            plan_ms_group: timing.ms_group,
            plan_ms_rows: timing.ms_rows,
        })
    }

    pub(crate) fn plan_timing_snapshot(&self) -> NativeResult<PlanTimingSnapshot> {
        let timing_stats = read_out_bytes(108, |ptr, len| unsafe {
            ffi::plan_copy_timing(self.plan, ptr, len)
        })?;
        Ok(PlanTimingSnapshot {
            chunks: if timing_stats.len() >= 4 {
                read_u32(&timing_stats, 0) as u64
            } else {
                0
            },
            ms_decode: if timing_stats.len() >= 12 {
                read_f64(&timing_stats, 4)
            } else {
                0.0
            },
            ms_filters: if timing_stats.len() >= 20 {
                read_f64(&timing_stats, 12)
            } else {
                0.0
            },
            ms_aggs: if timing_stats.len() >= 60 {
                read_f64(&timing_stats, 52)
            } else {
                0.0
            },
            ms_group: if timing_stats.len() >= 68 {
                read_f64(&timing_stats, 60)
            } else {
                0.0
            },
            ms_rows: if timing_stats.len() >= 76 {
                read_f64(&timing_stats, 68)
            } else {
                0.0
            },
        })
    }
}

pub(crate) fn initialize_runtime(runtime: u32, init: &RuntimeInit) -> NativeResult<()> {
    call_status("runtime_set_header", unsafe {
        ffi::runtime_set_header(runtime, init.header.as_ptr(), init.header.len())
    })?;
    call_status("runtime_set_schema", unsafe {
        ffi::runtime_set_schema(runtime, init.schema.as_ptr(), init.schema.len())
    })?;
    call_status("runtime_set_toc", unsafe {
        ffi::runtime_set_toc(runtime, init.toc.as_ptr(), init.toc.len())
    })?;
    if let Some(dicts) = &init.dicts {
        call_status("runtime_set_dicts", unsafe {
            ffi::runtime_set_dicts(runtime, dicts.as_ptr(), dicts.len())
        })?;
    }
    Ok(())
}

pub(crate) fn apply_sql(plan: u32, sql: &str) -> NativeResult<()> {
    let code = unsafe { ffi::plan_apply_sql(plan, sql.as_ptr(), sql.len()) };
    if code < 0 {
        return Err(NativeError::Status("plan_apply_sql", code));
    }
    Ok(())
}

pub(crate) fn required_pages(
    runtime: u32,
    plan: u32,
    header: HeaderInfo,
    chunk_id: u32,
    index_bytes: &[u8],
    has_filters: bool,
) -> NativeResult<RequiredPages> {
    let entry_bytes = INDEX_ENTRY_BYTES;

    let raw_len = header.ncols as usize * entry_bytes;
    let mut out_len = 4096usize;

    loop {
        let mut out = vec![0u32; out_len / 4];
        let count = unsafe {
            ffi::plan_required_pages(
                runtime,
                plan,
                chunk_id,
                index_bytes.as_ptr(),
                index_bytes.len(),
                raw_len,
                out.as_mut_ptr(),
                out_len,
            )
        };

        if count < 0 {
            let needed = (-count) as usize;
            if needed <= out_len {
                return Err(NativeError::Status("plan_required_pages", count as i32));
            }
            out_len = needed;
            continue;
        }

        if count == 0 {
            if has_filters {
                return Ok(RequiredPages::Skip);
            }
            return Ok(RequiredPages::Requests(Vec::new()));
        }

        let count = count as usize;
        let mut requests = Vec::with_capacity(count);
        for idx in 0..count {
            let base = idx * PAGE_REQ_WORDS;
            let offset_lo = out[base + 2] as u64;
            let offset_hi = out[base + 3] as u64;
            requests.push(PageRequest {
                kind: out[base],
                col_id: out[base + 1],
                offset: (offset_hi << 32) | offset_lo,
                comp_len: out[base + 4],
                raw_len: out[base + 5],
            });
        }
        return Ok(RequiredPages::Requests(requests));
    }
}

pub(crate) fn exec_chunk_payload(
    runtime: u32,
    plan: u32,
    chunk_id: u32,
    file: &File,
    read_cache: &ReadCache,
    requests: &[PageRequest],
) -> NativeResult<()> {
    if requests.is_empty() {
        let empty_descs: [u32; 0] = [];
        let empty_data: [u8; 0] = [];
        return call_status("plan_exec_chunk", unsafe {
            ffi::plan_exec_chunk(
                runtime,
                plan,
                chunk_id,
                empty_descs.as_ptr(),
                0,
                empty_data.as_ptr(),
                0,
            )
        });
    }

    let (descs, data) = read_and_pack_pages(file, read_cache, requests)?;
    call_status("plan_exec_chunk", unsafe {
        ffi::plan_exec_chunk(
            runtime,
            plan,
            chunk_id,
            descs.as_ptr(),
            descs.len(),
            data.as_ptr(),
            data.len(),
        )
    })
}

pub(crate) fn read_and_pack_pages(
    file: &File,
    read_cache: &ReadCache,
    requests: &[PageRequest],
) -> NativeResult<(Vec<u32>, Vec<u8>)> {
    let total = requests
        .iter()
        .map(|req| req.comp_len as usize)
        .sum::<usize>();
    let mut descs = vec![0u32; requests.len() * 5];
    let mut data = vec![0u8; total];

    let mut data_offset = 0usize;
    for (idx, req) in requests.iter().enumerate() {
        let bytes = read_cache.read_exact(file, req.offset, req.comp_len as usize)?;
        data[data_offset..data_offset + bytes.len()].copy_from_slice(bytes.as_ref());

        let base = idx * 5;
        descs[base] = req.kind;
        descs[base + 1] = req.col_id;
        descs[base + 2] = data_offset as u32;
        descs[base + 3] = req.comp_len;
        descs[base + 4] = req.raw_len;

        data_offset += bytes.len();
    }

    Ok((descs, data))
}

pub(crate) fn copy_rows_bytes(plan: u32) -> NativeResult<Vec<u8>> {
    let count = checked_count("plan_rows_len", unsafe { ffi::plan_rows_len(plan) })?;
    if count == 0 {
        return Ok(Vec::new());
    }
    read_out_bytes(count * 8, |ptr, len| unsafe {
        ffi::plan_copy_rows(plan, ptr, len)
    })
}

pub(crate) fn copy_row_candidates_bytes(plan: u32) -> NativeResult<Vec<u8>> {
    read_out_bytes(256, |ptr, len| unsafe {
        ffi::plan_copy_row_candidates(plan, ptr, len)
    })
}

pub(crate) fn copy_aggs_bytes(plan: u32) -> NativeResult<Vec<u8>> {
    let count = checked_count("plan_agg_count", unsafe { ffi::plan_agg_count(plan) })?;
    if count == 0 {
        return Ok(Vec::new());
    }
    read_out_bytes(count * AGG_RECORD_SIZE, |ptr, len| unsafe {
        ffi::plan_copy_aggs(plan, ptr, len)
    })
}

pub(crate) fn copy_group_hist_partial_bytes(plan: u32) -> NativeResult<Vec<u8>> {
    let dict_len = checked_count("plan_group_dict_hist_dict_len", unsafe {
        ffi::plan_group_dict_hist_dict_len(plan)
    })?;
    if dict_len == 0 {
        return Ok(Vec::new());
    }
    let need = 12usize + dict_len * 4;
    read_out_bytes(need, |ptr, len| unsafe {
        ffi::plan_copy_group_hist_partial(plan, ptr, len)
    })
}

pub(crate) fn copy_groups_bytes(plan: u32) -> NativeResult<Vec<u8>> {
    let count = checked_count("plan_group_count", unsafe { ffi::plan_group_count(plan) })?;
    if count == 0 {
        return Ok(Vec::new());
    }
    let agg_count = checked_count("plan_group_agg_count", unsafe {
        ffi::plan_group_agg_count(plan)
    })?;
    let record_size = 16 + agg_count * GROUP_AGG_RECORD_SIZE;
    read_out_bytes(count * record_size, |ptr, len| unsafe {
        ffi::plan_copy_groups(plan, ptr, len)
    })
}

pub(crate) fn copy_groups_bytes_with_keys(plan: u32) -> NativeResult<Vec<u8>> {
    let count = checked_count("plan_group_count", unsafe { ffi::plan_group_count(plan) })?;
    if count == 0 {
        return Ok(Vec::new());
    }
    let agg_count = checked_count("plan_group_agg_count", unsafe {
        ffi::plan_group_agg_count(plan)
    })?;
    let min_record_size = 24 + agg_count * GROUP_AGG_RECORD_SIZE;
    read_out_bytes(count * min_record_size.max(64), |ptr, len| unsafe {
        ffi::plan_copy_groups_with_keys(plan, ptr, len)
    })
}
