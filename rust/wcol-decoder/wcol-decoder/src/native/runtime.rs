use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Once;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::constants::{
    AGG_KIND_APPROX_DISTINCT, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR, FLAG_DICT, HEADER_BYTES,
    ROWS_PER_CHUNK, TOC_ENTRY_BYTES, TYPE_STRING, WCOL_VERSION,
};
use crate::ffi;

use super::cache::{ReadCache, ReadIoStats};
use super::config::{GroupEngineMode, MergeKeysMode, NativeRuntimeConfig, QueryExecutionConfig};
use super::error::{NativeError, NativeResult};
use super::exec::{apply_sql, exec_chunk_payload, required_pages};
use super::helpers::{
    call_status, checked_count, decompress_lz4, parse_header_info, read_exact_at_file,
    read_out_bytes, read_u32, read_u64,
};
use super::mem_budget::{detect_memory_basis, MemoryBasis, QueryMemoryBudget};
use super::pool::{ExecutePlan, ParallelPool};
use super::types::{GlobalDebugStats, HeaderInfo, PlanGuard, RuntimeInit, WorkerDebugStats};
use super::HEADER_FLAG_DICT_COMPRESSED;


pub struct NativeRuntime {
    pub(crate) file: Arc<File>,
    pub(crate) read_cache: Arc<ReadCache>,
    pub(crate) runtime: u32,
    pub(crate) header: HeaderInfo,
    pub(crate) init: RuntimeInit,
    runtime_config: NativeRuntimeConfig,
    memory_basis: MemoryBasis,
    query_seq: AtomicU64,
    pool: Mutex<Option<ParallelPool>>,
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        unsafe {
            ffi::destroy_runtime(self.runtime);
        }
    }
}

impl NativeRuntime {
    pub fn open(path: impl AsRef<Path>) -> NativeResult<Self> {
        let file = Arc::new(File::open(path)?);
        let runtime = unsafe { ffi::create_runtime() };
        if runtime == 0 {
            return Err(NativeError::Invalid("create_runtime returned 0"));
        }
        let runtime_config = NativeRuntimeConfig::from_env();
        let memory_basis = detect_memory_basis();

        let mut out = Self {
            file,
            read_cache: Arc::new(ReadCache::from_env()),
            runtime,
            header: HeaderInfo {
                version: 0,
                flags: 0,
                ncols: 0,
                nchunks: 0,
                rows_per_chunk: ROWS_PER_CHUNK as u32,
                total_rows: 0,
                schema_off: 0,
                schema_len: 0,
                index_off: 0,
                index_len: 0,
                dict_off: 0,
                dict_len: 0,
                data_off: 0,
                dict_raw_len: 0,
            },
            init: RuntimeInit {
                header: Vec::new(),
                schema: Vec::new(),
                toc: Vec::new(),
                dicts: None,
            },
            runtime_config,
            memory_basis,
            query_seq: AtomicU64::new(0),
            pool: Mutex::new(None),
        };

        if let Err(err) = out.init_runtime() {
            unsafe {
                ffi::destroy_runtime(runtime);
            }
            return Err(err);
        }

        if emit_mem_budget_logs() {
            let cfg = out.runtime_config.for_workers(1, out.memory_basis);
            eprintln!(
                "WCOL_MEM basis_source={} basis_bytes={} global_cap_bytes={} retained_global_cap_bytes={} arena_base_bytes={} arena_grow_bytes={} arena_max_bytes={}",
                cfg.memory_basis.source,
                cfg.memory_basis.total_bytes,
                cfg.global_cap_bytes,
                cfg.retained_global_cap_bytes,
                cfg.arena_base_bytes,
                cfg.arena_grow_bytes,
                cfg.arena_max_bytes
            );
        }

        Ok(out)
    }

    pub fn header(&self) -> HeaderInfo {
        self.header
    }

    pub fn read_io_stats(&self) -> ReadIoStats {
        self.read_cache.stats()
    }


    pub fn worker_debug_stats(&self) -> NativeResult<Vec<WorkerDebugStats>> {
        let mut guard = self
            .pool
            .lock()
            .map_err(|_| NativeError::Invalid("parallel pool mutex poisoned"))?;
        match guard.as_mut() {
            Some(pool) => pool.debug_stats(),
            None => Ok(Vec::new()),
        }
    }

    pub fn global_debug_stats(&self) -> NativeResult<GlobalDebugStats> {
        let plan = read_out_bytes(48, |ptr, len| unsafe { ffi::plan_global_stats(ptr, len) })?;
        let runtime = read_out_bytes(40, |ptr, len| unsafe {
            ffi::runtime_global_stats(ptr, len)
        })?;
        let locks = read_out_bytes(32, |ptr, len| unsafe { ffi::ffi_lock_stats(ptr, len) })?;
        Ok(GlobalDebugStats {
            plan_count: read_u64(&plan, 0),
            plan_rows_cap_total: read_u64(&plan, 8),
            plan_group_state_cap_total: read_u64(&plan, 16),
            plan_group_keys_cap_total: read_u64(&plan, 24),
            plan_row_heap_cap_total: read_u64(&plan, 32),
            plan_row_order_ranks_cap_total: read_u64(&plan, 40),
            runtime_count: read_u64(&runtime, 0),
            runtime_index_chunks_total: read_u64(&runtime, 8),
            runtime_index_entries_len_total: read_u64(&runtime, 16),
            runtime_index_entries_cap_total: read_u64(&runtime, 24),
            runtime_index_bytes_est_total: read_u64(&runtime, 32),
            ffi_plan_lock_count: read_u64(&locks, 0),
            ffi_plan_lock_wait_ns: read_u64(&locks, 8),
            ffi_runtime_lock_count: read_u64(&locks, 16),
            ffi_runtime_lock_wait_ns: read_u64(&locks, 24),
        })
    }

    pub fn query_sql(&self, sql: &str) -> NativeResult<super::types::QueryResult> {
        self.query_sql_with_workers(sql, 1)
    }

    pub fn query_sql_with_workers(
        &self,
        sql: &str,
        workers: usize,
    ) -> NativeResult<super::types::QueryResult> {
        maybe_configure_malloc_reclaim();
        let requested = workers.max(1);
        let max_workers = (self.header.nchunks as usize).max(1);
        let worker_count = requested.min(max_workers);

        if worker_count == 1 || self.header.nchunks <= 1 || has_parallel_unsafe_sql(sql) {
            return self.query_sql_single(sql);
        }

        self.query_sql_parallel(sql, worker_count)
    }

    fn query_sql_single(&self, sql: &str) -> NativeResult<super::types::QueryResult> {
        let result = {
            let plan = unsafe { ffi::create_plan(self.runtime) };
            if plan == 0 {
                return Err(NativeError::Invalid("create_plan returned 0"));
            }
            let _guard = PlanGuard { handle: plan };

            call_status("plan_reset_results", unsafe {
                ffi::plan_reset_results(plan)
            })?;
            apply_sql(plan, sql)?;
            self.execute_plan(plan)?;
            self.read_result(plan)?
        };
        if auto_trim_enabled() {
            self.trim_allocators();
        }
        Ok(result)
    }

    fn query_sql_parallel(
        &self,
        sql: &str,
        workers: usize,
    ) -> NativeResult<super::types::QueryResult> {
        let emit_phase = std::env::var("WCOL_QUERY_PHASE_STATS")
            .map(|v| v != "0")
            .unwrap_or(false);
        let emit_stage_timing = std::env::var("WCOL_QUERY_STAGE_TIMING")
            .map(|v| v != "0")
            .unwrap_or(false);
        let emit_stage_attribution = std::env::var("WCOL_STAGE_ATTRIBUTION")
            .map(|v| v != "0")
            .unwrap_or(false);
        let collect_plan_timing = cfg!(feature = "timing")
            && (emit_stage_attribution
                || std::env::var("WCOL_BENCH_WORKER_STATS")
                    .map(|v| v != "0")
                    .unwrap_or(false));
        let total_started = Instant::now();
        let mut trim_ms = 0.0f64;
        let query_cfg = self.runtime_config.for_workers(workers, self.memory_basis);
        let query_id = self.query_seq.fetch_add(1, Ordering::Relaxed) + 1;
        if emit_stage_timing || emit_mem_budget_logs() {
            eprintln!(
                "WCOL_QUERY_CFG query_id={} workers={} group_engine={:?} partition_count={} group_partitions={} merge_workers={} reduce_workers={} merge_keys={:?} string_window_bytes={} scan_partition_queue_cap_bytes={} partition_sort_chunk_bytes={} scan_chunk_batch_size={} global_cap_bytes={} retained_global_cap_bytes={} release_policy={:?} cache_counters={:?}",
                query_id,
                workers,
                query_cfg.group_engine_mode,
                query_cfg.partition_count,
                query_cfg.group_partitions,
                query_cfg.merge_workers,
                query_cfg.reduce_workers,
                query_cfg.merge_keys_mode,
                query_cfg.string_window_bytes,
                query_cfg.scan_partition_queue_cap_bytes.unwrap_or(0),
                query_cfg.partition_sort_chunk_bytes,
                query_cfg.scan_chunk_batch_size,
                query_cfg.global_cap_bytes,
                query_cfg.retained_global_cap_bytes,
                query_cfg.arena_release_policy,
                query_cfg.cache_counter_mode,
            );
        }
        let (
            result,
            apply_sql_ms,
            prepare_workers_ms,
            execute_workers_ms,
            sort_partials_ms,
            merge_partials_ms,
            reducer_finalize_ms,
            read_result_ms,
            partial_count,
        ) = {
            let apply_sql_ms: f64;
            let mut prepare_workers_ms = 0.0f64;
            let mut execute_workers_ms = 0.0f64;
            let sort_partials_ms: f64;
            let merge_partials_ms: f64;
            let reducer_finalize_ms: f64;
            let read_result_ms: f64;
            let partial_count: usize;
            let base_plan = unsafe { ffi::create_plan(self.runtime) };
            if base_plan == 0 {
                return Err(NativeError::Invalid("create_plan returned 0"));
            }
            let _base_guard = PlanGuard { handle: base_plan };
            call_status("plan_reset_results", unsafe {
                ffi::plan_reset_results(base_plan)
            })?;
            let apply_started = Instant::now();
            apply_sql(base_plan, sql)?;
            apply_sql_ms = apply_started.elapsed().as_secs_f64() * 1000.0;

            let workers = cap_group_query_workers(workers, base_plan)?;
            let query_cfg = self.runtime_config.for_workers(workers, self.memory_basis);

            let has_filters = checked_count("plan_filters_len", unsafe {
                ffi::plan_filters_len(base_plan)
            })? > 0;
            let group_agg_count = checked_count("plan_group_agg_count", unsafe {
                ffi::plan_group_agg_count(base_plan)
            })? as usize;
            let group_order_by_count = checked_count("plan_group_order_by_count", unsafe {
                ffi::plan_group_order_by_count(base_plan)
            })? > 0;
            let group_limit = checked_count("plan_limit", unsafe { ffi::plan_limit(base_plan) })?;
            let topk_count_agg = if group_order_by_count && group_limit > 0 && group_agg_count > 0 {
                let agg_bytes = read_out_bytes(group_agg_count * 8, |ptr, len| unsafe {
                    ffi::plan_copy_group_aggs(base_plan, ptr, len)
                })?;
                let mut found = None;
                for idx in 0..group_agg_count {
                    if 8 * (idx + 1) > agg_bytes.len() {
                        break;
                    }
                    let kind = agg_bytes[idx * 8 + 4];
                    if kind == AGG_KIND_COUNT || kind == AGG_KIND_COUNT_STAR {
                        found = Some((group_limit, idx));
                        break;
                    }
                }
                found
            } else {
                None
            };
            let uses_group_hist = checked_count("plan_group_dict_hist_active", unsafe {
                ffi::plan_group_dict_hist_active(base_plan)
            })? > 0;
            let effective_group_engine = if uses_group_hist {
                GroupEngineMode::Legacy
            } else {
                resolve_effective_group_engine(
                    query_cfg.group_engine_mode,
                    base_plan,
                    self.runtime,
                    query_cfg.merge_keys_mode,
                )?
            };
            let plan_offset =
                checked_count("plan_offset", unsafe { ffi::plan_offset(base_plan) })? as u32;

            let reducer = unsafe { ffi::plan_reducer_new(base_plan) };
            if reducer == 0 {
                return Err(NativeError::Invalid("plan_reducer_new returned 0"));
            }
            let _reducer_guard = PlanGuard { handle: reducer };
            let heavy_string = is_heavy_string_sql(sql);
            let mut partials = self.with_pool(workers, &query_cfg, |pool| {
                let prepare_started = Instant::now();
                pool.prepare(sql)?;
                prepare_workers_ms = prepare_started.elapsed().as_secs_f64() * 1000.0;
                let budget = Arc::new(QueryMemoryBudget::new(
                    query_cfg.global_cap_bytes,
                    query_id,
                    pool.retained_bytes(),
                ));
                let execute_started = Instant::now();
                let partials = pool.execute(
                    self.header.nchunks,
                    ExecutePlan {
                        has_filters,
                        worker_count: workers,
                        scan_chunk_batch_size: query_cfg.scan_chunk_batch_size,
                        group_engine_mode: effective_group_engine,
                        budget,
                        heavy_string,
                        string_window_bytes: query_cfg.string_window_bytes,
                        partition_sort_chunk_bytes: query_cfg.partition_sort_chunk_bytes,
                        arena_release_policy: query_cfg.arena_release_policy,
                        arena_keep_up_to_bytes: query_cfg.arena_keep_up_to_bytes,
                        retained_idle_decay_queries: query_cfg.retained_idle_decay_queries,
                        retained_global_cap_bytes: query_cfg.retained_global_cap_bytes,
                        merge_keys_mode: query_cfg.merge_keys_mode,
                        collect_plan_timing,
                        group_agg_count,
                        group_partition_count: query_cfg.partition_count,
                        partition_groups_during_scan: group_agg_count > 0
                            && matches!(
                                effective_group_engine,
                                GroupEngineMode::PartitionSort
                                    | GroupEngineMode::PartitionSortV2
                                    | GroupEngineMode::PartitionDirect
                            ),
                    },
                )?;
                execute_workers_ms = execute_started.elapsed().as_secs_f64() * 1000.0;
                if emit_phase {
                    let stats = pool.debug_stats()?;
                    log_worker_phase_stats("after_pool_execute", &stats);
                }
                Ok(partials)
            })?;
            if emit_phase {
                log_plan_phase_stats("after_pool_execute", reducer)?;
            }
            partial_count = partials.len();

            let sort_started = Instant::now();
            partials.sort_by_key(|partial| partial.chunk_id);
            sort_partials_ms = sort_started.elapsed().as_secs_f64() * 1000.0;

            let merged_groups = if group_agg_count > 0 {
                Some(if uses_group_hist && partials_use_group_hist(&partials) {
                    merge_group_hist_partials(
                        base_plan,
                        &mut partials,
                        group_order_by_count,
                        group_limit as u32,
                        plan_offset,
                    )?
                } else {
                    match effective_group_engine {
                    GroupEngineMode::Legacy => merge_group_partials_partitioned_legacy(
                        &mut partials,
                        group_agg_count,
                        query_cfg.group_partitions,
                        query_cfg.merge_workers,
                        query_cfg.merge_keys_mode,
                        query_cfg.hot_partition_threshold_records,
                    )?,
                    GroupEngineMode::PartitionSort => merge_group_partials_partition_sort(
                        &mut partials,
                        group_agg_count,
                        query_cfg.partition_count,
                        query_cfg.reduce_workers,
                        query_cfg.merge_keys_mode,
                        query_cfg.hot_partition_threshold_records,
                        topk_count_agg,
                    )?,
                    GroupEngineMode::PartitionSortV2 => merge_group_partials_partition_sort(
                        &mut partials,
                        group_agg_count,
                        query_cfg.partition_count,
                        query_cfg.reduce_workers,
                        query_cfg.merge_keys_mode,
                        query_cfg.hot_partition_threshold_records,
                        topk_count_agg,
                    )?,
                    GroupEngineMode::PartitionDirect => merge_group_partials_partition_direct(
                        &mut partials,
                        group_agg_count,
                        query_cfg.partition_count,
                        query_cfg.reduce_workers,
                        query_cfg.merge_keys_mode,
                        query_cfg.hot_partition_threshold_records,
                        topk_count_agg,
                    )?,
                }
                })
            } else {
                None
            };

            let merge_started = Instant::now();
            if let Some(groups) = merged_groups.as_ref() {
                if !groups.is_empty() {
                    call_status("plan_reducer_merge_groups", unsafe {
                        ffi::plan_reducer_merge_groups(reducer, groups.as_ptr(), groups.len())
                    })?;
                }
            }
            for partial in partials {
                if !partial.rows.is_empty() {
                    call_status("plan_reducer_merge_rows", unsafe {
                        ffi::plan_reducer_merge_rows(
                            reducer,
                            partial.rows.as_ptr(),
                            partial.rows.len(),
                        )
                    })?;
                }
                if !partial.row_candidates.is_empty() {
                    call_status("plan_reducer_merge_row_candidates", unsafe {
                        ffi::plan_reducer_merge_row_candidates(
                            reducer,
                            partial.row_candidates.as_ptr(),
                            partial.row_candidates.len(),
                        )
                    })?;
                }
                if !partial.aggs.is_empty() {
                    call_status("plan_reducer_merge_aggs", unsafe {
                        ffi::plan_reducer_merge_aggs(
                            reducer,
                            partial.aggs.as_ptr(),
                            partial.aggs.len(),
                        )
                    })?;
                }
            }
            merge_partials_ms = merge_started.elapsed().as_secs_f64() * 1000.0;
            if emit_phase {
                log_plan_phase_stats("after_merge", reducer)?;
            }

            let finalize_started = Instant::now();
            call_status("plan_reducer_finalize", unsafe {
                ffi::plan_reducer_finalize(reducer)
            })?;
            reducer_finalize_ms = finalize_started.elapsed().as_secs_f64() * 1000.0;
            if emit_phase {
                log_plan_phase_stats("after_reducer_finalize", reducer)?;
            }
            let read_started = Instant::now();
            let result = self.read_result(reducer)?;
            read_result_ms = read_started.elapsed().as_secs_f64() * 1000.0;
            if emit_phase {
                log_plan_phase_stats("after_read_result", reducer)?;
            }
            (
                result,
                apply_sql_ms,
                prepare_workers_ms,
                execute_workers_ms,
                sort_partials_ms,
                merge_partials_ms,
                reducer_finalize_ms,
                read_result_ms,
                partial_count,
            )
        };
        if auto_trim_enabled() {
            let trim_started = Instant::now();
            self.trim_allocators();
            trim_ms = trim_started.elapsed().as_secs_f64() * 1000.0;
        }
        let total_ms = total_started.elapsed().as_secs_f64() * 1000.0;
        if emit_stage_timing {
            eprintln!(
                "STAGE workers={} sql_len={} partials={} apply_sql_ms={:.2} prepare_workers_ms={:.2} execute_workers_ms={:.2} sort_partials_ms={:.2} merge_partials_ms={:.2} reducer_finalize_ms={:.2} read_result_ms={:.2} trim_ms={:.2} total_ms={:.2}",
                workers,
                sql.len(),
                partial_count,
                apply_sql_ms,
                prepare_workers_ms,
                execute_workers_ms,
                sort_partials_ms,
                merge_partials_ms,
                reducer_finalize_ms,
                read_result_ms,
                trim_ms,
                total_ms
            );
        }
        if emit_stage_attribution {
            match self.worker_debug_stats() {
                Ok(stats) => {
                    let workers_reporting = stats.len() as f64;
                    let timing_workers =
                        stats.iter().filter(|s| s.plan_timing_chunks > 0).count() as f64;
                    let timing_chunks_total: u64 = stats.iter().map(|s| s.plan_timing_chunks).sum();
                    let ms_decode_total: f64 = stats.iter().map(|s| s.plan_ms_decode).sum();
                    let ms_filters_total: f64 = stats.iter().map(|s| s.plan_ms_filters).sum();
                    let ms_aggs_total: f64 = stats.iter().map(|s| s.plan_ms_aggs).sum();
                    let ms_group_total: f64 = stats.iter().map(|s| s.plan_ms_group).sum();
                    let ms_rows_total: f64 = stats.iter().map(|s| s.plan_ms_rows).sum();
                    let plan_cpu_ms_total = ms_decode_total
                        + ms_filters_total
                        + ms_aggs_total
                        + ms_group_total
                        + ms_rows_total;
                    let divisor = if workers_reporting > 0.0 {
                        workers_reporting
                    } else {
                        1.0
                    };
                    let plan_wall_est_ms = plan_cpu_ms_total / divisor;
                    let unattributed_execute_ms = (execute_workers_ms - plan_wall_est_ms).max(0.0);
                    eprintln!(
                        "ATTRIB query_id={} workers={} workers_reporting={} timing_workers={} timing_chunks_total={} execute_workers_ms={:.2} merge_partials_ms={:.2} reducer_finalize_ms={:.2} read_result_ms={:.2} trim_ms={:.2} total_ms={:.2} plan_cpu_ms_total={:.2} plan_wall_est_ms={:.2} execute_unattributed_ms={:.2} plan_ms_decode_total={:.2} plan_ms_filters_total={:.2} plan_ms_aggs_total={:.2} plan_ms_group_total={:.2} plan_ms_rows_total={:.2}",
                        query_id,
                        workers,
                        stats.len(),
                        timing_workers as usize,
                        timing_chunks_total,
                        execute_workers_ms,
                        merge_partials_ms,
                        reducer_finalize_ms,
                        read_result_ms,
                        trim_ms,
                        total_ms,
                        plan_cpu_ms_total,
                        plan_wall_est_ms,
                        unattributed_execute_ms,
                        ms_decode_total,
                        ms_filters_total,
                        ms_aggs_total,
                        ms_group_total,
                        ms_rows_total
                    );
                }
                Err(err) => {
                    eprintln!(
                        "ATTRIB query_id={} workers={} error={}",
                        query_id, workers, err
                    );
                }
            }
        }
        Ok(result)
    }

    fn with_pool<T>(
        &self,
        workers: usize,
        query_cfg: &QueryExecutionConfig,
        f: impl FnOnce(&mut ParallelPool) -> NativeResult<T>,
    ) -> NativeResult<T> {
        let mut guard = self
            .pool
            .lock()
            .map_err(|_| NativeError::Invalid("parallel pool mutex poisoned"))?;
        let recreate_every_query = std::env::var("WCOL_POOL_RECREATE_EACH_QUERY")
            .map(|v| v != "0")
            .unwrap_or(false);
        let recreate = guard
            .as_ref()
            .map(|pool| pool.size() != workers)
            .unwrap_or(true);
        if recreate || recreate_every_query {
            *guard = Some(ParallelPool::new(
                workers,
                self.file.clone(),
                self.read_cache.clone(),
                self.header,
                self.init.clone(),
                query_cfg.arena_base_bytes,
                query_cfg.arena_grow_bytes,
                query_cfg.arena_max_bytes,
                query_cfg.cache_counter_mode,
            )?);
        }
        let pool = guard
            .as_mut()
            .ok_or(NativeError::Invalid("parallel pool unavailable"))?;
        f(pool)
    }

    fn trim_allocators(&self) {
        if let Ok(mut guard) = self.pool.lock() {
            if let Some(pool) = guard.as_mut() {
                let _ = pool.trim_workers();
            }
        }
        trim_current_thread_allocator();
    }

    fn init_runtime(&mut self) -> NativeResult<()> {
        let header_bytes = read_exact_at_file(&self.file, 0, HEADER_BYTES)?;
        call_status("runtime_set_header", unsafe {
            ffi::runtime_set_header(self.runtime, header_bytes.as_ptr(), header_bytes.len())
        })?;

        let header_info_bytes = read_out_bytes(92, |ptr, len| unsafe {
            ffi::runtime_header_info(self.runtime, ptr, len)
        })?;
        self.header = parse_header_info(&header_info_bytes)?;
        if self.header.version != WCOL_VERSION as u32 {
            return Err(NativeError::Invalid("unsupported wcol version (expected v7)"));
        }

        let schema_bytes = read_exact_at_file(
            &self.file,
            self.header.schema_off as u64,
            self.header.schema_len as usize,
        )?;
        call_status("runtime_set_schema", unsafe {
            ffi::runtime_set_schema(self.runtime, schema_bytes.as_ptr(), schema_bytes.len())
        })?;

        let toc_bytes = read_exact_at_file(
            &self.file,
            self.header.index_off,
            self.header.nchunks as usize * TOC_ENTRY_BYTES,
        )?;
        call_status("runtime_set_toc", unsafe {
            ffi::runtime_set_toc(self.runtime, toc_bytes.as_ptr(), toc_bytes.len())
        })?;

        let mut dict_bytes = if self.header.dict_len > 0 {
            Some(read_exact_at_file(
                &self.file,
                self.header.dict_off,
                self.header.dict_len as usize,
            )?)
        } else {
            None
        };

        if let Some(bytes) = dict_bytes.as_mut() {
            if (self.header.flags & HEADER_FLAG_DICT_COMPRESSED) != 0 {
                *bytes = decompress_lz4(bytes, self.header.dict_raw_len as usize)?;
            }
            call_status("runtime_set_dicts", unsafe {
                ffi::runtime_set_dicts(self.runtime, bytes.as_ptr(), bytes.len())
            })?;
        }

        self.init = RuntimeInit {
            header: header_bytes,
            schema: schema_bytes,
            toc: toc_bytes,
            dicts: dict_bytes,
        };

        Ok(())
    }

    fn execute_plan(&self, plan: u32) -> NativeResult<()> {
        let has_filters =
            checked_count("plan_filters_len", unsafe { ffi::plan_filters_len(plan) })? > 0;

        for chunk_id in 0..self.header.nchunks {
            let span = read_out_bytes(12, |ptr, len| unsafe {
                ffi::runtime_chunk_index_span(self.runtime, chunk_id, ptr, len)
            })?;
            if span.len() != 12 {
                return Err(NativeError::Invalid(
                    "runtime_chunk_index_span returned invalid size",
                ));
            }
            let index_offset = read_u64(&span, 0);
            let index_comp_len = read_u32(&span, 8);
            let index_bytes =
                self.read_cache
                    .read_exact(&self.file, index_offset, index_comp_len as usize)?;

            match required_pages(
                self.runtime,
                plan,
                self.header,
                chunk_id,
                index_bytes.as_ref(),
                has_filters,
            )? {
                super::types::RequiredPages::Skip => continue,
                super::types::RequiredPages::Requests(requests) => {
                    exec_chunk_payload(
                        self.runtime,
                        plan,
                        chunk_id,
                        &self.file,
                        &self.read_cache,
                        &requests,
                    )?;
                }
            }
        }

        call_status("plan_finalize_rows", unsafe {
            ffi::plan_finalize_rows(plan)
        })?;
        Ok(())
    }
}

fn log_worker_phase_stats(stage: &str, stats: &[WorkerDebugStats]) {
    let index_bytes_est_total: u64 = stats.iter().map(|s| s.runtime_index_bytes_est).sum();
    let group_state_cap_total: u64 = stats.iter().map(|s| s.plan_group_state_cap).sum();
    eprintln!(
        "PHASE stage={} workers={} worker_index_bytes_est_total={} worker_group_state_cap_total={}",
        stage,
        stats.len(),
        index_bytes_est_total,
        group_state_cap_total
    );
}

fn log_plan_phase_stats(stage: &str, plan_handle: u32) -> NativeResult<()> {
    let plan_global = read_out_bytes(48, |ptr, len| unsafe { ffi::plan_global_stats(ptr, len) })?;
    let runtime_global = read_out_bytes(40, |ptr, len| unsafe {
        ffi::runtime_global_stats(ptr, len)
    })?;
    let deep = read_out_bytes(112, |ptr, len| unsafe {
        ffi::plan_memory_deep_stats(plan_handle, ptr, len)
    })?;
    eprintln!(
        "PHASE stage={} plans={} runtimes={} runtime_index_bytes_est_total={} reducer_group_state_len={} reducer_group_state_cap={} reducer_group_aggs_vec_cap_total={} reducer_distinct_set_cap_total={} reducer_group_keys_cap={} reducer_row_heap_cap={} reducer_row_order_rank_vec_cap_total={} reducer_rows_cap={}",
        stage,
        read_u64(&plan_global, 0),
        read_u64(&runtime_global, 0),
        read_u64(&runtime_global, 32),
        read_u64(&deep, 0),
        read_u64(&deep, 8),
        read_u64(&deep, 24),
        read_u64(&deep, 48),
        read_u64(&deep, 64),
        read_u64(&deep, 80),
        read_u64(&deep, 96),
        read_u64(&deep, 104),
    );
    Ok(())
}

fn has_parallel_unsafe_sql(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains("approx_count_distinct(") || lower.contains("count(distinct")
}

fn is_heavy_string_sql(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    (lower.contains("group by") || lower.contains("order by"))
        && (lower.contains("url")
            || lower.contains("title")
            || lower.contains("searchphrase")
            || lower.contains("referer"))
}

fn auto_trim_enabled() -> bool {
    std::env::var("WCOL_AUTO_TRIM")
        .map(|v| v != "0")
        .unwrap_or(true)
}

fn emit_mem_budget_logs() -> bool {
    std::env::var("WCOL_QUERY_MEM_LOG")
        .map(|v| v != "0")
        .unwrap_or(true)
}

fn emit_partition_stats_logs() -> bool {
    std::env::var("WCOL_GROUP_PARTITION_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
}

fn cap_group_query_workers(requested: usize, plan: u32) -> NativeResult<usize> {
    let hist_active = checked_count("plan_group_dict_hist_active", unsafe {
        ffi::plan_group_dict_hist_active(plan)
    })? > 0;
    if !hist_active {
        return Ok(requested.max(1));
    }
    let cap = std::env::var("WCOL_GROUP_WORKER_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
        .max(1);
    Ok(requested.min(cap))
}

fn partials_use_group_hist(partials: &[super::types::ChunkPartial]) -> bool {
    partials
        .iter()
        .any(|p| crate::query::group_dict_hist::is_group_hist_partial(&p.groups))
}

fn merge_group_hist_partials(
    base_plan: u32,
    partials: &mut [super::types::ChunkPartial],
    order_by_count: bool,
    limit: u32,
    offset: u32,
) -> NativeResult<Vec<u8>> {
    use crate::query::group_dict_hist::{
        decode_group_hist_partial, group_hist_to_records, merge_group_hist_counts,
        merge_group_hist_sums,
    };

    let group_aggs = {
        let plans = crate::ffi::PLANS.lock().unwrap();
        plans
            .get(&base_plan)
            .map(|p| p.group_aggs.clone())
            .unwrap_or_default()
    };

    let mut merged_counts: Option<Vec<u32>> = None;
    let mut merged_sums: Option<Vec<f64>> = None;
    let mut dict_len = 0u32;
    for partial in partials.iter_mut() {
        if partial.groups.is_empty() {
            continue;
        }
        if !crate::query::group_dict_hist::is_group_hist_partial(&partial.groups) {
            return Err(NativeError::Invalid(
                "mixed group histogram and hash partials",
            ));
        }
        let (dl, counts, sums) = decode_group_hist_partial(&partial.groups)
            .ok_or_else(|| NativeError::Invalid("invalid group histogram partial"))?;
        if dict_len == 0 {
            dict_len = dl;
            let n = dl as usize + 1;
            merged_counts = Some(vec![0u32; n]);
            if sums.is_some() {
                merged_sums = Some(vec![0.0f64; n]);
            }
        } else if dl != dict_len {
            return Err(NativeError::Invalid("group histogram dict_len mismatch"));
        }
        if let Some(target) = merged_counts.as_mut() {
            merge_group_hist_counts(target, &counts);
        }
        if let (Some(target), Some(source)) = (merged_sums.as_mut(), sums.as_ref()) {
            merge_group_hist_sums(target, source);
        }
        partial.groups.clear();
    }
    let merged = merged_counts.unwrap_or_default();
    Ok(group_hist_to_records(
        &merged,
        merged_sums.as_deref(),
        dict_len,
        &group_aggs,
        order_by_count,
        limit,
        offset,
    ))
}

fn group_engine_v2_min_cardinality() -> u64 {
    std::env::var("WCOL_GROUP_ENGINE_V2_MIN_CARDINALITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000)
}

fn group_engine_v2_force() -> bool {
    std::env::var("WCOL_GROUP_ENGINE_V2_FORCE")
        .map(|v| v != "0")
        .unwrap_or(false)
}

fn estimate_group_cardinality(runtime_handle: u32, plan: u32) -> NativeResult<u64> {
    let key_count = checked_count("plan_group_key_count", unsafe {
        ffi::plan_group_key_count(plan)
    })? as usize;
    if key_count == 0 {
        return Ok(0);
    }
    const KEY_INFO_BYTES: usize = 8;
    let info = read_out_bytes(key_count * KEY_INFO_BYTES, |ptr, len| unsafe {
        ffi::plan_group_key_info(plan, ptr, len) as isize
    })?;
    if info.len() < key_count * KEY_INFO_BYTES {
        return Ok(u64::MAX);
    }

    let runtimes = ffi::lock_runtimes_timed();
    let runtime = runtimes
        .get(&runtime_handle)
        .ok_or(NativeError::Invalid("runtime handle missing"))?;

    let mut est = 1u64;
    for chunk in info.chunks_exact(KEY_INFO_BYTES) {
        let col_id = read_u32(chunk, 0) as usize;
        let flags = chunk[5];
        let card = match runtime.schema.get(col_id) {
            Some(col) if (flags & FLAG_DICT) != 0 || col.logical_type == TYPE_STRING => {
                runtime
                    .dicts
                    .get(&col.dict_id)
                    .map(|dict| dict.len() as u64)
                    .unwrap_or(65_536)
            }
            _ => 1,
        };
        est = est.saturating_mul(card.max(1));
    }
    Ok(est)
}

fn resolve_effective_group_engine(
    requested: GroupEngineMode,
    plan: u32,
    runtime_handle: u32,
    merge_keys_mode: MergeKeysMode,
) -> NativeResult<GroupEngineMode> {
    match requested {
        GroupEngineMode::Legacy | GroupEngineMode::PartitionSort | GroupEngineMode::PartitionDirect => {
            Ok(requested)
        }
        GroupEngineMode::PartitionSortV2 => {
            if !supports_partition_sort_v2(plan, merge_keys_mode)? {
                if emit_partition_stats_logs() {
                    eprintln!(
                        "WCOL_GROUP_ENGINE_FALLBACK requested=PartitionSortV2 effective=PartitionSort reason=unsupported-query-shape"
                    );
                }
                return Ok(GroupEngineMode::PartitionSort);
            }
            if group_engine_v2_force() {
                return Ok(GroupEngineMode::PartitionSortV2);
            }
            let est = estimate_group_cardinality(runtime_handle, plan)?;
            let min_card = group_engine_v2_min_cardinality();
            if est >= min_card {
                Ok(GroupEngineMode::PartitionSortV2)
            } else {
                if emit_partition_stats_logs() {
                    eprintln!(
                        "WCOL_GROUP_ENGINE_FALLBACK requested=PartitionSortV2 effective=Legacy estimated_cardinality={est} min_for_v2={min_card}"
                    );
                }
                Ok(GroupEngineMode::Legacy)
            }
        }
    }
}

fn supports_partition_sort_v2(plan: u32, merge_keys_mode: MergeKeysMode) -> NativeResult<bool> {
    if merge_keys_mode != MergeKeysMode::Bytes {
        return Ok(false);
    }
    let key_count = checked_count("plan_group_key_count", unsafe {
        ffi::plan_group_key_count(plan)
    })?;
    if key_count != 1 {
        return Ok(false);
    }
    let agg_count = checked_count("plan_group_agg_count", unsafe {
        ffi::plan_group_agg_count(plan)
    })?;
    if agg_count == 0 {
        return Ok(false);
    }
    let aggs = read_out_bytes(agg_count * 8, |ptr, len| unsafe {
        ffi::plan_copy_group_aggs(plan, ptr, len)
    })?;
    if aggs.len() % 8 != 0 {
        return Ok(false);
    }
    for rec in aggs.chunks_exact(8) {
        let kind = rec[4];
        if kind == AGG_KIND_APPROX_DISTINCT {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug, Default)]
struct GroupAggState {
    sum: f64,
    min: f64,
    max: f64,
    count: u32,
}

fn merge_group_partials_partitioned_legacy(
    partials: &mut [super::types::ChunkPartial],
    agg_count: usize,
    partitions: usize,
    merge_workers: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
) -> NativeResult<Vec<u8>> {
    if agg_count == 0 {
        for partial in partials {
            partial.groups.clear();
        }
        return Ok(Vec::new());
    }

    let fixed_record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if fixed_record_size == 0 {
        return Err(NativeError::Invalid("invalid group record size"));
    }
    let partition_count = partitions.max(1).next_power_of_two().min(2048);
    let mut partition_buffers = vec![Vec::<u8>::new(); partition_count];
    for partial in partials.iter_mut() {
        if partial.groups.is_empty() {
            continue;
        }
        let bytes = std::mem::take(&mut partial.groups);
        match merge_keys_mode {
            MergeKeysMode::Hash => {
                if bytes.len() % fixed_record_size != 0 {
                    return Err(NativeError::Invalid(
                        "partial groups payload has invalid record size",
                    ));
                }
                for record in bytes.chunks_exact(fixed_record_size) {
                    let key_a = read_u64(record, 0);
                    let key_b = read_u64(record, 8);
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_buffers[p].extend_from_slice(record);
                }
            }
            MergeKeysMode::Bytes => {
                let mut offset = 0usize;
                while offset < bytes.len() {
                    if offset + 24 > bytes.len() {
                        return Err(NativeError::Invalid(
                            "partial groups key payload header truncated",
                        ));
                    }
                    let key_a = read_u64(&bytes, offset);
                    let key_b = read_u64(&bytes, offset + 8);
                    let key_len = read_u32(&bytes, offset + 16) as usize;
                    let payload_len = 24usize
                        .saturating_add(key_len)
                        .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
                    if offset + payload_len > bytes.len() {
                        return Err(NativeError::Invalid("partial groups key payload truncated"));
                    }
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_buffers[p].extend_from_slice(&bytes[offset..offset + payload_len]);
                    offset += payload_len;
                }
            }
        }
    }

    if emit_partition_stats_logs() {
        let mut record_counts = Vec::with_capacity(partition_count);
        for bytes in &partition_buffers {
            let count = match merge_keys_mode {
                MergeKeysMode::Hash => bytes.len() / fixed_record_size,
                MergeKeysMode::Bytes => count_group_records_with_keys(bytes, agg_count)?,
            };
            record_counts.push(count);
        }

        let non_empty = record_counts.iter().filter(|&&v| v > 0).count();
        let total_records: usize = record_counts.iter().sum();
        let max_records = record_counts.iter().copied().max().unwrap_or(0);
        let mut sorted = record_counts.clone();
        sorted.sort_unstable();
        let p50 = if sorted.is_empty() {
            0
        } else {
            sorted[sorted.len() / 2]
        };
        let p95 = if sorted.is_empty() {
            0
        } else {
            let idx = ((sorted.len() - 1) * 95) / 100;
            sorted[idx]
        };
        let avg_non_empty = if non_empty > 0 {
            total_records as f64 / non_empty as f64
        } else {
            0.0
        };
        println!(
            "PARTITIONS total={} non_empty={} total_records={} avg_non_empty={:.2} max_records={} p50_records={} p95_records={}",
            partition_count,
            non_empty,
            total_records,
            avg_non_empty,
            max_records,
            p50,
            p95
        );
    }

    let workers = merge_workers.max(1).min(partition_count);
    if workers == 1 {
        let mut merged = Vec::new();
        for bytes in partition_buffers {
            let reduced = reduce_partition_groups(
                bytes,
                agg_count,
                merge_keys_mode,
                hot_partition_threshold_records,
            )?;
            merged.extend_from_slice(&reduced);
        }
        return Ok(merged);
    }

    let shared = Arc::new(partition_buffers);
    let next = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel::<NativeResult<(usize, Vec<u8>)>>();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let shared = shared.clone();
            let next = next.clone();
            scope.spawn(move || loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= shared.len() {
                    break;
                }
                let result = reduce_partition_groups(
                    shared[idx].clone(),
                    agg_count,
                    merge_keys_mode,
                    hot_partition_threshold_records,
                )
                .map(|bytes| (idx, bytes));
                let _ = tx.send(result);
            });
        }
    });
    drop(tx);

    let mut reduced_partitions = vec![Vec::<u8>::new(); partition_count];
    let mut first_err: Option<NativeError> = None;
    for message in rx {
        match message {
            Ok((idx, bytes)) => reduced_partitions[idx] = bytes,
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }

    let mut merged = Vec::new();
    for bytes in reduced_partitions {
        merged.extend_from_slice(&bytes);
    }
    Ok(merged)
}

fn merge_group_partials_partition_sort(
    partials: &mut [super::types::ChunkPartial],
    agg_count: usize,
    partitions: usize,
    reduce_workers: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
    topk_count_agg: Option<(usize, usize)>,
) -> NativeResult<Vec<u8>> {
    if agg_count == 0 {
        for partial in partials {
            partial.groups.clear();
        }
        return Ok(Vec::new());
    }

    let fixed_record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if fixed_record_size == 0 {
        return Err(NativeError::Invalid("invalid group record size"));
    }

    let partition_count = partitions.max(1).next_power_of_two().min(4096);
    let mut partition_fragments = vec![Vec::<Vec<u8>>::new(); partition_count];
    let mut partition_spill = vec![Vec::<u8>::new(); partition_count];
    let mut bytes_emitted = 0usize;

    for partial in partials.iter_mut() {
        if !partial.partitioned_groups.is_empty() {
            if partial.partitioned_groups.len() != partition_count {
                return Err(NativeError::Invalid(
                    "partitioned_groups count does not match partition_count",
                ));
            }
            for (idx, partition_bytes) in partial.partitioned_groups.iter_mut().enumerate() {
                if partition_bytes.is_empty() {
                    continue;
                }
                bytes_emitted = bytes_emitted.saturating_add(partition_bytes.len());
                partition_fragments[idx].push(std::mem::take(partition_bytes));
            }
        }
        if partial.groups.is_empty() {
            continue;
        }
        let bytes = std::mem::take(&mut partial.groups);
        match merge_keys_mode {
            MergeKeysMode::Hash => {
                if bytes.len() % fixed_record_size != 0 {
                    return Err(NativeError::Invalid(
                        "partial groups payload has invalid record size",
                    ));
                }
                for record in bytes.chunks_exact(fixed_record_size) {
                    let key_a = read_u64(record, 0);
                    let key_b = read_u64(record, 8);
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_spill[p].extend_from_slice(record);
                    bytes_emitted = bytes_emitted.saturating_add(record.len());
                }
            }
            MergeKeysMode::Bytes => {
                let mut offset = 0usize;
                while offset < bytes.len() {
                    if offset + 24 > bytes.len() {
                        return Err(NativeError::Invalid(
                            "partial groups key payload header truncated",
                        ));
                    }
                    let key_a = read_u64(&bytes, offset);
                    let key_b = read_u64(&bytes, offset + 8);
                    let key_len = read_u32(&bytes, offset + 16) as usize;
                    let payload_len = 24usize
                        .saturating_add(key_len)
                        .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
                    if offset + payload_len > bytes.len() {
                        return Err(NativeError::Invalid("partial groups key payload truncated"));
                    }
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_spill[p].extend_from_slice(&bytes[offset..offset + payload_len]);
                    bytes_emitted = bytes_emitted.saturating_add(payload_len);
                    offset += payload_len;
                }
            }
        }
    }

    let mut record_counts = Vec::with_capacity(partition_count);
    let mut max_partition_bytes = 0usize;
    for idx in 0..partition_count {
        let spill = &partition_spill[idx];
        let fragments = &partition_fragments[idx];
        let mut count = match merge_keys_mode {
            MergeKeysMode::Hash => spill.len() / fixed_record_size,
            MergeKeysMode::Bytes => count_group_records_with_keys(spill, agg_count)?,
        };
        let mut bytes_len = spill.len();
        for bytes in fragments {
            count = count.saturating_add(match merge_keys_mode {
                MergeKeysMode::Hash => bytes.len() / fixed_record_size,
                MergeKeysMode::Bytes => count_group_records_with_keys(bytes, agg_count)?,
            });
            bytes_len = bytes_len.saturating_add(bytes.len());
        }
        record_counts.push(count);
        max_partition_bytes = max_partition_bytes.max(bytes_len);
    }
    let non_empty = record_counts.iter().filter(|&&v| v > 0).count();
    let total_records: usize = record_counts.iter().sum();
    let mut sorted_counts = record_counts.clone();
    sorted_counts.sort_unstable();
    let p50 = if sorted_counts.is_empty() {
        0
    } else {
        sorted_counts[sorted_counts.len() / 2]
    };
    let p95 = if sorted_counts.is_empty() {
        0
    } else {
        let idx = ((sorted_counts.len() - 1) * 95) / 100;
        sorted_counts[idx]
    };
    let avg_non_empty = if non_empty > 0 {
        total_records as f64 / non_empty as f64
    } else {
        0.0
    };
    if emit_partition_stats_logs() {
        println!(
            "PARTITIONS total={} non_empty={} total_records={} avg_non_empty={:.2} max_records={} p50_records={} p95_records={}",
            partition_count,
            non_empty,
            total_records,
            avg_non_empty,
            record_counts.iter().copied().max().unwrap_or(0),
            p50,
            p95
        );
        println!(
            "PARTITION_SCAN records_emitted={} bytes_emitted={} queue_peak_bytes={} queue_backpressure_count={}",
            total_records,
            bytes_emitted,
            max_partition_bytes,
            0
        );
    }

    let reduce_started = Instant::now();
    let workers = reduce_workers.max(1).min(partition_count);
    let mut reduced_partitions = vec![Vec::<u8>::new(); partition_count];
    if workers == 1 {
        for idx in 0..partition_count {
            let bytes = flatten_partition_fragments(
                std::mem::take(&mut partition_fragments[idx]),
                std::mem::take(&mut partition_spill[idx]),
            );
            reduced_partitions[idx] = reduce_partition_groups_sort(
                bytes,
                agg_count,
                merge_keys_mode,
                hot_partition_threshold_records,
            )?;
        }
    } else {
        let mut tasks = Vec::with_capacity(partition_count);
        for idx in 0..partition_count {
            tasks.push((
                idx,
                std::mem::take(&mut partition_fragments[idx]),
                std::mem::take(&mut partition_spill[idx]),
            ));
        }
        let task_iter = Arc::new(Mutex::new(tasks.into_iter()));
        let (tx, rx) = mpsc::channel::<NativeResult<(usize, Vec<u8>)>>();
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let tx = tx.clone();
                let task_iter = task_iter.clone();
                scope.spawn(move || loop {
                    let Some((idx, fragments, spill)) = ({
                        let mut guard = task_iter.lock().unwrap();
                        guard.next()
                    }) else {
                        break;
                    };
                    let bytes = flatten_partition_fragments(fragments, spill);
                    let result = reduce_partition_groups_sort(
                        bytes,
                        agg_count,
                        merge_keys_mode,
                        hot_partition_threshold_records,
                    )
                    .map(|bytes| (idx, bytes));
                    let _ = tx.send(result);
                });
            }
        });
        drop(tx);

        let mut first_err: Option<NativeError> = None;
        for message in rx {
            match message {
                Ok((idx, bytes)) => reduced_partitions[idx] = bytes,
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
    }
    let reduce_ms = reduce_started.elapsed().as_secs_f64() * 1000.0;
    if emit_partition_stats_logs() {
        println!(
            "PARTITION_REDUCE tasks={} task_p50_ms={} task_p95_ms={} sort_ms={:.2} reduce_ms={:.2}",
            partition_count, 0, 0, 0.0, reduce_ms
        );
        println!(
            "COLLISION hash_conflict_candidates={} exact_key_mismatches={} canonical_bytes_materialized={}",
            0,
            0,
            0
        );
    }

    if let Some((limit, count_agg_idx)) = topk_count_agg {
        return trim_group_records_topk_by_count(
            reduced_partitions,
            agg_count,
            count_agg_idx,
            limit,
        );
    }

    let mut merged = Vec::new();
    for bytes in reduced_partitions {
        merged.extend_from_slice(&bytes);
    }
    Ok(merged)
}

fn merge_group_partials_partition_direct(
    partials: &mut [super::types::ChunkPartial],
    agg_count: usize,
    partitions: usize,
    reduce_workers: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
    topk_count_agg: Option<(usize, usize)>,
) -> NativeResult<Vec<u8>> {
    if agg_count == 0 {
        for partial in partials {
            partial.groups.clear();
            partial.partitioned_groups.clear();
        }
        return Ok(Vec::new());
    }

    let fixed_record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if fixed_record_size == 0 {
        return Err(NativeError::Invalid("invalid group record size"));
    }

    let partition_count = partitions.max(1).next_power_of_two().min(4096);
    let mut partition_fragments = vec![Vec::<Vec<u8>>::new(); partition_count];
    let mut partition_spill = vec![Vec::<u8>::new(); partition_count];
    let mut bytes_emitted = 0usize;

    for partial in partials.iter_mut() {
        if !partial.partitioned_groups.is_empty() {
            if partial.partitioned_groups.len() != partition_count {
                return Err(NativeError::Invalid(
                    "partitioned_groups count does not match partition_count",
                ));
            }
            for (idx, partition_bytes) in partial.partitioned_groups.iter_mut().enumerate() {
                if partition_bytes.is_empty() {
                    continue;
                }
                bytes_emitted = bytes_emitted.saturating_add(partition_bytes.len());
                partition_fragments[idx].push(std::mem::take(partition_bytes));
            }
            partial.partitioned_groups.clear();
        }
        if partial.groups.is_empty() {
            continue;
        }
        let bytes = std::mem::take(&mut partial.groups);
        match merge_keys_mode {
            MergeKeysMode::Hash => {
                if bytes.len() % fixed_record_size != 0 {
                    return Err(NativeError::Invalid(
                        "partial groups payload has invalid record size",
                    ));
                }
                for record in bytes.chunks_exact(fixed_record_size) {
                    let key_a = read_u64(record, 0);
                    let key_b = read_u64(record, 8);
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_spill[p].extend_from_slice(record);
                    bytes_emitted = bytes_emitted.saturating_add(record.len());
                }
            }
            MergeKeysMode::Bytes => {
                let mut offset = 0usize;
                while offset < bytes.len() {
                    if offset + 24 > bytes.len() {
                        return Err(NativeError::Invalid(
                            "partial groups key payload header truncated",
                        ));
                    }
                    let key_a = read_u64(&bytes, offset);
                    let key_b = read_u64(&bytes, offset + 8);
                    let key_len = read_u32(&bytes, offset + 16) as usize;
                    let payload_len = 24usize
                        .saturating_add(key_len)
                        .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
                    if offset + payload_len > bytes.len() {
                        return Err(NativeError::Invalid("partial groups key payload truncated"));
                    }
                    let p = group_partition_for_keys(key_a, key_b, partition_count);
                    partition_spill[p].extend_from_slice(&bytes[offset..offset + payload_len]);
                    bytes_emitted = bytes_emitted.saturating_add(payload_len);
                    offset += payload_len;
                }
            }
        }
    }

    let mut record_counts = Vec::with_capacity(partition_count);
    let mut max_partition_bytes = 0usize;
    for idx in 0..partition_count {
        let spill = &partition_spill[idx];
        let fragments = &partition_fragments[idx];
        let mut count = match merge_keys_mode {
            MergeKeysMode::Hash => spill.len() / fixed_record_size,
            MergeKeysMode::Bytes => count_group_records_with_keys(spill, agg_count)?,
        };
        let mut bytes_len = spill.len();
        for bytes in fragments {
            count = count.saturating_add(match merge_keys_mode {
                MergeKeysMode::Hash => bytes.len() / fixed_record_size,
                MergeKeysMode::Bytes => count_group_records_with_keys(bytes, agg_count)?,
            });
            bytes_len = bytes_len.saturating_add(bytes.len());
        }
        record_counts.push(count);
        max_partition_bytes = max_partition_bytes.max(bytes_len);
    }
    let non_empty = record_counts.iter().filter(|&&v| v > 0).count();
    let total_records: usize = record_counts.iter().sum();
    let mut sorted_counts = record_counts.clone();
    sorted_counts.sort_unstable();
    let p50 = if sorted_counts.is_empty() {
        0
    } else {
        sorted_counts[sorted_counts.len() / 2]
    };
    let p95 = if sorted_counts.is_empty() {
        0
    } else {
        let idx = ((sorted_counts.len() - 1) * 95) / 100;
        sorted_counts[idx]
    };
    let avg_non_empty = if non_empty > 0 {
        total_records as f64 / non_empty as f64
    } else {
        0.0
    };
    if emit_partition_stats_logs() {
        println!(
            "PARTITIONS total={} non_empty={} total_records={} avg_non_empty={:.2} max_records={} p50_records={} p95_records={}",
            partition_count,
            non_empty,
            total_records,
            avg_non_empty,
            record_counts.iter().copied().max().unwrap_or(0),
            p50,
            p95
        );
        println!(
            "PARTITION_SCAN records_emitted={} bytes_emitted={} queue_peak_bytes={} queue_backpressure_count={}",
            total_records,
            bytes_emitted,
            max_partition_bytes,
            0
        );
    }

    let reduce_started = Instant::now();
    let workers = reduce_workers.max(1).min(partition_count);
    let mut reduced_partitions = vec![Vec::<u8>::new(); partition_count];
    if workers == 1 {
        for idx in 0..partition_count {
            reduced_partitions[idx] = reduce_partition_group_fragments_direct(
                std::mem::take(&mut partition_fragments[idx]),
                std::mem::take(&mut partition_spill[idx]),
                agg_count,
                merge_keys_mode,
                hot_partition_threshold_records,
            )?;
        }
    } else {
        let mut tasks = Vec::with_capacity(partition_count);
        for idx in 0..partition_count {
            tasks.push((
                idx,
                std::mem::take(&mut partition_fragments[idx]),
                std::mem::take(&mut partition_spill[idx]),
            ));
        }
        let task_iter = Arc::new(Mutex::new(tasks.into_iter()));
        let (tx, rx) = mpsc::channel::<NativeResult<(usize, Vec<u8>)>>();
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let tx = tx.clone();
                let task_iter = task_iter.clone();
                scope.spawn(move || loop {
                    let Some((idx, fragments, spill)) = ({
                        let mut guard = task_iter.lock().unwrap();
                        guard.next()
                    }) else {
                        break;
                    };
                    let result = reduce_partition_group_fragments_direct(
                        fragments,
                        spill,
                        agg_count,
                        merge_keys_mode,
                        hot_partition_threshold_records,
                    )
                    .map(|bytes| (idx, bytes));
                    let _ = tx.send(result);
                });
            }
        });
        drop(tx);

        let mut first_err: Option<NativeError> = None;
        for message in rx {
            match message {
                Ok((idx, bytes)) => reduced_partitions[idx] = bytes,
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
    }
    let reduce_ms = reduce_started.elapsed().as_secs_f64() * 1000.0;
    if emit_partition_stats_logs() {
        println!(
            "PARTITION_REDUCE tasks={} task_p50_ms={} task_p95_ms={} sort_ms={:.2} reduce_ms={:.2}",
            partition_count, 0, 0, 0.0, reduce_ms
        );
        println!(
            "COLLISION hash_conflict_candidates={} exact_key_mismatches={} canonical_bytes_materialized={}",
            0,
            0,
            0
        );
    }

    if let Some((limit, count_agg_idx)) = topk_count_agg {
        return trim_group_records_topk_by_count(
            reduced_partitions,
            agg_count,
            count_agg_idx,
            limit,
        );
    }

    let mut merged = Vec::new();
    for bytes in reduced_partitions {
        merged.extend_from_slice(&bytes);
    }
    Ok(merged)
}

fn reduce_partition_group_fragments_direct(
    fragments: Vec<Vec<u8>>,
    spill: Vec<u8>,
    agg_count: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
) -> NativeResult<Vec<u8>> {
    if fragments.is_empty() && spill.is_empty() {
        return Ok(Vec::new());
    }

    let total_records = if merge_keys_mode == MergeKeysMode::Hash {
        let record_size =
            16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
        if record_size == 0 {
            return Err(NativeError::Invalid("invalid group record size"));
        }
        let mut count = spill.len() / record_size;
        for bytes in &fragments {
            count = count.saturating_add(bytes.len() / record_size);
        }
        count
    } else {
        let mut count = count_group_records_with_keys(&spill, agg_count)?;
        for bytes in &fragments {
            count = count.saturating_add(count_group_records_with_keys(bytes, agg_count)?);
        }
        count
    };

    if total_records > hot_partition_threshold_records.max(1) {
        return reduce_hot_partition_groups(
            flatten_partition_fragments(fragments, spill),
            agg_count,
            merge_keys_mode,
        );
    }

    match merge_keys_mode {
        MergeKeysMode::Hash => {
            reduce_group_records_from_fragments_hash(&fragments, &spill, agg_count)
        }
        MergeKeysMode::Bytes => {
            reduce_group_records_with_keys_from_fragments(&fragments, &spill, agg_count)
        }
    }
}

fn reduce_group_records_from_fragments_hash(
    fragments: &[Vec<u8>],
    spill: &[u8],
    agg_count: usize,
) -> NativeResult<Vec<u8>> {
    use std::collections::BTreeMap;

    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if record_size == 0 {
        return Err(NativeError::Invalid("invalid group record size"));
    }

    let mut groups = BTreeMap::<(u64, u64), Vec<GroupAggState>>::new();
    for bytes in fragments
        .iter()
        .map(|v| v.as_slice())
        .chain(std::iter::once(spill))
    {
        if bytes.is_empty() {
            continue;
        }
        if bytes.len() % record_size != 0 {
            return Err(NativeError::Invalid(
                "group bytes are not aligned to record size",
            ));
        }
        for record in bytes.chunks_exact(record_size) {
            let key = (read_u64(record, 0), read_u64(record, 8));
            let states = groups
                .entry(key)
                .or_insert_with(|| vec![GroupAggState::default(); agg_count]);
            let mut offset = 16usize;
            for state in states.iter_mut() {
                let sum = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let min = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let max = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let count = u32::from_le_bytes(record[offset..offset + 4].try_into().unwrap());
                offset += 8;

                state.sum += sum;
                if count > 0 {
                    if state.count == 0 {
                        state.min = min;
                        state.max = max;
                    } else {
                        state.min = state.min.min(min);
                        state.max = state.max.max(max);
                    }
                }
                state.count = state.count.saturating_add(count);
            }
        }
    }

    let mut out = Vec::with_capacity(groups.len().saturating_mul(record_size));
    for ((a, b), states) in groups {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        for state in states {
            out.extend_from_slice(&state.sum.to_le_bytes());
            let min = if state.count > 0 { state.min } else { 0.0 };
            let max = if state.count > 0 { state.max } else { 0.0 };
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
            out.extend_from_slice(&state.count.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    Ok(out)
}

fn reduce_group_records_with_keys_from_fragments(
    fragments: &[Vec<u8>],
    spill: &[u8],
    agg_count: usize,
) -> NativeResult<Vec<u8>> {
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Clone)]
    struct EntryState {
        key_bytes: Vec<u8>,
        aggs: Vec<GroupAggState>,
        out_key: (u64, u64),
    }

    let mut by_hash = BTreeMap::<(u64, u64), Vec<EntryState>>::new();
    let mut used_keys = BTreeSet::<(u64, u64)>::new();

    for bytes in fragments
        .iter()
        .map(|v| v.as_slice())
        .chain(std::iter::once(spill))
    {
        if bytes.is_empty() {
            continue;
        }
        let mut offset = 0usize;
        while offset < bytes.len() {
            if offset + 24 > bytes.len() {
                return Err(NativeError::Invalid(
                    "group records-with-keys header truncated",
                ));
            }
            let key_a = read_u64(bytes, offset);
            let key_b = read_u64(bytes, offset + 8);
            let key_len = read_u32(bytes, offset + 16) as usize;
            let key_start = offset + 24;
            let key_end = key_start + key_len;
            let payload_len = 24usize
                .saturating_add(key_len)
                .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
            if offset + payload_len > bytes.len() {
                return Err(NativeError::Invalid(
                    "group records-with-keys payload truncated",
                ));
            }
            let key_bytes = bytes[key_start..key_end].to_vec();
            let mut aggs = vec![GroupAggState::default(); agg_count];
            merge_group_states_from_payload(bytes, key_end, &mut aggs)?;

            let bucket = by_hash.entry((key_a, key_b)).or_default();
            if let Some(existing) = bucket.iter_mut().find(|entry| entry.key_bytes == key_bytes) {
                merge_group_states(&mut existing.aggs, &aggs);
            } else {
                let out_key = allocate_collision_key((key_a, key_b), &key_bytes, &used_keys);
                used_keys.insert(out_key);
                bucket.push(EntryState {
                    key_bytes,
                    aggs,
                    out_key,
                });
            }
            offset += payload_len;
        }
    }

    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    let total_records: usize = by_hash.values().map(|entries| entries.len()).sum();
    let mut out = Vec::with_capacity(total_records.saturating_mul(record_size));
    for entries in by_hash.values() {
        for entry in entries {
            out.extend_from_slice(&entry.out_key.0.to_le_bytes());
            out.extend_from_slice(&entry.out_key.1.to_le_bytes());
            for agg in &entry.aggs {
                out.extend_from_slice(&agg.sum.to_le_bytes());
                let min = if agg.count > 0 { agg.min } else { 0.0 };
                let max = if agg.count > 0 { agg.max } else { 0.0 };
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
                out.extend_from_slice(&agg.count.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
    Ok(out)
}

fn flatten_partition_fragments(mut fragments: Vec<Vec<u8>>, mut spill: Vec<u8>) -> Vec<u8> {
    if fragments.is_empty() {
        return spill;
    }
    let mut total = spill.len();
    for bytes in &fragments {
        total = total.saturating_add(bytes.len());
    }
    let mut out = Vec::with_capacity(total);
    if !spill.is_empty() {
        out.append(&mut spill);
    }
    for mut bytes in fragments.drain(..) {
        if !bytes.is_empty() {
            out.append(&mut bytes);
        }
    }
    out
}

fn trim_group_records_topk_by_count(
    partitions: Vec<Vec<u8>>,
    agg_count: usize,
    count_agg_idx: usize,
    limit: usize,
) -> NativeResult<Vec<u8>> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    if limit == 0 {
        return Ok(Vec::new());
    }
    if count_agg_idx >= agg_count {
        return Err(NativeError::Invalid("count_agg_idx out of range"));
    }

    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if record_size == 0 {
        return Ok(Vec::new());
    }
    let count_offset = 16usize
        .saturating_add(count_agg_idx.saturating_mul(super::GROUP_AGG_RECORD_SIZE))
        .saturating_add(24);

    let mut heap = BinaryHeap::<(Reverse<u32>, Reverse<u64>, Reverse<u64>, Vec<u8>)>::new();

    for bytes in partitions {
        if bytes.is_empty() {
            continue;
        }
        if bytes.len() % record_size != 0 {
            return Err(NativeError::Invalid(
                "partition groups payload has invalid record size",
            ));
        }
        for record in bytes.chunks_exact(record_size) {
            let count = read_u32(record, count_offset);
            let key_a = read_u64(record, 0);
            let key_b = read_u64(record, 8);
            if heap.len() < limit {
                heap.push((
                    Reverse(count),
                    Reverse(key_a),
                    Reverse(key_b),
                    record.to_vec(),
                ));
                continue;
            }
            let replace = if let Some((Reverse(min_count), Reverse(min_a), Reverse(min_b), _)) =
                heap.peek()
            {
                count > *min_count || (count == *min_count && (key_a, key_b) < (*min_a, *min_b))
            } else {
                true
            };
            if replace {
                let _ = heap.pop();
                heap.push((
                    Reverse(count),
                    Reverse(key_a),
                    Reverse(key_b),
                    record.to_vec(),
                ));
            }
        }
    }

    let mut records = heap.into_vec();
    records.sort_by(|a, b| {
        let (ca, ka, kb) = (a.0 .0, a.1 .0, a.2 .0);
        let (cb, kca, kcb) = (b.0 .0, b.1 .0, b.2 .0);
        cb.cmp(&ca)
            .then_with(|| ka.cmp(&kca))
            .then_with(|| kb.cmp(&kcb))
    });

    let mut out = Vec::with_capacity(records.len().saturating_mul(record_size));
    for (_, _, _, bytes) in records {
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

fn reduce_partition_groups_sort(
    bytes: Vec<u8>,
    agg_count: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
) -> NativeResult<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let records = match merge_keys_mode {
        MergeKeysMode::Hash => {
            let record_size =
                16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
            if bytes.len() % record_size != 0 {
                return Err(NativeError::Invalid(
                    "partition groups payload has invalid record size",
                ));
            }
            bytes.len() / record_size
        }
        MergeKeysMode::Bytes => count_group_records_with_keys(&bytes, agg_count)?,
    };
    if records > hot_partition_threshold_records.max(1) {
        return reduce_hot_partition_groups(bytes, agg_count, merge_keys_mode);
    }
    match merge_keys_mode {
        MergeKeysMode::Hash => reduce_group_records_sort(&bytes, agg_count),
        MergeKeysMode::Bytes => reduce_group_records_with_keys_sort(&bytes, agg_count),
    }
}

fn reduce_partition_groups(
    bytes: Vec<u8>,
    agg_count: usize,
    merge_keys_mode: MergeKeysMode,
    hot_partition_threshold_records: usize,
) -> NativeResult<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let records = match merge_keys_mode {
        MergeKeysMode::Hash => {
            let record_size =
                16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
            if bytes.len() % record_size != 0 {
                return Err(NativeError::Invalid(
                    "partition groups payload has invalid record size",
                ));
            }
            bytes.len() / record_size
        }
        MergeKeysMode::Bytes => count_group_records_with_keys(&bytes, agg_count)?,
    };
    if records > hot_partition_threshold_records.max(1) {
        return reduce_hot_partition_groups(bytes, agg_count, merge_keys_mode);
    }
    match merge_keys_mode {
        MergeKeysMode::Hash => reduce_group_records(&bytes, agg_count),
        MergeKeysMode::Bytes => reduce_group_records_with_keys(&bytes, agg_count),
    }
}

fn reduce_hot_partition_groups(
    bytes: Vec<u8>,
    agg_count: usize,
    merge_keys_mode: MergeKeysMode,
) -> NativeResult<Vec<u8>> {
    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    let mut radix = vec![Vec::<u8>::new(); 16];
    match merge_keys_mode {
        MergeKeysMode::Hash => {
            for record in bytes.chunks_exact(record_size) {
                let key_a = read_u64(record, 0);
                let key_b = read_u64(record, 8);
                let h = hash_u64_pair(key_a, key_b);
                let bucket = ((h >> 60) & 0x0f) as usize;
                radix[bucket].extend_from_slice(record);
            }
        }
        MergeKeysMode::Bytes => {
            let mut offset = 0usize;
            while offset < bytes.len() {
                if offset + 24 > bytes.len() {
                    return Err(NativeError::Invalid(
                        "hot partition key payload header truncated",
                    ));
                }
                let key_a = read_u64(&bytes, offset);
                let key_b = read_u64(&bytes, offset + 8);
                let key_len = read_u32(&bytes, offset + 16) as usize;
                let payload_len = 24usize
                    .saturating_add(key_len)
                    .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
                if offset + payload_len > bytes.len() {
                    return Err(NativeError::Invalid("hot partition key payload truncated"));
                }
                let h = hash_u64_pair(key_a, key_b);
                let bucket = ((h >> 60) & 0x0f) as usize;
                radix[bucket].extend_from_slice(&bytes[offset..offset + payload_len]);
                offset += payload_len;
            }
        }
    }
    let mut out = Vec::new();
    for bucket in radix {
        let reduced = match merge_keys_mode {
            MergeKeysMode::Hash => reduce_group_records(&bucket, agg_count)?,
            MergeKeysMode::Bytes => reduce_group_records_with_keys(&bucket, agg_count)?,
        };
        out.extend_from_slice(&reduced);
    }
    Ok(out)
}

fn reduce_group_records(bytes: &[u8], agg_count: usize) -> NativeResult<Vec<u8>> {
    use std::collections::BTreeMap;

    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if bytes.len() % record_size != 0 {
        return Err(NativeError::Invalid(
            "group bytes are not aligned to record size",
        ));
    }

    let mut groups = BTreeMap::<(u64, u64), Vec<GroupAggState>>::new();
    for record in bytes.chunks_exact(record_size) {
        let key = (read_u64(record, 0), read_u64(record, 8));
        let states = groups
            .entry(key)
            .or_insert_with(|| vec![GroupAggState::default(); agg_count]);
        let mut offset = 16usize;
        for state in states.iter_mut() {
            let sum = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let min = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let max = f64::from_le_bytes(record[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let count = u32::from_le_bytes(record[offset..offset + 4].try_into().unwrap());
            offset += 4;
            offset += 4;

            state.sum += sum;
            if count > 0 {
                if state.count == 0 {
                    state.min = min;
                    state.max = max;
                } else {
                    state.min = state.min.min(min);
                    state.max = state.max.max(max);
                }
            }
            state.count = state.count.saturating_add(count);
        }
    }

    let mut out = Vec::with_capacity(groups.len().saturating_mul(record_size));
    for ((a, b), states) in groups {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        for state in states {
            out.extend_from_slice(&state.sum.to_le_bytes());
            let min = if state.count > 0 { state.min } else { 0.0 };
            let max = if state.count > 0 { state.max } else { 0.0 };
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
            out.extend_from_slice(&state.count.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    Ok(out)
}

fn reduce_group_records_sort(bytes: &[u8], agg_count: usize) -> NativeResult<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    if bytes.len() % record_size != 0 {
        return Err(NativeError::Invalid(
            "group bytes are not aligned to record size",
        ));
    }

    let mut offsets: Vec<usize> = (0..bytes.len()).step_by(record_size).collect();
    offsets.sort_unstable_by(|a, b| {
        let ka = (read_u64(bytes, *a), read_u64(bytes, *a + 8));
        let kb = (read_u64(bytes, *b), read_u64(bytes, *b + 8));
        ka.cmp(&kb)
    });

    let mut out = Vec::new();
    let mut i = 0usize;
    while i < offsets.len() {
        let off0 = offsets[i];
        let key_a = read_u64(bytes, off0);
        let key_b = read_u64(bytes, off0 + 8);
        let mut states = vec![GroupAggState::default(); agg_count];

        while i < offsets.len() {
            let off = offsets[i];
            if read_u64(bytes, off) != key_a || read_u64(bytes, off + 8) != key_b {
                break;
            }
            let mut offset = off + 16;
            for state in states.iter_mut() {
                let sum = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let min = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let max = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 8;

                state.sum += sum;
                if count > 0 {
                    if state.count == 0 {
                        state.min = min;
                        state.max = max;
                    } else {
                        state.min = state.min.min(min);
                        state.max = state.max.max(max);
                    }
                }
                state.count = state.count.saturating_add(count);
            }
            i += 1;
        }

        out.extend_from_slice(&key_a.to_le_bytes());
        out.extend_from_slice(&key_b.to_le_bytes());
        for state in states {
            out.extend_from_slice(&state.sum.to_le_bytes());
            let min = if state.count > 0 { state.min } else { 0.0 };
            let max = if state.count > 0 { state.max } else { 0.0 };
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
            out.extend_from_slice(&state.count.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }

    Ok(out)
}

fn count_group_records_with_keys(bytes: &[u8], agg_count: usize) -> NativeResult<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut offset = 0usize;
    let mut count = 0usize;
    while offset < bytes.len() {
        if offset + 24 > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys header truncated",
            ));
        }
        let key_len = read_u32(bytes, offset + 16) as usize;
        let payload_len = 24usize
            .saturating_add(key_len)
            .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
        if offset + payload_len > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys payload truncated",
            ));
        }
        offset += payload_len;
        count += 1;
    }
    Ok(count)
}

fn reduce_group_records_with_keys_sort(bytes: &[u8], agg_count: usize) -> NativeResult<Vec<u8>> {
    struct RecRef {
        a: u64,
        b: u64,
        key_start: usize,
        key_len: usize,
        agg_start: usize,
    }

    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut records = Vec::<RecRef>::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if offset + 24 > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys header truncated",
            ));
        }
        let a = read_u64(bytes, offset);
        let b = read_u64(bytes, offset + 8);
        let key_len = read_u32(bytes, offset + 16) as usize;
        let key_start = offset + 24;
        let key_end = key_start + key_len;
        let payload_len = 24usize
            .saturating_add(key_len)
            .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
        if offset + payload_len > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys payload truncated",
            ));
        }
        records.push(RecRef {
            a,
            b,
            key_start,
            key_len,
            agg_start: key_end,
        });
        offset += payload_len;
    }

    records.sort_unstable_by(|x, y| {
        let x_key = &bytes[x.key_start..x.key_start + x.key_len];
        let y_key = &bytes[y.key_start..y.key_start + y.key_len];
        (x.a, x.b, x_key).cmp(&(y.a, y.b, y_key))
    });

    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    let mut out = Vec::with_capacity(records.len().saturating_mul(record_size));
    let mut used_keys = std::collections::BTreeSet::<(u64, u64)>::new();
    let mut i = 0usize;
    let mut collision_candidates = 0usize;
    let mut exact_mismatches = 0usize;
    let mut canonical_bytes_materialized = 0usize;

    while i < records.len() {
        let rec = &records[i];
        let base_hash = (rec.a, rec.b);
        let mut hash_bucket_start = i;

        while hash_bucket_start < records.len() {
            let seed = &records[hash_bucket_start];
            if seed.a != base_hash.0 || seed.b != base_hash.1 {
                break;
            }

            let key = &bytes[seed.key_start..seed.key_start + seed.key_len];
            let mut aggs = vec![GroupAggState::default(); agg_count];
            merge_group_states_from_payload(bytes, seed.agg_start, &mut aggs)?;
            i = hash_bucket_start + 1;

            while i < records.len() {
                let n = &records[i];
                if n.a != base_hash.0 || n.b != base_hash.1 {
                    break;
                }
                collision_candidates = collision_candidates.saturating_add(1);
                let n_key = &bytes[n.key_start..n.key_start + n.key_len];
                if n_key == key {
                    merge_group_states_from_payload(bytes, n.agg_start, &mut aggs)?;
                } else {
                    exact_mismatches = exact_mismatches.saturating_add(1);
                    break;
                }
                i += 1;
            }

            canonical_bytes_materialized = canonical_bytes_materialized.saturating_add(key.len());
            let out_key = allocate_collision_key(base_hash, key, &used_keys);
            used_keys.insert(out_key);
            out.extend_from_slice(&out_key.0.to_le_bytes());
            out.extend_from_slice(&out_key.1.to_le_bytes());
            for agg in &aggs {
                out.extend_from_slice(&agg.sum.to_le_bytes());
                let min = if agg.count > 0 { agg.min } else { 0.0 };
                let max = if agg.count > 0 { agg.max } else { 0.0 };
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
                out.extend_from_slice(&agg.count.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }

            hash_bucket_start = i;
        }
    }

    let _ = (
        collision_candidates,
        exact_mismatches,
        canonical_bytes_materialized,
    );

    Ok(out)
}

fn merge_group_states_from_payload(
    bytes: &[u8],
    mut agg_offset: usize,
    target: &mut [GroupAggState],
) -> NativeResult<()> {
    for state in target.iter_mut() {
        if agg_offset + super::GROUP_AGG_RECORD_SIZE > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys aggregate payload truncated",
            ));
        }
        let sum = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
        agg_offset += 8;
        let min = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
        agg_offset += 8;
        let max = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
        agg_offset += 8;
        let count = u32::from_le_bytes(bytes[agg_offset..agg_offset + 4].try_into().unwrap());
        agg_offset += 8;

        state.sum += sum;
        if count > 0 {
            if state.count == 0 {
                state.min = min;
                state.max = max;
            } else {
                state.min = state.min.min(min);
                state.max = state.max.max(max);
            }
        }
        state.count = state.count.saturating_add(count);
    }
    Ok(())
}

fn reduce_group_records_with_keys(bytes: &[u8], agg_count: usize) -> NativeResult<Vec<u8>> {
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Clone)]
    struct EntryState {
        key_bytes: Vec<u8>,
        aggs: Vec<GroupAggState>,
        out_key: (u64, u64),
    }

    let mut by_hash = BTreeMap::<(u64, u64), Vec<EntryState>>::new();
    let mut used_keys = BTreeSet::<(u64, u64)>::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let key_a = read_u64(bytes, offset);
        let key_b = read_u64(bytes, offset + 8);
        let key_len = read_u32(bytes, offset + 16) as usize;
        let key_start = offset + 24;
        let key_end = key_start + key_len;
        let payload_len = 24usize
            .saturating_add(key_len)
            .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
        if offset + payload_len > bytes.len() {
            return Err(NativeError::Invalid(
                "group records-with-keys payload truncated",
            ));
        }
        let key_bytes = bytes[key_start..key_end].to_vec();
        let mut aggs = vec![GroupAggState::default(); agg_count];
        let mut agg_offset = key_end;
        for state in aggs.iter_mut() {
            state.sum = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
            agg_offset += 8;
            state.min = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
            agg_offset += 8;
            state.max = f64::from_le_bytes(bytes[agg_offset..agg_offset + 8].try_into().unwrap());
            agg_offset += 8;
            state.count = u32::from_le_bytes(bytes[agg_offset..agg_offset + 4].try_into().unwrap());
            agg_offset += 8;
        }

        let bucket = by_hash.entry((key_a, key_b)).or_default();
        if let Some(existing) = bucket.iter_mut().find(|entry| entry.key_bytes == key_bytes) {
            merge_group_states(&mut existing.aggs, &aggs);
        } else {
            let out_key = allocate_collision_key((key_a, key_b), &key_bytes, &used_keys);
            used_keys.insert(out_key);
            bucket.push(EntryState {
                key_bytes,
                aggs,
                out_key,
            });
        }
        offset += payload_len;
    }

    let record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    let total_records: usize = by_hash.values().map(|entries| entries.len()).sum();
    let mut out = Vec::with_capacity(total_records.saturating_mul(record_size));
    for entries in by_hash.values() {
        for entry in entries {
            out.extend_from_slice(&entry.out_key.0.to_le_bytes());
            out.extend_from_slice(&entry.out_key.1.to_le_bytes());
            for agg in &entry.aggs {
                out.extend_from_slice(&agg.sum.to_le_bytes());
                let min = if agg.count > 0 { agg.min } else { 0.0 };
                let max = if agg.count > 0 { agg.max } else { 0.0 };
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
                out.extend_from_slice(&agg.count.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
    Ok(out)
}

fn allocate_collision_key(
    base: (u64, u64),
    key_bytes: &[u8],
    used: &std::collections::BTreeSet<(u64, u64)>,
) -> (u64, u64) {
    if !used.contains(&base) {
        return base;
    }
    let mut seed = 1u64;
    loop {
        let candidate_b = xxh3_64_with_seed(key_bytes, base.1 ^ seed);
        let candidate = (base.0, candidate_b);
        if !used.contains(&candidate) {
            return candidate;
        }
        seed = seed.saturating_add(1);
    }
}

fn merge_group_states(target: &mut [GroupAggState], source: &[GroupAggState]) {
    for (dst, src) in target.iter_mut().zip(source.iter()) {
        dst.sum += src.sum;
        if src.count > 0 {
            if dst.count == 0 {
                dst.min = src.min;
                dst.max = src.max;
            } else {
                dst.min = dst.min.min(src.min);
                dst.max = dst.max.max(src.max);
            }
        }
        dst.count = dst.count.saturating_add(src.count);
    }
}

fn group_partition_for_keys(a: u64, b: u64, partitions: usize) -> usize {
    (hash_u64_pair(a, b) as usize) & (partitions.saturating_sub(1))
}

fn hash_u64_pair(a: u64, b: u64) -> u64 {
    let mut x = a.wrapping_mul(0x9e3779b97f4a7c15);
    x ^= b.rotate_left(17).wrapping_mul(0xc2b2ae3d27d4eb4f);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::types::ChunkPartial;

    fn push_group_record(
        out: &mut Vec<u8>,
        a: u64,
        b: u64,
        sum: f64,
        min: f64,
        max: f64,
        count: u32,
    ) {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        out.extend_from_slice(&sum.to_le_bytes());
        out.extend_from_slice(&min.to_le_bytes());
        out.extend_from_slice(&max.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
    }

    fn push_group_record_with_key(
        out: &mut Vec<u8>,
        a: u64,
        b: u64,
        key: &[u8],
        sum: f64,
        min: f64,
        max: f64,
        count: u32,
    ) {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&sum.to_le_bytes());
        out.extend_from_slice(&min.to_le_bytes());
        out.extend_from_slice(&max.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
    }

    #[test]
    fn reduce_group_records_merges_duplicate_keys() {
        let mut bytes = Vec::new();
        push_group_record(&mut bytes, 11, 22, 3.0, 3.0, 3.0, 1);
        push_group_record(&mut bytes, 11, 22, 7.0, 7.0, 7.0, 1);
        let reduced = reduce_group_records(&bytes, 1).expect("reduce groups");
        assert_eq!(reduced.len(), 16 + super::super::GROUP_AGG_RECORD_SIZE);
        assert_eq!(read_u64(&reduced, 0), 11);
        assert_eq!(read_u64(&reduced, 8), 22);
        let sum = f64::from_le_bytes(reduced[16..24].try_into().unwrap());
        let min = f64::from_le_bytes(reduced[24..32].try_into().unwrap());
        let max = f64::from_le_bytes(reduced[32..40].try_into().unwrap());
        let count = u32::from_le_bytes(reduced[40..44].try_into().unwrap());
        assert_eq!(sum, 10.0);
        assert_eq!(min, 3.0);
        assert_eq!(max, 7.0);
        assert_eq!(count, 2);
    }

    #[test]
    fn merge_group_partials_partitioned_reduces_across_partials() {
        let mut p1 = ChunkPartial::empty(0);
        let mut p2 = ChunkPartial::empty(1);
        push_group_record(&mut p1.groups, 5, 9, 2.0, 2.0, 2.0, 1);
        push_group_record(&mut p2.groups, 5, 9, 4.0, 4.0, 4.0, 1);
        let mut partials = vec![p1, p2];
        let reduced = merge_group_partials_partitioned_legacy(
            &mut partials,
            1,
            64,
            2,
            MergeKeysMode::Hash,
            1000,
        )
        .expect("partitioned merge");
        assert_eq!(reduced.len(), 16 + super::super::GROUP_AGG_RECORD_SIZE);
        let count = u32::from_le_bytes(reduced[40..44].try_into().unwrap());
        assert_eq!(count, 2);
        assert!(partials.iter().all(|p| p.groups.is_empty()));
    }

    #[test]
    fn reduce_group_records_with_keys_keeps_hash_collisions_distinct() {
        let mut bytes = Vec::new();
        push_group_record_with_key(&mut bytes, 77, 88, b"alpha", 2.0, 2.0, 2.0, 1);
        push_group_record_with_key(&mut bytes, 77, 88, b"beta", 4.0, 4.0, 4.0, 1);
        let reduced =
            reduce_group_records_with_keys(&bytes, 1).expect("reduce groups with key bytes");
        let record_size = 16 + super::super::GROUP_AGG_RECORD_SIZE;
        assert_eq!(reduced.len(), record_size * 2);
        let first_b = read_u64(&reduced, 8);
        let second_b = read_u64(&reduced, 8 + record_size);
        assert_ne!(first_b, second_b);
    }

    #[test]
    fn reduce_group_records_sort_matches_map_reduce() {
        let mut bytes = Vec::new();
        push_group_record(&mut bytes, 11, 22, 3.0, 3.0, 3.0, 1);
        push_group_record(&mut bytes, 11, 22, 7.0, 7.0, 7.0, 1);
        push_group_record(&mut bytes, 2, 4, 1.0, 1.0, 1.0, 1);
        let reduced_map = reduce_group_records(&bytes, 1).expect("reduce map");
        let reduced_sort = reduce_group_records_sort(&bytes, 1).expect("reduce sort");
        assert_eq!(reduced_sort, reduced_map);
    }

    #[test]
    fn reduce_group_records_with_keys_sort_keeps_hash_collisions_distinct() {
        let mut bytes = Vec::new();
        push_group_record_with_key(&mut bytes, 77, 88, b"alpha", 2.0, 2.0, 2.0, 1);
        push_group_record_with_key(&mut bytes, 77, 88, b"beta", 4.0, 4.0, 4.0, 1);
        let reduced = reduce_group_records_with_keys_sort(&bytes, 1).expect("reduce sort bytes");
        let record_size = 16 + super::super::GROUP_AGG_RECORD_SIZE;
        assert_eq!(reduced.len(), record_size * 2);
        let first_b = read_u64(&reduced, 8);
        let second_b = read_u64(&reduced, 8 + record_size);
        assert_ne!(first_b, second_b);
    }

    #[test]
    fn merge_group_partials_partition_direct_matches_partition_sort_hash() {
        let mut p1_sort = ChunkPartial::empty(0);
        let mut p2_sort = ChunkPartial::empty(1);
        push_group_record(&mut p1_sort.groups, 5, 9, 2.0, 2.0, 2.0, 1);
        push_group_record(&mut p2_sort.groups, 5, 9, 4.0, 4.0, 4.0, 1);
        let mut partials_sort = vec![p1_sort, p2_sort];

        let mut p1_direct = ChunkPartial::empty(0);
        let mut p2_direct = ChunkPartial::empty(1);
        push_group_record(&mut p1_direct.groups, 5, 9, 2.0, 2.0, 2.0, 1);
        push_group_record(&mut p2_direct.groups, 5, 9, 4.0, 4.0, 4.0, 1);
        let mut partials_direct = vec![p1_direct, p2_direct];

        let sort_out = merge_group_partials_partition_sort(
            &mut partials_sort,
            1,
            64,
            2,
            MergeKeysMode::Hash,
            1000,
            None,
        )
        .expect("partition-sort merge");
        let direct_out = merge_group_partials_partition_direct(
            &mut partials_direct,
            1,
            64,
            2,
            MergeKeysMode::Hash,
            1000,
            None,
        )
        .expect("partition-direct merge");
        assert_eq!(direct_out, sort_out);
    }

    #[test]
    fn merge_group_partials_partition_direct_matches_partition_sort_bytes() {
        let mut p1_sort = ChunkPartial::empty(0);
        let mut p2_sort = ChunkPartial::empty(1);
        push_group_record_with_key(&mut p1_sort.groups, 77, 88, b"alpha", 2.0, 2.0, 2.0, 1);
        push_group_record_with_key(&mut p2_sort.groups, 77, 88, b"alpha", 4.0, 4.0, 4.0, 1);
        push_group_record_with_key(&mut p2_sort.groups, 77, 88, b"beta", 1.0, 1.0, 1.0, 1);
        let mut partials_sort = vec![p1_sort, p2_sort];

        let mut p1_direct = ChunkPartial::empty(0);
        let mut p2_direct = ChunkPartial::empty(1);
        push_group_record_with_key(&mut p1_direct.groups, 77, 88, b"alpha", 2.0, 2.0, 2.0, 1);
        push_group_record_with_key(&mut p2_direct.groups, 77, 88, b"alpha", 4.0, 4.0, 4.0, 1);
        push_group_record_with_key(&mut p2_direct.groups, 77, 88, b"beta", 1.0, 1.0, 1.0, 1);
        let mut partials_direct = vec![p1_direct, p2_direct];

        let sort_out = merge_group_partials_partition_sort(
            &mut partials_sort,
            1,
            64,
            2,
            MergeKeysMode::Bytes,
            1000,
            None,
        )
        .expect("partition-sort merge");
        let direct_out = merge_group_partials_partition_direct(
            &mut partials_direct,
            1,
            64,
            2,
            MergeKeysMode::Bytes,
            1000,
            None,
        )
        .expect("partition-direct merge");
        assert_eq!(direct_out, sort_out);
    }
}

fn trim_current_thread_allocator() {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        let _ = malloc_trim(0);
    }
}

fn maybe_configure_malloc_reclaim() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[cfg(target_os = "linux")]
        unsafe {
            unsafe extern "C" {
                fn mallopt(param: i32, value: i32) -> i32;
            }
            // glibc malloc tuning knobs.
            const M_TRIM_THRESHOLD: i32 = -1;
            const M_MMAP_THRESHOLD: i32 = -3;
            const M_ARENA_MAX: i32 = -8;

            let arena_max = std::env::var("WCOL_MALLOC_ARENA_MAX")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(2);
            let trim_threshold = std::env::var("WCOL_MALLOC_TRIM_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(131_072);
            let mmap_threshold = std::env::var("WCOL_MALLOC_MMAP_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(131_072);

            let _ = mallopt(M_ARENA_MAX, arena_max.max(1));
            let _ = mallopt(M_TRIM_THRESHOLD, trim_threshold.max(0));
            let _ = mallopt(M_MMAP_THRESHOLD, mmap_threshold.max(0));
        }
    });
}
