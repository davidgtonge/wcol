use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

use super::cache::ReadCache;
use super::config::{ArenaReleasePolicy, CacheCounterMode, GroupEngineMode, MergeKeysMode};
use super::error::{NativeError, NativeResult};
use super::exec::{PlanTimingSnapshot, WorkerContext};
use super::helpers::{read_u32, read_u64};
use super::mem_budget::{QueryMemoryBudget, RetainedMemoryCap, ThreadArenaState};
use super::perf_counters::{CachePerfCollector, CachePerfSnapshot};
use super::types::{ChunkPartial, HeaderInfo, RuntimeInit, WorkerDebugStats};

enum WorkerCommand {
    Prepare {
        sql: Arc<str>,
        reply: mpsc::Sender<NativeResult<()>>,
    },
    Execute {
        request: ExecuteRequest,
        reply: mpsc::Sender<NativeResult<Vec<ChunkPartial>>>,
    },
    Stats {
        reply: mpsc::Sender<NativeResult<WorkerDebugStats>>,
    },
    Trim {
        reply: mpsc::Sender<NativeResult<()>>,
    },
    Shutdown,
}

struct SharedExecution {
    next_chunk: AtomicU32,
    total_chunks: u32,
}

impl SharedExecution {
    fn new(total_chunks: u32) -> Self {
        Self {
            next_chunk: AtomicU32::new(0),
            total_chunks,
        }
    }
}

#[derive(Clone)]
struct ExecuteRequest {
    has_filters: bool,
    shared: Arc<SharedExecution>,
    worker_count: usize,
    scan_chunk_batch_size: usize,
    group_engine_mode: GroupEngineMode,
    budget: Arc<QueryMemoryBudget>,
    heavy_string: bool,
    flush_window_bytes: u64,
    arena_release_policy: ArenaReleasePolicy,
    arena_keep_up_to_bytes: u64,
    retained_idle_decay_queries: u32,
    merge_keys_mode: MergeKeysMode,
    collect_plan_timing: bool,
    group_agg_count: usize,
    group_partition_count: usize,
    partition_groups_during_scan: bool,
}

#[derive(Clone)]
pub(super) struct ExecutePlan {
    pub has_filters: bool,
    pub worker_count: usize,
    pub scan_chunk_batch_size: usize,
    pub group_engine_mode: GroupEngineMode,
    pub budget: Arc<QueryMemoryBudget>,
    pub heavy_string: bool,
    pub string_window_bytes: u64,
    pub partition_sort_chunk_bytes: u64,
    pub arena_release_policy: ArenaReleasePolicy,
    pub arena_keep_up_to_bytes: u64,
    pub retained_idle_decay_queries: u32,
    pub retained_global_cap_bytes: u64,
    pub merge_keys_mode: MergeKeysMode,
    pub collect_plan_timing: bool,
    pub group_agg_count: usize,
    pub group_partition_count: usize,
    pub partition_groups_during_scan: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct WorkerPlanTimingTotals {
    chunks: u64,
    ms_decode: f64,
    ms_filters: f64,
    ms_aggs: f64,
    ms_group: f64,
    ms_rows: f64,
}

impl WorkerPlanTimingTotals {
    fn add_snapshot(&mut self, snapshot: PlanTimingSnapshot) {
        self.chunks = self.chunks.saturating_add(snapshot.chunks);
        self.ms_decode += snapshot.ms_decode;
        self.ms_filters += snapshot.ms_filters;
        self.ms_aggs += snapshot.ms_aggs;
        self.ms_group += snapshot.ms_group;
        self.ms_rows += snapshot.ms_rows;
    }
}


struct WorkerThread {
    tx: mpsc::Sender<WorkerCommand>,
    join: Option<JoinHandle<()>>,
}

pub(super) struct ParallelPool {
    workers: Vec<WorkerThread>,
    retained: Arc<RetainedMemoryCap>,
}

impl ParallelPool {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        size: usize,
        file: Arc<File>,
        read_cache: Arc<ReadCache>,
        header: HeaderInfo,
        init: RuntimeInit,
        arena_base_bytes: u64,
        arena_grow_bytes: u64,
        arena_max_bytes: u64,
        cache_counter_mode: CacheCounterMode,
    ) -> NativeResult<Self> {
        let initial_retained = arena_base_bytes.saturating_mul(size as u64);
        let retained = RetainedMemoryCap::with_initial(u64::MAX, initial_retained);
        let mut workers = Vec::with_capacity(size);
        for worker_id in 0..size {
            let (tx, rx) = mpsc::channel::<WorkerCommand>();
            let file = file.clone();
            let read_cache = read_cache.clone();
            let init = init.clone();
            let retained_for_worker = retained.clone();
            let join = thread::spawn(move || {
                worker_loop(
                    worker_id,
                    rx,
                    file,
                    read_cache,
                    header,
                    init,
                    retained_for_worker,
                    arena_base_bytes,
                    arena_grow_bytes,
                    arena_max_bytes,
                    cache_counter_mode,
                )
            });
            workers.push(WorkerThread {
                tx,
                join: Some(join),
            });
        }
        Ok(Self { workers, retained })
    }

    pub(super) fn size(&self) -> usize {
        self.workers.len()
    }

    pub(super) fn retained_bytes(&self) -> u64 {
        self.retained.current_retained()
    }

    pub(super) fn prepare(&self, sql: &str) -> NativeResult<()> {
        let sql = Arc::<str>::from(sql);
        let mut replies = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let (reply_tx, reply_rx) = mpsc::channel();
            worker
                .tx
                .send(WorkerCommand::Prepare {
                    sql: sql.clone(),
                    reply: reply_tx,
                })
                .map_err(|_| NativeError::Invalid("failed to send prepare to worker"))?;
            replies.push(reply_rx);
        }
        let mut first_err: Option<NativeError> = None;
        for reply in replies {
            if let Err(err) = recv_reply(reply) {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    pub(super) fn execute(
        &self,
        total_chunks: u32,
        plan: ExecutePlan,
    ) -> NativeResult<Vec<ChunkPartial>> {
        self.retained.set_cap_bytes(plan.retained_global_cap_bytes);
        let shared = Arc::new(SharedExecution::new(total_chunks));
        let request = ExecuteRequest {
            has_filters: plan.has_filters,
            shared: shared.clone(),
            worker_count: plan.worker_count.max(1),
            scan_chunk_batch_size: plan.scan_chunk_batch_size.max(1),
            group_engine_mode: plan.group_engine_mode,
            budget: plan.budget,
            heavy_string: plan.heavy_string,
            flush_window_bytes: if matches!(
                plan.group_engine_mode,
                GroupEngineMode::PartitionSort | GroupEngineMode::PartitionDirect
            ) {
                plan.partition_sort_chunk_bytes
                    .max(plan.string_window_bytes)
                    .max(1)
            } else {
                plan.string_window_bytes.max(1)
            },
            arena_release_policy: plan.arena_release_policy,
            arena_keep_up_to_bytes: plan.arena_keep_up_to_bytes,
            retained_idle_decay_queries: plan.retained_idle_decay_queries.max(1),
            merge_keys_mode: plan.merge_keys_mode,
            collect_plan_timing: plan.collect_plan_timing,
            group_agg_count: plan.group_agg_count,
            group_partition_count: plan.group_partition_count.max(1).next_power_of_two(),
            partition_groups_during_scan: plan.partition_groups_during_scan,
        };
        let mut replies = Vec::with_capacity(self.workers.len());

        for worker in &self.workers {
            let (reply_tx, reply_rx) = mpsc::channel();
            worker
                .tx
                .send(WorkerCommand::Execute {
                    request: request.clone(),
                    reply: reply_tx,
                })
                .map_err(|_| NativeError::Invalid("failed to send execute to worker"))?;
            replies.push(reply_rx);
        }

        let mut first_err: Option<NativeError> = None;
        let mut all_partials = Vec::with_capacity(total_chunks as usize);
        for reply in replies {
            match recv_reply(reply) {
                Ok(mut partials) => all_partials.append(&mut partials),
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
        Ok(all_partials)
    }

    pub(super) fn debug_stats(&self) -> NativeResult<Vec<WorkerDebugStats>> {
        let mut replies = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let (reply_tx, reply_rx) = mpsc::channel();
            worker
                .tx
                .send(WorkerCommand::Stats { reply: reply_tx })
                .map_err(|_| NativeError::Invalid("failed to send stats to worker"))?;
            replies.push(reply_rx);
        }
        let mut out = Vec::with_capacity(self.workers.len());
        for reply in replies {
            out.push(recv_reply(reply)?);
        }
        Ok(out)
    }

    pub(super) fn trim_workers(&self) -> NativeResult<()> {
        let mut replies = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let (reply_tx, reply_rx) = mpsc::channel();
            worker
                .tx
                .send(WorkerCommand::Trim { reply: reply_tx })
                .map_err(|_| NativeError::Invalid("failed to send trim to worker"))?;
            replies.push(reply_rx);
        }
        for reply in replies {
            recv_reply(reply)?;
        }
        Ok(())
    }
}

impl Drop for ParallelPool {
    fn drop(&mut self) {
        for worker in &self.workers {
            let _ = worker.tx.send(WorkerCommand::Shutdown);
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                let _ = join.join();
            }
        }
    }
}

fn recv_reply<T>(rx: mpsc::Receiver<NativeResult<T>>) -> NativeResult<T> {
    match rx.recv() {
        Ok(result) => result,
        Err(_) => Err(NativeError::Invalid("worker reply channel closed")),
    }
}

#[allow(clippy::too_many_arguments)]
fn worker_loop(
    worker_id: usize,
    rx: mpsc::Receiver<WorkerCommand>,
    file: Arc<File>,
    read_cache: Arc<ReadCache>,
    header: HeaderInfo,
    init: RuntimeInit,
    retained: Arc<RetainedMemoryCap>,
    arena_base_bytes: u64,
    arena_grow_bytes: u64,
    arena_max_bytes: u64,
    cache_counter_mode: CacheCounterMode,
) {
    let worker = match WorkerContext::new(file, read_cache, header, &init) {
        Ok(worker) => worker,
        Err(_) => return,
    };
    let mut arena = ThreadArenaState::new(
        worker_id,
        arena_base_bytes,
        arena_grow_bytes,
        arena_max_bytes,
    );
    let perf = match CachePerfCollector::new(cache_counter_mode) {
        Ok(perf) => perf,
        Err(_) => return,
    };
    let mut perf_totals = CachePerfSnapshot::default();
    let mut last_plan_timing = WorkerPlanTimingTotals::default();

    while let Ok(cmd) = rx.recv() {
        match cmd {
            WorkerCommand::Prepare { sql, reply } => {
                let _ = reply.send(worker.prepare_sql(&sql));
            }
            WorkerCommand::Execute { request, reply } => {
                let before = perf.snapshot();
                arena.begin_query();
                last_plan_timing = WorkerPlanTimingTotals::default();
                let result = execute_chunks(&worker, &request, &mut arena, &retained).map(
                    |(partials, timing)| {
                        last_plan_timing = timing;
                        partials
                    },
                );
                let after = perf.snapshot();
                accumulate_perf(&mut perf_totals, &before, &after);
                arena.end_query(
                    request.arena_release_policy,
                    request.arena_keep_up_to_bytes,
                    &retained,
                    request.retained_idle_decay_queries,
                );
                maybe_trim_worker_allocator();
                let _ = reply.send(result);
            }
            WorkerCommand::Stats { reply } => {
                let mut stats = match worker.debug_stats() {
                    Ok(stats) => stats,
                    Err(err) => {
                        let _ = reply.send(Err(err));
                        continue;
                    }
                };
                stats.arena_reserved_bytes = arena.reserved_bytes();
                stats.arena_used_bytes = arena.used_bytes();
                stats.arena_peak_bytes = arena.last_query_reserved_peak();
                stats.perf_available = u8::from(perf.available());
                stats.perf_cycles = perf_totals.cycles;
                stats.perf_instructions = perf_totals.instructions;
                stats.perf_cache_refs = perf_totals.cache_refs;
                stats.perf_cache_misses = perf_totals.cache_misses;
                stats.perf_llc_refs = perf_totals.llc_refs;
                stats.perf_llc_misses = perf_totals.llc_misses;
                stats.perf_l1d_misses = perf_totals.l1d_misses;
                stats.perf_l2_misses = perf_totals.l2_misses;
                stats.plan_timing_chunks = last_plan_timing.chunks;
                stats.plan_ms_decode = last_plan_timing.ms_decode;
                stats.plan_ms_filters = last_plan_timing.ms_filters;
                stats.plan_ms_aggs = last_plan_timing.ms_aggs;
                stats.plan_ms_group = last_plan_timing.ms_group;
                stats.plan_ms_rows = last_plan_timing.ms_rows;
                let _ = reply.send(Ok(stats));
            }
            WorkerCommand::Trim { reply } => {
                force_trim_worker_allocator();
                let _ = reply.send(Ok(()));
            }
            WorkerCommand::Shutdown => break,
        }
    }
}

fn accumulate_perf(
    out: &mut CachePerfSnapshot,
    before: &CachePerfSnapshot,
    after: &CachePerfSnapshot,
) {
    out.cycles = out
        .cycles
        .saturating_add(after.cycles.saturating_sub(before.cycles));
    out.instructions = out
        .instructions
        .saturating_add(after.instructions.saturating_sub(before.instructions));
    out.cache_refs = out
        .cache_refs
        .saturating_add(after.cache_refs.saturating_sub(before.cache_refs));
    out.cache_misses = out
        .cache_misses
        .saturating_add(after.cache_misses.saturating_sub(before.cache_misses));
    out.llc_refs = out
        .llc_refs
        .saturating_add(after.llc_refs.saturating_sub(before.llc_refs));
    out.llc_misses = out
        .llc_misses
        .saturating_add(after.llc_misses.saturating_sub(before.llc_misses));
    out.l1d_misses = out
        .l1d_misses
        .saturating_add(after.l1d_misses.saturating_sub(before.l1d_misses));
    out.l2_misses = out
        .l2_misses
        .saturating_add(after.l2_misses.saturating_sub(before.l2_misses));
}

fn maybe_trim_worker_allocator() {
    if !std::env::var("WCOL_WORKER_TRIM")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        return;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        let _ = malloc_trim(0);
    }
}

fn force_trim_worker_allocator() {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        let _ = malloc_trim(0);
    }
}

fn execute_chunks(
    worker: &WorkerContext,
    request: &ExecuteRequest,
    arena: &mut ThreadArenaState,
    retained: &RetainedMemoryCap,
) -> NativeResult<(Vec<ChunkPartial>, WorkerPlanTimingTotals)> {
    let expected = (request.shared.total_chunks as usize)
        .saturating_div(request.worker_count)
        .saturating_add(8);
    let mut local_partials = Vec::<ChunkPartial>::with_capacity(expected);
    let mut local_window_bytes = 0u64;
    let mut plan_timing_totals = WorkerPlanTimingTotals::default();
    let mut scan_partitioned_groups = if request.partition_groups_during_scan
        && request.group_agg_count > 0
    {
        Some(vec![Vec::<u8>::new(); request.group_partition_count.max(1)])
    } else {
        None
    };
    let batch_size = request.scan_chunk_batch_size.max(1) as u32;
    loop {
        let chunk_start = request
            .shared
            .next_chunk
            .fetch_add(batch_size, Ordering::Relaxed);
        if chunk_start >= request.shared.total_chunks {
            break;
        }
        let chunk_end = chunk_start
            .saturating_add(batch_size)
            .min(request.shared.total_chunks);
        for chunk_id in chunk_start..chunk_end {
            let mut partial = worker.execute_chunk(
                chunk_id,
                request.has_filters,
                request.merge_keys_mode,
                request.group_engine_mode,
            )?;
            if let Some(partitioned) = scan_partitioned_groups.as_mut() {
                if !partial.groups.is_empty() {
                    repartition_group_records(
                        &partial.groups,
                        request.group_agg_count,
                        request.merge_keys_mode,
                        partitioned,
                    )?;
                    partial.groups.clear();
                }
            }
            local_window_bytes = local_window_bytes.saturating_add(partial.work_bytes_est);
            arena.set_live_bytes(local_window_bytes, &request.budget, retained, "scan")?;
            local_partials.push(partial);
            if request.collect_plan_timing {
                if let Ok(snapshot) = worker.plan_timing_snapshot() {
                    plan_timing_totals.add_snapshot(snapshot);
                }
            }
            if request.heavy_string && local_window_bytes >= request.flush_window_bytes {
                local_window_bytes = 0;
                arena.reset_live_bytes();
            }
        }
    }
    arena.reset_live_bytes();
    if let Some(partitioned) = scan_partitioned_groups {
        if partitioned.iter().any(|b| !b.is_empty()) {
            local_partials.push(ChunkPartial {
                chunk_id: u32::MAX,
                rows: Vec::new(),
                row_candidates: Vec::new(),
                aggs: Vec::new(),
                groups: Vec::new(),
                partitioned_groups: partitioned,
                work_bytes_est: 0,
            });
        }
    }
    Ok((local_partials, plan_timing_totals))
}


fn repartition_group_records(
    bytes: &[u8],
    agg_count: usize,
    merge_keys_mode: MergeKeysMode,
    partitions: &mut [Vec<u8>],
) -> NativeResult<()> {
    if partitions.is_empty() {
        return Ok(());
    }
    let partition_count = partitions.len();
    let fixed_record_size =
        16usize.saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
    match merge_keys_mode {
        MergeKeysMode::Hash => {
            if fixed_record_size == 0 || bytes.len() % fixed_record_size != 0 {
                return Err(NativeError::Invalid(
                    "partial groups payload has invalid record size",
                ));
            }
            for record in bytes.chunks_exact(fixed_record_size) {
                let key_a = read_u64(record, 0);
                let key_b = read_u64(record, 8);
                let p = group_partition_for_keys(key_a, key_b, partition_count);
                partitions[p].extend_from_slice(record);
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
                let key_a = read_u64(bytes, offset);
                let key_b = read_u64(bytes, offset + 8);
                let key_len = read_u32(bytes, offset + 16) as usize;
                let payload_len = 24usize
                    .saturating_add(key_len)
                    .saturating_add(agg_count.saturating_mul(super::GROUP_AGG_RECORD_SIZE));
                if offset + payload_len > bytes.len() {
                    return Err(NativeError::Invalid("partial groups key payload truncated"));
                }
                let p = group_partition_for_keys(key_a, key_b, partition_count);
                partitions[p].extend_from_slice(&bytes[offset..offset + payload_len]);
                offset += payload_len;
            }
        }
    }
    Ok(())
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
