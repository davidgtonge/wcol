use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use wcol_decoder::native::NativeRuntime;

use crate::cli::{NativeExecOpts, QuerySpec};

use super::shared::{apply_native_exec_opts, extract_queries, parse_index_filter, summarize};

pub(crate) fn run_bench_cmd(
    file: &Path,
    sql_file: &Path,
    runs: usize,
    warmup: usize,
    workers: usize,
    only: Option<String>,
    native: NativeExecOpts,
) -> Result<()> {
    apply_native_exec_opts(&native);
    let mut queries = extract_queries(sql_file)?;
    if let Some(filter) = only {
        let wanted = parse_index_filter(&filter)?;
        queries.retain(|query| wanted.contains(&query.index));
    }
    let total = queries.len();
    println!("queries={total} runs={runs} warmup={warmup}");
    match bench_isolation_mode() {
        BenchIsolationMode::PerRun => {
            println!("isolation=process_per_query_run");
            return run_bench_cmd_isolated(file, &queries, runs, warmup, workers);
        }
        BenchIsolationMode::Adaptive => {
            return run_bench_cmd_adaptive(file, &queries, runs, warmup, workers);
        }
        BenchIsolationMode::None => {}
    }

    let runtime = NativeRuntime::open(file)
        .with_context(|| format!("opening wcol file {}", file.display()))?;
    let emit_mem = std::env::var("WCOL_BENCH_MEM_STATS")
        .map(|v| v != "0")
        .unwrap_or(false);
    let trim_after_query = std::env::var("WCOL_BENCH_MALLOC_TRIM")
        .map(|v| v != "0")
        .unwrap_or(false);
    let emit_worker_stats = std::env::var("WCOL_BENCH_WORKER_STATS")
        .map(|v| v != "0")
        .unwrap_or(false);
    let emit_sample_stats = std::env::var("WCOL_BENCH_SAMPLE_STATS")
        .map(|v| v != "0")
        .unwrap_or(false);
    for query in queries {
        let io_before = runtime.read_io_stats();
        let mem_before = if emit_mem {
            read_proc_mem_kb()
        } else {
            ProcMemKb::default()
        };
        let smaps_before = if emit_mem {
            read_proc_smaps_rollup_kb()
        } else {
            ProcSmapsRollupKb::default()
        };
        let malloc_before = if emit_mem {
            read_malloc_stats()
        } else {
            MallocStats::default()
        };
        let mut samples = Vec::with_capacity(runs);
        let mut failed: Option<String> = None;
        for idx in 0..(warmup + runs) {
            let started = Instant::now();
            match runtime.query_sql_with_workers(&query.sql, workers) {
                Ok(result) => {
                    drop(result);
                    if emit_sample_stats {
                        match runtime.global_debug_stats() {
                            Ok(g) => println!(
                                "SAMPLE Q{} run={} plans={} runtimes={} runtime_index_bytes_est_total={} plan_group_state_cap_total={}",
                                query.index,
                                idx,
                                g.plan_count,
                                g.runtime_count,
                                g.runtime_index_bytes_est_total,
                                g.plan_group_state_cap_total
                            ),
                            Err(err) => println!("SAMPLE Q{} run={} error {}", query.index, idx, err),
                        }
                    }
                    let ms = started.elapsed().as_secs_f64() * 1000.0;
                    if idx >= warmup {
                        samples.push(ms);
                    }
                }
                Err(err) => {
                    failed = Some(err.to_string());
                    break;
                }
            }
        }
        if let Some(err) = failed {
            println!("Q{} error {}", query.index, err);
            continue;
        }
        let stats = summarize(&samples).ok_or_else(|| anyhow!("no samples collected"))?;
        println!(
            "Q{} mean={:.2}ms p50={:.2}ms p95={:.2}ms min={:.2}ms max={:.2}ms",
            query.index, stats.mean, stats.p50, stats.p95, stats.min, stats.max
        );
        if emit_mem {
            let mem_after = read_proc_mem_kb();
            let smaps_after = read_proc_smaps_rollup_kb();
            let malloc_after = read_malloc_stats();
            let io_after = runtime.read_io_stats();
            println!(
                "MEM Q{} rss_before_kb={} rss_after_kb={} rss_delta_kb={} hwm_kb={} swap_before_kb={} swap_after_kb={} cache_used_before={} cache_used_after={} cache_entries_before={} cache_entries_after={} io_total_delta={} io_miss_delta={} io_disk_bytes_delta={}",
                query.index,
                mem_before.rss_kb,
                mem_after.rss_kb,
                saturating_diff(mem_after.rss_kb, mem_before.rss_kb),
                mem_after.hwm_kb,
                mem_before.swap_kb,
                mem_after.swap_kb,
                io_before.cache_used_bytes,
                io_after.cache_used_bytes,
                io_before.cache_entries,
                io_after.cache_entries,
                saturating_diff(io_after.total_requests, io_before.total_requests),
                saturating_diff(io_after.cache_misses, io_before.cache_misses),
                saturating_diff(io_after.bytes_from_disk, io_before.bytes_from_disk),
            );
            println!(
                "MEMMAP Q{} rss_before_kb={} rss_after_kb={} rss_delta_kb={} pss_before_kb={} pss_after_kb={} pss_delta_kb={} anon_before_kb={} anon_after_kb={} anon_delta_kb={} private_dirty_before_kb={} private_dirty_after_kb={} private_dirty_delta_kb={} shmem_before_kb={} shmem_after_kb={} shmem_delta_kb={}",
                query.index,
                smaps_before.rss_kb,
                smaps_after.rss_kb,
                diff_i64(smaps_after.rss_kb, smaps_before.rss_kb),
                smaps_before.pss_kb,
                smaps_after.pss_kb,
                diff_i64(smaps_after.pss_kb, smaps_before.pss_kb),
                smaps_before.anon_kb,
                smaps_after.anon_kb,
                diff_i64(smaps_after.anon_kb, smaps_before.anon_kb),
                smaps_before.private_dirty_kb,
                smaps_after.private_dirty_kb,
                diff_i64(smaps_after.private_dirty_kb, smaps_before.private_dirty_kb),
                smaps_before.shmem_kb,
                smaps_after.shmem_kb,
                diff_i64(smaps_after.shmem_kb, smaps_before.shmem_kb),
            );
            println!(
                "MALLOC Q{} uordblks_before={} uordblks_after={} uordblks_delta={} fordblks_before={} fordblks_after={} fordblks_delta={} hblkhd_before={} hblkhd_after={} hblkhd_delta={} arena_before={} arena_after={} arena_delta={}",
                query.index,
                malloc_before.uordblks,
                malloc_after.uordblks,
                diff_i64(malloc_after.uordblks, malloc_before.uordblks),
                malloc_before.fordblks,
                malloc_after.fordblks,
                diff_i64(malloc_after.fordblks, malloc_before.fordblks),
                malloc_before.hblkhd,
                malloc_after.hblkhd,
                diff_i64(malloc_after.hblkhd, malloc_before.hblkhd),
                malloc_before.arena,
                malloc_after.arena,
                diff_i64(malloc_after.arena, malloc_before.arena),
            );
        }
        if trim_after_query {
            let trimmed = malloc_trim_supported();
            println!("MEMTRIM Q{} trimmed={}", query.index, trimmed as u8);
        }
        if emit_worker_stats {
            match runtime.global_debug_stats() {
                Ok(g) => println!(
                    "GLOBAL Q{} plans={} plan_group_state_cap_total={} plan_group_keys_cap_total={} runtimes={} runtime_index_chunks_total={} runtime_index_bytes_est_total={} ffi_plan_lock_count={} ffi_plan_lock_wait_ns={} ffi_runtime_lock_count={} ffi_runtime_lock_wait_ns={}",
                    query.index,
                    g.plan_count,
                    g.plan_group_state_cap_total,
                    g.plan_group_keys_cap_total,
                    g.runtime_count,
                    g.runtime_index_chunks_total,
                    g.runtime_index_bytes_est_total,
                    g.ffi_plan_lock_count,
                    g.ffi_plan_lock_wait_ns,
                    g.ffi_runtime_lock_count,
                    g.ffi_runtime_lock_wait_ns
                ),
                Err(err) => println!("GLOBAL Q{} error {}", query.index, err),
            }
            match runtime.worker_debug_stats() {
                Ok(stats) => {
                    let total_index_bytes: u64 =
                        stats.iter().map(|s| s.runtime_index_bytes_est).sum();
                    let total_index_chunks: u64 =
                        stats.iter().map(|s| s.runtime_index_chunks).sum();
                    let total_group_cap: u64 = stats.iter().map(|s| s.plan_group_state_cap).sum();
                    let total_arena_reserved: u64 =
                        stats.iter().map(|s| s.arena_reserved_bytes).sum();
                    let total_arena_peak: u64 = stats.iter().map(|s| s.arena_peak_bytes).sum();
                    let total_perf_cycles: u64 = stats.iter().map(|s| s.perf_cycles).sum();
                    let total_perf_instructions: u64 =
                        stats.iter().map(|s| s.perf_instructions).sum();
                    let total_perf_cache_misses: u64 =
                        stats.iter().map(|s| s.perf_cache_misses).sum();
                    let total_perf_llc_refs: u64 = stats.iter().map(|s| s.perf_llc_refs).sum();
                    let total_perf_llc_misses: u64 = stats.iter().map(|s| s.perf_llc_misses).sum();
                    let total_plan_timing_chunks: u64 =
                        stats.iter().map(|s| s.plan_timing_chunks).sum();
                    let total_plan_ms_decode: f64 = stats.iter().map(|s| s.plan_ms_decode).sum();
                    let total_plan_ms_filters: f64 = stats.iter().map(|s| s.plan_ms_filters).sum();
                    let total_plan_ms_aggs: f64 = stats.iter().map(|s| s.plan_ms_aggs).sum();
                    let total_plan_ms_group: f64 = stats.iter().map(|s| s.plan_ms_group).sum();
                    let total_plan_ms_rows: f64 = stats.iter().map(|s| s.plan_ms_rows).sum();
                    let total_plan_ms: f64 = total_plan_ms_decode
                        + total_plan_ms_filters
                        + total_plan_ms_aggs
                        + total_plan_ms_group
                        + total_plan_ms_rows;
                    let plan_wall_est_ms = if stats.is_empty() {
                        0.0
                    } else {
                        total_plan_ms / stats.len() as f64
                    };
                    let llc_miss_rate = if total_perf_llc_refs > 0 {
                        total_perf_llc_misses as f64 / total_perf_llc_refs as f64
                    } else {
                        0.0
                    };
                    println!(
                        "WORKERS Q{} count={} index_chunks_total={} index_bytes_est_total={} group_state_cap_total={} arena_reserved_total={} arena_peak_total={} perf_cycles_total={} perf_instructions_total={} perf_cache_misses_total={} perf_llc_refs_total={} perf_llc_misses_total={} perf_llc_miss_rate={:.6} plan_timing_chunks_total={} plan_ms_decode_total={:.2} plan_ms_filters_total={:.2} plan_ms_aggs_total={:.2} plan_ms_group_total={:.2} plan_ms_rows_total={:.2} plan_ms_total={:.2} plan_wall_est_ms={:.2}",
                        query.index,
                        stats.len(),
                        total_index_chunks,
                        total_index_bytes,
                        total_group_cap
                        ,
                        total_arena_reserved,
                        total_arena_peak,
                        total_perf_cycles,
                        total_perf_instructions,
                        total_perf_cache_misses,
                        total_perf_llc_refs,
                        total_perf_llc_misses,
                        llc_miss_rate,
                        total_plan_timing_chunks,
                        total_plan_ms_decode,
                        total_plan_ms_filters,
                        total_plan_ms_aggs,
                        total_plan_ms_group,
                        total_plan_ms_rows,
                        total_plan_ms,
                        plan_wall_est_ms
                    );
                    for (idx, s) in stats.iter().enumerate() {
                        println!(
                            "WORKER Q{} id={} index_chunks={} index_entries_len={} index_entries_cap={} index_bytes_est={} rows_cap={} group_len={} group_cap={} group_keys_cap={} row_heap_cap={} row_order_ranks_cap={} arena_reserved={} arena_used={} arena_peak={} perf_available={} perf_cycles={} perf_instructions={} perf_cache_refs={} perf_cache_misses={} perf_llc_refs={} perf_llc_misses={} perf_l1d_misses={} perf_l2_misses={} plan_timing_chunks={} plan_ms_decode={:.2} plan_ms_filters={:.2} plan_ms_aggs={:.2} plan_ms_group={:.2} plan_ms_rows={:.2}",
                            query.index,
                            idx,
                            s.runtime_index_chunks,
                            s.runtime_index_entries_len,
                            s.runtime_index_entries_cap,
                            s.runtime_index_bytes_est,
                            s.plan_rows_cap,
                            s.plan_group_state_len,
                            s.plan_group_state_cap,
                            s.plan_group_keys_cap,
                            s.plan_row_heap_cap,
                            s.plan_row_order_ranks_cap,
                            s.arena_reserved_bytes,
                            s.arena_used_bytes,
                            s.arena_peak_bytes,
                            s.perf_available,
                            s.perf_cycles,
                            s.perf_instructions,
                            s.perf_cache_refs,
                            s.perf_cache_misses,
                            s.perf_llc_refs,
                            s.perf_llc_misses,
                            s.perf_l1d_misses,
                            s.perf_l2_misses,
                            s.plan_timing_chunks,
                            s.plan_ms_decode,
                            s.plan_ms_filters,
                            s.plan_ms_aggs,
                            s.plan_ms_group,
                            s.plan_ms_rows,
                        );
                    }
                }
                Err(err) => println!("WORKERS Q{} error {}", query.index, err),
            }
        }
    }
    if std::env::var("WCOL_BENCH_IO_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        let stats = runtime.read_io_stats();
        let duplicate_ratio = if stats.total_requests > 0 {
            1.0 - (stats.unique_requests as f64 / stats.total_requests as f64)
        } else {
            0.0
        };
        let overlap_unique_ratio = if stats.unique_requests > 0 {
            stats.overlap_unique_requests as f64 / stats.unique_requests as f64
        } else {
            0.0
        };
        println!(
            "IO total_requests={} unique_requests={} duplicate_ratio={:.6} overlap_unique_requests={} overlap_unique_ratio={:.6} cache_hits={} cache_misses={} bytes_requested={} bytes_from_disk={} cache_entries={} cache_used_bytes={} cache_capacity_bytes={}",
            stats.total_requests,
            stats.unique_requests,
            duplicate_ratio,
            stats.overlap_unique_requests,
            overlap_unique_ratio,
            stats.cache_hits,
            stats.cache_misses,
            stats.bytes_requested,
            stats.bytes_from_disk,
            stats.cache_entries,
            stats.cache_used_bytes,
            stats.cache_capacity_bytes
        );
    }
    Ok(())
}

enum BenchIsolationMode {
    None,
    PerRun,
    Adaptive,
}

fn bench_isolation_mode() -> BenchIsolationMode {
    let raw = std::env::var("WCOL_BENCH_ISOLATE_PROCESS").unwrap_or_default();
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "0" || normalized == "false" || normalized == "off" {
        return BenchIsolationMode::None;
    }
    if normalized == "adaptive" {
        return BenchIsolationMode::Adaptive;
    }
    BenchIsolationMode::PerRun
}

fn run_bench_cmd_isolated(
    file: &Path,
    queries: &[QuerySpec],
    runs: usize,
    warmup: usize,
    workers: usize,
) -> Result<()> {
    let exe = std::env::current_exe().context("resolving current executable")?;
    for query in queries {
        let mut samples = Vec::with_capacity(runs);
        let mut failed: Option<String> = None;
        for idx in 0..(warmup + runs) {
            let started = Instant::now();
            match run_query_subprocess(&exe, file, &query.sql, workers) {
                Ok(()) => {
                    let ms = started.elapsed().as_secs_f64() * 1000.0;
                    if idx >= warmup {
                        samples.push(ms);
                    }
                }
                Err(err) => {
                    failed = Some(err.to_string());
                    break;
                }
            }
        }
        if let Some(err) = failed {
            println!("Q{} error {}", query.index, err);
            continue;
        }
        let stats = summarize(&samples).ok_or_else(|| anyhow!("no samples collected"))?;
        println!(
            "Q{} mean={:.2}ms p50={:.2}ms p95={:.2}ms min={:.2}ms max={:.2}ms",
            query.index, stats.mean, stats.p50, stats.p95, stats.min, stats.max
        );
    }
    if std::env::var("WCOL_BENCH_IO_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        println!("IO unavailable in process-isolated mode");
    }
    if std::env::var("WCOL_BENCH_MEM_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        println!("MEM unavailable in process-isolated mode");
    }
    Ok(())
}

fn run_query_subprocess(exe: &Path, file: &Path, sql: &str, workers: usize) -> Result<()> {
    let output = Command::new(exe)
        .arg("query")
        .arg("--file")
        .arg(file)
        .arg("--sql")
        .arg(sql)
        .arg("--workers")
        .arg(workers.to_string())
        .arg("--format")
        .arg("summary")
        .output()
        .context("spawning isolated query process")?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "isolated query failed exit={:?} stderr_tail={} stdout_tail={}",
        output.status.code(),
        tail_text(&output.stderr, 240),
        tail_text(&output.stdout, 240),
    ))
}

pub(crate) fn run_bench_worker_cmd(file: &Path) -> Result<()> {
    let runtime = NativeRuntime::open(file)
        .with_context(|| format!("opening wcol file {}", file.display()))?;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(trimmed)
            .with_context(|| "decoding bench worker request JSON".to_string())?;
        let sql = request
            .get("sql")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("bench worker request missing sql"))?;
        let workers = request
            .get("workers")
            .and_then(|v| v.as_u64())
            .unwrap_or(1)
            .max(1) as usize;
        let response = match runtime.query_sql_with_workers(sql, workers) {
            Ok(_) => json!({"ok": true, "rss_kb": read_proc_mem_kb().rss_kb}),
            Err(err) => {
                json!({"ok": false, "error": err.to_string(), "rss_kb": read_proc_mem_kb().rss_kb})
            }
        };
        writeln!(stdout, "{}", response)?;
        stdout.flush()?;
    }
    Ok(())
}

fn run_bench_cmd_adaptive(
    file: &Path,
    queries: &[QuerySpec],
    runs: usize,
    warmup: usize,
    workers: usize,
) -> Result<()> {
    let exe = std::env::current_exe().context("resolving current executable")?;
    let max_queries = std::env::var("WCOL_BENCH_ADAPTIVE_MAX_QUERIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8)
        .max(1);
    let max_rss_kb = std::env::var("WCOL_BENCH_ADAPTIVE_MAX_RSS_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24 * 1024)
        .saturating_mul(1024);
    println!(
        "isolation=adaptive max_queries={} max_rss_mb={}",
        max_queries,
        max_rss_kb / 1024
    );

    let mut worker: Option<BenchWorkerProcess> = None;
    for query in queries {
        let heavy = is_high_cardinality_query(&query.sql);
        let query_max = if heavy { 1 } else { max_queries };
        let mut samples = Vec::with_capacity(runs);
        let mut failed: Option<String> = None;
        for idx in 0..(warmup + runs) {
            if worker
                .as_ref()
                .map(|w| w.should_recycle(query_max, max_rss_kb))
                .unwrap_or(true)
            {
                if let Some(mut old) = worker.take() {
                    old.shutdown();
                }
                worker = Some(BenchWorkerProcess::start(&exe, file)?);
            }
            let started = Instant::now();
            let run_result = worker
                .as_mut()
                .ok_or_else(|| anyhow!("adaptive worker unavailable"))?
                .run_query(&query.sql, workers);
            if heavy {
                if let Some(mut old) = worker.take() {
                    old.shutdown();
                }
            }
            match run_result {
                Ok(()) => {
                    let ms = started.elapsed().as_secs_f64() * 1000.0;
                    if idx >= warmup {
                        samples.push(ms);
                    }
                }
                Err(err) => {
                    failed = Some(err.to_string());
                    if let Some(mut old) = worker.take() {
                        old.shutdown();
                    }
                    break;
                }
            }
        }
        if let Some(err) = failed {
            println!("Q{} error {}", query.index, err);
            continue;
        }
        let stats = summarize(&samples).ok_or_else(|| anyhow!("no samples collected"))?;
        println!(
            "Q{} mean={:.2}ms p50={:.2}ms p95={:.2}ms min={:.2}ms max={:.2}ms",
            query.index, stats.mean, stats.p50, stats.p95, stats.min, stats.max
        );
    }
    if let Some(mut old) = worker.take() {
        old.shutdown();
    }
    if std::env::var("WCOL_BENCH_IO_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        println!("IO unavailable in adaptive process-isolated mode");
    }
    if std::env::var("WCOL_BENCH_MEM_STATS")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        println!("MEM unavailable in adaptive process-isolated mode");
    }
    Ok(())
}

fn is_high_cardinality_query(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let has_group = lower.contains("group by");
    let has_order = lower.contains("order by");
    let has_distinct = lower.contains("count(distinct") || lower.contains("approx_count_distinct(");
    let string_key = [
        "url",
        "title",
        "searchphrase",
        "referer",
        "params",
        "utm",
        "originalurl",
    ]
    .iter()
    .any(|name| lower.contains(name));
    (has_group && string_key)
        || (has_group && has_order && string_key)
        || (has_group && has_distinct)
}

struct BenchWorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    served: usize,
    last_rss_kb: u64,
}

impl BenchWorkerProcess {
    fn start(exe: &Path, file: &Path) -> Result<Self> {
        let mut child = Command::new(exe)
            .arg("bench-worker")
            .arg("--file")
            .arg(file)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("starting adaptive bench worker")?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open worker stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to open worker stdout"))?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            served: 0,
            last_rss_kb: 0,
        })
    }

    fn run_query(&mut self, sql: &str, workers: usize) -> Result<()> {
        let request = json!({
            "sql": sql,
            "workers": workers.max(1),
        });
        writeln!(self.stdin, "{}", request).context("writing request to adaptive bench worker")?;
        self.stdin
            .flush()
            .context("flushing adaptive bench worker request")?;

        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .context("reading response from adaptive bench worker")?;
        if read == 0 {
            return Err(anyhow!("adaptive bench worker closed stdout unexpectedly"));
        }
        let response: Value =
            serde_json::from_str(line.trim()).context("parsing adaptive bench worker response")?;
        self.last_rss_kb = response.get("rss_kb").and_then(|v| v.as_u64()).unwrap_or(0);
        self.served = self.served.saturating_add(1);
        if response
            .get("ok")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let err = response
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown bench worker error");
        Err(anyhow!("adaptive bench worker query failed: {err}"))
    }

    fn should_recycle(&self, max_queries: usize, max_rss_kb: u64) -> bool {
        (max_queries > 0 && self.served >= max_queries)
            || (max_rss_kb > 0 && self.last_rss_kb >= max_rss_kb)
    }

    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tail_text(bytes: &[u8], max_chars: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    let chars = text.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        return text.trim().to_string();
    }
    chars[chars.len() - max_chars..]
        .iter()
        .collect::<String>()
        .trim()
        .to_string()
}

#[derive(Default, Clone, Copy)]
struct ProcMemKb {
    rss_kb: u64,
    hwm_kb: u64,
    swap_kb: u64,
}

fn read_proc_mem_kb() -> ProcMemKb {
    #[cfg(target_os = "linux")]
    {
        let mut out = ProcMemKb::default();
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(v) = parse_status_kb(line, "VmRSS:") {
                    out.rss_kb = v;
                } else if let Some(v) = parse_status_kb(line, "VmHWM:") {
                    out.hwm_kb = v;
                } else if let Some(v) = parse_status_kb(line, "VmSwap:") {
                    out.swap_kb = v;
                }
            }
        }
        out
    }
    #[cfg(not(target_os = "linux"))]
    {
        ProcMemKb::default()
    }
}

#[cfg(target_os = "linux")]
fn parse_status_kb(line: &str, key: &str) -> Option<u64> {
    if !line.starts_with(key) {
        return None;
    }
    line.split_whitespace().nth(1)?.parse::<u64>().ok()
}

fn saturating_diff(a: u64, b: u64) -> u64 {
    a.saturating_sub(b)
}

fn diff_i64(a: u64, b: u64) -> i64 {
    a as i64 - b as i64
}

#[derive(Default, Clone, Copy)]
struct ProcSmapsRollupKb {
    rss_kb: u64,
    pss_kb: u64,
    anon_kb: u64,
    private_dirty_kb: u64,
    shmem_kb: u64,
}

fn read_proc_smaps_rollup_kb() -> ProcSmapsRollupKb {
    #[cfg(target_os = "linux")]
    {
        let mut out = ProcSmapsRollupKb::default();
        if let Ok(smaps) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            for line in smaps.lines() {
                if let Some(v) = parse_status_kb(line, "Rss:") {
                    out.rss_kb = v;
                } else if let Some(v) = parse_status_kb(line, "Pss:") {
                    out.pss_kb = v;
                } else if let Some(v) = parse_status_kb(line, "Anonymous:") {
                    out.anon_kb = v;
                } else if let Some(v) = parse_status_kb(line, "Private_Dirty:") {
                    out.private_dirty_kb = v;
                } else if let Some(v) = parse_status_kb(line, "ShmemPmdMapped:") {
                    out.shmem_kb = v;
                }
            }
        }
        out
    }
    #[cfg(not(target_os = "linux"))]
    {
        ProcSmapsRollupKb::default()
    }
}

#[derive(Default, Clone, Copy)]
struct MallocStats {
    uordblks: u64,
    fordblks: u64,
    hblkhd: u64,
    arena: u64,
}

fn read_malloc_stats() -> MallocStats {
    #[cfg(target_os = "linux")]
    {
        #[repr(C)]
        struct Mallinfo2 {
            arena: usize,
            ordblks: usize,
            smblks: usize,
            hblks: usize,
            hblkhd: usize,
            usmblks: usize,
            fsmblks: usize,
            uordblks: usize,
            fordblks: usize,
            keepcost: usize,
        }

        unsafe extern "C" {
            fn mallinfo2() -> Mallinfo2;
        }

        let mi = unsafe { mallinfo2() };
        MallocStats {
            uordblks: mi.uordblks as u64,
            fordblks: mi.fordblks as u64,
            hblkhd: mi.hblkhd as u64,
            arena: mi.arena as u64,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        MallocStats::default()
    }
}

#[cfg(target_os = "linux")]
fn malloc_trim_supported() -> bool {
    unsafe {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        malloc_trim(0) != 0
    }
}

#[cfg(not(target_os = "linux"))]
fn malloc_trim_supported() -> bool {
    false
}
