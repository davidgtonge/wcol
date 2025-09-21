use clap::{Parser, ValueEnum};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Engine {
    ThreadLocalMerge,
    PartitionHash,
    PartitionSort,
    PartitionHashDirect,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Engine::ThreadLocalMerge => f.write_str("thread-local-merge"),
            Engine::PartitionHash => f.write_str("partition-hash"),
            Engine::PartitionSort => f.write_str("partition-sort"),
            Engine::PartitionHashDirect => f.write_str("partition-hash-direct"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "wcol-synth-groupby",
    about = "Synthetic GROUP BY scaling runner (separate from wcol query engine)",
    version
)]
struct Cli {
    #[arg(long, value_enum, default_value_t = Engine::PartitionSort)]
    engine: Engine,

    #[arg(long, default_value_t = 50_000_000)]
    rows: u64,

    #[arg(long, default_value_t = 20_000_000)]
    distinct_keys: u64,

    #[arg(long)]
    workers: Option<usize>,

    #[arg(long)]
    reduce_workers: Option<usize>,

    #[arg(long)]
    partitions: Option<usize>,

    #[arg(long, default_value_t = 3)]
    runs: usize,

    #[arg(long, default_value_t = 1)]
    warmup: usize,

    #[arg(long)]
    sweep_workers: Option<String>,

    #[arg(long, default_value_t = 0.0)]
    hot_probability: f64,

    #[arg(long, default_value_t = 1024)]
    hot_keys: u64,

    #[arg(long, default_value_t = 1)]
    seed: u64,

    #[arg(long)]
    output_csv: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct Workload {
    rows: u64,
    distinct_keys: u64,
    hot_threshold: u64,
    hot_keys: u64,
    seed: u64,
}

#[derive(Debug, Clone)]
struct RunConfig {
    engine: Engine,
    workers: usize,
    reduce_workers: usize,
    partitions: usize,
    workload: Workload,
}

#[derive(Debug, Clone, Copy)]
struct Record {
    key: u64,
    count: u64,
}

#[derive(Debug, Clone)]
struct RunMetrics {
    workers: usize,
    reduce_workers: usize,
    partitions: usize,
    mean_total_ms: f64,
    mean_scan_emit_ms: f64,
    mean_gather_ms: f64,
    mean_reduce_ms: f64,
    throughput_mrows_s: f64,
    groups: u64,
    row_sum: u64,
    speedup_vs_base: f64,
    efficiency_vs_base: f64,
}

#[derive(Debug, Clone, Copy)]
struct IterMetrics {
    total_ms: f64,
    scan_emit_ms: f64,
    gather_ms: f64,
    reduce_ms: f64,
    groups: u64,
    row_sum: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PartitionSummary {
    groups: u64,
    row_sum: u64,
    records: u64,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let default_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);

    let mut worker_cases = if let Some(csv) = &cli.sweep_workers {
        parse_workers_csv(csv)?
    } else {
        vec![cli.workers.unwrap_or(default_workers)]
    };

    worker_cases.sort_unstable();
    worker_cases.dedup();

    if worker_cases.is_empty() {
        anyhow::bail!("no worker counts resolved");
    }

    let hot_probability = cli.hot_probability.clamp(0.0, 1.0);
    let hot_threshold = (hot_probability * (u64::MAX as f64)) as u64;

    let workload = Workload {
        rows: cli.rows,
        distinct_keys: cli.distinct_keys.max(1),
        hot_threshold,
        hot_keys: cli.hot_keys.max(1).min(cli.distinct_keys.max(1)),
        seed: cli.seed,
    };

    println!(
        "SYNTH_CONFIG engine={} rows={} distinct_keys={} hot_probability={} hot_keys={} runs={} warmup={} worker_cases={}",
        cli.engine,
        workload.rows,
        workload.distinct_keys,
        hot_probability,
        workload.hot_keys,
        cli.runs,
        cli.warmup,
        worker_cases
            .iter()
            .map(|w| w.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    let mut metrics = Vec::with_capacity(worker_cases.len());
    for &workers in &worker_cases {
        let partitions = cli.partitions.unwrap_or_else(|| auto_partitions(workers));
        let reduce_workers = cli
            .reduce_workers
            .unwrap_or(workers)
            .max(1)
            .min(worker_cases.iter().copied().max().unwrap_or(workers));

        let cfg = RunConfig {
            engine: cli.engine,
            workers: workers.max(1),
            reduce_workers,
            partitions: partitions.max(1).next_power_of_two(),
            workload: workload.clone(),
        };

        let run = run_case(&cfg, cli.runs, cli.warmup)?;
        metrics.push(run);
    }

    if metrics.is_empty() {
        anyhow::bail!("no measured metrics produced");
    }

    let base_total_ms = metrics[0].mean_total_ms;
    let base_workers = metrics[0].workers as f64;

    for m in &mut metrics {
        m.speedup_vs_base = base_total_ms / m.mean_total_ms;
        let worker_scale = (m.workers as f64) / base_workers;
        m.efficiency_vs_base = if worker_scale > 0.0 {
            m.speedup_vs_base / worker_scale
        } else {
            0.0
        };
    }

    print_summary_table(cli.engine, &metrics);

    if let Some(path) = &cli.output_csv {
        write_csv(path, cli.engine, &metrics)?;
        println!("SYNTH_CSV path={}", path.display());
    }

    Ok(())
}

fn run_case(cfg: &RunConfig, runs: usize, warmup: usize) -> anyhow::Result<RunMetrics> {
    let mut measured = Vec::with_capacity(runs);
    let total_iters = warmup + runs;
    if total_iters == 0 {
        anyhow::bail!("runs + warmup must be > 0");
    }

    for iter in 0..total_iters {
        let metrics = match cfg.engine {
            Engine::ThreadLocalMerge => run_thread_local_merge(cfg)?,
            Engine::PartitionHash => run_partition_owner(cfg, false)?,
            Engine::PartitionSort => run_partition_owner(cfg, true)?,
            Engine::PartitionHashDirect => run_partition_owner_direct_hash(cfg)?,
        };

        if metrics.row_sum != cfg.workload.rows {
            anyhow::bail!(
                "row sum mismatch: expected {}, got {}",
                cfg.workload.rows,
                metrics.row_sum
            );
        }

        if iter >= warmup {
            measured.push(metrics);
        }
    }

    if measured.is_empty() {
        anyhow::bail!("no measured iterations; increase --runs");
    }

    let first_groups = measured[0].groups;
    for m in &measured {
        if m.groups != first_groups {
            anyhow::bail!(
                "group count unstable across runs: {} vs {}",
                first_groups,
                m.groups
            );
        }
    }

    let n = measured.len() as f64;
    let mean_total_ms = measured.iter().map(|m| m.total_ms).sum::<f64>() / n;
    let mean_scan_emit_ms = measured.iter().map(|m| m.scan_emit_ms).sum::<f64>() / n;
    let mean_gather_ms = measured.iter().map(|m| m.gather_ms).sum::<f64>() / n;
    let mean_reduce_ms = measured.iter().map(|m| m.reduce_ms).sum::<f64>() / n;

    let throughput_mrows_s = if mean_total_ms > 0.0 {
        (cfg.workload.rows as f64 / 1_000_000.0) / (mean_total_ms / 1_000.0)
    } else {
        0.0
    };

    Ok(RunMetrics {
        workers: cfg.workers,
        reduce_workers: cfg.reduce_workers,
        partitions: cfg.partitions,
        mean_total_ms,
        mean_scan_emit_ms,
        mean_gather_ms,
        mean_reduce_ms,
        throughput_mrows_s,
        groups: first_groups,
        row_sum: cfg.workload.rows,
        speedup_vs_base: 1.0,
        efficiency_vs_base: 1.0,
    })
}

fn run_thread_local_merge(cfg: &RunConfig) -> anyhow::Result<IterMetrics> {
    let total_start = Instant::now();
    let scan_start = Instant::now();

    let pool = ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build worker pool: {e}"))?;

    let locals: Vec<HashMap<u64, u64>> = pool.install(|| {
        (0..cfg.workers)
            .into_par_iter()
            .map(|worker| {
                let (start, end) = worker_row_range(cfg.workload.rows, cfg.workers, worker);
                let expected = (end - start).max(1) as usize;
                let mut map = HashMap::<u64, u64>::with_capacity(expected / 8 + 1024);

                let mut row = start;
                while row < end {
                    let key = synth_key(row, &cfg.workload);
                    *map.entry(key).or_insert(0) += 1;
                    row += 1;
                }

                map
            })
            .collect()
    });

    let scan_emit_ms = scan_start.elapsed().as_secs_f64() * 1_000.0;

    let gather_start = Instant::now();
    let mut final_map =
        HashMap::<u64, u64>::with_capacity(cfg.workload.distinct_keys as usize / 2 + 1);
    for local in locals {
        for (k, v) in local {
            *final_map.entry(k).or_insert(0) += v;
        }
    }
    let gather_ms = gather_start.elapsed().as_secs_f64() * 1_000.0;

    let reduce_ms = 0.0;
    let row_sum = final_map.values().copied().sum::<u64>();
    let groups = final_map.len() as u64;
    let total_ms = total_start.elapsed().as_secs_f64() * 1_000.0;

    Ok(IterMetrics {
        total_ms,
        scan_emit_ms,
        gather_ms,
        reduce_ms,
        groups,
        row_sum,
    })
}

fn run_partition_owner(cfg: &RunConfig, use_sort: bool) -> anyhow::Result<IterMetrics> {
    let total_start = Instant::now();
    let scan_start = Instant::now();

    let scan_pool = ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build scan pool: {e}"))?;

    let partitions = cfg.partitions;
    let per_worker: Vec<Vec<Vec<Record>>> = scan_pool.install(|| {
        (0..cfg.workers)
            .into_par_iter()
            .map(|worker| {
                let (start, end) = worker_row_range(cfg.workload.rows, cfg.workers, worker);
                let expected_rows = (end - start).max(1) as usize;
                let per_partition_reserve = expected_rows / partitions + 64;
                let mut parts = (0..partitions)
                    .map(|_| Vec::<Record>::with_capacity(per_partition_reserve))
                    .collect::<Vec<_>>();

                let mut row = start;
                while row < end {
                    let key = synth_key(row, &cfg.workload);
                    let p = partition_for_key(key, partitions);
                    parts[p].push(Record { key, count: 1 });
                    row += 1;
                }

                parts
            })
            .collect()
    });

    let scan_emit_ms = scan_start.elapsed().as_secs_f64() * 1_000.0;

    let gather_start = Instant::now();
    let mut by_partition = (0..partitions)
        .map(|_| Vec::<Record>::new())
        .collect::<Vec<_>>();
    for worker_parts in per_worker {
        for (p, mut part) in worker_parts.into_iter().enumerate() {
            by_partition[p].append(&mut part);
        }
    }
    let gather_ms = gather_start.elapsed().as_secs_f64() * 1_000.0;

    let reduce_start = Instant::now();
    let reduce_threads = cfg.reduce_workers.max(1);
    let reduce_pool = ThreadPoolBuilder::new()
        .num_threads(reduce_threads)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build reduce pool: {e}"))?;

    let partition_summaries: Vec<PartitionSummary> = reduce_pool.install(|| {
        by_partition
            .into_par_iter()
            .map(|mut part| {
                if part.is_empty() {
                    return PartitionSummary::default();
                }

                if use_sort {
                    part.sort_unstable_by_key(|r| r.key);
                    let mut groups = 0_u64;
                    let mut row_sum = 0_u64;
                    let mut i = 0_usize;
                    while i < part.len() {
                        let key = part[i].key;
                        let mut count = 0_u64;
                        while i < part.len() && part[i].key == key {
                            count += part[i].count;
                            i += 1;
                        }
                        groups += 1;
                        row_sum += count;
                    }

                    PartitionSummary {
                        groups,
                        row_sum,
                        records: part.len() as u64,
                    }
                } else {
                    let mut map = HashMap::<u64, u64>::with_capacity(part.len() / 4 + 64);
                    for rec in part {
                        *map.entry(rec.key).or_insert(0) += rec.count;
                    }
                    let row_sum = map.values().copied().sum::<u64>();
                    PartitionSummary {
                        groups: map.len() as u64,
                        row_sum,
                        records: row_sum,
                    }
                }
            })
            .collect()
    });

    let reduce_ms = reduce_start.elapsed().as_secs_f64() * 1_000.0;
    let total_ms = total_start.elapsed().as_secs_f64() * 1_000.0;

    let row_sum = partition_summaries.iter().map(|s| s.row_sum).sum::<u64>();
    let _records = partition_summaries.iter().map(|s| s.records).sum::<u64>();
    let groups = partition_summaries.iter().map(|s| s.groups).sum::<u64>();

    Ok(IterMetrics {
        total_ms,
        scan_emit_ms,
        gather_ms,
        reduce_ms,
        groups,
        row_sum,
    })
}

fn run_partition_owner_direct_hash(cfg: &RunConfig) -> anyhow::Result<IterMetrics> {
    let total_start = Instant::now();
    let scan_start = Instant::now();

    let scan_pool = ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build scan pool: {e}"))?;

    let partitions = cfg.partitions;
    let per_worker: Vec<Vec<Vec<Record>>> = scan_pool.install(|| {
        (0..cfg.workers)
            .into_par_iter()
            .map(|worker| {
                let (start, end) = worker_row_range(cfg.workload.rows, cfg.workers, worker);
                let expected_rows = (end - start).max(1) as usize;
                let per_partition_reserve = expected_rows / partitions + 64;
                let mut parts = (0..partitions)
                    .map(|_| Vec::<Record>::with_capacity(per_partition_reserve))
                    .collect::<Vec<_>>();

                let mut row = start;
                while row < end {
                    let key = synth_key(row, &cfg.workload);
                    let p = partition_for_key(key, partitions);
                    parts[p].push(Record { key, count: 1 });
                    row += 1;
                }
                parts
            })
            .collect()
    });

    let scan_emit_ms = scan_start.elapsed().as_secs_f64() * 1_000.0;
    let gather_ms = 0.0;

    let reduce_start = Instant::now();
    let reduce_threads = cfg.reduce_workers.max(1);
    let reduce_pool = ThreadPoolBuilder::new()
        .num_threads(reduce_threads)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build reduce pool: {e}"))?;

    let per_worker_ref = &per_worker;
    let partition_summaries: Vec<PartitionSummary> = reduce_pool.install(|| {
        (0..partitions)
            .into_par_iter()
            .map(|p| {
                let total_records = per_worker_ref
                    .iter()
                    .map(|worker_parts| worker_parts[p].len())
                    .sum::<usize>();
                if total_records == 0 {
                    return PartitionSummary::default();
                }

                let mut map = HashMap::<u64, u64>::with_capacity(total_records / 2 + 64);
                for worker_parts in per_worker_ref {
                    for rec in &worker_parts[p] {
                        *map.entry(rec.key).or_insert(0) += rec.count;
                    }
                }
                let row_sum = map.values().copied().sum::<u64>();
                PartitionSummary {
                    groups: map.len() as u64,
                    row_sum,
                    records: total_records as u64,
                }
            })
            .collect()
    });

    let reduce_ms = reduce_start.elapsed().as_secs_f64() * 1_000.0;
    let total_ms = total_start.elapsed().as_secs_f64() * 1_000.0;

    let row_sum = partition_summaries.iter().map(|s| s.row_sum).sum::<u64>();
    let groups = partition_summaries.iter().map(|s| s.groups).sum::<u64>();

    Ok(IterMetrics {
        total_ms,
        scan_emit_ms,
        gather_ms,
        reduce_ms,
        groups,
        row_sum,
    })
}

fn parse_workers_csv(csv: &str) -> anyhow::Result<Vec<usize>> {
    let mut out = Vec::new();
    for token in csv.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: usize = trimmed
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid worker count '{}': {e}", trimmed))?;
        if value == 0 {
            anyhow::bail!("worker count must be >= 1");
        }
        out.push(value);
    }
    Ok(out)
}

fn auto_partitions(workers: usize) -> usize {
    let base = (8 * workers).max(64);
    let pow2 = base.next_power_of_two();
    pow2.min(2048)
}

fn worker_row_range(total_rows: u64, workers: usize, worker: usize) -> (u64, u64) {
    let workers_u64 = workers as u64;
    let worker_u64 = worker as u64;
    let start = total_rows * worker_u64 / workers_u64;
    let end = total_rows * (worker_u64 + 1) / workers_u64;
    (start, end)
}

fn partition_for_key(key: u64, partitions: usize) -> usize {
    let mixed = splitmix64(key ^ 0x9E37_79B9_7F4A_7C15);
    (mixed as usize) & (partitions - 1)
}

fn synth_key(row: u64, cfg: &Workload) -> u64 {
    let base = splitmix64(row ^ cfg.seed);
    let hot_draw = splitmix64(base ^ 0xD1B5_4A32_D192_ED03);

    if cfg.hot_threshold > 0 && hot_draw <= cfg.hot_threshold {
        splitmix64(base ^ 0x94D0_49BB_1331_11EB) % cfg.hot_keys
    } else {
        splitmix64(base ^ 0xC6BC_2796_92B5_C323) % cfg.distinct_keys
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn print_summary_table(engine: Engine, metrics: &[RunMetrics]) {
    println!(
        "SYNTH_HEADER engine={} columns=workers,reduce_workers,partitions,mean_total_ms,scan_emit_ms,gather_ms,reduce_ms,throughput_mrows_s,groups,speedup_vs_base,efficiency_vs_base",
        engine
    );

    for m in metrics {
        println!(
            "SYNTH_RESULT workers={} reduce_workers={} partitions={} mean_total_ms={:.2} scan_emit_ms={:.2} gather_ms={:.2} reduce_ms={:.2} throughput_mrows_s={:.2} groups={} row_sum={} speedup_vs_base={:.3} efficiency_vs_base={:.3}",
            m.workers,
            m.reduce_workers,
            m.partitions,
            m.mean_total_ms,
            m.mean_scan_emit_ms,
            m.mean_gather_ms,
            m.mean_reduce_ms,
            m.throughput_mrows_s,
            m.groups,
            m.row_sum,
            m.speedup_vs_base,
            m.efficiency_vs_base
        );
    }
}

fn write_csv(path: &PathBuf, engine: Engine, metrics: &[RunMetrics]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "engine,workers,reduce_workers,partitions,mean_total_ms,scan_emit_ms,gather_ms,reduce_ms,throughput_mrows_s,groups,row_sum,speedup_vs_base,efficiency_vs_base"
    )?;

    for m in metrics {
        writeln!(
            writer,
            "{},{},{},{},{:.4},{:.4},{:.4},{:.4},{:.4},{},{},{:.6},{:.6}",
            engine,
            m.workers,
            m.reduce_workers,
            m.partitions,
            m.mean_total_ms,
            m.mean_scan_emit_ms,
            m.mean_gather_ms,
            m.mean_reduce_ms,
            m.throughput_mrows_s,
            m.groups,
            m.row_sum,
            m.speedup_vs_base,
            m.efficiency_vs_base
        )?;
    }

    writer.flush()?;
    Ok(())
}
