use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use wcol_decoder::native::{AggregateStats, QueryResult};

use crate::cli::{NativeExecOpts, QuerySpec};

pub(crate) fn extract_queries(path: &Path) -> Result<Vec<QuerySpec>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading SQL file {}", path.display()))?;
    let is_markdown = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "md" | "markdown"))
        .unwrap_or(false);
    let mut out = Vec::new();
    if is_markdown {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("SELECT ") {
                out.push(QuerySpec {
                    index: out.len() + 1,
                    sql: trimmed.to_string(),
                });
            }
        }
    } else {
        for stmt in content.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(QuerySpec {
                index: out.len() + 1,
                sql: format!("{trimmed};"),
            });
        }
    }
    Ok(out)
}

pub(crate) fn result_to_json(result: &QueryResult) -> Value {
    let aggregates = result
        .aggregates
        .iter()
        .map(|(name, stats)| {
            (
                name.clone(),
                json!({
                    "count": stats.count,
                    "sum": stats.sum,
                    "min": stats.min,
                    "max": stats.max,
                    "mean": stats.mean,
                }),
            )
        })
        .collect::<serde_json::Map<String, Value>>();
    let groups = result.groups.as_ref().map(|group| {
        json!({
            "keys": group.keys,
            "keys2": group.keys2,
            "keyInfo": group.key_info.iter().map(|k| json!({
                "colId": k.col_id,
                "physicalType": k.physical_type,
                "flags": k.flags,
            })).collect::<Vec<_>>(),
            "aggs": group.aggs.iter().map(|a| json!({
                "colId": a.col_id,
                "kind": a.kind,
            })).collect::<Vec<_>>(),
            "values": group.values.iter().map(|row| {
                row.iter().map(|stats| json!({
                    "count": stats.count,
                    "sum": stats.sum,
                    "min": stats.min,
                    "max": stats.max,
                    "mean": stats.mean,
                })).collect::<Vec<_>>()
            }).collect::<Vec<_>>(),
        })
    });
    json!({
        "rows": result.rows,
        "aggregates": Value::Object(aggregates),
        "groups": groups,
    })
}

pub(crate) fn default_output_path(input: &Path) -> PathBuf {
    input.with_extension("wcol")
}

pub(crate) struct SummaryStats {
    pub(crate) mean: f64,
    pub(crate) p50: f64,
    pub(crate) p95: f64,
    pub(crate) min: f64,
    pub(crate) max: f64,
}

pub(crate) fn summarize(samples: &[f64]) -> Option<SummaryStats> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    Some(SummaryStats {
        mean,
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
        min: *sorted.first().unwrap_or(&0.0),
        max: *sorted.last().unwrap_or(&0.0),
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let w = rank - lo as f64;
    sorted[lo] * (1.0 - w) + sorted[hi] * w
}

pub(crate) fn parse_index_filter(value: &str) -> Result<std::collections::BTreeSet<usize>> {
    let mut out = std::collections::BTreeSet::new();
    for part in value.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((a, b)) = trimmed.split_once('-') {
            let start = a.parse::<usize>()?;
            let end = b.parse::<usize>()?;
            if end < start {
                return Err(anyhow!("invalid range {trimmed}"));
            }
            for idx in start..=end {
                out.insert(idx);
            }
        } else {
            out.insert(trimmed.parse::<usize>()?);
        }
    }
    Ok(out)
}

pub(crate) fn agg_value_by_kind(kind: u8, stats: &AggregateStats) -> f64 {
    match kind {
        0 | 5 => stats.count as f64,
        1 => stats.sum,
        2 => stats.mean,
        3 => stats.min,
        4 => stats.max,
        6 => stats.mean,
        _ => stats.mean,
    }
}

pub(crate) fn agg_value_by_name(name: &str, stats: &AggregateStats) -> f64 {
    if name == "count_star()" || name.starts_with("count(") {
        stats.count as f64
    } else if name.starts_with("sum(") {
        stats.sum
    } else if name.starts_with("avg(") {
        stats.mean
    } else if name.starts_with("min(") {
        stats.min
    } else if name.starts_with("max(") {
        stats.max
    } else if name.starts_with("approx_count_distinct(") {
        stats.mean
    } else {
        stats.mean
    }
}

pub(crate) fn apply_native_exec_opts(opts: &NativeExecOpts) {
    set_env_u64("WCOL_THREAD_ARENA_BASE_MB", opts.arena_base_mb);
    set_env_u64("WCOL_THREAD_ARENA_GROW_MB", opts.arena_grow_mb);
    set_env_u64("WCOL_THREAD_ARENA_MAX_MB", opts.arena_max_mb);
    set_env_str(
        "WCOL_QUERY_GLOBAL_CAP_MB",
        opts.arena_global_cap_mb.as_deref(),
    );
    set_env_str(
        "WCOL_THREAD_ARENA_RELEASE",
        opts.arena_release.map(|v| v.as_env()),
    );
    set_env_u64("WCOL_THREAD_ARENA_KEEP_UP_TO_MB", opts.arena_keep_up_to_mb);
    set_env_str(
        "WCOL_QUERY_RETAINED_GLOBAL_CAP_MB",
        opts.arena_retained_global_cap_mb.as_deref(),
    );
    set_env_u32(
        "WCOL_QUERY_RETAINED_IDLE_DECAY_QUERIES",
        opts.arena_retained_idle_decay_queries,
    );
    set_env_u64("WCOL_STRING_WINDOW_MB", opts.string_window_mb);
    set_env_usize("WCOL_GROUP_PARTITIONS", opts.group_partitions);
    set_env_usize("WCOL_PARTITION_COUNT", opts.partition_count);
    set_env_usize("WCOL_MERGE_WORKERS", opts.merge_workers);
    set_env_usize("WCOL_REDUCE_WORKERS", opts.reduce_workers);
    set_env_str("WCOL_GROUP_ENGINE", opts.group_engine.map(|v| v.as_env()));
    set_env_str(
        "WCOL_SCAN_PARTITION_QUEUE_MB",
        opts.scan_partition_queue_mb.as_deref(),
    );
    set_env_u64("WCOL_PARTITION_SORT_CHUNK_MB", opts.partition_sort_chunk_mb);
    set_env_str("WCOL_MERGE_KEYS", opts.merge_keys.map(|v| v.as_env()));
    set_env_str(
        "WCOL_CACHE_COUNTERS",
        opts.cache_counters.map(|v| v.as_env()),
    );
}

fn set_env_str(name: &str, value: Option<&str>) {
    if let Some(v) = value {
        std::env::set_var(name, v);
    }
}

fn set_env_u32(name: &str, value: Option<u32>) {
    if let Some(v) = value {
        std::env::set_var(name, v.to_string());
    }
}

fn set_env_u64(name: &str, value: Option<u64>) {
    if let Some(v) = value {
        std::env::set_var(name, v.to_string());
    }
}

fn set_env_usize(name: &str, value: Option<usize>) {
    if let Some(v) = value {
        std::env::set_var(name, v.to_string());
    }
}
