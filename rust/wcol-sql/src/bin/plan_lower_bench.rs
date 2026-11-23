//! Native planner benchmark (same work as wasm `wcol_sql_plan_and_lower`).

use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

use wcol::open_plan;
use wcol_sql::native::catalog_from_wcol_leaves;
use wcol_sql::{lower_to_ingest_spec, plan_sql};

const QUERIES: &[(&str, &str)] = &[
    ("Q3", "SELECT AVG(UserID) FROM hits"),
    (
        "Q12",
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "Q8",
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    ),
];

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits_subset_500k.parquet")
        });
    if !path.is_file() {
        return Err(format!("missing parquet: {}", path.display()));
    }
    let warmup: usize = std::env::var("WCOL_PLANNER_WARMUP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let iters: usize = std::env::var("WCOL_PLANNER_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let (_f, plan) = open_plan(&path).map_err(|e| e.to_string())?;
    let catalog = catalog_from_wcol_leaves("hits", &plan.leaves);
    eprintln!("catalog: hits, {} columns", plan.leaves.len());
    eprintln!("warmup={warmup} iters={iters}\n");

    let mut out = io::stdout().lock();
    writeln!(out, "query     mean_ms   p50_ms   p95_ms").map_err(|e| e.to_string())?;
    for (name, sql) in QUERIES {
        let stats = bench_query(sql, &catalog, warmup, iters)?;
        writeln!(
            out,
            "{name:<8}  {:>8.2}  {:>7.2}  {:>7.2}",
            stats.mean_ms, stats.p50_ms, stats.p95_ms
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

struct Stats {
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
}

fn bench_query(
    sql: &str,
    catalog: &wcol_sql::WcolCatalog,
    warmup: usize,
    iters: usize,
) -> Result<Stats, String> {
    let mut run_one = || {
        let planned = plan_sql(sql, catalog).map_err(|e| e.to_string())?;
        let _spec = lower_to_ingest_spec(planned.optimized.as_ref(), catalog).map_err(|e| e.to_string())?;
        Ok::<(), String>(())
    };
    for _ in 0..warmup {
        run_one()?;
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        run_one()?;
        samples.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sum: f64 = samples.iter().sum();
    Ok(Stats {
        mean_ms: sum / samples.len() as f64,
        p50_ms: percentile(&samples, 0.5),
        p95_ms: percentile(&samples, 0.95),
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let i = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[i.min(sorted.len() - 1)]
}
