#!/usr/bin/env node
/**
 * Benchmark SQL parse + wcol lowering (no execution).
 *
 *   node scripts/bench-sql-planner-wasm.mjs          # tries release wasm (Node)
 *   node scripts/bench-sql-planner-wasm.mjs --native # same work, native release bin
 *
 * Default: release wasm32-unknown-unknown via wasm-bindgen (same target as V8/Workers).
 */
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

const useNative = process.argv.includes("--native");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const rustDir = resolve(root, "rust");
const pkgJs = resolve(rustDir, "wcol-sql-wasm/pkg/wcol_sql_wasm.cjs");

const WARMUP = process.env.WCOL_PLANNER_WARMUP ?? "10";
const ITERS = process.env.WCOL_PLANNER_ITERS ?? "100";

const QUERIES = {
  Q3: "SELECT AVG(UserID) FROM hits",
  Q12: "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
  Q8: "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
};

function runNative() {
  const parquet = resolve(root, "data/hits_subset_500k.parquet");
  const r = spawnSync(
    "cargo",
    [
      "run",
      "--release",
      "-p",
      "wcol-sql",
      "--features",
      "native,datafusion",
      "--bin",
      "plan_lower_bench",
      "--",
      parquet,
    ],
    {
      cwd: rustDir,
      env: { ...process.env, WCOL_PLANNER_WARMUP: WARMUP, WCOL_PLANNER_ITERS: ITERS },
      stdio: "inherit",
    },
  );
  process.exit(r.status ?? 1);
}

function ensureCatalog() {
  const catalogPath = resolve(rustDir, "wcol-sql-wasm/fixtures/hits_catalog.json");
  if (existsSync(catalogPath)) return readFileSync(catalogPath, "utf8");
  const parquet = resolve(root, "data/hits_subset_500k.parquet");
  const r = spawnSync(
    "cargo",
    ["run", "-q", "-p", "wcol-sql", "--features", "native,datafusion", "--bin", "export_hits_catalog", "--", parquet],
    { cwd: rustDir, encoding: "utf8" },
  );
  if (r.status !== 0) process.exit(1);
  mkdirSync(dirname(catalogPath), { recursive: true });
  writeFileSync(catalogPath, r.stdout);
  return r.stdout;
}

function percentile(sorted, p) {
  const i = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[i];
}

function benchOne(fn, warmup, iters) {
  for (let i = 0; i < warmup; i++) fn();
  const samples = [];
  for (let i = 0; i < iters; i++) {
    const t0 = performance.now();
    fn();
    samples.push(performance.now() - t0);
  }
  samples.sort((a, b) => a - b);
  const sum = samples.reduce((a, b) => a + b, 0);
  return { mean: sum / samples.length, p50: percentile(samples, 0.5), p95: percentile(samples, 0.95) };
}

function runWasm() {
  if (!existsSync(pkgJs)) {
    console.error("Run: node scripts/build-sql-planner-wasm.mjs");
    process.exit(1);
  }
  const catalogJson = ensureCatalog();
  const cols = JSON.parse(catalogJson).columns?.length ?? "?";
  console.log(`mode: release wasm (nodejs bindgen)`);
  console.log(`catalog: hits, ${cols} columns`);
  console.log(`warmup=${WARMUP} iters=${ITERS}\n`);

  const require = createRequire(import.meta.url);
  const wasm = require(pkgJs);
  console.log(`planner wasm version: ${wasm.wcol_sql_wasm_version()}\n`);

  const sample = JSON.parse(wasm.wcol_sql_plan_and_lower(QUERIES.Q3, catalogJson));
  if (!sample.ok) {
    console.error("Planner failed:", sample);
    process.exit(1);
    process.exit(1);
  }

  console.log("query     mean(ms)  p50     p95");
  const warmup = Number(WARMUP);
  const iters = Number(ITERS);
  for (const [name, sql] of Object.entries(QUERIES)) {
    const stats = benchOne(() => {
      const out = JSON.parse(wasm.wcol_sql_plan_and_lower(sql, catalogJson));
      if (!out.ok) throw new Error(out.error);
    }, warmup, iters);
    console.log(
      `${name.padEnd(8)}  ${stats.mean.toFixed(2).padStart(8)}  ${stats.p50.toFixed(2).padStart(6)}  ${stats.p95.toFixed(2).padStart(6)}`,
    );
  }
}

if (useNative) {
  console.log("mode: native release (plan_sql + lower_to_ingest_spec)\n");
  runNative();
} else {
  try {
    runWasm();
  } catch (e) {
    console.error(e?.message ?? e);
    process.exit(1);
  }
}
