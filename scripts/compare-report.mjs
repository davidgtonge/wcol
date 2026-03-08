#!/usr/bin/env node
/**
 * Print Parquet vs wcol sizes, wasm footprint, and native query timings.
 * Requires: built wcol-cli, local data/ fixtures, @duckdb/node-api for DuckDB timings.
 */
import { spawnSync } from "node:child_process";
import { existsSync, statSync, readFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dataDir = join(root, "data");
const rustDir = join(root, "rust");
const cliBin = join(rustDir, "target", "release", "wcol-cli");
const sqlFile = join(rustDir, "wcol-sql-parser", "readme.md");

const STORAGE_PAIRS = [
  ["crates_versions", "crates_versions.parquet", "crates_versions.wcol"],
  ["crates_dependencies", "crates_dependencies.parquet", "crates_dependencies.wcol"],
  ["hits_subset_500k", "hits_subset_500k.parquet", "hits_subset_500k.refactor.wcol"],
  ["hits_subset_2m", "hits_subset_2m.parquet", "hits_subset_2m.wcol"],
  ["crates_categories", "crates_categories.parquet", "crates_categories.wcol"],
  ["flights-1m", "flights-1m.parquet", "flights-1m.wcol"],
];

const BENCH_QUERIES = [1, 2, 7, 14];
const BENCH_WCOL = join(dataDir, "hits_subset_500k.refactor.wcol");
const BENCH_PARQUET = join(dataDir, "hits_subset_500k.parquet");
const BENCH_RUNS = 5;
const BENCH_WARMUP = 2;

const SQL_BY_Q = {
  1: "SELECT COUNT(*) FROM hits",
  2: "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
  7: "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
  14: "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
};

function mb(path) {
  return statSync(path).size / (1024 * 1024);
}

function gzBytes(path) {
  return gzipSync(readFileSync(path)).length;
}

function kb(n) {
  return (n / 1024).toFixed(1);
}

function run(cmd, args, opts = {}) {
  return spawnSync(cmd, args, { encoding: "utf8", ...opts });
}

function section(title) {
  console.log(`\n## ${title}\n`);
}

function printStorage() {
  section("Storage: Parquet vs .wcol");
  console.log(
    "dataset".padEnd(22) +
      "parquet_mb".padStart(12) +
      "wcol_mb".padStart(12) +
      "ratio".padStart(10),
  );
  for (const [name, pq, wc] of STORAGE_PAIRS) {
    const pqPath = join(dataDir, pq);
    const wcPath = join(dataDir, wc);
    if (!existsSync(pqPath) || !existsSync(wcPath)) {
      console.log(`${name.padEnd(22)} (missing)`);
      continue;
    }
    const pqMb = mb(pqPath);
    const wcMb = mb(wcPath);
    const ratio = Math.round((wcMb / pqMb) * 100);
    console.log(
      `${name.padEnd(22)}${pqMb.toFixed(1).padStart(12)}${wcMb
        .toFixed(1)
        .padStart(12)}${(`${ratio}%`).padStart(10)}`,
    );
  }
}

function wasmPath() {
  const simd = join(root, "rust", "wcol-wasm", "pkg", "wasm", "wcol_wasm.simd.wasm");
  if (existsSync(simd)) return simd;
  const plain = join(root, "rust", "wcol-wasm", "pkg", "wasm", "wcol_wasm.wasm");
  return existsSync(plain) ? plain : null;
}

function printWasmSizes() {
  section("Decoder size: wcol Wasm");
  const wasm = wasmPath();
  if (!wasm) {
    console.log("No built wasm found — run npm run build:wasm first.");
    return;
  }
  const raw = statSync(wasm).size;
  const gz = gzBytes(wasm);
  console.log(`wcol_wasm simd  raw=${kb(raw)} KB  gzip=${kb(gz)} KB`);

  for (const rel of ["dist/browser/main.js", "dist/browser/worker.js"]) {
    const p = join(root, rel);
    if (existsSync(p)) {
      console.log(`${rel}  gzip=${kb(gzBytes(p))} KB`);
    }
  }
  const mainGz = existsSync(join(root, "dist/browser/main.js"))
    ? gzBytes(join(root, "dist/browser/main.js"))
    : 0;
  const workerGz = existsSync(join(root, "dist/browser/worker.js"))
    ? gzBytes(join(root, "dist/browser/worker.js"))
    : 0;
  console.log(`minimal stack gzip ≈ ${kb(gz + mainGz + workerGz)} KB`);
}

function parseBenchLine(line) {
  const m = line.match(/^Q(\d+) mean=([\d.]+)ms/);
  return m ? { q: Number(m[1]), mean: Number(m[2]) } : null;
}

function benchWcol() {
  if (!existsSync(cliBin)) {
    console.log("wcol-cli not built — cargo build -p wcol-cli --release");
    return {};
  }
  if (!existsSync(BENCH_WCOL)) {
    console.log(`Missing ${BENCH_WCOL}`);
    return {};
  }
  const out = run(
    cliBin,
    [
      "bench",
      "--file",
      BENCH_WCOL,
      "--sql-file",
      sqlFile,
      "--workers",
      "4",
      "--runs",
      String(BENCH_RUNS),
      "--warmup",
      String(BENCH_WARMUP),
      "--only",
      BENCH_QUERIES.join(","),
    ],
    { cwd: rustDir },
  );
  const timings = {};
  for (const line of (out.stdout + out.stderr).split("\n")) {
    const row = parseBenchLine(line.trim());
    if (row) timings[row.q] = row.mean;
  }
  return timings;
}

async function benchDuck() {
  if (!existsSync(BENCH_PARQUET)) {
    console.log(`Missing ${BENCH_PARQUET}`);
    return {};
  }

  const instance = await DuckDBInstance.create(":memory:");
  const conn = await instance.connect();
  await conn.run(
    `CREATE OR REPLACE VIEW hits AS SELECT * FROM read_parquet('${BENCH_PARQUET}')`,
  );

  const timings = {};
  for (const q of BENCH_QUERIES) {
    const sql = SQL_BY_Q[q];
    for (let i = 0; i < BENCH_WARMUP; i++) {
      await conn.runAndReadAll(sql);
    }
    const samples = [];
    for (let i = 0; i < BENCH_RUNS; i++) {
      const t0 = performance.now();
      await conn.runAndReadAll(sql);
      samples.push(performance.now() - t0);
    }
    timings[q] = samples.reduce((a, b) => a + b, 0) / samples.length;
  }

  return timings;
}

function printSpeed(wcol, duck) {
  section("Query speed: wcol vs DuckDB (hits_subset_500k, native)");
  console.log(
    "query".padEnd(8) +
      "wcol_ms".padStart(10) +
      "duck_ms".padStart(10) +
      "ratio".padStart(10),
  );
  for (const q of BENCH_QUERIES) {
    const w = wcol[q];
    const d = duck[q];
    let ratio = "—";
    if (w && d) {
      ratio = w < d ? `~${(d / w).toFixed(0)}× wcol` : `~${(w / d).toFixed(0)}× duck`;
    }
    console.log(
      `Q${q}`.padEnd(8) +
        (w ? w.toFixed(2) : "—").padStart(10) +
        (d ? d.toFixed(2) : "—").padStart(10) +
        ratio.padStart(10),
    );
  }
  console.log(
    "\nDuckDB: single in-process connection via @duckdb/node-api (same warmup/runs as wcol-cli bench).",
  );
}

console.log("# wcol comparison report");
printStorage();
printWasmSizes();
const wcol = benchWcol();
const duck = await benchDuck();
printSpeed(wcol, duck);
