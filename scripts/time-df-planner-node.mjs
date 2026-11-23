#!/usr/bin/env node
/**
 * Time wasm load + df_plan_sql in Node. Use --spawn for a fresh process (cold require).
 */
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const pkgDir = join(root, "rust", "df-planner-wasm", "pkg");
const pkgJs = join(pkgDir, "df_planner_wasm.js");

if (process.argv.includes("--spawn")) {
  const r = spawnSync(process.execPath, [fileURLToPath(import.meta.url)], {
    stdio: "inherit",
    cwd: root,
  });
  process.exit(r.status ?? 1);
}

const catalog = readFileSync(
  join(root, "rust", "df-planner-wasm", "fixtures", "hits_catalog.json"),
  "utf8",
);
const sql =
  "SELECT AdvEngineID, COUNT(*) AS cnt FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY cnt DESC LIMIT 10";

const require = createRequire(import.meta.url);

const tLoad = performance.now();
const { df_plan_sql } = require(pkgJs);
const loadMs = performance.now() - tLoad;

const tPlan1 = performance.now();
const raw1 = df_plan_sql(sql, catalog);
const plan1Ms = performance.now() - tPlan1;

const tPlan2 = performance.now();
df_plan_sql(sql, catalog);
const plan2Ms = performance.now() - tPlan2;

const out = JSON.parse(raw1);
if (!out.ok) {
  console.error(out);
  process.exit(1);
}

const wasmBytes = readFileSync(join(pkgDir, "df_planner_wasm_bg.wasm")).length;

const rows = [
  ["wasm bg file", `${(wasmBytes / 1024 / 1024).toFixed(2)} MiB`],
  ["require (read+compile+instantiate+start)", `${loadMs.toFixed(1)} ms`],
  ["first df_plan_sql", `${plan1Ms.toFixed(1)} ms`],
  ["second df_plan_sql (warm)", `${plan2Ms.toFixed(1)} ms`],
  ["load + first plan", `${(loadMs + plan1Ms).toFixed(1)} ms`],
  ["optimized_logical chars", String(out.optimized_logical.length)],
  ["required_columns", out.required_columns.length],
];

console.log("df-planner-wasm Node timing (nodejs wasm-bindgen target)");
console.log("sql:", `${sql.slice(0, 72)}…`);
console.table(rows);
