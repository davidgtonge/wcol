/**
 * Every planner-supported ClickBench query on hits_subset_500k, through the
 * coordinator and node workers on local Miniflare. DuckDB is the oracle.
 * `HASH_REDUCERS` (default 2) is a hard override: a small footer bound cannot keep a
 * grouped query on the gather plan. `MAX_NODES` (default 2) is the scan count.
 * A GROUP BY or COUNT(DISTINCT) that opens
 * no hash edges fails, even when its rows match. An ungrouped `approx_count_distinct`
 * stays on gather. Queries the planner cannot partition still run.
 *
 *   npm run test:clickbench-miniflare
 *   CLICKBENCH_ONLY=Q15 npm run test:clickbench-miniflare
 */

import { createServer } from "node:http";
import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, coordinatorOut, distDir, nodeOut, ROOT } from "./build-workers.ts";

const parquet = process.argv[2] ?? path.join(ROOT, "data/hits_subset_500k.parquet");
const only = (process.env.CLICKBENCH_ONLY ?? "")
  .split(",")
  .map((label) => label.trim())
  .filter((label) => label.length > 0);
const EXPECTED_UNSUPPORTED = ["Q18", "Q35", "Q42"];
/** Production full-hits shape is 32 scans and 64 reducers. The default stays small. */
const HASH_REDUCERS = Number(process.env.HASH_REDUCERS ?? 2);
const MAX_NODES = process.env.MAX_NODES ?? "2";
const MAX_ACTIVE_SCANS = process.env.WCOL_MAX_ACTIVE_SCANS ?? MAX_NODES;
const QUERY_TIMEOUT_MS = Number(process.env.CLICKBENCH_TIMEOUT_MS ?? 180_000);
/** `parquet`: export each query to R2 instead of returning rows; reports memory, not rows. */
const OUTPUT_PARQUET = process.env.CLICKBENCH_OUTPUT === "parquet";
/** Below the sample's widest row group, so a `SELECT *` top-k reads its rows late. */
const LATE_WIDE_BYTES = process.env.CLICKBENCH_LATE_WIDE_BYTES
  ? Number(process.env.CLICKBENCH_LATE_WIDE_BYTES)
  : undefined;
const TARGET_GROUP_BYTES = process.env.CLICKBENCH_TARGET_GROUP_BYTES
  ? Number(process.env.CLICKBENCH_TARGET_GROUP_BYTES)
  : undefined;

type Cell = string | null;
type Query = { label: string; sql: string };
type JobBody = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
  columns?: string[];
  rows?: unknown[][];
  result?: { rows?: unknown[][] };
  schedule?: { openedEdges?: number };
};

/** Grouped and distinct queries must take the hash exchange. An ungrouped approx sketch does not. */
function expectsHashExchange(sql: string): boolean {
  return /\bgroup\s+by\b/i.test(sql) || /count\s*\(\s*distinct\b/i.test(sql);
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

/**
 * Spill runs every task of `jobId` committed, and the largest task Wasm memory, from its
 * progress records in R2. Two reducers can share a 128 MB isolate, so the peak is the budget.
 */
async function spillStatsOf(
  mf: Miniflare,
  jobId: string,
): Promise<{ spillRuns: number; peakWasmMiB: number; peakTask?: Record<string, unknown> }> {
  type Progress = { spillRuns?: number; wasmMemoryBytes?: number; spillBoundaries?: unknown[] };
  const bucket = await mf.getR2Bucket("OUTPUT");
  let spillRuns = 0;
  let peak: Progress = {};
  let cursor: string | undefined;
  do {
    const page = await bucket.list({ prefix: `_jobs/${jobId}/`, cursor });
    for (const object of page.objects) {
      if (!object.key.endsWith("-progress.json")) continue;
      const body = await bucket.get(object.key);
      const progress: Progress = body ? JSON.parse(await body.text()) : {};
      spillRuns += progress.spillRuns ?? 0;
      if ((progress.wasmMemoryBytes ?? 0) > (peak.wasmMemoryBytes ?? 0)) peak = progress;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  const { spillBoundaries, ...peakTask } = peak;
  return {
    spillRuns,
    peakWasmMiB: Math.round((peak.wasmMemoryBytes ?? 0) / 2 ** 20),
    // The last boundary is the state the peak task flushed from; earlier ones repeat it.
    ...(peak.wasmMemoryBytes ? { peakTask: { ...peakTask, lastSpill: spillBoundaries?.at(-1) } } : {}),
  };
}

function loadQueries(): Query[] {
  const tsv = fs.readFileSync(path.join(ROOT, "scripts/clickbench-queries.tsv"), "utf8");
  return tsv
    .split("\n")
    .filter((line) => line.includes("\t"))
    .map((line) => {
      const [label, sql] = line.split("\t");
      return { label: label!, sql: sql!.trim().replace(/;$/, "") };
    })
    .filter((query) => only.length === 0 || only.includes(query.label));
}

function splitCommas(text: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === "(") depth++;
    else if (char === ")") depth--;
    else if (char === "," && depth === 0) {
      parts.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }
  parts.push(text.slice(start).trim());
  return parts.filter((part) => part.length > 0);
}

function selectItems(sql: string): string[] {
  const match = sql.match(/^\s*select\s+([\s\S]+?)\s+from\s+/i);
  if (!match) return [];
  return splitCommas(match[1]!);
}

function approxIndexes(selects: string[]): number[] {
  return selects.flatMap((item, index) => (/approx_count_distinct\s*\(/i.test(item) ? [index] : []));
}

function orderSpec(sql: string, selects: string[]): { index: number } | null {
  const match = sql.match(/\border\s+by\s+([\s\S]+?)(?:\blimit\b|\boffset\b|$)/i);
  if (!match) return null;
  const raw = match[1]!.trim();
  const name = raw.replace(/\s+(?:asc|desc)\s*$/i, "").trim();
  const index = selects.findIndex((item) => {
    const alias = item.match(/\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$/i);
    if (alias && alias[1]!.toLowerCase() === name.toLowerCase()) return true;
    if (item.toLowerCase() === name.toLowerCase()) return true;
    return name.toLowerCase() === "count(*)" && /count\s*\(\s*\*\s*\)/i.test(item);
  });
  return index < 0 ? null : { index };
}

function dayNumber(value: unknown): string | null {
  if (value == null) return null;
  if (value instanceof Date) return String(Math.round(value.getTime() / 86_400_000));
  if (typeof value === "bigint") return value.toString();
  const text = String(value);
  const date = /^(\d{4}-\d{2}-\d{2})(?:[T ].*)?$/.exec(text);
  if (!date) return text;
  const parsed = Date.parse(`${date[1]}T00:00:00Z`);
  return Number.isNaN(parsed) ? text : String(Math.round(parsed / 86_400_000));
}

function rowsOf(value: unknown): Cell[][] {
  if (!Array.isArray(value)) return [];
  return value.map((row) => (Array.isArray(row) ? row.map((cell) => dayNumber(cell)) : []));
}

function mask(rows: Cell[][], indexes: number[]): Cell[][] {
  if (indexes.length === 0) return rows;
  const skip = new Set(indexes);
  return rows.map((row) => row.map((cell, index) => (skip.has(index) ? "*" : cell)));
}

function multiset(rows: Cell[][]): string {
  return JSON.stringify(rows.map((row) => JSON.stringify(row)).sort());
}

function rankingAgrees(ours: Cell[][], duck: Cell[][], spec: { index: number }): boolean {
  if (ours.length !== duck.length) return false;
  const value = (row: Cell[]) => row[spec.index];
  if (JSON.stringify(ours.map(value)) !== JSON.stringify(duck.map(value))) return false;
  if (ours.length === 0) return true;
  const cutoff = value(duck[duck.length - 1]!);
  const above = (rows: Cell[][]) => rows.filter((row) => value(row) !== cutoff);
  return multiset(above(ours)) === multiset(above(duck));
}

function agrees(sql: string, ours: Cell[][], duck: Cell[][]): boolean {
  const selects = selectItems(sql);
  const approx = approxIndexes(selects);
  const order = orderSpec(sql, selects);
  const left = mask(ours, approx);
  const right = mask(duck, approx);
  if (order && approx.includes(order.index)) {
    return ours.length === duck.length && ours.every((row) => row.length === (duck[0]?.length ?? row.length));
  }
  if (!order) {
    if (left.length === right.length) return multiset(left) === multiset(right);
    if (left.length === 1000 && left.length < right.length) {
      const have = new Set(right.map((row) => JSON.stringify(row)));
      return left.every((row) => have.has(JSON.stringify(row)));
    }
    return false;
  }
  return rankingAgrees(left, right, order);
}

function serveParquet(file: Buffer): Promise<{ url: string; close: () => Promise<void> }> {
  const server = createServer((req, res) => {
    const range = req.headers.range;
    if (!range) {
      res.writeHead(200, {
        "content-type": "application/octet-stream",
        "content-length": file.length,
        "accept-ranges": "bytes",
        etag: '"hits-subset"',
      });
      res.end(file);
      return;
    }
    const suffix = /bytes=-(\d+)/.exec(range);
    const match = suffix ? null : /bytes=(\d+)-(\d*)/.exec(range);
    if (!suffix && !match) {
      res.writeHead(416);
      res.end();
      return;
    }
    const start = suffix ? Math.max(0, file.length - Number(suffix[1])) : Number(match![1]);
    const end = suffix ? file.length - 1 : match![2] ? Number(match![2]) : file.length - 1;
    const slice = file.subarray(start, end + 1);
    res.writeHead(206, {
      "content-type": "application/octet-stream",
      "content-range": `bytes ${start}-${end}/${file.length}`,
      "content-length": slice.length,
      "accept-ranges": "bytes",
      etag: '"hits-subset"',
    });
    res.end(slice);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}/hits_subset_500k.parquet`,
        close: () => new Promise((done) => server.close(() => done())),
      });
    });
  });
}

function miniflare(): Miniflare {
  return new Miniflare({
    // Exposes the V8 inspector, one target per worker, for heap profiling a query.
    ...(process.env.MINIFLARE_INSPECTOR_PORT
      ? { inspectorPort: Number(process.env.MINIFLARE_INSPECTOR_PORT) }
      : {}),
    workers: [
      {
        name: "wcol-coordinator",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: coordinatorOut },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_coordinator.wasm") },
        ],
        compatibilityDate: "2026-01-01",
        bindings: {
          WCOL_MAX_NODES: MAX_NODES,
          WCOL_MAX_ACTIVE_SCANS: MAX_ACTIVE_SCANS,
          WCOL_DISPATCH_CONCURRENCY: process.env.WCOL_DISPATCH_CONCURRENCY ?? "32",
          WCOL_JOB_SYNC_MS: String(QUERY_TIMEOUT_MS),
        },
        durableObjects: {
          WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
          WCOL_NODE: { className: "WcolNode", scriptName: "wcol-node", useSQLite: true },
        },
        r2Buckets: ["OUTPUT", "ANALYTICS"],
      },
      {
        name: "wcol-node",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: nodeOut },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_node.wasm") },
        ],
        compatibilityDate: "2026-01-01",
        durableObjects: {
          WCOL_NODE: { className: "WcolNode", useSQLite: true },
          // Hash scans claim their units on the job's coordinator, as in wrangler.node.toml.
          WCOL_COORDINATOR: { className: "WcolCoordinator", scriptName: "wcol-coordinator" },
        },
        r2Buckets: ["OUTPUT", "ANALYTICS"],
      },
    ],
  });
}

async function postJob(mf: Miniflare, body: unknown): Promise<{ status: number; body: JobBody }> {
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: JobBody = {};
  try {
    parsed = JSON.parse(text) as JobBody;
  } catch {
    parsed = { error: text.slice(0, 500) };
  }
  return { status: response.status, body: parsed };
}

/** `timing.json` is written after the job completes. Poll until the edge count lands. */
async function openedEdgesOf(mf: Miniflare, jobId: string): Promise<number | undefined> {
  const deadline = Date.now() + 5_000;
  for (;;) {
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await response.json()) as JobBody;
    const edges = record.schedule?.openedEdges;
    if (edges != null) return edges;
    if (Date.now() > deadline) return undefined;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

async function waitFor(mf: Miniflare, jobId: string): Promise<JobBody> {
  const started = Date.now();
  for (;;) {
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await response.json()) as JobBody;
    if (record.state === "completed" || record.state === "failed") return record;
    if (Date.now() - started > QUERY_TIMEOUT_MS) {
      return { ...record, error: `timed out in ${record.state ?? "unknown"}` };
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
}

assert(fs.existsSync(parquet), `missing ${parquet}`);
const queries = loadQueries();
await buildWorkers();
const file = fs.readFileSync(parquet);
const served = await serveParquet(file);
const mf = miniflare();
const { DuckDBInstance } = await import("@duckdb/node-api");
const duck = await (await DuckDBInstance.create(":memory:")).connect();
// An uncapped DuckDB oracle on full hits once exhausted the machine.
await duck.run("SET memory_limit='4GB'");
const escaped = parquet.replaceAll("'", "''");
await duck.run(`
  CREATE VIEW hits AS
  SELECT * REPLACE (
    CAST(DATE '1970-01-01' + CAST(EventDate AS INTEGER) AS DATE) AS EventDate
  )
  FROM read_parquet('${escaped}')
`);

const unsupported: string[] = [];
const failures: string[] = [];
const multiplexed: string[] = [];
let ran = 0;

try {
  for (const { label, sql } of queries) {
    const started = Date.now();
    const jobId = `clickbench-${label}-${Date.now().toString(36)}`;
    const posted = await postJob(mf, {
      jobId,
      sources: { hits: { key: served.url } },
      sql,
      output: OUTPUT_PARQUET
        ? { format: "parquet", bucket: "output", prefix: `clickbench/${jobId}/`, ...(process.env.CLICKBENCH_ROW_GROUP_ROWS ? { targetRowGroupRows: Number(process.env.CLICKBENCH_ROW_GROUP_ROWS) } : {}) }
        : { format: "json" },
      hashReducers: HASH_REDUCERS,
      maxNodes: Number(MAX_NODES),
      minBytesPerNode: 1,
      // A small budget forces reducers to spill runs to R2 (eg. 65536).
      // The hard ceiling stays the 32 MiB default; only the flush target shrinks.
      ...(TARGET_GROUP_BYTES ? { targetGroupBytes: TARGET_GROUP_BYTES, hardGroupBytes: 32 * 1024 * 1024 } : {}),
      ...(LATE_WIDE_BYTES != null ? { lateWideBytes: LATE_WIDE_BYTES } : {}),
    });
    const message = posted.body.error ?? "";
    if (posted.status === 400 && /unsupported|unexpected token/i.test(message)) {
      unsupported.push(label);
      console.log(JSON.stringify({ label, status: "unsupported", wallMs: Date.now() - started }));
      continue;
    }
    if (OUTPUT_PARQUET && (posted.status === 200 || posted.status === 202)) {
      const record = await waitFor(mf, posted.body.jobId ?? jobId);
      if (record.state !== "completed") failures.push(`${label}: ${record.error ?? record.state}`);
      console.log(JSON.stringify({
        label,
        status: record.state === "completed" ? "exported" : "failed",
        error: record.error,
        wallMs: Date.now() - started,
        ...(await spillStatsOf(mf, posted.body.jobId ?? jobId)),
      }));
      continue;
    }
    let rows = posted.body.rows;
    let nodeCount = posted.body.nodeCount ?? 0;
    if (posted.status === 202 || (posted.status === 200 && !rows)) {
      const record = await waitFor(mf, posted.body.jobId ?? jobId);
      if (record.state !== "completed") {
        failures.push(`${label}: ${record.error ?? record.state ?? "did not complete"}`);
        console.log(JSON.stringify({ label, status: "failed", error: record.error, wallMs: Date.now() - started }));
        continue;
      }
      rows = record.result?.rows ?? record.rows;
      nodeCount = record.nodeCount ?? nodeCount;
    } else if (posted.status !== 200) {
      failures.push(`${label}: ${posted.status} ${message}`);
      console.log(JSON.stringify({ label, status: posted.status, error: message, wallMs: Date.now() - started }));
      continue;
    }
    const duckRows = rowsOf((await duck.runAndReadAll(sql)).getRows());
    const ours = rowsOf(rows);
    ran++;
    if ((nodeCount ?? 0) > 1) multiplexed.push(label);
    const ok = agrees(sql, ours, duckRows);
    if (!ok) failures.push(`${label} rows ${ours.length} vs ${duckRows.length}`);
    const openedEdges = expectsHashExchange(sql) ? await openedEdgesOf(mf, posted.body.jobId ?? jobId) : undefined;
    const gathered = expectsHashExchange(sql) && !openedEdges;
    if (gathered) {
      failures.push(`${label} gathered instead of hash exchange (openedEdges ${openedEdges ?? "missing"})`);
    }
    console.log(
      JSON.stringify({
        label,
        status: !ok ? "mismatch" : gathered ? "gathered" : "match",
        nodeCount,
        rows: ours.length,
        ...(openedEdges != null ? { openedEdges } : {}),
        wallMs: Date.now() - started,
        ...(TARGET_GROUP_BYTES ? await spillStatsOf(mf, posted.body.jobId ?? jobId) : {}),
      }),
    );
  }
} finally {
  await mf.dispose();
  await served.close();
}

const summary = {
  ran,
  unsupported: unsupported.sort(),
  multiplexed,
  failures,
};
console.log(JSON.stringify(summary, null, 2));
if (only.length === 0) {
  assert(JSON.stringify(unsupported.sort()) === JSON.stringify(EXPECTED_UNSUPPORTED), `unsupported ${unsupported.join(",")}`);
  assert(ran >= 47, `ran ${ran}`);
  assert(multiplexed.length >= 8, `only ${multiplexed.length} queries used more than one node`);
}
assert(failures.length === 0, failures.join("\n"));
