/**
 * Every planner-supported ClickBench query on hits_subset_500k, through the
 * coordinator and node workers on local Miniflare. DuckDB is the oracle.
 * `HASH_REDUCERS` (default 2) is a hard override: a small footer bound cannot keep a
 * grouped query on the gather plan. `MAX_NODES` (default 2) is the scan count.
 * A GROUP BY or COUNT(DISTINCT) that opens
 * no hash edges fails, even when its rows match. An ungrouped `approx_count_distinct`
 * stays on gather. Queries the planner cannot partition still run.
 *
 *   npm run clickbench:local
 *   CLICKBENCH_ONLY=Q15 npm run clickbench:local
 */

import { createServer } from "node:http";
import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, coordinatorOut, distDir, nodeOut } from "./build-workers.ts";
import { tsvQueries, UNSUPPORTED } from "./clickbench-catalog.ts";
import { oracleFor, rowsMatch } from "./clickbench-oracle.ts";
import { assert, asText, hashPartitions, type JobRecord, jobRows, localDuckDb, ROOT, scheduleOf, waitForJob } from "./harness.ts";

const parquet = process.argv[2] ?? path.join(ROOT, "data/hits_subset_500k.parquet");
const only = (process.env.CLICKBENCH_ONLY ?? "")
  .split(",")
  .map((label) => label.trim())
  .filter((label) => label.length > 0);
/** Production full-hits shape is 32 scans and 64 reducers. The default stays small. */
const HASH_REDUCERS = Number(process.env.HASH_REDUCERS ?? 2);
const MAX_NODES = process.env.MAX_NODES ?? "2";
const MAX_ACTIVE_SCANS = process.env.WCOL_MAX_ACTIVE_SCANS ?? MAX_NODES;
const QUERY_TIMEOUT_MS = Number(process.env.CLICKBENCH_TIMEOUT_MS ?? 180_000);
/** `parquet`: export each query to R2 instead of returning rows; reports memory, not rows. */
const OUTPUT_PARQUET = process.env.CLICKBENCH_OUTPUT === "parquet";
/** Rows a JSON answer may print, so a checked answer is never cut short. */
const ROW_CAP = 100_000;
/** Below the sample's widest row group, so a `SELECT *` top-k reads its rows late. */
const LATE_WIDE_BYTES = process.env.CLICKBENCH_LATE_WIDE_BYTES
  ? Number(process.env.CLICKBENCH_LATE_WIDE_BYTES)
  : undefined;
const TARGET_GROUP_BYTES = process.env.CLICKBENCH_TARGET_GROUP_BYTES
  ? Number(process.env.CLICKBENCH_TARGET_GROUP_BYTES)
  : undefined;

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
          WCOL_ALLOW_ANONYMOUS: "1",
          WCOL_MAX_NODES: MAX_NODES,
          WCOL_MAX_ACTIVE_SCANS: MAX_ACTIVE_SCANS,
          WCOL_DISPATCH_CONCURRENCY: process.env.WCOL_DISPATCH_CONCURRENCY ?? "32",
          WCOL_JOB_SYNC_MS: String(QUERY_TIMEOUT_MS),
          // Unbounded GROUP BYs on the sample print more than the 1 MiB default allows.
          WCOL_JSON_MAX_BYTES: String(64 * 1024 * 1024),
          // The sample is served from this process, as a URL source.
          WCOL_URL_SOURCE_HOSTS: "127.0.0.1",
        },
        durableObjects: {
          WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
          WCOL_NODE: { className: "WcolNode", scriptName: "wcol-node", useSQLite: true },
        },
        r2Buckets: ["OUTPUT"],
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
        r2Buckets: ["OUTPUT"],
      },
    ],
  });
}

async function postJob(mf: Miniflare, body: unknown): Promise<{ status: number; body: JobRecord }> {
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: JobRecord = {};
  try {
    parsed = JSON.parse(text) as JobRecord;
  } catch {
    parsed = { error: text.slice(0, 500) };
  }
  return { status: response.status, body: parsed };
}

assert(fs.existsSync(parquet), `missing ${parquet}`);
const queries = tsvQueries(ROOT).filter((query) => only.length === 0 || only.includes(query.name));
await buildWorkers();
const file = fs.readFileSync(parquet);
const served = await serveParquet(file);
const mf = miniflare();
const getJob = async (jobId: string) => (await (await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`)).json()) as JobRecord;
const duckRows = await localDuckDb(parquet);

const unsupported: string[] = [];
const failures: string[] = [];
const multiplexed: string[] = [];
let ran = 0;

try {
  for (const { name: label, sql } of queries) {
    const started = Date.now();
    const jobId = `clickbench-${label}-${Date.now().toString(36)}`;
    const posted = await postJob(mf, {
      jobId,
      sources: { hits: { key: served.url } },
      sql,
      output: OUTPUT_PARQUET
        ? { format: "parquet", bucket: "output", prefix: `clickbench/${jobId}/`, ...(process.env.CLICKBENCH_ROW_GROUP_ROWS ? { targetRowGroupRows: Number(process.env.CLICKBENCH_ROW_GROUP_ROWS) } : {}) }
        : { format: "json", rowCap: ROW_CAP },
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
    if (posted.status !== 200 && posted.status !== 202) {
      failures.push(`${label}: ${posted.status} ${message}`);
      console.log(JSON.stringify({ label, status: posted.status, error: message, wallMs: Date.now() - started }));
      continue;
    }
    let record = posted.body;
    if (posted.status === 202 || !record.rows) {
      try {
        record = await waitForJob(getJob, posted.body.jobId ?? jobId, QUERY_TIMEOUT_MS, 300);
      } catch (error) {
        record = { state: "timeout", error: String(error) };
      }
      if (record.state !== "completed") {
        failures.push(`${label}: ${record.error ?? record.state ?? "did not complete"}`);
        console.log(JSON.stringify({ label, status: "failed", error: record.error, wallMs: Date.now() - started }));
        continue;
      }
    }
    if (OUTPUT_PARQUET) {
      console.log(JSON.stringify({ label, status: "exported", wallMs: Date.now() - started, ...(await spillStatsOf(mf, posted.body.jobId ?? jobId)) }));
      continue;
    }
    const nodeCount = record.nodeCount ?? posted.body.nodeCount ?? 0;
    const oracle = oracleFor(sql);
    const ours = asText(jobRows(record));
    ran++;
    if (nodeCount > 1) multiplexed.push(label);
    const truncated = (record.truncated ?? record.result?.truncated) === true;
    const ok = !truncated && rowsMatch(ours, await duckRows(oracle.sql), oracle);
    if (!ok) failures.push(`${label} rows ${ours.length}${truncated ? " (truncated)" : ""} do not match DuckDB`);
    const openedEdges = hashPartitions(sql) ? (await scheduleOf(getJob, posted.body.jobId ?? jobId))?.openedEdges : undefined;
    // A GROUP BY or COUNT(DISTINCT) must take the hash exchange, even when its rows match.
    const gathered = hashPartitions(sql) && !openedEdges;
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
  const expected = [...UNSUPPORTED].sort();
  assert(JSON.stringify(unsupported) === JSON.stringify(expected), `unsupported ${unsupported.join(",")}, expected ${expected.join(",")}`);
  assert(ran >= 47, `ran ${ran}`);
  assert(multiplexed.length >= 8, `only ${multiplexed.length} queries used more than one node`);
}
assert(failures.length === 0, failures.join("\n"));
