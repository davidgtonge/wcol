/** Shared Miniflare spill harness helpers (quick smoke + full smoke). */

import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { distDir } from "./build-workers.ts";

/**
 * The R2 bucket methods these Miniflare runs use. Miniflare types its buckets with
 * `@cloudflare/workers-types`, which this repo's root type check does not load.
 */
export type SpillOutputBucket = {
  put(key: string, value: Uint8Array | string): Promise<unknown>;
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  list(options: { prefix: string }): Promise<{ objects: { key: string }[] }>;
};

export async function spillOutputBucket(mf: Miniflare): Promise<SpillOutputBucket> {
  return (await mf.getR2Bucket("OUTPUT")) as unknown as SpillOutputBucket;
}
/** @deprecated Legacy slot timing from DO SQLite spill era; may appear on old jobs. */
export type SpillTelemetry = {
  begin: number;
  append: number;
  appendRows: number;
  appendBytes: number;
  scan: number;
  scanRows: number;
  end: number;
};

export function emptySpillTelemetry(): SpillTelemetry {
  return {
    begin: 0,
    append: 0,
    appendRows: 0,
    appendBytes: 0,
    scan: 0,
    scanRows: 0,
    end: 0,
  };
}

export type SpillDiag = {
  pollIterations: number;
  spillExec: number;
  spillExecByOp: { begin: number; append: number; scan: number; end: number };
};

export type SlotTiming = {
  taskId?: number;
  slot?: number;
  wallMs?: number;
  wasmMemoryBytes?: number;
  spill?: SpillTelemetry;
  spillDiag?: SpillDiag;
};

export type SpillRunResult = {
  wallMs: number;
  wasmMemoryBytes: number;
  spill: SpillTelemetry;
  spillDiag: SpillDiag;
  taskSlots: number;
};

export function createSpillMiniflare(
  nodeBindings?: Record<string, string>,
  nodeWorkerPath = path.join(distDir, "node-worker.js"),
): Miniflare {
  return new Miniflare({
    workers: [
      {
        name: "wcol-coordinator",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: path.join(distDir, "coordinator-worker.js") },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_coordinator.wasm") },
        ],
        compatibilityDate: "2026-01-01",
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
          { type: "ESModule", path: nodeWorkerPath },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_node.wasm") },
        ],
        compatibilityDate: "2026-01-01",
        bindings: nodeBindings,
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

export function verifyNodeWasmExports(): void {
  const wasmPath = path.join(distDir, "wcol_node.wasm");
  const bytes = fs.readFileSync(wasmPath);
  const module = new WebAssembly.Module(bytes);
  const names = new Set(WebAssembly.Module.exports(module).map((e) => e.name));
  for (const sym of [
    "wcs_sp_open_graph",
    "wcs_sp_spill_op",
    "wcs_sp_spill_complete",
    "wcs_sp_spill_append_ptr",
    "wcs_last_spill_runs",
    "wcs_last_spill_groups_flushed",
    "wcs_last_spill_peak_live_bytes",
    "wcs_last_spill_peak_interner_bytes",
    "wcs_spill_boundary_len",
    "wcs_spill_boundary_ptr",
    "wcs_spill_note_host_ack",
  ]) {
    if (!names.has(sym)) throw new Error(`node wasm missing export ${sym}`);
  }
}

export function sumSpill(into: SpillTelemetry, add: SpillTelemetry): void {
  into.begin += add.begin;
  into.append += add.append;
  into.appendRows += add.appendRows;
  into.appendBytes += add.appendBytes;
  into.scan += add.scan;
  into.scanRows += add.scanRows;
  into.end += add.end;
}

export function sumDiag(into: SpillDiag, add: SpillDiag): void {
  into.pollIterations += add.pollIterations;
  into.spillExec += add.spillExec;
  into.spillExecByOp.begin += add.spillExecByOp.begin;
  into.spillExecByOp.append += add.spillExecByOp.append;
  into.spillExecByOp.scan += add.spillExecByOp.scan;
  into.spillExecByOp.end += add.spillExecByOp.end;
}

export function spillReport(host: SpillDiag, wallMs: number, targetGroupBytes: number) {
  return {
    targetGroupBytes,
    wallMs: Math.round(wallMs),
    wasmHost: {
      spillExecTotal: host.spillExec,
      pollIterations: host.pollIterations,
      byOp: host.spillExecByOp,
    },
    derived: {
      spillExecPerSecond: wallMs > 0 ? Math.round((host.spillExec / wallMs) * 1000) : 0,
    },
  };
}

async function loadJobSpillTimingOnce(
  output: SpillOutputBucket,
  jobId: string,
  executionAttempt: number,
): Promise<SpillRunResult> {
  const prefix = `_jobs/${jobId}/a${executionAttempt}/t`;
  const listed = await output.list({ prefix });
  let wallMs = 0;
  let wasmMemoryBytes = 0;
  let taskSlots = 0;
  const spill = emptySpillTelemetry();
  const spillDiag: SpillDiag = {
    pollIterations: 0,
    spillExec: 0,
    spillExecByOp: { begin: 0, append: 0, scan: 0, end: 0 },
  };
  for (const obj of listed.objects) {
    if (!obj.key.endsWith("-slots.json")) continue;
    taskSlots += 1;
    const parsed = JSON.parse(await (await output.get(obj.key))!.text()) as {
      timing?: SlotTiming;
    };
    const timing = parsed.timing;
    if (!timing) continue;
    if (timing.wallMs != null) wallMs = Math.max(wallMs, timing.wallMs);
    if (timing.wasmMemoryBytes != null) {
      wasmMemoryBytes = Math.max(wasmMemoryBytes, timing.wasmMemoryBytes);
    }
    if (timing.spill) sumSpill(spill, timing.spill);
    if (timing.spillDiag) sumDiag(spillDiag, timing.spillDiag);
  }
  return { wallMs, wasmMemoryBytes, spill, spillDiag, taskSlots };
}

/** Slot JSON is written in waitUntil after the sync JSON body, so poll briefly. */
export async function loadJobSpillTiming(
  output: SpillOutputBucket,
  jobId: string,
  executionAttempt = 1,
  waitMs = 5_000,
): Promise<SpillRunResult> {
  const deadline = Date.now() + waitMs;
  let last = await loadJobSpillTimingOnce(output, jobId, executionAttempt);
  while (last.taskSlots === 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    last = await loadJobSpillTimingOnce(output, jobId, executionAttempt);
  }
  return last;
}

export async function listJobObjectKeys(
  output: SpillOutputBucket,
  jobId: string,
): Promise<string[]> {
  const listed = await output.list({ prefix: `_jobs/${jobId}/` });
  return listed.objects.map((obj) => obj.key);
}

async function jobRows(mf: Miniflare, jobId: string, postBody: Record<string, unknown>): Promise<string[][]> {
  if (Array.isArray(postBody.rows)) {
    return (postBody.rows as unknown[][]).map((row) =>
      row.map((v) => (v === null ? "null" : String(v))),
    );
  }
  const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
  const record = (await response.json()) as { result?: { rows?: unknown[][] }; rows?: unknown[][] };
  const rows = record.result?.rows ?? record.rows;
  if (!Array.isArray(rows)) throw new Error(`job ${jobId} has no rows in status`);
  return rows.map((row) => row.map((v) => (v === null ? "null" : String(v))));
}

async function post(mf: Miniflare, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: JSON.parse(text) as Record<string, unknown> };
}

async function waitForJob(mf: Miniflare, jobId: string, timeoutMs: number): Promise<void> {
  const started = Date.now();
  for (;;) {
    if (Date.now() - started > timeoutMs) {
      throw new Error(`job ${jobId} timed out after ${timeoutMs}ms`);
    }
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await response.json()) as Record<string, unknown>;
    if (record.state === "completed") return;
    if (record.state === "failed") return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

export type ReducerProgressRow = {
  taskId: number;
  spillBoundaries: Array<{
    run: number;
    liveBefore: number;
    liveAfter: number;
    liveSqlAcked: number;
    sqliteWriteBytes?: number;
    serializeBytes?: number;
    rustInflightSpillBytes?: number;
    rustQueuedSpillBytes?: number;
    hostPendingSqlBytes?: number;
    sqliteAppendBytes?: number;
    mergeScanBegins: number;
    mergeSteps: number;
  }>;
  spillRuns?: number;
  sqliteAppendBytes?: number;
};

export type SpillOwnershipSample = {
  run: number;
  liveBefore: number;
  liveSqlAcked: number;
  liveAfter: number;
  sqliteWriteBytes: number;
  sqliteAppendBytesCumulative: number;
  rustInflightSpillBytes: number;
  rustQueuedSpillBytes: number;
  hostPendingSqlBytes: number;
  serializeBytes: number;
};

export function analyzeSpillOwnership(
  boundaries: ReducerProgressRow["spillBoundaries"],
): SpillOwnershipSample[] {
  return boundaries.map((row) => ({
    run: row.run,
    liveBefore: row.liveBefore,
    liveSqlAcked: row.liveSqlAcked,
    liveAfter: row.liveAfter,
    sqliteWriteBytes: row.sqliteWriteBytes ?? 0,
    sqliteAppendBytesCumulative: row.sqliteAppendBytes ?? 0,
    rustInflightSpillBytes: row.rustInflightSpillBytes ?? 0,
    rustQueuedSpillBytes: row.rustQueuedSpillBytes ?? 0,
    hostPendingSqlBytes: row.hostPendingSqlBytes ?? 0,
    serializeBytes: row.serializeBytes ?? 0,
  }));
}

export async function loadReducerProgressSnapshots(
  output: SpillOutputBucket,
  jobId: string,
  executionAttempt = 1,
): Promise<ReducerProgressRow[]> {
  const prefix = `_jobs/${jobId}/a${executionAttempt}/t`;
  const listed = await output.list({ prefix });
  const rows: ReducerProgressRow[] = [];
  for (const obj of listed.objects) {
    const match = obj.key.match(/\/t(\d+)-progress\.json$/);
    if (!match) continue;
    const taskId = Number(match[1]);
    const text = await (await output.get(obj.key))!.text();
    const parsed = JSON.parse(text) as {
      spillBoundaries?: ReducerProgressRow["spillBoundaries"];
      spillRuns?: number;
    };
    const boundaries = parsed.spillBoundaries ?? [];
    if (boundaries.length === 0) continue;
    const last = boundaries[boundaries.length - 1]!;
    rows.push({
      taskId,
      spillBoundaries: boundaries,
      spillRuns: parsed.spillRuns,
      sqliteAppendBytes: last.sqliteAppendBytes,
    });
  }
  return rows.sort((a, b) => b.spillBoundaries.length - a.spillBoundaries.length);
}

export async function runSpillJobUntilSettled(
  mf: Miniflare,
  body: Record<string, unknown>,
  jobTimeoutMs: number,
): Promise<{ jobId: string; state: string; error?: string }> {
  const res = await post(mf, body);
  if (res.status !== 200 && res.status !== 202) {
    throw new Error(`job POST ${res.status}: ${JSON.stringify(res.body).slice(0, 400)}`);
  }
  const jobId = String(res.body.jobId);
  await waitForJob(mf, jobId, jobTimeoutMs);
  const status = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
  const record = (await status.json()) as { state?: string; error?: string };
  return { jobId, state: String(record.state ?? ""), error: record.error };
}

export async function runGroupBySpillJob(
  mf: Miniflare,
  output: SpillOutputBucket,
  opts: {
    sql: string;
    duckExpected: string[][];
    hashReducers: number;
    groupSpill: boolean;
    targetGroupBytes: number;
    jobTimeoutMs?: number;
  },
): Promise<{ rows: string[][]; timing: SpillRunResult; jobId: string }> {
  const res = await post(mf, {
    sources: { hits: { bucket: "output", key: "hits.parquet" } },
    sql: opts.sql,
    output: { format: "json" },
    hashReducers: opts.hashReducers,
    groupSpill: opts.groupSpill,
    targetGroupBytes: opts.targetGroupBytes,
  });
  if (res.status !== 200) {
    throw new Error(`job POST ${res.status}: ${JSON.stringify(res.body).slice(0, 400)}`);
  }
  const jobId = String(res.body.jobId);
  const started = performance.now();
  await waitForJob(mf, jobId, opts.jobTimeoutMs ?? 120_000);
  const rows = await jobRows(mf, jobId, res.body);
  if (JSON.stringify(rows) !== JSON.stringify(opts.duckExpected)) {
    throw new Error("rows differ from DuckDB");
  }

  const timing = await loadJobSpillTiming(output, jobId, 1);
  if (timing.wallMs <= 0) timing.wallMs = performance.now() - started;
  return { rows, timing, jobId };
}
