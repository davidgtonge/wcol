/**
 * Hosted Q15 with the in-Wasm discard spill sink.
 *
 * Measures isolate memory while the reducer flushes and resets live group state.
 * Discarded runs are not a query result.
 *
 *   WCOL_BASE_URL=https://wcol-coordinator....workers.dev \
 *   npx tsx scripts/cf-q15-discard.ts
 */

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const Q15 = "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10";
const HASH_REDUCERS = Number(process.env.HASH_REDUCERS ?? 32);
const TARGET_GROUP_BYTES = Number(process.env.TARGET_GROUP_BYTES ?? 4 * 1024 * 1024);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 600_000);
const ISOLATE_LIMIT_BYTES = 128 * 1024 * 1024;
const WASM_BUDGET_BYTES = Number(process.env.WASM_BUDGET_BYTES ?? 100 * 1024 * 1024);

type JobBody = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
};

type ProgressSnap = {
  taskId: string;
  wallMs?: number;
  inputBytes?: number;
  outputBytes?: number;
  wasmMemoryBytes?: number;
  leftoverBytes?: number;
  retainedIngressBytes?: number;
  ingressLocks?: number;
  spillRuns?: number;
  spillGroupsFlushed?: number;
  spillPeakLiveBytes?: number;
  spillPeakInternerBytes?: number;
  residentGroups?: number;
  residentGroupBytes?: number;
};


/** An environment variable the script cannot run without. */
function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`set ${name} (for example https://wcol-coordinator.<your-subdomain>.workers.dev)`);
  return value;
}

const base = requiredEnv("WCOL_BASE_URL").replace(
  /\/$/,
  "",
);
const hitsKey = process.env.HITS_KEY ?? "hits.parquet";

async function postJob(body: unknown): Promise<{ status: number; body: JobBody }> {
  const response = await fetch(`${base}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as JobBody };
}

async function getJob(jobId: string): Promise<JobBody> {
  const response = await fetch(`${base}/jobs/${jobId}`);
  return (await response.json()) as JobBody;
}

async function waitFor(jobId: string): Promise<JobBody> {
  const started = Date.now();
  for (;;) {
    const body = await getJob(jobId);
    if (body.state === "completed" || body.state === "failed") return body;
    if (Date.now() - started > TIMEOUT_MS) {
      return { ...body, error: `timed out in ${body.state}` };
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

function wrangler(args: string[]): { status: number; stdout: string; stderr: string } {
  const result = spawnSync("npx", ["wrangler", ...args], {
    cwd: ROOT,
    encoding: "utf8",
  });
  return {
    status: result.status ?? 1,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
}

function r2Json(key: string): Record<string, unknown> | null {
  const result = wrangler([
    "r2",
    "object",
    "get",
    `wcol-hits-exports/${key}`,
    "--remote",
    "--pipe",
    "-c",
    "cloudflare/wrangler.toml",
  ]);
  const text = (result.stdout ?? "").trim();
  if (result.status !== 0 || !text.startsWith("{")) return null;
  try {
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function r2List(prefix: string): string[] {
  const result = wrangler([
    "r2",
    "object",
    "list",
    "wcol-hits-exports",
    "--prefix",
    prefix,
    "--remote",
    "-c",
    "cloudflare/wrangler.toml",
  ]);
  const text = `${result.stdout}\n${result.stderr}`;
  const keys = new Set<string>();
  for (const match of text.matchAll(/_jobs\/[^\s"]+/g)) {
    keys.add(match[0].replace(/[",]$/, ""));
  }
  return [...keys];
}

function asProgress(taskId: string, snap: Record<string, unknown> | null): ProgressSnap | null {
  if (!snap) return null;
  const timing = (snap.timing as Record<string, unknown> | undefined) ?? snap;
  return {
    taskId,
    wallMs: Number(timing.wallMs ?? 0) || undefined,
    inputBytes: Number(timing.inputBytes ?? 0) || undefined,
    outputBytes: Number(timing.outputBytes ?? 0) || undefined,
    wasmMemoryBytes: Number(timing.wasmMemoryBytes ?? 0) || undefined,
    leftoverBytes: Number(timing.leftoverBytes ?? 0) || undefined,
    retainedIngressBytes: Number(timing.retainedIngressBytes ?? 0) || undefined,
    ingressLocks: Number(timing.ingressLocks ?? 0) || undefined,
    spillRuns: Number(timing.spillRuns ?? 0) || undefined,
    spillGroupsFlushed: Number(timing.spillGroupsFlushed ?? 0) || undefined,
    spillPeakLiveBytes: Number(timing.spillPeakLiveBytes ?? 0) || undefined,
    spillPeakInternerBytes: Number(timing.spillPeakInternerBytes ?? 0) || undefined,
    residentGroups: Number(timing.residentGroups ?? 0) || undefined,
    residentGroupBytes: Number(timing.residentGroupBytes ?? 0) || undefined,
  };
}

function maxOf(rows: ProgressSnap[], key: keyof ProgressSnap): number {
  let max = 0;
  for (const row of rows) {
    const value = row[key];
    if (typeof value === "number" && value > max) max = value;
  }
  return max;
}

function sumOf(rows: ProgressSnap[], key: keyof ProgressSnap): number {
  let sum = 0;
  for (const row of rows) {
    const value = row[key];
    if (typeof value === "number") sum += value;
  }
  return sum;
}

const runId = Date.now().toString(36);
const wallStart = performance.now();
console.log(JSON.stringify({ event: "posting", hashReducers: HASH_REDUCERS, targetGroupBytes: TARGET_GROUP_BYTES }));
const posted = await postJob({
  sources: { hits: { bucket: "output", key: hitsKey } },
  sql: Q15,
  output: { format: "json" },
  hashReducers: HASH_REDUCERS,
  groupSpill: true,
  spillSink: "discard",
  targetGroupBytes: TARGET_GROUP_BYTES,
  idempotencyKey: `q15-discard-${runId}`,
});
const jobId = String(posted.body.jobId ?? "");
console.log(JSON.stringify({ event: "posted", jobId, status: posted.status, state: posted.body.state }));
const body = jobId ? await waitFor(jobId) : posted.body;
const wallMs = Math.round(performance.now() - wallStart);
console.log(JSON.stringify({ event: "finished", jobId, state: body.state, error: body.error, wallMs }));

const progress: ProgressSnap[] = [];
const errors: Record<string, unknown> = {};
const named: string[] = [];
if (jobId) {
  const listed = r2List(`_jobs/${jobId}/`);
  if (listed.length > 0) {
    named.push(...listed);
  } else {
    for (let taskId = 0; taskId <= HASH_REDUCERS + 8; taskId++) {
      named.push(`_jobs/${jobId}/a1/t${taskId}-progress.json`);
      named.push(`_jobs/${jobId}/a1/t${taskId}-slots.json`);
      named.push(`_jobs/${jobId}/a1/t${taskId}-error.json`);
    }
  }
}
for (const key of named) {
  const taskMatch = key.match(/t(\d+)-(progress|slots|error)\.json$/);
  if (!taskMatch) continue;
  const taskId = `t${taskMatch[1]}`;
  const kind = taskMatch[2];
  const snap = r2Json(key);
  if (!snap) continue;
  if (kind === "error") {
    errors[taskId] = snap;
    continue;
  }
  const row = asProgress(taskId, snap);
  if (row) progress.push(row);
}

const reducers = progress.filter((row) => (row.spillRuns ?? 0) > 0 || (row.spillPeakLiveBytes ?? 0) > 0);
const observed = reducers.length > 0 ? reducers : progress;
const maxWasm = maxOf(observed, "wasmMemoryBytes");
const maxRetained = maxOf(observed, "retainedIngressBytes");
const maxPeakLive = maxOf(observed, "spillPeakLiveBytes");
const totalRuns = sumOf(observed, "spillRuns");
const totalFlushed = sumOf(observed, "spillGroupsFlushed");
const maxInput = maxOf(observed, "inputBytes");
const completed = body.state === "completed";
const bounded = maxWasm > 0 && maxWasm < WASM_BUDGET_BYTES;
const flushed = totalRuns > 0;
const ok = completed && bounded && flushed;

const line = {
  ok,
  jobId,
  state: body.state,
  error: body.error,
  wallMs,
  nodeCount: body.nodeCount,
  hashReducers: HASH_REDUCERS,
  targetGroupBytes: TARGET_GROUP_BYTES,
  isolateLimitBytes: ISOLATE_LIMIT_BYTES,
  wasmBudgetBytes: WASM_BUDGET_BYTES,
  maxWasmMemoryBytes: maxWasm,
  maxRetainedIngressBytes: maxRetained,
  maxSpillPeakLiveBytes: maxPeakLive,
  totalSpillRuns: totalRuns,
  totalSpillGroupsFlushed: totalFlushed,
  maxInputBytes: maxInput,
  reducers: observed,
  errors,
  keys: named.filter((key) => key.includes("-progress.json") || key.includes("-slots.json") || key.includes("-error.json")),
};

console.log(JSON.stringify(line, null, 2));
if (!ok) process.exitCode = 1;
