/**
 * Hosted Q15 isolate-peak matrix: reducer fan-in variants.
 * Records progress JSON that survives isolate reset.
 *
 *   WCOL_BASE_URL=https://wcol-coordinator....workers.dev \
 *   npx tsx scripts/cf-q15-isolate-matrix.ts
 */

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const Q15 = "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10";
const HASH_REDUCERS = 32;
const TARGET_GROUP_BYTES = 16 * 1024 * 1024;
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 240_000);

type JobBody = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
};

type Cell = {
  name: string;
  maxActiveIngress: number;
};

const CELLS: Cell[] = [
  { name: "fanin-8", maxActiveIngress: 8 },
  { name: "fanin-2", maxActiveIngress: 2 },
  { name: "fanin-1", maxActiveIngress: 1 },
];

const base = (process.env.WCOL_BASE_URL ?? "").replace(/\/$/, "");
const hitsKey = process.env.HITS_KEY ?? "hits.parquet";
if (!base) throw new Error("set WCOL_BASE_URL to the deployed coordinator");

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
    await new Promise((resolve) => setTimeout(resolve, 400));
  }
}

function r2Json(key: string): unknown | null {
  const result = spawnSync(
    "npx",
    [
      "wrangler",
      "r2",
      "object",
      "get",
      `wcol-hits-exports/${key}`,
      "--remote",
      "--pipe",
      "-c",
      "cloudflare/wrangler.toml",
    ],
    { cwd: ROOT, encoding: "utf8" },
  );
  const text = (result.stdout ?? "").trim();
  if (result.status !== 0 || !text.startsWith("{")) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

const runId = Date.now().toString(36);
const results: unknown[] = [];

for (const cell of CELLS) {
  const wallStart = performance.now();
  const posted = await postJob({
    sources: { hits: { bucket: "output", key: hitsKey } },
    sql: Q15,
    output: { format: "json" },
    hashReducers: HASH_REDUCERS,
    groupSpill: true,
    targetGroupBytes: TARGET_GROUP_BYTES,
    maxActiveIngress: cell.maxActiveIngress,
    idempotencyKey: `q15-isolate-${cell.name}-${runId}`,
  });
  const jobId = String(posted.body.jobId ?? "");
  const body = jobId ? await waitFor(jobId) : posted.body;
  const wallMs = Math.round(performance.now() - wallStart);
  const progress: Record<string, unknown> = {};
  const errors: Record<string, unknown> = {};
  if (jobId) {
    for (const taskId of [0, 32, 33, 48, 63]) {
      const snap = r2Json(`_jobs/${jobId}/a1/t${taskId}-progress.json`);
      if (snap) progress[`t${taskId}`] = snap;
      const err = r2Json(`_jobs/${jobId}/a1/t${taskId}-error.json`);
      if (err) errors[`t${taskId}`] = err;
    }
  }
  const line = {
    cell: cell.name,
    ok: body.state === "completed",
    state: body.state,
    error: body.error,
    wallMs,
    jobId,
    nodeCount: body.nodeCount,
    maxActiveIngress: cell.maxActiveIngress,
    progress,
    errors,
  };
  results.push(line);
  console.log(JSON.stringify(line, null, 2));
}

console.log(JSON.stringify({ ok: results.every((row) => (row as { ok?: boolean }).ok), cells: results.length }));
if (!results.every((row) => (row as { ok?: boolean }).ok)) process.exitCode = 1;
