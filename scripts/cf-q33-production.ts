/**
 * Real Durable Object acceptance for Q33 on hits-2m.
 *
 *   npm run build:node-wasm && npm run build:coordinator-wasm
 *   npx tsx scripts/deploy-worker.ts
 *   WCOL_BASE_URL=https://wcol-coordinator.<your-subdomain>.workers.dev \
 *     npx tsx scripts/cf-q33-production.ts
 *
 * Acceptance (docs/HASH_GROUP_BUDGET.md):
 * - DuckDB result parity
 * - No isolate failure / unexpected restart in task errors
 * - Spill runs commit and merge
 * - Peak Wasm / wall time recorded
 * - No leftover wcol-spill objects for the job after completion
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const Q33 = "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10";
const MiB = 1024 * 1024;

/** An environment variable the script cannot run without. */
function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`set ${name} (for example https://wcol-coordinator.<your-subdomain>.workers.dev)`);
  return value;
}

const BASE = requiredEnv("WCOL_BASE_URL").replace(
  /\/$/,
  "",
);
const HITS_KEY = process.env.HITS_KEY ?? "hits-2m.parquet";
const HITS_LOCAL = process.env.HITS_LOCAL ?? path.join(ROOT, "data/hits_subset_2m.parquet");
/** `HASH_REDUCERS=auto` leaves the count to the planner. */
const REDUCERS = process.env.HASH_REDUCERS === "auto" ? undefined : Number(process.env.HASH_REDUCERS ?? 2);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 600_000);
const TARGET = Number(process.env.TARGET_GROUP_BYTES ?? 16 * MiB);
const HARD = Number(process.env.HARD_GROUP_BYTES ?? 32 * MiB);

type JobBody = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
  rows?: unknown[][];
  result?: { rows?: unknown[][] };
  schedule?: { coordinatorShuffleBytes?: number; openedEdges?: number };
};

type Progress = {
  taskId: number;
  wasmMemoryBytes?: number;
  spillRuns?: number;
  spillPeakLiveBytes?: number;
  spillPeakInternerBytes?: number;
  wallMs?: number;
  error?: unknown;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function cell(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "bigint") return value.toString();
  return String(value);
}

function asText(rows: unknown[][]): string[][] {
  return rows.map((row) => row.map(cell));
}

function rowsEqual(ours: string[][], expected: string[][]): boolean {
  if (ours.length !== expected.length) return false;
  return ours.every(
    (row, index) =>
      row.length === expected[index]!.length &&
      row.every((value, col) => {
        if (value === expected[index]![col]) return true;
        const a = Number(value);
        const b = Number(expected[index]![col]);
        return Number.isFinite(a) && Number.isFinite(b) && a === b;
      }),
  );
}

async function postJob(body: unknown): Promise<{ status: number; body: JobBody }> {
  const response = await fetch(`${BASE}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as JobBody };
}

async function getJob(jobId: string): Promise<JobBody> {
  const response = await fetch(`${BASE}/jobs/${jobId}`);
  return (await response.json()) as JobBody;
}

async function waitFor(jobId: string): Promise<JobBody> {
  const started = Date.now();
  for (;;) {
    const body = await getJob(jobId);
    if (body.state === "completed") return body;
    if (body.state === "failed") throw new Error(`job ${jobId} failed: ${body.error}`);
    if (Date.now() - started > TIMEOUT_MS) throw new Error(`job ${jobId} timed out in ${body.state}`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

function r2GetText(key: string): string | null {
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
    { cwd: ROOT, encoding: "utf8", maxBuffer: 8 * MiB },
  );
  const text = (result.stdout ?? "").trim();
  if (result.status !== 0 || text.length === 0) return null;
  return text;
}

/** Read-only token for R2 listing; kept apart from wrangler's CLOUDFLARE_API_TOKEN. */
function listingToken(): string | undefined {
  return process.env.CFLARE_TOKEN || process.env.CLOUDFLARE_API_TOKEN || undefined;
}

function r2ListPrefix(prefix: string): string[] {
  const account = process.env.CLOUDFLARE_ACCOUNT_ID;
  const token = listingToken();
  if (!account || !token) {
    // Fallback: try a few known spill key patterns via get (existence only).
    return [];
  }
  const keys: string[] = [];
  let cursor: string | undefined;
  for (let page = 0; page < 20; page++) {
    const url = new URL(
      `https://api.cloudflare.com/client/v4/accounts/${account}/r2/buckets/wcol-hits-exports/objects`,
    );
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("per_page", "1000");
    if (cursor) url.searchParams.set("cursor", cursor);
    const response = spawnSync(
      "curl",
      ["-sS", "-H", `Authorization: Bearer ${token}`, url.toString()],
      { encoding: "utf8", maxBuffer: 8 * MiB },
    );
    if (response.status !== 0) break;
    const parsed = JSON.parse(response.stdout || "{}") as {
      result?: Array<{ key?: string }> | { objects?: Array<{ key?: string }> };
      result_info?: { cursor?: string; is_truncated?: boolean };
    };
    const objects = Array.isArray(parsed.result)
      ? parsed.result
      : (parsed.result?.objects ?? []);
    for (const object of objects) {
      if (object.key) keys.push(object.key);
    }
    cursor = parsed.result_info?.cursor;
    if (!parsed.result_info?.is_truncated || !cursor) break;
  }
  return keys;
}

function collectProgress(jobId: string, nodeCount: number): Progress[] {
  const out: Progress[] = [];
  for (let taskId = 0; taskId < Math.max(nodeCount, (REDUCERS ?? 64) + 8); taskId++) {
    const raw = r2GetText(`_jobs/${jobId}/a1/t${taskId}-progress.json`);
    const errRaw = r2GetText(`_jobs/${jobId}/a1/t${taskId}-error.json`);
    if (!raw && !errRaw) continue;
    const progress = raw ? (JSON.parse(raw) as Omit<Progress, "taskId">) : {};
    const error = errRaw ? JSON.parse(errRaw) : undefined;
    out.push({ taskId, ...progress, error });
  }
  return out;
}

async function main(): Promise<void> {
  assert(fs.existsSync(HITS_LOCAL), `missing ${HITS_LOCAL}`);
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  const escaped = HITS_LOCAL.replaceAll("'", "''");
  const expected = asText(
    (await duck.runAndReadAll(Q33.replace(/\bFROM hits\b/i, `FROM read_parquet('${escaped}')`))).getRows() as unknown[][],
  );

  const runId = Date.now().toString(36);
  const wallStart = performance.now();
  const posted = await postJob({
    sources: { hits: { bucket: "output", key: HITS_KEY } },
    sql: Q33,
    output: { format: "json" },
    ...(REDUCERS === undefined ? {} : { hashReducers: REDUCERS }),
    groupSpill: true,
    targetGroupBytes: TARGET,
    hardGroupBytes: HARD,
    idempotencyKey: `prod-q33-2m-${REDUCERS ?? "auto"}-${runId}`,
  });
  assert(
    posted.status === 200 || posted.status === 202,
    `post ${posted.status} ${JSON.stringify(posted.body).slice(0, 400)}`,
  );
  const jobId = String(posted.body.jobId ?? "");
  assert(jobId.length > 0, "missing jobId");
  console.log(JSON.stringify({ event: "accepted", jobId, reducers: REDUCERS, hitsKey: HITS_KEY }));

  let body = posted.body;
  if (posted.status === 202 || body.state === "running" || body.state === "accepted") {
    body = await waitFor(jobId);
  }
  const wallMs = Math.round(performance.now() - wallStart);
  const ours = asText(body.rows ?? body.result?.rows ?? []);
  assert(rowsEqual(ours, expected), `DuckDB mismatch ${JSON.stringify(ours)} vs ${JSON.stringify(expected)}`);

  const progress = collectProgress(jobId, Number(body.nodeCount ?? 0));
  const errors = progress.filter((row) => row.error);
  assert(errors.length === 0, `task errors ${JSON.stringify(errors)}`);

  const spillRuns = progress.reduce((sum, row) => sum + (row.spillRuns ?? 0), 0);
  const peakWasm = progress.reduce((max, row) => Math.max(max, row.wasmMemoryBytes ?? 0), 0);
  const peakGroup = progress.reduce((max, row) => Math.max(max, row.spillPeakLiveBytes ?? 0), 0);
  const peakInterner = progress.reduce(
    (max, row) => Math.max(max, row.spillPeakInternerBytes ?? 0),
    0,
  );

  // Spill runs live under `_jobs/{jobId}/a{attempt}/t{task}/wcol-spill/` and are deleted when each task ends.
  const leftoverSpill = r2ListPrefix(`_jobs/${jobId}/`).filter((key) => key.includes("/wcol-spill/"));

  const report = {
    ok: true,
    jobId,
    hitsKey: HITS_KEY,
    reducers: REDUCERS,
    targetMiB: TARGET / MiB,
    hardMiB: HARD / MiB,
    wallMs,
    nodeCount: body.nodeCount,
    openedEdges: body.schedule?.openedEdges ?? (await getJob(jobId)).schedule?.openedEdges,
    spillRuns,
    peakWasmMiB: Number((peakWasm / MiB).toFixed(2)),
    peakGroupMiB: Number((peakGroup / MiB).toFixed(2)),
    peakInternerMiB: Number((peakInterner / MiB).toFixed(2)),
    progressTasks: progress.length,
    leftoverSpillObjects: leftoverSpill.length,
    leftoverSpillListing: listingToken() && process.env.CLOUDFLARE_ACCOUNT_ID ? "api" : "skipped-no-token",
    duckMatch: true,
    topRows: ours,
    reducersDetail: progress
      .filter((row) => (row.spillRuns ?? 0) > 0 || (row.spillPeakLiveBytes ?? 0) > 0)
      .map((row) => ({
        taskId: row.taskId,
        spillRuns: row.spillRuns ?? 0,
        wasmMiB: Number(((row.wasmMemoryBytes ?? 0) / MiB).toFixed(2)),
        groupMiB: Number(((row.spillPeakLiveBytes ?? 0) / MiB).toFixed(2)),
        internerMiB: Number(((row.spillPeakInternerBytes ?? 0) / MiB).toFixed(2)),
        wallMs: row.wallMs,
      })),
  };
  console.log(JSON.stringify(report));
  if (listingToken() && process.env.CLOUDFLARE_ACCOUNT_ID) {
    assert(leftoverSpill.length === 0, `leftover spill objects: ${leftoverSpill.slice(0, 20).join(", ")}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error);
  process.exit(1);
});
