/**
 * Scaling study: one query over a grid of scan and reducer counts on the deployed
 * coordinator. Each run records wall time, the attempt's usage (task busy time, R2 reads,
 * spill I/O; `src/runtime/coordinator/attempt-usage.ts`) and a list-price cost estimate.
 *
 *   WCOL_BASE_URL=https://wcol-coordinator....workers.dev \
 *   QUERY=Q8 SCANS=8,16,32,64 REDUCERS=8,16,32,64 REPEATS=3 \
 *   npx tsx scripts/cf-scaling.ts
 *
 * Results append to data/scaling/{query}-{runId}.jsonl, one line per run. Every setting must
 * return the rows the first completed run returned; a run that differs is marked `rowsMatch:
 * false`. Settings run in a shuffled order inside each repeat, so drift over the session
 * (a warming pool, a busy colo) spreads across settings instead of biasing one.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  estimateCost,
  type AttemptUsage,
} from "../src/runtime/coordinator/attempt-usage.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const QUERIES: Record<string, string> = {
  Q8: "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
  Q15: "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
  REGION:
    "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5",
};

type Setting = { readonly scans: number; readonly reducers: number };

type JobBody = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
  executionAttempt?: number;
  rows?: unknown[][];
  result?: { rows?: unknown[][] };
  schedule?: { openedEdges?: number };
  usage?: AttemptUsage;
  stages?: { stage: string; fromMs: number; toMs: number; maxMs: number }[];
};

function list(value: string | undefined, fallback: number[]): number[] {
  return value ? value.split(",").map((part) => Number(part.trim())) : fallback;
}

/** Each repeat runs every setting once, in an order shuffled by a fixed seed per repeat. */
export function schedule(
  scans: readonly number[],
  reducers: readonly number[],
  repeats: number,
): Setting[][] {
  const grid = scans.flatMap((s) =>
    reducers.map((r) => ({ scans: s, reducers: r })),
  );
  return Array.from({ length: repeats }, (_, repeat) => {
    let seed = 0x9e3779b9 ^ (repeat + 1);
    const next = () => {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return (seed >>> 0) / 0x100000000;
    };
    const order = [...grid];
    for (let i = order.length - 1; i > 0; i--) {
      const j = Math.floor(next() * (i + 1));
      [order[i], order[j]] = [order[j]!, order[i]!];
    }
    return order;
  });
}

async function request(
  base: string,
  url: string,
  init?: RequestInit,
): Promise<{ status: number; body: JobBody }> {
  const response = await fetch(`${base}${url}`, init);
  const text = await response.text();
  try {
    return { status: response.status, body: JSON.parse(text) as JobBody };
  } catch {
    return { status: response.status, body: { error: text.slice(0, 300) } };
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll until the job ends, then until its background `timing.json` carries usage. */
async function settle(
  base: string,
  jobId: string,
  timeoutMs: number,
): Promise<JobBody> {
  const started = Date.now();
  let body: JobBody = {};
  for (;;) {
    body = (await request(base, `/jobs/${jobId}`)).body;
    if (body.state === "completed" || body.state === "failed") break;
    if (Date.now() - started > timeoutMs)
      return { ...body, error: `timed out in ${body.state}` };
    await sleep(500);
  }
  for (let wait = 0; wait < 20 && !body.usage; wait++) {
    await sleep(1000);
    body = (await request(base, `/jobs/${jobId}`)).body;
  }
  return body;
}

/** RPC sessions an attempt opens: one per edge, plus a handful of calls per task. */
function rpcSessions(body: JobBody, setting: Setting): number {
  return (
    (body.schedule?.openedEdges ?? setting.scans * setting.reducers) +
    4 * (setting.scans + setting.reducers)
  );
}

async function main(): Promise<void> {
  const base = process.env.WCOL_BASE_URL;
  if (!base) throw new Error("set WCOL_BASE_URL");
  const query = process.env.QUERY ?? "Q8";
  const sql = QUERIES[query];
  if (!sql)
    throw new Error(`QUERY must be one of ${Object.keys(QUERIES).join(", ")}`);
  const scans = list(process.env.SCANS, [8, 16, 32, 64]);
  const reducers = list(process.env.REDUCERS, [8, 16, 32, 64]);
  const repeats = Number(process.env.REPEATS ?? 3);
  const timeoutMs = Number(process.env.TIMEOUT_MS ?? 900_000);
  const runId = Date.now().toString(36);
  const outDir = path.join(ROOT, "data/scaling");
  fs.mkdirSync(outDir, { recursive: true });
  const out = path.join(outDir, `${query}-${runId}.jsonl`);
  let reference: string | undefined;

  const rounds = schedule(scans, reducers, repeats);
  const tries = Number(process.env.TRIES ?? 2);
  for (const [repeat, round] of rounds.entries()) {
    for (const setting of round) {
      for (let attemptNo = 1; attemptNo <= tries; attemptNo++) {
        if (attemptNo > 1) await sleep(60_000);
        const started = Date.now();
        const posted = await request(base, "/jobs", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sources: { hits: { bucket: "output", key: "hits.parquet" } },
            sql,
            output: { format: "json" },
            hashReducers: setting.reducers,
            maxNodes: setting.scans,
            idempotencyKey: `scaling-${query}-${runId}-${repeat}-${setting.scans}-${setting.reducers}-${attemptNo}`,
          }),
        });
        const jobId = posted.body.jobId;
        const body =
          jobId && (posted.status === 200 || posted.status === 202)
            ? await settle(base, jobId, timeoutMs)
            : { ...posted.body, state: "failed" };
        const wallMs = Date.now() - started;
        const rows = JSON.stringify(body.rows ?? body.result?.rows ?? null);
        const ok = body.state === "completed";
        if (ok && reference === undefined) reference = rows;
        const line = {
          query,
          runId,
          repeat,
          try: attemptNo,
          ...setting,
          jobId,
          ok,
          error: ok ? undefined : body.error,
          attempt: body.executionAttempt,
          nodeCount: body.nodeCount,
          wallMs,
          rowsMatch: ok ? rows === reference : undefined,
          stages: body.stages?.map(({ stage, fromMs, toMs, maxMs }) => ({
            stage,
            fromMs,
            toMs,
            maxMs,
          })),
          usage: body.usage,
          cost:
            ok && body.usage
              ? estimateCost(body.usage, wallMs, rpcSessions(body, setting))
              : undefined,
        };
        fs.appendFileSync(out, `${JSON.stringify(line)}\n`);
        console.log(
          JSON.stringify({
            repeat,
            scans: setting.scans,
            reducers: setting.reducers,
            ok,
            attempt: body.executionAttempt,
            wallMs,
            rowsMatch: line.rowsMatch,
            spillPuts: body.usage?.spillPuts,
            doSeconds: line.cost ? Math.round(line.cost.doSeconds) : undefined,
            usd: line.cost ? Number(line.cost.total.toFixed(5)) : undefined,
            error: line.error?.slice(0, 120),
          }),
        );
        // A failed setting waits a minute and runs once more (see `tries`); both lines are kept.
        if (ok) break;
      }
    }
  }
  console.log(JSON.stringify({ event: "done", out }));
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await main();
}
