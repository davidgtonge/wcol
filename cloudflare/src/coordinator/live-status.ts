/**
 * `GET /jobs/live`: the snapshot the console builds its view from (see `live.ts`). It reads
 * the job record and the coordinator's in-memory attempt; an evicted coordinator answers
 * with the record alone.
 */
import type { LiveSnapshot } from "./live.ts";
import { jobKey, readJobRecord, type GraphJobContext } from "./record.ts";

export async function liveSnapshot(ctx: GraphJobContext, jobId: string, now: number): Promise<LiveSnapshot | null> {
  const record = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!record) return null;
  const live = ctx.live.get(jobId);
  const ended = record.state === "completed" || record.state === "failed";
  return {
    jobId,
    state: record.state,
    attempt: live?.attempt ?? record.executionAttempt,
    startedAt: live?.startedAt ?? record.createdAt,
    // A finished job's clock stops at its last update.
    now: ended ? record.updatedAt : now,
    graph: live?.graph ?? { tasks: [], edges: [], rootTaskId: 0 },
    spans: live?.timeline.spans() ?? [],
    open: live?.timeline.open() ?? [],
    facts: live ? [...live.facts] : [],
    reducers: record.reducerRuntime === "container" ? { runtime: "container", instance: record.reducerInstance } : { runtime: "node" },
    reducerReason: record.reducerReason ?? null,
    coordinatorColo: live?.coordinatorColo ?? null,
    sourceBytes: live?.sourceBytes ?? 0,
    error: record.error ?? null,
    result: record.state === "completed" ? await readResult(ctx, record.resultKey) : null,
  };
}

async function readResult(ctx: GraphJobContext, key: string | undefined): Promise<unknown> {
  if (!key || !ctx.env.OUTPUT) return null;
  const body = await ctx.env.OUTPUT.get(key);
  return body && body.body !== null ? (JSON.parse(await body.text()) as unknown) : null;
}
