/**
 * Ops helper: abort + purge a job's node DOs, abort its container reducers, and purge its
 * coordinator DO, so alarms, SQLite and running containers cannot outlive a stalled run.
 *
 * Gated by `WCOL_SHUTDOWN_SECRET` on `POST /admin/shutdown`.
 */

import { createLimiter } from "../../src/runtime/concurrency.ts";
import { sameSecret } from "./digest.ts";
import { JOB_ID, jobIdsUnder } from "./job-id.ts";
import { graphSlotObjectName } from "./node/session.ts";
import type { R2Env, R2OutputBucket } from "./r2.ts";
import { raceTimeout, type Timed } from "./timeout.ts";

export type ShutdownEnv = R2Env & {
  WCOL_NODE: DurableObjectNamespace;
  WCOL_COORDINATOR: DurableObjectNamespace;
  WCOL_REDUCER_CONTAINER?: DurableObjectNamespace;
  WCOL_SHUTDOWN_SECRET?: string;
};

type AbortParams = { jobId: string; executionAttempt: number; error?: string };

type NodeStub = {
  abortHashAttempt(params: AbortParams): Promise<void>;
  purgeStorage(): Promise<{ ok: true; tablesBefore: number }>;
};

type ReducerStub = { abortHashAttempt(params: AbortParams): Promise<void> };

type CoordinatorStub = { purgeStorage(): Promise<{ ok: true }> };

export type ShutdownJobRequest = {
  /** Explicit job ids. If omitted/empty, discover the ids under `_jobs/` in OUTPUT. */
  jobIds?: string[];
  /** Attempts to touch when slots are discovered. Default [1, 2, 3]. */
  attempts?: number[];
  /** Explicit slots. If omitted, discover from job progress/slots object keys in R2. */
  slots?: number[];
  /** Per-RPC timeout ms. Default 5000. */
  timeoutMs?: number;
};

export type ShutdownTargetResult = {
  name: string;
  kind: "node" | "reducer" | "coordinator";
  abort?: "ok" | "timeout" | "error";
  purge?: "ok" | "timeout" | "error";
  tablesBefore?: number;
  error?: string;
};

export type ShutdownResponse = {
  ok: true;
  jobIds: string[];
  targets: number;
  aborted: number;
  purged: number;
  timedOut: number;
  errors: number;
  results: ShutdownTargetResult[];
};

const naturals = (value: unknown): value is number[] =>
  Array.isArray(value) && value.every((item) => Number.isInteger(item) && item >= 0);

/** The request body, checked once: a malformed body is refused, never read as "every job". */
export function parseShutdownRequest(value: unknown): ShutdownJobRequest | { error: string } {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return { error: "body must be a JSON object" };
  const { jobIds, attempts, slots, timeoutMs } = value as Record<string, unknown>;
  if (jobIds !== undefined && !(Array.isArray(jobIds) && jobIds.every((id) => typeof id === "string" && JOB_ID.test(id)))) {
    return { error: "jobIds must be job ids" };
  }
  if (attempts !== undefined && !naturals(attempts)) return { error: "attempts must be non-negative integers" };
  if (slots !== undefined && !naturals(slots)) return { error: "slots must be non-negative integers" };
  if (timeoutMs !== undefined && !(Number.isInteger(timeoutMs) && Number(timeoutMs) > 0)) {
    return { error: "timeoutMs must be a positive integer" };
  }
  return { jobIds, attempts, slots, timeoutMs } as ShutdownJobRequest;
}

async function listKeys(bucket: R2OutputBucket, prefix: string): Promise<string[]> {
  if (!bucket.list) throw new Error("the output bucket cannot list objects");
  const keys: string[] = [];
  let cursor: string | undefined;
  for (;;) {
    const page = await bucket.list({ prefix, cursor, limit: 1000 });
    for (const object of page.objects) keys.push(object.key);
    if (!page.truncated || !page.cursor || page.cursor === cursor) break;
    cursor = page.cursor;
  }
  return keys;
}

/**
 * Slots per attempt from a job's task records (`_jobs/<job>/a<attempt>/t<task>-…`). Hash
 * graphs use the task id as the slot, so every slot up to the largest seen is covered too,
 * in case a slot wrote no record. Pure.
 */
export function slotsFromKeys(jobId: string, keys: readonly string[], attempts: readonly number[]): Map<number, Set<number>> {
  const byAttempt = new Map<number, Set<number>>(attempts.map((attempt) => [attempt, new Set<number>()]));
  const root = `_jobs/${jobId}/`;
  for (const key of keys) {
    if (!key.startsWith(root)) continue;
    const match = /^a(\d+)\/t(\d+)-/.exec(key.slice(root.length));
    if (!match) continue;
    const attempt = Number(match[1]);
    const slots = byAttempt.get(attempt) ?? new Set<number>();
    slots.add(Number(match[2]));
    byAttempt.set(attempt, slots);
  }
  for (const slots of byAttempt.values()) {
    if (slots.size === 0) continue;
    const max = Math.max(...slots);
    for (let slot = 0; slot <= max; slot++) slots.add(slot);
  }
  return byAttempt;
}

function outcome<T>(timed: Timed<T>): { state: "ok" | "timeout" | "error"; error?: string } {
  return timed.kind === "error" ? { state: "error", error: timed.error } : { state: timed.kind };
}

async function shutdownNode(env: ShutdownEnv, name: string, params: AbortParams, timeoutMs: number): Promise<ShutdownTargetResult> {
  const stub = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(name)) as unknown as NodeStub;
  const aborted = outcome(await raceTimeout(stub.abortHashAttempt(params), timeoutMs));
  const purgedTimed = await raceTimeout(stub.purgeStorage(), timeoutMs);
  const purged = outcome(purgedTimed);
  const error = [aborted.error, purged.error].filter(Boolean).join("; ");
  return {
    name,
    kind: "node",
    abort: aborted.state,
    purge: purged.state,
    ...(purgedTimed.kind === "ok" ? { tablesBefore: purgedTimed.value.tablesBefore } : {}),
    ...(error ? { error } : {}),
  };
}

async function shutdownReducer(namespace: DurableObjectNamespace, name: string, params: AbortParams, timeoutMs: number): Promise<ShutdownTargetResult> {
  const stub = namespace.get(namespace.idFromName(name)) as unknown as ReducerStub;
  const aborted = outcome(await raceTimeout(stub.abortHashAttempt(params), timeoutMs));
  return { name, kind: "reducer", abort: aborted.state, ...(aborted.error ? { error: aborted.error } : {}) };
}

async function shutdownCoordinator(env: ShutdownEnv, jobId: string, timeoutMs: number): Promise<ShutdownTargetResult> {
  const stub = env.WCOL_COORDINATOR.get(env.WCOL_COORDINATOR.idFromName(jobId)) as unknown as CoordinatorStub;
  const purged = outcome(await raceTimeout(stub.purgeStorage(), timeoutMs));
  return { name: `coordinator:${jobId}`, kind: "coordinator", purge: purged.state, ...(purged.error ? { error: purged.error } : {}) };
}

type Target =
  | { kind: "slot"; name: string; jobId: string; executionAttempt: number }
  | { kind: "coordinator"; jobId: string };

export async function shutdownJobs(env: ShutdownEnv, request: ShutdownJobRequest): Promise<ShutdownResponse> {
  const attempts = request.attempts ?? [1, 2, 3];
  const timeoutMs = request.timeoutMs ?? 5_000;
  const bucket = env.OUTPUT;
  const keysUnder = (prefix: string) => (bucket ? listKeys(bucket, prefix) : Promise.resolve([]));
  // Discovery lists every job's records once; explicit ids list only their own.
  const everything = request.jobIds?.length ? null : await keysUnder("_jobs/");
  const jobIds = everything ? jobIdsUnder(everything) : [...new Set(request.jobIds)];

  const targets: Target[] = [];
  for (const jobId of jobIds) {
    const discovered = slotsFromKeys(jobId, everything ?? (await keysUnder(`_jobs/${jobId}/`)), attempts);
    for (const attempt of attempts) {
      const slots = request.slots?.length ? request.slots : [...(discovered.get(attempt) ?? [])].sort((a, b) => a - b);
      // No records: still try a small default range (a few reducers and scans).
      const effective = slots.length > 0 ? slots : Array.from({ length: 16 }, (_, i) => i);
      for (const slot of effective) {
        targets.push({ kind: "slot", name: graphSlotObjectName(jobId, attempt, slot), jobId, executionAttempt: attempt });
      }
    }
    targets.push({ kind: "coordinator", jobId });
  }

  const limit = createLimiter(8);
  const reducers = env.WCOL_REDUCER_CONTAINER;
  const results = (
    await Promise.all(
      targets.map((target) =>
        limit(async (): Promise<ShutdownTargetResult[]> => {
          if (target.kind === "coordinator") return [await shutdownCoordinator(env, target.jobId, timeoutMs)];
          const params = { jobId: target.jobId, executionAttempt: target.executionAttempt, error: "admin shutdown" };
          // A slot's reducer may be a container rather than the node: stop both.
          return Promise.all([
            shutdownNode(env, target.name, params, timeoutMs),
            ...(reducers ? [shutdownReducer(reducers, target.name, params, timeoutMs)] : []),
          ]);
        }),
      ),
    )
  ).flat();

  return {
    ok: true,
    jobIds,
    targets: results.length,
    aborted: results.filter((row) => row.abort === "ok").length,
    purged: results.filter((row) => row.purge === "ok").length,
    timedOut: results.filter((row) => row.abort === "timeout" || row.purge === "timeout").length,
    errors: results.filter((row) => row.abort === "error" || row.purge === "error").length,
    results,
  };
}

export async function shutdownAuthorized(env: ShutdownEnv, request: Request): Promise<boolean> {
  const secret = env.WCOL_SHUTDOWN_SECRET;
  if (!secret) return false;
  return sameSecret(request.headers.get("x-wcol-shutdown") ?? "", secret);
}
