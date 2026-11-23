/**
 * Ops helper: abort + purge graph node DOs and their coordinator DO so alarms
 * and SQLite cannot keep isolates awake after stalled runs.
 *
 * Gated by `WCOL_SHUTDOWN_SECRET` on `POST /admin/shutdown`.
 */

import { graphSlotObjectName } from "./node/session.ts";
import type { DualR2Env } from "./r2.ts";

export type ShutdownEnv = DualR2Env & {
  WCOL_NODE: DurableObjectNamespace;
  WCOL_COORDINATOR: DurableObjectNamespace;
  WCOL_SHUTDOWN_SECRET?: string;
};

type NodeStub = {
  abortHashAttempt(params: {
    jobId: string;
    executionAttempt: number;
    error?: string;
  }): Promise<void>;
  purgeStorage(): Promise<{ ok: true; tablesBefore: number }>;
};

type CoordinatorStub = {
  purgeStorage(): Promise<{ ok: true }>;
};

export type ShutdownJobRequest = {
  /** Explicit job ids. If omitted/empty, discover recent ids under `_jobs/` in OUTPUT. */
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
  kind: "node" | "coordinator";
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

async function raceTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<{ ok: true; value: T } | { ok: false; timedOut: true } | { ok: false; error: string }> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      promise.then((value) => ({ kind: "value" as const, value })),
      new Promise<{ kind: "timeout" }>((resolve) => {
        timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
      }),
    ]);
    if (result.kind === "timeout") return { ok: false, timedOut: true };
    return { ok: true, value: result.value };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function mapPool<T, R>(items: T[], concurrency: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, Math.max(items.length, 1)) }, async () => {
    for (;;) {
      const index = next;
      next += 1;
      if (index >= items.length) return;
      out[index] = await fn(items[index]!);
    }
  });
  await Promise.all(workers);
  return out;
}

async function listKeys(bucket: R2Bucket, prefix: string): Promise<string[]> {
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

async function discoverJobIds(env: ShutdownEnv): Promise<string[]> {
  if (!env.OUTPUT) return [];
  const keys = await listKeys(env.OUTPUT, "_jobs/");
  const ids = new Set<string>();
  for (const key of keys) {
    const match = key.match(/^_jobs\/(k-[a-f0-9]+)\//);
    if (match) ids.add(match[1]!);
  }
  return [...ids].sort();
}

async function discoverSlots(env: ShutdownEnv, jobId: string, attempts: number[]): Promise<Map<number, Set<number>>> {
  const byAttempt = new Map<number, Set<number>>();
  for (const attempt of attempts) byAttempt.set(attempt, new Set());
  if (!env.OUTPUT) return byAttempt;
  const keys = await listKeys(env.OUTPUT, `_jobs/${jobId}/`);
  for (const key of keys) {
    const match = key.match(new RegExp(`^_jobs/${jobId}/a(\\d+)/t(\\d+)-`));
    if (!match) continue;
    const attempt = Number(match[1]);
    const slot = Number(match[2]);
    if (!Number.isInteger(attempt) || !Number.isInteger(slot)) continue;
    const set = byAttempt.get(attempt) ?? new Set<number>();
    set.add(slot);
    byAttempt.set(attempt, set);
  }
  // Hash graphs use taskId as the DO slot today; also cover a small pad around
  // discovered ids in case a slot wrote no progress object.
  for (const [attempt, slots] of byAttempt) {
    if (slots.size === 0) continue;
    const max = Math.max(...slots);
    for (let slot = 0; slot <= max; slot++) slots.add(slot);
  }
  return byAttempt;
}

async function shutdownNode(
  env: ShutdownEnv,
  name: string,
  kind: "node",
  params: { jobId?: string; executionAttempt?: number },
  timeoutMs: number,
): Promise<ShutdownTargetResult> {
  const stub = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(name)) as unknown as NodeStub;
  const row: ShutdownTargetResult = { name, kind };
  if (kind === "node" && params.jobId != null && params.executionAttempt != null) {
    const aborted = await raceTimeout(
      stub.abortHashAttempt({
        jobId: params.jobId,
        executionAttempt: params.executionAttempt,
        error: "admin shutdown",
      }),
      timeoutMs,
    );
    if (aborted.ok) row.abort = "ok";
    else if ("timedOut" in aborted && aborted.timedOut) row.abort = "timeout";
    else {
      row.abort = "error";
      row.error = "error" in aborted ? aborted.error : "abort failed";
    }
  }
  const purged = await raceTimeout(stub.purgeStorage(), timeoutMs);
  if (purged.ok) {
    row.purge = "ok";
    row.tablesBefore = purged.value.tablesBefore;
  } else if ("timedOut" in purged && purged.timedOut) {
    row.purge = "timeout";
  } else {
    row.purge = "error";
    row.error = [row.error, "error" in purged ? purged.error : "purge failed"].filter(Boolean).join("; ");
  }
  return row;
}

export async function shutdownJobs(env: ShutdownEnv, request: ShutdownJobRequest): Promise<ShutdownResponse> {
  const attempts = request.attempts ?? [1, 2, 3];
  const timeoutMs = request.timeoutMs ?? 5_000;
  let jobIds = [...new Set((request.jobIds ?? []).map((id) => id.trim()).filter(Boolean))];
  if (jobIds.length === 0) jobIds = await discoverJobIds(env);

  type Work =
    | { kind: "node"; name: string; jobId: string; executionAttempt: number }
    | { kind: "coordinator"; jobId: string };

  const work: Work[] = [];
  for (const jobId of jobIds) {
    const discovered = await discoverSlots(env, jobId, attempts);
    for (const attempt of attempts) {
      const slots = request.slots?.length
        ? request.slots
        : [...(discovered.get(attempt) ?? [])].sort((a, b) => a - b);
      // No artifacts: still try a small default range used by Q33 (2 reducers + scans).
      const effective = slots.length > 0 ? slots : Array.from({ length: 16 }, (_, i) => i);
      for (const slot of effective) {
        work.push({
          kind: "node",
          name: graphSlotObjectName(jobId, attempt, slot),
          jobId,
          executionAttempt: attempt,
        });
      }
    }
    work.push({ kind: "coordinator", jobId });
  }

  const results = await mapPool(work, 8, async (item) => {
    if (item.kind === "coordinator") {
      const stub = env.WCOL_COORDINATOR.get(
        env.WCOL_COORDINATOR.idFromName(item.jobId),
      ) as unknown as CoordinatorStub;
      const purged = await raceTimeout(stub.purgeStorage(), timeoutMs);
      if (purged.ok) return { name: `coordinator:${item.jobId}`, kind: "coordinator" as const, purge: "ok" as const };
      if ("timedOut" in purged && purged.timedOut) {
        return { name: `coordinator:${item.jobId}`, kind: "coordinator" as const, purge: "timeout" as const };
      }
      return {
        name: `coordinator:${item.jobId}`,
        kind: "coordinator" as const,
        purge: "error" as const,
        error: "error" in purged ? purged.error : "purge failed",
      };
    }
    return shutdownNode(
      env,
      item.name,
      "node",
      { jobId: item.jobId, executionAttempt: item.executionAttempt },
      timeoutMs,
    );
  });

  return {
    ok: true,
    jobIds,
    targets: work.length,
    aborted: results.filter((row) => row.abort === "ok").length,
    purged: results.filter((row) => row.purge === "ok").length,
    timedOut: results.filter((row) => row.abort === "timeout" || row.purge === "timeout").length,
    errors: results.filter((row) => row.abort === "error" || row.purge === "error").length,
    results,
  };
}

export function shutdownAuthorized(env: ShutdownEnv, request: Request): boolean {
  const secret = env.WCOL_SHUTDOWN_SECRET;
  if (!secret) return false;
  return request.headers.get("x-wcol-shutdown") === secret;
}
