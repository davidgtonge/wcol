/** Lease renewal for a running attempt, and the stall watchdog that rides on it. */
import { ALARM_MS, type GraphJobContext, jobKey, LEASE_MS, readJobRecord, RENEW_MS, STALL_MS } from "./record.ts";

async function latestProgressAt(ctx: GraphJobContext, jobId: string, attempt: number): Promise<number | null | undefined> {
  // celld: listing/getting progress objects while scans read the same local store deadlocks.
  if (ctx.env.WCOL_BUFFER_SCAN_EDGES === "1") return undefined;
  const bucket = ctx.env.OUTPUT;
  if (!bucket?.list) return undefined;
  const prefix = `_jobs/${jobId}/a${attempt}/`;
  try {
    const keys: string[] = [];
    let cursor: string | undefined;
    for (;;) {
      const page = await bucket.list({ prefix, cursor });
      for (const object of page.objects) {
        if (object.key.endsWith("-progress.json")) keys.push(object.key);
      }
      if (!page.truncated || !page.cursor || page.cursor === cursor) break;
      cursor = page.cursor;
    }
    if (keys.length === 0) return null;
    const stamps = await Promise.all(
      keys.map(async (key) => {
        const object = await bucket.get(key);
        if (!object) return null;
        const at = Number((JSON.parse(await object.text()) as { at?: unknown }).at);
        return Number.isFinite(at) ? at : null;
      }),
    );
    let newest: number | null = null;
    for (const at of stamps) {
      if (at != null && (newest == null || at > newest)) newest = at;
    }
    return newest;
  } catch {
    return undefined;
  }
}

export type Renewal = { stop: () => void; stalled: Promise<string> };

export function startRenewal(ctx: GraphJobContext, jobId: string, attempt: number): Renewal {
  let stopped = false;
  let fired = false;
  let resolveStall: (reason: string) => void = () => undefined;
  const stalled = new Promise<string>((resolve) => {
    resolveStall = resolve;
  });
  let progressAt = Date.now();
  const configured = Number(ctx.env.WCOL_STALL_MS);
  const stallMs = Number.isFinite(configured) && configured >= 1_000 ? configured : STALL_MS;
  const timer = setInterval(() => {
    if (stopped) return;
    // celld: extra coordinator events during concurrent scan RPCs stall outbound calls.
    if (ctx.env.WCOL_BUFFER_SCAN_EDGES === "1") return;
    void ctx.transaction(async (txn) => {
      if (stopped) return;
      const current = readJobRecord(await txn.get(jobKey(jobId)));
      if (stopped) return;
      if (!current || current.executionAttempt !== attempt) return;
      if (current.state === "completed" || current.state === "failed") return;
      current.leaseUntil = Date.now() + LEASE_MS;
      current.updatedAt = Date.now();
      await txn.put(jobKey(jobId), current);
      if (stopped) return;
      await txn.setAlarm?.(Date.now() + ALARM_MS);
    });
    if (fired) return;
    void (async () => {
      if (stopped || fired) return;
      const newest = await latestProgressAt(ctx, jobId, attempt);
      if (stopped || fired || newest === undefined) return;
      if (newest != null && newest > progressAt) progressAt = newest;
      if (Date.now() - progressAt < stallMs) return;
      fired = true;
      resolveStall(`stalled: no task progress for ${stallMs}ms`);
    })();
  }, RENEW_MS);
  return {
    stalled,
    stop() {
      stopped = true;
      clearInterval(timer);
    },
  };
}

export async function recordStall(ctx: GraphJobContext, jobId: string, attempt: number, reason: string): Promise<void> {
  await ctx.transaction(async (txn) => {
    const current = readJobRecord(await txn.get(jobKey(jobId)));
    if (!current || current.executionAttempt !== attempt) return;
    if (current.state === "completed" || current.state === "failed") return;
    if (current.error?.includes("stalled:")) return;
    current.error = reason;
    current.updatedAt = Date.now();
    await txn.put(jobKey(jobId), current);
  });
}
