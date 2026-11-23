/**
 * Deleting a finished job's scratch from R2. Attempts write under `_jobs/<jobId>/`: task
 * outputs at `<task hex>/<attempt>/...`, finalizer outputs at `finalizer/<attempt>/...`, and
 * spill runs under `a<attempt>/t<task>/wcol-spill/`. Once the job is terminal, only the
 * outputs its result references are needed; the job's own records (timing, plan, result,
 * manifest, task progress) stay.
 */
import type { R2OutputBucket } from "../r2.ts";
import type { GraphJobContext } from "./record.ts";

/** Keys under `_jobs/<jobId>/` that no reader of the finished job needs. Pure. */
export function objectsToSweep(jobId: string, keys: readonly string[], keep: ReadonlySet<string>): string[] {
  const root = `_jobs/${jobId}/`;
  return keys.filter((key) => {
    if (!key.startsWith(root) || keep.has(key)) return false;
    const rest = key.slice(root.length);
    return /^([0-9a-f]{16}|finalizer)\/\d+\//.test(rest) || rest.includes("/wcol-spill/");
  });
}

const DELETE_BATCH = 1000;

/** Delete `jobId`'s attempt outputs that `keep` does not name, and its spill runs. */
export async function sweepJob(bucket: R2OutputBucket, jobId: string, keep: readonly string[]): Promise<number> {
  if (!bucket.list || !bucket.delete) throw new Error("R2 binding cannot list or delete");
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await bucket.list({ prefix: `_jobs/${jobId}/`, cursor });
    for (const object of page.objects) keys.push(object.key);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  const doomed = objectsToSweep(jobId, keys, new Set(keep));
  for (let at = 0; at < doomed.length; at += DELETE_BATCH) {
    await bucket.delete(doomed.slice(at, at + DELETE_BATCH));
  }
  return doomed.length;
}

/**
 * A job just became terminal: drop its alarm, then sweep its scratch in the background.
 * Cleanup never changes the result, so a failed sweep is logged rather than raised; the
 * objects it left are under `_jobs/` and named by attempt, so no reader mistakes them.
 */
export async function jobEnded(ctx: GraphJobContext, jobId: string, keep: readonly string[]): Promise<void> {
  await ctx.deleteAlarm().catch(() => undefined);
  const bucket = ctx.env.OUTPUT;
  if (!bucket) return;
  ctx.waitUntil(
    sweepJob(bucket, jobId, keep).catch((error: unknown) => {
      console.warn(JSON.stringify({ event: "sweep failed", jobId, error: String(error) }));
    }),
  );
}
