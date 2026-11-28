/**
 * Job ids name R2 prefixes (`_jobs/<id>/`) and Durable Objects, so they stay plain. The
 * Worker issues three shapes, all within `JOB_ID`: the client's own id, a random UUID, and
 * `k-<hex>` from an idempotency key.
 */
import { hex, sha256 } from "./digest.ts";

export const JOB_ID = /^[A-Za-z0-9_-]{1,64}$/;

/** A client `idempotencyKey` names the job, so a repeat reaches the same coordinator. */
export async function jobIdFor(idempotencyKey: string | undefined): Promise<string> {
  if (!idempotencyKey) return crypto.randomUUID();
  const digest = await sha256(new TextEncoder().encode(idempotencyKey));
  return `k-${hex(digest.subarray(0, 16))}`;
}

/** The job ids that own objects among `keys` (any listing under `_jobs/`). Pure. */
export function jobIdsUnder(keys: Iterable<string>): string[] {
  const ids = new Set<string>();
  for (const key of keys) {
    const id = /^_jobs\/([^/]+)\//.exec(key)?.[1];
    if (id !== undefined && JOB_ID.test(id)) ids.add(id);
  }
  return [...ids].sort();
}
