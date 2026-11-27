/**
 * Abort + purge Durable Objects for recent (or listed) wcol jobs.
 *
 *   CF_ACCOUNT_ID=… WCOL_SHUTDOWN_SECRET=… npx tsx scripts/cf-shutdown-dos.ts
 *   WCOL_SHUTDOWN_SECRET=… npx tsx scripts/cf-shutdown-dos.ts k-abc…
 */

import { listR2Objects } from "./r2-cloudflare-api.ts";

const BASE = process.env.WCOL_BASE_URL;
if (!BASE) throw new Error("set WCOL_BASE_URL (for example https://wcol-coordinator.<your-subdomain>.workers.dev)");
const SECRET = process.env.WCOL_SHUTDOWN_SECRET;
const BATCH = Number(process.env.WCOL_SHUTDOWN_BATCH ?? 4);
if (!SECRET) {
  console.error("WCOL_SHUTDOWN_SECRET is required");
  process.exit(1);
}

async function discoverJobIds(): Promise<string[]> {
  const objects = await listR2Objects("wcol-hits-exports", "_jobs/", { limit: 1000 });
  const ids = new Set<string>();
  for (const object of objects) {
    const match = object.key.match(/^_jobs\/(k-[a-f0-9]+)\//);
    if (match) ids.add(match[1]!);
  }
  return [...ids].sort();
}

async function shutdownBatch(jobIds: string[]): Promise<unknown> {
  const response = await fetch(`${BASE}/admin/shutdown`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-wcol-shutdown": SECRET!,
    },
    body: JSON.stringify({
      jobIds,
      attempts: [1, 2, 3],
      timeoutMs: 8_000,
    }),
  });
  const text = await response.text();
  let body: unknown = text;
  try {
    body = JSON.parse(text);
  } catch {
    // keep text
  }
  if (!response.ok) {
    throw new Error(`shutdown failed ${response.status}: ${text.slice(0, 500)}`);
  }
  return body;
}

const explicit = process.argv.slice(2).filter((arg) => arg.startsWith("k-"));
const jobIds = explicit.length ? explicit : await discoverJobIds();
console.log(JSON.stringify({ event: "discovered", jobCount: jobIds.length, jobIds }, null, 2));

const summary = { batches: 0, aborted: 0, purged: 0, timedOut: 0, errors: 0, targets: 0 };
for (let i = 0; i < jobIds.length; i += BATCH) {
  const batch = jobIds.slice(i, i + BATCH);
  const body = (await shutdownBatch(batch)) as {
    aborted?: number;
    purged?: number;
    timedOut?: number;
    errors?: number;
    targets?: number;
    results?: unknown[];
  };
  summary.batches += 1;
  summary.aborted += body.aborted ?? 0;
  summary.purged += body.purged ?? 0;
  summary.timedOut += body.timedOut ?? 0;
  summary.errors += body.errors ?? 0;
  summary.targets += body.targets ?? 0;
  console.log(
    JSON.stringify({
      event: "batch",
      batch,
      aborted: body.aborted,
      purged: body.purged,
      timedOut: body.timedOut,
      errors: body.errors,
      targets: body.targets,
      sample: (body.results ?? []).slice(0, 8),
    }),
  );
}

console.log(JSON.stringify({ event: "done", ...summary }, null, 2));
