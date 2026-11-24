/**
 * Record ClickBench runs for the public console. Each query runs once on the deployed
 * coordinator; its final live snapshot (which holds the whole timeline) and its rows are
 * saved to `cloudflare/console/recordings/<name>.json`. Visitors without a token replay them.
 *
 *   WCOL_BASE_URL=https://wcol-coordinator.<subdomain>.workers.dev \
 *     node --env-file=.env --import tsx scripts/record-console-runs.ts Q1 Q13 Q32
 */
import fs from "node:fs";
import path from "node:path";
import { clickbenchQueries } from "./clickbench-catalog.ts";

const ROOT = path.resolve(import.meta.dirname, "..");
const OUT = path.join(ROOT, "cloudflare/console/recordings");
const POLL_MS = 1000;

const base = process.env.WCOL_BASE_URL;
if (!base) throw new Error("set WCOL_BASE_URL");
const headers = {
  "content-type": "application/json",
  ...(process.env.WCOL_API_TOKEN ? { authorization: `Bearer ${process.env.WCOL_API_TOKEN}` } : {}),
};

type Snapshot = { state: string; result: unknown };
type Rows = { columns?: string[]; rows?: unknown[][] };

async function record(name: string, sql: string): Promise<void> {
  const jobId = `recording-${name}-${crypto.randomUUID()}`;
  // The console's own request, so a replay shows what a visitor's run would have done.
  const body = {
    jobId,
    sources: { hits: { bucket: "output", key: "hits.parquet" } },
    sql,
    output: { format: "json", rowCap: 1000 },
    reducers: "auto",
    reducerInstance: "basic",
  };
  const posted = fetch(`${base}/jobs`, { method: "POST", headers, body: JSON.stringify(body) }).then(async (response) => {
    const answer = (await response.json()) as Rows & { error?: string };
    if (!response.ok) throw new Error(`${name}: ${answer.error ?? response.status}`);
    return answer.rows ? answer : null;
  });
  let snapshot: Snapshot | null = null;
  while (!snapshot || (snapshot.state !== "completed" && snapshot.state !== "failed")) {
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
    const response = await fetch(`${base}/jobs/${encodeURIComponent(jobId)}/live`, { headers });
    if (response.status === 404) continue;
    if (!response.ok) throw new Error(`${name}: live ${response.status} ${await response.text()}`);
    snapshot = (await response.json()) as Snapshot;
  }
  const rows = await posted;
  if (snapshot.state !== "completed") throw new Error(`${name}: ${snapshot.state}`);
  const recording = { name, sql, recordedAt: new Date().toISOString(), snapshot, result: snapshot.result ?? rows };
  fs.mkdirSync(OUT, { recursive: true });
  const file = path.join(OUT, `${name}.json`);
  fs.writeFileSync(file, JSON.stringify(recording));
  console.log(`${name}: ${(fs.statSync(file).size / 1024).toFixed(0)} KiB`);
}

const catalog = clickbenchQueries(ROOT);
for (const name of process.argv.slice(2)) {
  const query = catalog.find((candidate) => candidate.name === name);
  if (!query) throw new Error(`${name} is not a supported ClickBench query`);
  await record(name, query.sql);
}
