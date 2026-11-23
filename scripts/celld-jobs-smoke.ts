/**
 * POST /jobs against a running `celld dev` (default http://127.0.0.1:9876).
 * Legacy alternate runtime — not the production Cloudflare path (use Miniflare / workerd).
 *
 *   npm run celld:smoke
 *   CELLD_URL=http://127.0.0.1:9876 npx tsx scripts/celld-jobs-smoke.ts [parquet]
 */
import fs from "node:fs";
import path from "node:path";
import { ROOT } from "./build-workers.ts";

const baseUrl = (process.env.CELLD_URL ?? "http://127.0.0.1:9876").replace(/\/$/, "");
const hitsPath = process.argv[2] ?? path.join(ROOT, "data/hits_subset_500k.parquet");
const timeoutMs = Number(process.env.CELLD_SMOKE_TIMEOUT_MS ?? 300_000);

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

type Json = Record<string, unknown> & { rows?: unknown[][]; columns?: string[]; result?: { rows?: unknown[][] } };

async function request(route: string, init?: RequestInit): Promise<{ status: number; body: Json | string }> {
  const response = await fetch(`${baseUrl}${route}`, init);
  const text = await response.text();
  if (!text) return { status: response.status, body: {} };
  try {
    return { status: response.status, body: JSON.parse(text) as Json };
  } catch {
    return { status: response.status, body: text };
  }
}

async function postJobs(body: unknown): Promise<{ status: number; body: Json }> {
  const res = await request("/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  assert(typeof res.body === "object", `POST /jobs returned non-JSON: ${String(res.body).slice(0, 300)}`);
  return { status: res.status, body: res.body };
}

function rowsOf(body: Json): unknown[][] {
  return body.rows ?? body.result?.rows ?? [];
}

async function waitFor(jobId: string): Promise<Json> {
  const started = Date.now();
  for (;;) {
    const res = await request(`/jobs/${jobId}`);
    assert(typeof res.body === "object", `GET /jobs/${jobId} returned non-JSON`);
    const record = res.body;
    if (record.state === "completed") return record;
    if (record.state === "failed") throw new Error(`job ${jobId} failed: ${record.error}`);
    if (Date.now() - started > timeoutMs) throw new Error(`job ${jobId} timed out in ${record.state}`);
    if ((Date.now() - started) % 5000 < 300) {
      console.error(`waiting for ${jobId}: ${String(record.state)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

const asText = (rows: unknown[][]) => rows.map((row) => row.map((v) => (v === null ? "null" : String(v))));

assert(fs.existsSync(hitsPath), `missing parquet fixture ${hitsPath}`);
console.error(`celld smoke -> ${baseUrl}`);

const health = await request("/nope");
assert(health.status === 404, `celld is not serving ${baseUrl} (GET /nope => ${health.status})`);

const parquet = fs.readFileSync(hitsPath);
console.error(`seeding hits.parquet (${parquet.byteLength} bytes)`);
const seed = await fetch(`${baseUrl}/dev/r2/hits.parquet?bucket=output`, { method: "PUT", body: parquet });
assert(seed.status === 204, `seed R2: ${seed.status} ${await seed.text()}`);
console.error("seeded OUTPUT/hits.parquet");

const retired = await request("/row-export", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: "{}",
});
assert(retired.status === 410, `/row-export is not retired: ${retired.status}`);

const { DuckDBInstance } = await import("@duckdb/node-api");
const duck = await (await DuckDBInstance.create(":memory:")).connect();
const quoted = (file: string) => `'${file.replaceAll("'", "''")}'`;
const duckResult = await duck.runAndReadAll(
  `SELECT RegionID, COUNT(*) AS c FROM ${quoted(hitsPath)} GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5`,
);
const expected = asText((duckResult.getRows() as unknown[][]).map((row) => row.map((v) => (v === null ? null : v))));

const groupSql = "SELECT RegionID, COUNT(*) AS c FROM t GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5";
const jsonBody = {
  sources: { t: { bucket: "output", key: "hits.parquet" } },
  sql: groupSql,
  output: { format: "json" },
  minBytesPerNode: 1,
};
console.error("POST /jobs");
const sync = await postJobs({ ...jsonBody, idempotencyKey: "celld-group-serial-preload-1" });
const jobId = String(sync.body.jobId);
console.error(`job ${jobId} status=${sync.status} state=${String(sync.body.state ?? "inline")}`);
let body = sync.body;
if (sync.status === 202) {
  const done = await waitFor(jobId);
  body = { ...done, ...(typeof done.result === "object" && done.result ? done.result : {}) };
} else {
  assert(sync.status === 200, `json: ${sync.status} ${JSON.stringify(sync.body).slice(0, 500)}`);
}
assert(Number(body.nodeCount) > 1, `json used ${body.nodeCount} node(s); concurrent scans should fan out`);
assert(JSON.stringify(asText(rowsOf(body))) === JSON.stringify(expected), "json rows differ from DuckDB");

const repeat = await postJobs({ ...jsonBody, idempotencyKey: "celld-group-serial-preload-1" });
assert(String(repeat.body.jobId) === jobId && repeat.body.idempotent === true, "idempotency key started a new job");

console.log(
  JSON.stringify({
    celld: "ok",
    url: baseUrl,
    json: "match",
    nodeCount: body.nodeCount,
    idempotent: true,
  }),
);
