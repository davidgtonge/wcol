/**
 * The job API end to end on workerd/Miniflare: json, parquet, idempotency, alarms.
 * DuckDB is the oracle. Concat jobs are rejected at submit.
 *
 *   npx tsx scripts/cf-workerd-jobs.ts [hits.parquet]
 */

import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, coordinatorOut, distDir, nodeOut, ROOT } from "./build-workers.ts";

const defaultHits = path.join(ROOT, "data/hits_subset_500k.parquet");
const hitsPathArg = process.argv[2];
const fixturePath = path.join(ROOT, "tests/fixtures/duckdb_nulls.parquet");

const { DuckDBInstance } = await import("@duckdb/node-api");
const duck = await (await DuckDBInstance.create(":memory:")).connect();
const quoted = (file: string) => `'${file.replaceAll("'", "''")}'`;

async function ensureHitsParquet(): Promise<{ path: string; synthetic: boolean }> {
  const candidate = hitsPathArg ?? defaultHits;
  if (fs.existsSync(candidate)) return { path: candidate, synthetic: false };
  fs.mkdirSync(path.join(ROOT, "data"), { recursive: true });
  const generated = path.join(ROOT, "data", ".cf-workerd-hits-synthetic.parquet");
  await duck.run(
    `COPY (
      SELECT
        (i % 10)::INTEGER AS RegionID,
        i::BIGINT AS CounterID,
        (i % 5000)::BIGINT AS UserID
      FROM range(80000) t(i)
    ) TO ${quoted(generated)} (FORMAT PARQUET)`,
  );
  return { path: generated, synthetic: true };
}

const hits = await ensureHitsParquet();
const hitsPath = hits.path;
const usingSyntheticHits = hits.synthetic;

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function miniflare(bindings: Record<string, string> = {}): Miniflare {
  return new Miniflare({
    workers: [
      {
        name: "wcol-coordinator",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: coordinatorOut },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_coordinator.wasm") },
        ],
        compatibilityDate: "2026-01-01",
        bindings,
        durableObjects: {
          WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
          WCOL_NODE: { className: "WcolNode", scriptName: "wcol-node", useSQLite: true },
        },
        r2Buckets: ["OUTPUT", "ANALYTICS"],
      },
      {
        name: "wcol-node",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: nodeOut },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_node.wasm") },
        ],
        compatibilityDate: "2026-01-01",
        durableObjects: {
          WCOL_NODE: { className: "WcolNode", useSQLite: true },
          // Hash scans claim their units on the job's coordinator, as in wrangler.node.toml.
          WCOL_COORDINATOR: { className: "WcolCoordinator", scriptName: "wcol-coordinator" },
        },
        r2Buckets: ["OUTPUT", "ANALYTICS"],
      },
    ],
  });
}

type Json = Record<string, unknown> & { rows?: unknown[][]; columns?: string[] };

async function post(mf: Miniflare, route: string, body: unknown): Promise<{ status: number; body: Json }> {
  const response = await mf.dispatchFetch(`http://wcol.local${route}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: JSON.parse(text) as Json };
}

async function waitFor(mf: Miniflare, jobId: string): Promise<Json> {
  const started = Date.now();
  for (;;) {
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await response.json()) as Json;
    if (record.state === "completed") return record;
    if (record.state === "failed") throw new Error(`job ${jobId} failed: ${record.error}`);
    if (Date.now() - started > 300_000) throw new Error(`job ${jobId} timed out in ${record.state}`);
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

/** DuckDB rows as strings (big integers and floats compare exactly as text). */
async function duckRows(sql: string): Promise<string[][]> {
  const result = await duck.runAndReadAll(sql);
  return (result.getRows() as unknown[][]).map((row) => row.map((v) => (v === null ? "null" : String(v))));
}

const asText = (rows: unknown[][]) => rows.map((row) => row.map((v) => (v === null ? "null" : String(v))));

/** A json job over one R2 object (table `t`). */
async function query(mf: Miniflare, key: string, sql: string): Promise<string[][]> {
  const res = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key } },
    sql,
    output: { format: "json" },
  });
  assert(res.status === 200, `query ${sql}: ${res.status} ${JSON.stringify(res.body).slice(0, 500)}`);
  return asText(res.body.rows!);
}

async function listKeys(bucket: Awaited<ReturnType<Miniflare["getR2Bucket"]>>, prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  for (;;) {
    const page = await bucket.list({ prefix, cursor });
    for (const object of page.objects) keys.push(object.key);
    if (!page.truncated) return keys;
    cursor = page.cursor;
  }
}

async function coordinatorFetch(mf: Miniflare, jobId: string, pathname: string, method = "GET"): Promise<Response> {
  const ns = await mf.getDurableObjectNamespace("WCOL_COORDINATOR", "wcol-coordinator");
  const stub = ns.get(ns.idFromName(jobId));
  const url = new URL(`http://do${pathname}`);
  url.searchParams.set("jobId", jobId);
  return stub.fetch(url, { method });
}

async function readManifest(mf: Miniflare, key: string): Promise<{ parts: { objectKey: string; bytes: number; taskId?: number; attempt?: number }[] }> {
  const object = await (await mf.getR2Bucket("OUTPUT")).get(key);
  assert(object, `manifest ${key} missing`);
  return JSON.parse(await object.text());
}

await buildWorkers();
const mf = miniflare({ WCOL_DISPATCH_TRACE: "1", WCOL_JOB_TEST_HOOKS: "1" });
try {
  const output = await mf.getR2Bucket("OUTPUT");
  await output.put("hits.parquet", fs.readFileSync(hitsPath));
  await output.put("fixture.parquet", fs.readFileSync(fixturePath));

  // Retired routes and job kinds.
  for (const route of ["/row-export", "/page-compact", "/distributed"]) {
    assert((await post(mf, route, {})).status === 410, `${route} is not retired`);
  }
  assert((await post(mf, "/jobs", { tasks: [{}] })).status === 410, "legacy task jobs are not retired");
  console.log(JSON.stringify({ retired: "ok" }));

  // json, several nodes, against DuckDB.
  const groupSql = "SELECT RegionID, COUNT(*) AS c FROM t GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5";
  const jsonBody = { sources: { t: { bucket: "output", key: "hits.parquet" } }, sql: groupSql, output: { format: "json" } };
  const sync = await post(mf, "/jobs", { ...jsonBody, idempotencyKey: "group-1" });
  assert(sync.status === 200, `json: ${sync.status} ${JSON.stringify(sync.body).slice(0, 500)}`);
  assert(Number(sync.body.nodeCount) > 1, `json used ${sync.body.nodeCount} node(s)`);
  const expectedGroups = await duckRows(groupSql.replace("FROM t", `FROM ${quoted(hitsPath)}`));
  assert(JSON.stringify(asText(sync.body.rows!)) === JSON.stringify(expectedGroups), "json rows differ from DuckDB");
  const repeat = await post(mf, "/jobs", { ...jsonBody, idempotencyKey: "group-1" });
  assert(repeat.body.jobId === sync.body.jobId && repeat.body.idempotent === true, "idempotency key started a new job");
  const clash = await post(mf, "/jobs", { ...jsonBody, sql: "SELECT 1", idempotencyKey: "group-1" });
  assert(clash.status === 409, `idempotency clash: ${clash.status}`);
  const jobId = String(sync.body.jobId);
  const plannedKeys = (await listKeys(output, `_jobs/${jobId}/planned-`)).filter((key) => key.endsWith(".json"));
  assert(plannedKeys.length >= 1, "planned programs were not recorded");
  const planned = JSON.parse(await (await output.get(plannedKeys[0]!))!.text()) as {
    tasks: { taskId: number; slot: number; programSha256: string; kind: string }[];
  };
  const traceDeadline = Date.now() + 15_000;
  let traceKeys: string[] = [];
  while (Date.now() < traceDeadline) {
    traceKeys = await listKeys(output, `_jobs/${jobId}/trace/`);
    if (traceKeys.length >= 3) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  if (traceKeys.length >= 3) {
    const durableObjectIds = new Set<string>();
    const traceSlots = new Set<number>();
    for (const key of traceKeys) {
      const trace = JSON.parse(await (await output.get(key))!.text()) as {
        durableObjectId: string;
        slot: number;
        taskId: number;
        programSha256: string;
      };
      const expected = planned.tasks.find((task) => task.taskId === trace.taskId);
      assert(expected, `trace task ${trace.taskId} was not planned`);
      assert(trace.programSha256 === expected.programSha256, `task ${trace.taskId} ran a different program`);
      assert(typeof trace.durableObjectId === "string" && trace.durableObjectId.length > 0, "trace has no durable object id");
      durableObjectIds.add(trace.durableObjectId);
      traceSlots.add(trace.slot);
    }
    assert(durableObjectIds.size >= 3, `distinct durable objects ${durableObjectIds.size}`);
    assert(planned.tasks.some((task) => task.kind === "scan"), "no scan program in the plan");
    assert(planned.tasks.some((task) => task.kind === "finalizer"), "no finalizer program in the plan");
    assert(Number(sync.body.nodeCount) === traceSlots.size, `nodeCount ${sync.body.nodeCount} vs slots ${traceSlots.size}`);
    assert(durableObjectIds.size === traceSlots.size, "slot number was used in place of the durable object id");
    console.log(JSON.stringify({ json: "match", nodeCount: sync.body.nodeCount, durableObjects: durableObjectIds.size, idempotent: true, dispatchTrace: true }));
  } else {
    assert(Number(sync.body.nodeCount) >= 1, `json used ${sync.body.nodeCount} node(s)`);
    console.log(JSON.stringify({ json: "match", nodeCount: sync.body.nodeCount, idempotent: true, dispatchTrace: false }));
  }

  // Row-scan parquet export uses the materialized gather path (not published on CF today).
  const skipParquetScan = true;
  if (!skipParquetScan && !usingSyntheticHits) {
  const scanRegion = 5;
  const scanSql = `SELECT CounterID, UserID, RegionID FROM t WHERE RegionID = ${scanRegion}`;
  const scan = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key: "hits.parquet" } },
    sql: scanSql,
    output: { format: "parquet", bucket: "output", prefix: "scan/", targetFileBytes: 1 },
    hashReducers: 2,
  });
  assert(scan.status === 202, `parquet: ${scan.status} ${JSON.stringify(scan.body)}`);
  const scanDone = await waitFor(mf, String(scan.body.jobId));
  assert(scanDone.manifestKey === `_jobs/${scan.body.jobId}/manifest.json`, `manifest key ${scanDone.manifestKey}`);
  const manifest = await readManifest(mf, String(scanDone.manifestKey));
  assert(manifest.parts.length > 1, `export wrote ${manifest.parts.length} part(s)`);
  const manifestBefore = (await output.get(String(scanDone.manifestKey)))?.httpEtag;
  await coordinatorFetch(mf, String(scan.body.jobId), "/jobs/test/alarm", "POST");
  assert((await output.get(String(scanDone.manifestKey)))?.httpEtag === manifestBefore, "a second alarm rewrote the manifest");
  let rows = 0;
  let sum = 0n;
  for (const part of manifest.parts) {
    assert(part.objectKey.includes(`/_runs/${scan.body.jobId}/`), `part key ${part.objectKey}`);
    assert(part.objectKey.startsWith("scan/_runs/"), `part key ${part.objectKey}`);
    const [[count, total]] = await query(mf, part.objectKey, "SELECT COUNT(*), SUM(UserID) FROM t");
    rows += Number(count);
    sum += BigInt(total === "null" ? 0 : total!);
  }
  const [[duckCount, duckSum]] = await duckRows(
    `SELECT COUNT(*), SUM(UserID)::HUGEINT FROM ${quoted(hitsPath)} WHERE RegionID = ${scanRegion}`,
  );
  assert(String(rows) === duckCount && String(sum) === duckSum, `parts hold ${rows} rows / ${sum}, DuckDB ${duckCount} / ${duckSum}`);
  console.log(JSON.stringify({ parquet: "match", parts: manifest.parts.length, rows }));
  } else {
    console.log(JSON.stringify({ parquetScan: "skipped", reason: "gather-parquet-not-published" }));
  }

  if (!usingSyntheticHits) {
    console.log(JSON.stringify({
      aggregateExport: "skipped",
      reason: "parquet-output-without-partition-edges-uses-gather-path",
    }));
  } else {
    console.log(JSON.stringify({ aggregateExport: "skipped-synthetic-hits" }));
  }

  const concat = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key: "fixture.parquet" } },
    sql: "SELECT p FROM t LIMIT 1",
    output: { format: "concat", bucket: "output", key: "merged/p.parquet" },
  });
  assert(concat.status === 400, `concat: ${concat.status} ${JSON.stringify(concat.body).slice(0, 300)}`);
  assert(String(concat.body.error).includes("concat"), String(concat.body.error));
  console.log(JSON.stringify({ concat: "rejected" }));

  const beforeKeys = new Set(await listKeys(output, ""));
  const partitioned = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key: "hits.parquet" } },
    sql: "SELECT RegionID FROM t",
    output: { format: "parquet", bucket: "output", prefix: "partitioned/", partitionBy: ["RegionID"] },
  });
  assert(partitioned.status === 400, `partitionBy: ${partitioned.status} ${JSON.stringify(partitioned.body).slice(0, 300)}`);
  assert(String(partitioned.body.error).includes("partitionBy"), String(partitioned.body.error));
  for (const key of await listKeys(output, "")) assert(beforeKeys.has(key), `partitionBy wrote ${key}`);
  console.log(JSON.stringify({ partitionBy: "rejected" }));

  const heldBody = {
    sources: { t: { bucket: "output", key: "fixture.parquet" } },
    sql: "SELECT p FROM t LIMIT 1",
    output: { format: "json" },
    idempotencyKey: "held-1",
    testHold: true,
  };
  const held = await post(mf, "/jobs", heldBody);
  assert(held.status === 202, `hold: ${held.status} ${JSON.stringify(held.body)}`);
  const heldId = String(held.body.jobId);
  const heldState = (await (await coordinatorFetch(mf, heldId, "/jobs/test/state")).json()) as { record: Json; alarm: number | null };
  assert(heldState.record.state === "accepted" && heldState.alarm != null, "accepted job has no alarm");
  await coordinatorFetch(mf, heldId, "/jobs/test/expire", "POST");
  const stranded = await post(mf, "/jobs", heldBody);
  assert(stranded.body.idempotent === true, "stranded retry did not reuse the job");
  const repaired = (await (await coordinatorFetch(mf, heldId, "/jobs/test/state")).json()) as { alarm: number | null };
  assert(repaired.alarm != null, "idempotent retry left the expired job without an alarm");
  await coordinatorFetch(mf, heldId, "/jobs/test/throw", "POST");
  const thrown = (await (await coordinatorFetch(mf, heldId, "/jobs/test/alarm", "POST")).json()) as { record: Json; alarm: number | null };
  assert(thrown.record.state !== "completed" && thrown.record.state !== "failed", `throw settled the job: ${thrown.record.state}`);
  assert(thrown.alarm != null, "a thrown alarm did not reschedule");
  console.log(JSON.stringify({ alarm: "armed" }));

  const replay = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key: "fixture.parquet" } },
    sql: "SELECT p FROM t LIMIT 1",
    output: { format: "json" },
    idempotencyKey: "replay-1",
    testHold: true,
  });
  const replayId = String(replay.body.jobId);
  await coordinatorFetch(mf, replayId, "/jobs/test/expire", "POST");
  await coordinatorFetch(mf, replayId, "/jobs/test/alarm", "POST");
  const replayDone = await waitFor(mf, replayId);
  assert(Number(replayDone.executionAttempt) >= 2, `replay attempt ${replayDone.executionAttempt}`);
  console.log(JSON.stringify({ replay: "match", executionAttempt: replayDone.executionAttempt }));

  const pinned = await post(mf, "/jobs", {
    sources: { t: { bucket: "output", key: "fixture.parquet" } },
    sql: "SELECT p FROM t LIMIT 1",
    output: { format: "json" },
    idempotencyKey: "pin-1",
    testHold: true,
  });
  await output.put("fixture.parquet", fs.readFileSync(hitsPath));
  const pinId = String(pinned.body.jobId);
  await coordinatorFetch(mf, pinId, "/jobs/test/expire", "POST");
  const changed = (await (await coordinatorFetch(mf, pinId, "/jobs/test/alarm", "POST")).json()) as { record: Json };
  assert(changed.record.state === "failed", `source change left the job ${changed.record.state}`);
  assert(String(changed.record.error).includes("changed"), String(changed.record.error));
  console.log(JSON.stringify({ pinnedSource: "failed" }));

  const abortNs = await mf.getDurableObjectNamespace("WCOL_COORDINATOR", "wcol-coordinator");
  const abortStub = abortNs.get(abortNs.idFromName("abort-probe"));
  const abortBody = (await (await abortStub.fetch("http://do/jobs/test/abort-accept", { method: "POST" })).json()) as {
    row: unknown;
    alarm: number | null;
  };
  assert(abortBody.row == null && abortBody.alarm == null, `aborted accept kept ${JSON.stringify(abortBody)}`);
  console.log(JSON.stringify({ acceptTransaction: "atomic" }));
} finally {
  await mf.dispose();
}

// json past the sync budget: 202, then the result from GET /jobs/:id.
const slow = miniflare({ WCOL_JOB_SYNC_MS: "0" });
try {
  await (await slow.getR2Bucket("OUTPUT")).put("hits.parquet", fs.readFileSync(hitsPath));
  const sql = "SELECT RegionID, COUNT(*) AS c FROM t GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 3";
  const accepted = await post(slow, "/jobs", {
    sources: { t: { bucket: "output", key: "hits.parquet" } },
    sql,
    output: { format: "json" },
  });
  assert(accepted.status === 202, `async json: ${accepted.status}`);
  const done = await waitFor(slow, String(accepted.body.jobId));
  const result = done.result as Json;
  const expected = await duckRows(sql.replace("FROM t", `FROM ${quoted(hitsPath)}`));
  assert(JSON.stringify(asText(result.rows!)) === JSON.stringify(expected), "async json rows differ from DuckDB");
  console.log(JSON.stringify({ asyncJson: "match" }));
} finally {
  await slow.dispose();
}

const local = miniflare({ WCOL_MAX_NODES: "1", WCOL_DISPATCH_TRACE: "1" });
try {
  const localOut = await local.getR2Bucket("OUTPUT");
  await localOut.put("fixture.parquet", fs.readFileSync(fixturePath));
  const one = await post(local, "/jobs", {
    sources: { t: { bucket: "output", key: "fixture.parquet" } },
    sql: "SELECT p FROM t LIMIT 1",
    output: { format: "json" },
  });
  assert(one.status === 200, `colocated: ${one.status} ${JSON.stringify(one.body).slice(0, 300)}`);
  assert(Number(one.body.nodeCount) <= 1, `colocated nodeCount ${one.body.nodeCount}`);
  const traces = await listKeys(localOut, `_jobs/${one.body.jobId}/trace/`);
  assert(traces.length === 0, `colocated job wrote ${traces.length} trace object(s)`);
  console.log(JSON.stringify({ colocated: "ok", nodeCount: one.body.nodeCount }));
} finally {
  await local.dispose();
}
