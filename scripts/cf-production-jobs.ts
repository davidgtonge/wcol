/**
 * Hosted POST /jobs against a deployed coordinator. DuckDB is the oracle.
 *
 *   WCOL_BASE_URL=https://wcol-coordinator....workers.dev \
 *   STAGE=a npx tsx scripts/cf-production-jobs.ts
 *
 * Stage A: hits-500k.parquet. Stage B: hits-2m.parquet.
 * Stage C: existing R2 hits.parquet (full ClickBench). DuckDB oracle uses the
 * ClickHouse HTTP file when data/hits.parquet is absent. Remote oracle answers are cached
 * in data/oracle-cache/: each full-hits query runs in DuckDB once, under a memory cap,
 * instead of pulling the 14 GB file through range requests on every run.
 *
 * ONLY=Q8 or ONLY=Q15-16 runs a subset.
 *
 * CLICKBENCH_TSV=1 runs every planner-supported query in scripts/clickbench-queries.tsv
 * (stage c). ORACLE=cached checks only queries whose DuckDB answer is already cached and
 * runs no new full-hits DuckDB query; other results are reported as unverified. Failures
 * are recorded per query and the run continues.
 */

import { execFile } from "node:child_process";
import { catalogQueries, tsvQueries } from "./clickbench-catalog.ts";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createLimiter } from "../src/runtime/concurrency.ts";
import { oracleFor, rowsMatch, type Oracle } from "./clickbench-oracle.ts";
import { assert, asText, hashPartitions, type JobRecord, jobRows, localDuckDb, ROOT, scheduleOf, waitForJob } from "./harness.ts";

/** A ClickBench query's SQL from the TSV, by name. */
function tsvSql(name: string): string {
  const query = tsvQueries(ROOT).find((candidate) => candidate.name === name);
  if (!query) throw new Error(`${name} is not in scripts/clickbench-queries.tsv`);
  return query.sql;
}

const [Q1, Q3, Q7, Q8, Q15, Q31] = ["Q1", "Q3", "Q7", "Q8", "Q15", "Q31"].map(tsvSql) as [string, string, string, string, string, string];
/** Not a ClickBench query: a small top-k whose ties are broken, for the reducer-count ladder. */
const REGION =
  "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5";

type Stage = "a" | "b" | "c";

/**
 * `parquet` exports the result to R2 rather than answering in JSON: an unbounded GROUP BY
 * returns more groups than a JSON answer may hold. Exports are checked by their manifest,
 * not against DuckDB.
 */
type QuerySpec = { name: string; sql: string; hashReducers?: number; output?: "parquet" };

const STAGES: Record<Stage, { key: string; local: string; timeoutMs: number }> = {
  a: {
    key: "hits-500k.parquet",
    local: path.join(ROOT, "data/hits_subset_500k.parquet"),
    timeoutMs: 120_000,
  },
  b: {
    key: "hits-2m.parquet",
    local: path.join(ROOT, "data/hits_subset_2m.parquet"),
    timeoutMs: 180_000,
  },
  c: {
    key: "hits.parquet",
    local:
      process.env.HITS_PARQUET ??
      (fs.existsSync(path.join(ROOT, "data/hits.parquet"))
        ? path.join(ROOT, "data/hits.parquet")
        : "https://datasets.clickhouse.com/hits_compatible/hits.parquet"),
    timeoutMs: 600_000,
  },
};

function stageOf(value: string | undefined): Stage {
  if (value === "a" || value === "b" || value === "c") return value;
  throw new Error(`STAGE must be a, b, or c (got ${value ?? "unset"})`);
}

function catalogFor(stage: Stage): QuerySpec[] {
  const simple: QuerySpec[] = [
    { name: "Q1", sql: Q1 },
    { name: "Q3", sql: Q3 },
    { name: "Q7", sql: Q7 },
  ];
  const hash: QuerySpec[] =
    stage === "c"
      ? [
          { name: "RegionID-16", sql: REGION, hashReducers: 16 },
          // No override: the planner sizes reducers from the data.
          { name: "RegionID-auto", sql: REGION },
          // One reducer is no exchange: scans pre-aggregate and the coordinator merges.
          { name: "RegionID-gather", sql: REGION, hashReducers: 1 },
        ]
      : [
          { name: "RegionID-4", sql: REGION, hashReducers: 4 },
          { name: "RegionID-16", sql: REGION, hashReducers: 16 },
        ];
  return [
    ...simple,
    ...hash,
    { name: "Q8", sql: Q8 },
    { name: "Q15-16", sql: Q15, hashReducers: 16 },
    { name: "Q15-32", sql: Q15, hashReducers: 32 },
    { name: "Q15-64", sql: Q15, hashReducers: 64 },
    { name: "Q31", sql: Q31 },
    { name: "Q31-64", sql: Q31, hashReducers: 64 },
  ];
}

/** Unbounded GROUP BYs whose answers exceed the JSON cap. */
const EXPORTS = new Set(["S7", "S10"]);

function clickbenchQueries(): QuerySpec[] {
  return catalogQueries(ROOT)
    .map((query) => (EXPORTS.has(query.name) ? { ...query, output: "parquet" as const } : query))
    .map((query) => (process.env.HASH_REDUCERS ? { ...query, hashReducers: Number(process.env.HASH_REDUCERS) } : query));
}

function queriesFor(stage: Stage, skipQ15: boolean, only: Set<string>): QuerySpec[] {
  if (process.env.CLICKBENCH_TSV === "1") {
    return clickbenchQueries().filter((query) => only.size === 0 || only.has(query.name));
  }
  const catalog = catalogFor(stage);
  if (only.size > 0) {
    const selected = catalog.filter((query) => only.has(query.name));
    assert(selected.length > 0, "no queries selected");
    return selected;
  }
  return catalog.filter((query) => {
    if (query.name === "Q8") return false;
    if (query.name.startsWith("Q15-")) return stage !== "c" && !skipQ15 && query.name === "Q15-16";
    return true;
  });
}

function quoted(file: string): string {
  return `'${file.replaceAll("'", "''")}'`;
}

/** DuckDB on the PATH, or where its installer puts it. */
const DUCKDB_BINS = ["duckdb", path.join(os.homedir(), ".duckdb/cli/latest/duckdb")];

function duckRowsCli(statement: string, timeoutMs: number): Promise<string[][]> {
  const attempt = (index: number): Promise<string[][]> => {
    const bin = DUCKDB_BINS[index];
    if (!bin) return Promise.reject(new Error("duckdb not found"));
    return new Promise((resolve, reject) => {
      execFile(bin, ["-csv", "-c", statement], { timeout: timeoutMs, maxBuffer: 8_000_000 }, (err, stdout, stderr) => {
        if (err && "code" in err && err.code === "ENOENT") {
          attempt(index + 1).then(resolve, reject);
          return;
        }
        if (err) reject(new Error((stderr || err.message).trim()));
        else {
          resolve(
            stdout
              .trim()
              .split("\n")
              .slice(1)
              .filter((line) => line.length > 0)
              .map((line) => line.split(",")),
          );
        }
      });
    });
  };
  return attempt(0);
}

const ORACLE_CACHE = path.join(ROOT, "data/oracle-cache");
/** Rows a JSON answer may print, so a verified answer is never cut short. */
const ORACLE_ROW_CAP = 100_000;
/** DuckDB's own default is 80% of RAM: Q31's ~100M groups over full hits took the machine. */
const ORACLE_LIMITS = `SET memory_limit='${process.env.ORACLE_MEMORY ?? "4GB"}'; SET threads=4; SET temp_directory='${ORACLE_CACHE}/spill';`;

function remoteOracleSql(sql: string, file: string): string {
  return sql.replace(/\bFROM hits\b/i, `FROM read_parquet(${quoted(file)})`);
}

function oracleCacheKey(rewritten: string): string {
  return createHash("sha256").update(rewritten).digest("hex").slice(0, 24);
}

/** Undefined when `ORACLE=cached` and the full-hits answer is not cached (no DuckDB run). */
async function duckRows(sql: string, local: string, timeoutMs: number): Promise<string[][] | undefined> {
  const remote = local.startsWith("http://") || local.startsWith("https://");
  if (remote) {
    const rewritten = remoteOracleSql(sql, local);
    const cached = path.join(ORACLE_CACHE, `${oracleCacheKey(rewritten)}.json`);
    if (fs.existsSync(cached)) return JSON.parse(fs.readFileSync(cached, "utf8")) as string[][];
    if (process.env.ORACLE === "cached") return undefined;
    fs.mkdirSync(path.join(ORACLE_CACHE, "spill"), { recursive: true });
    const rows = await duckRowsCli(`${ORACLE_LIMITS} ${rewritten}`, timeoutMs);
    fs.writeFileSync(cached, JSON.stringify(rows));
    return rows;
  }
  localOracle ??= localDuckDb(local);
  return (await localOracle)(sql);
}

let localOracle: ReturnType<typeof localDuckDb> | undefined;

/** `WCOL_API_TOKEN`: the coordinator's API token, when it requires one. */
const auth: Record<string, string> = process.env.WCOL_API_TOKEN ? { authorization: `Bearer ${process.env.WCOL_API_TOKEN}` } : {};

async function postJob(base: string, body: unknown): Promise<{ status: number; body: JobRecord }> {
  const response = await fetch(`${base}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json", ...auth },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as JobRecord };
}

const stage = stageOf(process.env.STAGE ?? "a");
const preset = STAGES[stage];
const base = (process.env.WCOL_BASE_URL ?? "").replace(/\/$/, "");
const hitsKey = process.env.HITS_KEY ?? preset.key;
const hitsLocal = process.env.HITS_LOCAL ?? preset.local;
const skipQ15 = process.env.SKIP_Q15 === "1";
const groupSpill = process.env.GROUP_SPILL === "1";
/** Query `hits-parts/<N>/part-*.parquet`, uploaded beforehand, as one table. */
const hitsParts = process.env.HITS_PARTS ? Number(process.env.HITS_PARTS) : undefined;
const hitsSource = hitsParts
  ? { bucket: "output", keys: Array.from({ length: hitsParts }, (_, i) => `hits-parts/${hitsParts}/part-${String(i).padStart(4, "0")}.parquet`) }
  : { bucket: "output", key: hitsKey };
/** Fail this task's first attempt mid-stream (`FAIL_TASK=40`): exercises abort and retry. */
const failTask = process.env.FAIL_TASK;
/** Scan count override (the coordinator defaults to WCOL_MAX_NODES). */
const maxNodes = process.env.MAX_NODES ? Number(process.env.MAX_NODES) : undefined;
/** `REDUCERS=container` or `REDUCERS=node` forces where hash reducers run; unset, the coordinator picks. */
const reducerRuntime = process.env.REDUCERS === "container" || process.env.REDUCERS === "node" ? process.env.REDUCERS : undefined;
/** `REDUCER_INSTANCE=lite` (with `REDUCERS=container`) picks the reducers' container size. */
const reducerInstance = process.env.REDUCER_INSTANCE;
/** How many scans may be open at once. Each reducer then holds at most this many edges. */
const maxActiveIngress = process.env.MAX_ACTIVE_INGRESS ? Number(process.env.MAX_ACTIVE_INGRESS) : undefined;
const targetGroupBytes = Number(process.env.TARGET_GROUP_BYTES ?? 16 * 1024 * 1024);
/** Unset: the planner makes an explicit target the hard ceiling too. */
const hardGroupBytes = process.env.HARD_GROUP_BYTES ? Number(process.env.HARD_GROUP_BYTES) : undefined;
const only = new Set(
  (process.env.ONLY ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter((name) => name.length > 0),
);
assert(base.length > 0, "set WCOL_BASE_URL to the deployed coordinator");
assert(
  hitsLocal.startsWith("http://") || hitsLocal.startsWith("https://") || fs.existsSync(hitsLocal),
  `missing local parquet ${hitsLocal}`,
)

const queries = queriesFor(stage, skipQ15, only);
const getJob = async (jobId: string) => (await (await fetch(`${base}/jobs/${jobId}`, { headers: auth })).json()) as JobRecord;
// Unique per process: two runs started together must not share an idempotency key.
const runId = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
const results: Record<string, unknown>[] = [];

async function runQuery(
  query: QuerySpec,
  oracle: Oracle,
  expectedPromise: Promise<string[][] | undefined>,
): Promise<Record<string, unknown>> {
  const wallStart = performance.now();
  const posted = await postJob(base, {
    sources: { hits: hitsSource },
    sql: query.sql,
    output:
      query.output === "parquet"
        ? { format: "parquet", bucket: "output", prefix: `ladder/${stage}-${query.name}-${runId}/` }
        : // The JSON default prints 1000 rows; RegionID groups number about 9000.
          { format: "json", rowCap: ORACLE_ROW_CAP },
    ...(query.hashReducers != null ? { hashReducers: query.hashReducers } : {}),
    ...(groupSpill ? { groupSpill: true, targetGroupBytes, ...(hardGroupBytes ? { hardGroupBytes } : {}) } : {}),
    ...(maxNodes != null ? { maxNodes } : {}),
    ...(maxActiveIngress != null ? { maxActiveIngress } : {}),
    ...(reducerRuntime ? { reducers: reducerRuntime } : {}),
    ...(reducerInstance ? { reducerInstance } : {}),
    ...(failTask ? { failFirstAttemptTaskId: `n${failTask}` } : {}),
    idempotencyKey: `prod-${stage}-${query.name}-${runId}`,
  });
  assert(
    posted.status === 200 || posted.status === 202,
    `${query.name} ${posted.status} ${JSON.stringify(posted.body).slice(0, 400)}`,
  );
  let body = posted.body;
  const jobId = String(body.jobId ?? "");
  assert(jobId.length > 0, `${query.name} missing jobId`);
  if (posted.status === 202 || body.state === "running" || body.state === "accepted") {
    body = await waitForJob(getJob, jobId, preset.timeoutMs);
    if (body.state === "failed") throw new Error(`job ${jobId} failed: ${body.error}`);
  }
  const wallMs = Math.round(performance.now() - wallStart);
  if (query.output === "parquet") {
    assert(body.state === "completed", `${query.name} export ended ${body.state}`);
    assert(Number(body.outputObjects) > 0, `${query.name} export wrote no objects`);
  }
  const expected = await expectedPromise;
  if (expected) {
    assert((body.truncated ?? body.result?.truncated) !== true, `${query.name} answer was truncated at ${ORACLE_ROW_CAP} rows`);
    const ours = asText(jobRows(body));
    assert(rowsMatch(ours, expected, oracle), `${query.name} DuckDB mismatch ${JSON.stringify(ours)} vs ${JSON.stringify(expected)}`);
  }
  const schedule = body.schedule ?? (await scheduleOf(getJob, jobId));
  // Only a grouped or distinct query hash-partitions; a bare COUNT(*) is answered from the
  // footer with no nodes, whatever `HASH_REDUCERS` says.
  if (query.hashReducers != null && query.hashReducers > 1 && hashPartitions(query.sql)) {
    assert(Number(body.nodeCount) > 1, `${query.name} used ${body.nodeCount} node(s)`);
    // Scans stream straight to reducers; the coordinator relays no shuffle bytes by construction.
    assert(schedule, `${query.name} missing hash schedule`);
  }
  return {
    stage,
    name: query.name,
    ok: true,
    verified: expected !== undefined,
    wallMs,
    nodeCount: body.nodeCount,
    hashReducers: query.hashReducers,
    groupSpill: groupSpill || undefined,
    openedEdges: schedule?.openedEdges,
    ...(query.output === "parquet" ? { outputObjects: body.outputObjects, outputBytes: body.outputBytes } : {}),
    jobId,
  };
}

// ORACLE=print lists each query's DuckDB oracle (cache key, name, SQL over the remote file)
// for computing the answers elsewhere, eg. `scripts/oracle-on-host.sh`.
if (process.env.ORACLE === "print") {
  for (const query of queries.filter((query) => query.output !== "parquet")) {
    const rewritten = remoteOracleSql(oracleFor(query.sql).sql, hitsLocal);
    console.log([oracleCacheKey(rewritten), query.name, rewritten].join("\t"));
  }
  process.exit(0);
}

// CONCURRENCY=n runs n queries at once (default 1, the timing run). Concurrent jobs run on
// their own objects but share isolates and colos, so their wall times include each other's load.
const limit = createLimiter(Math.max(1, Number(process.env.CONCURRENCY ?? 1)));
await Promise.all(
  queries.map((query) =>
    limit(async () => {
      // The DuckDB answer is computed and cached even when the job fails: the process must not
      // exit first and orphan the DuckDB child, or the answer is lost.
      const oracle = oracleFor(query.sql);
      const expectedPromise =
        query.output === "parquet" ? Promise.resolve(undefined) : duckRows(oracle.sql, hitsLocal, preset.timeoutMs);
      const line = await runQuery(query, oracle, expectedPromise).catch(async (error: unknown) => {
        await expectedPromise.catch(() => undefined);
        return { stage, name: query.name, ok: false, error: String(error).slice(0, 600) };
      });
      results.push(line);
      console.log(JSON.stringify(line));
    }),
  ),
);

const failed = results.filter((line) => line.ok !== true).map((line) => line.name);
console.log(JSON.stringify({ ok: failed.length === 0, stage, hitsKey, queries: results.length, failed }));
