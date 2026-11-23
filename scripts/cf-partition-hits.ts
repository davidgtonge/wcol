/**
 * Split one Parquet object into parts of contiguous row groups. Each part is a concat job
 * whose one input names a `rowGroups` range: pages are copied unchanged (no decode), so
 * parts keep the source's encoding and compression and their sizes add up to the source.
 *
 * Local (Miniflare, small file):
 *   PARTS=4 npx tsx scripts/cf-partition-hits.ts [data/hits_subset_2m.parquet]
 * Deployed (an object already in the R2 bucket):
 *   WCOL_BASE_URL=https://wcol-coordinator....workers.dev SOURCE_KEY=hits.parquet PARTS=64 \
 *     npx tsx scripts/cf-partition-hits.ts
 *
 * The check runs WCOL json jobs over each part (COUNT, SUM, MIN, MAX of a few columns)
 * and compares the folded totals with DuckDB on the source file.
 */

import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, coordinatorOut, distDir, nodeOut, ROOT } from "./build-workers.ts";
import { createLimiter } from "../src/runtime/concurrency.ts";

type Json = Record<string, unknown> & { rows?: unknown[][] };

const base = (process.env.WCOL_BASE_URL ?? "").replace(/\/$/, "");
const parts = Number(process.env.PARTS ?? 4);
const sourceKey = process.env.SOURCE_KEY ?? "hits-small.parquet";
const prefix = process.env.OUTPUT_PREFIX ?? `${sourceKey.replace(/\.parquet$/, "")}-parts/${parts}/`;
const localFile = process.argv[2] ?? path.join(ROOT, "data/hits_subset_2m.parquet");
const oracleFile =
  process.env.ORACLE_FILE ?? (base ? "https://datasets.clickhouse.com/hits_compatible/hits.parquet" : localFile);
const concurrency = Number(process.env.CONCURRENCY ?? 16);
const timeoutMs = Number(process.env.TIMEOUT_MS ?? 1_800_000);

/** Totals that change if any row is lost, duplicated or altered. */
const CHECK_SQL =
  "SELECT COUNT(*), SUM(WatchID), SUM(ClientIP), SUM(RegionID), MIN(EventTime), MAX(EventTime), COUNT(SearchPhrase) FROM hits";
/** A hash-partitioned aggregate, so the parts are also checked as one table through reducers. */
const GROUP_SQL = "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5";
const reuse = process.env.REUSE === "1";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

/** `count` row groups as `parts` contiguous half-open ranges, sizes differing by at most one. */
export function rowGroupRanges(count: number, parts: number): [number, number][] {
  const n = Math.min(parts, count);
  return Array.from({ length: n }, (_, index) => [
    Math.floor((index * count) / n),
    Math.floor(((index + 1) * count) / n),
  ]);
}

type Fetcher = (route: string, init?: RequestInit) => Promise<Response>;

async function call(fetcher: Fetcher, route: string, body?: unknown): Promise<{ status: number; body: Json }> {
  const response = await fetcher(
    route,
    body === undefined
      ? undefined
      : { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) },
  );
  const text = await response.text();
  try {
    return { status: response.status, body: JSON.parse(text) as Json };
  } catch {
    throw new Error(`${route} ${response.status}: ${text.slice(0, 300)}`);
  }
}

async function waitFor(fetcher: Fetcher, jobId: string): Promise<Json> {
  const started = Date.now();
  for (;;) {
    const { body } = await call(fetcher, `/jobs/${jobId}`);
    if (body.state === "completed") return body;
    if (body.state === "failed") throw new Error(`job ${jobId} failed: ${body.error}`);
    if (Date.now() - started > timeoutMs) throw new Error(`job ${jobId} timed out in ${body.state}`);
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

/** A WCOL json job over `keys` as one table `hits`. */
async function query(fetcher: Fetcher, keys: string[], sql: string): Promise<string[][]> {
  const res = await call(fetcher, "/jobs", {
    sources: { hits: keys.length === 1 ? { bucket: "output", key: keys[0] } : { bucket: "output", keys } },
    sql,
    output: { format: "json" },
    maxNodes: Math.max(32, keys.length),
  });
  let body = res.body;
  if (res.status === 202) body = await waitFor(fetcher, String(body.jobId));
  else assert(res.status === 200, `${sql} over ${keys.length} object(s): ${res.status} ${JSON.stringify(body).slice(0, 300)}`);
  const rows = (body.rows ?? (body.result as Json | undefined)?.rows) as unknown[][] | undefined;
  assert(rows, `${sql}: ${JSON.stringify(body).slice(0, 300)}`);
  return rows.map((row) => row.map((value) => (value === null ? "null" : String(value))));
}

/** Counts and sums add; MIN/MAX fold. Column order follows CHECK_SQL. */
function fold(left: string[] | null, right: string[]): string[] {
  if (!left) return right;
  const add = (a: string, b: string) => (BigInt(a) + BigInt(b)).toString();
  const min = (a: string, b: string) => (BigInt(a) < BigInt(b) ? a : b);
  const max = (a: string, b: string) => (BigInt(a) > BigInt(b) ? a : b);
  const ops = [add, add, add, add, min, max, add];
  return ops.map((op, index) => op(left[index]!, right[index]!));
}

const quoted = (file: string) => `'${file.replaceAll("'", "''")}'`;

/** The DuckDB CLI: `@duckdb/node-api` fails on the remote full-hits URL ("stoi: out of range"). */
function duck(sql: string): Promise<string[][]> {
  const bin = ["duckdb", path.join(process.env.HOME ?? "", ".duckdb/cli/latest/duckdb")].find(
    (candidate) => candidate === "duckdb" || fs.existsSync(candidate),
  )!;
  return new Promise((resolve, reject) => {
    execFile(bin, ["-csv", "-noheader", "-c", sql], { maxBuffer: 1 << 20, timeout: timeoutMs }, (error, stdout, stderr) => {
      if (error) reject(new Error(`duckdb: ${stderr || error.message}`));
      else resolve(stdout.trim().split("\n").map((line) => line.split(",")));
    });
  });
}

async function sourceRowGroups(file: string): Promise<number> {
  return Number((await duck(`SELECT num_row_groups FROM parquet_file_metadata(${quoted(file)})`))[0]![0]);
}

function duckRows(sql: string, file: string): Promise<string[][]> {
  return duck(sql.replace("FROM hits", `FROM ${quoted(file)}`).replace("SUM(WatchID)", "SUM(WatchID)::HUGEINT"));
}

async function split(fetcher: Fetcher): Promise<void> {
  const ranges = rowGroupRanges(await sourceRowGroups(oracleFile), parts);
  const runId = Date.now().toString(36);
  const limit = createLimiter(concurrency);
  const started = performance.now();
  const partKey = (index: number) => `${prefix}part-${String(index).padStart(4, "0")}.parquet`;
  const written = reuse
    ? ranges.map((_, index) => partKey(index))
    : await Promise.all(
    ranges.map(([start, end], index) =>
      limit(async () => {
        const key = partKey(index);
        const submitted = await call(fetcher, "/jobs", {
          idempotencyKey: `split-${runId}-${index}`,
          output: { format: "concat", bucket: "output", key },
          inputs: [{ bucket: "output", key: sourceKey, rowGroups: [start, end] }],
        });
        assert(submitted.status === 202, `part ${index}: ${submitted.status} ${JSON.stringify(submitted.body).slice(0, 400)}`);
        await waitFor(fetcher, String(submitted.body.jobId));
        return key;
      }),
    ),
  );
  console.log(JSON.stringify({ parts: written.length, rowGroups: ranges.at(-1)![1], wallMs: Math.round(performance.now() - started), prefix }));

  const same = (label: string, ours: string[][], expected: string[][]) =>
    assert(JSON.stringify(ours) === JSON.stringify(expected), `${label}: ${JSON.stringify(ours)} vs source ${JSON.stringify(expected)}`);
  const [expected, expectedGroups] = await Promise.all([duckRows(CHECK_SQL, oracleFile), duckRows(GROUP_SQL, oracleFile)]);
  // Each part on its own, folded: no row lost or duplicated by the copy.
  const each = await Promise.all(written.map((key) => limit(async () => (await query(fetcher, [key], CHECK_SQL))[0]!)));
  same("parts one by one", [each.reduce<string[] | null>(fold, null)!], expected);
  // The parts as one table: one scan per object, and a hash aggregate across them.
  same("parts as one table", await query(fetcher, written, CHECK_SQL), expected);
  same("grouped over the parts", await query(fetcher, written, GROUP_SQL), expectedGroups);
  console.log(JSON.stringify({ ok: true, parts: written.length, totals: expected[0], groups: expectedGroups.length }));
}

if (base) {
  await split((route, init) => fetch(`${base}${route}`, init));
} else {
  assert(fs.existsSync(localFile), `missing ${localFile}`);
  await buildWorkers();
  const worker = (name: string, out: string, wasm: string, extra: Record<string, unknown>) => ({
    name,
    modulesRoot: distDir,
    modules: [
      { type: "ESModule" as const, path: out },
      { type: "CompiledWasm" as const, path: path.join(distDir, wasm) },
    ],
    compatibilityDate: "2026-01-01",
    r2Buckets: ["OUTPUT"],
    ...extra,
  });
  const mf = new Miniflare({
    workers: [
      worker("wcol-coordinator", coordinatorOut, "wcol_coordinator.wasm", {
        durableObjects: {
          WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
          WCOL_NODE: { className: "WcolNode", scriptName: "wcol-node", useSQLite: true },
        },
      }),
      worker("wcol-node", nodeOut, "wcol_node.wasm", {
        durableObjects: {
          WCOL_NODE: { className: "WcolNode", useSQLite: true },
          // Hash scans claim their units on the job's coordinator, as in wrangler.node.toml.
          WCOL_COORDINATOR: { className: "WcolCoordinator", scriptName: "wcol-coordinator" },
        },
      }),
    ],
  });
  try {
    const output = await mf.getR2Bucket("OUTPUT");
    await output.put(sourceKey, fs.readFileSync(localFile));
    await split((route, init) => mf.dispatchFetch(`http://wcol.local${route}`, init as never) as unknown as Promise<Response>);
    const listed = await output.list({ prefix });
    const bytes = listed.objects.reduce((sum, object) => sum + object.size, 0);
    console.log(JSON.stringify({ sourceBytes: fs.statSync(localFile).size, partBytes: bytes, objects: listed.objects.length }));
  } finally {
    await mf.dispose();
  }
}
