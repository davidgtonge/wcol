/**
 * Real hits UserIDs, 8 MiB group target, stop after 8 ingest spills.
 * Two row groups of hits_subset_2m have only ~17k distinct users, so an 8 MiB
 * table would flush once. This takes the first rows of data/hits_subset.parquet
 * (real UserID) until the reducers can fill that budget several times.
 *
 *   npx tsx scripts/spill-hits-prefix.mts
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "../tests/helpers/memory-r2-spill-bucket.ts";
import type { SpillBoundary } from "../src/runtime/host/host-types.ts";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";
import { createSpillMiniflare, loadReducerProgressSnapshots } from "./cf-spill-shared.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ROWS = Number(process.env.SPILL_HITS_PREFIX_ROWS ?? 2_200_000);
const TARGET = Number(process.env.SPILL_HITS_PREFIX_TARGET_BYTES ?? 8 * 1024 * 1024);
const CAP = Number(process.env.SPILL_HITS_PREFIX_CAP ?? 8);
const SQL = "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10";
const MODE = process.env.SPILL_HITS_PREFIX_MODE ?? "both";

function slope(rows: SpillBoundary[]) {
  const ingest = rows.filter((row) => row.mergeScanBegins === 0 && row.mergeSteps === 0);
  const used = ingest.length >= 2 ? ingest : rows;
  if (used.length < 2) return { runs: used.length };
  const warm = used[Math.min(2, used.length - 1)]!;
  const last = used[used.length - 1]!;
  const span = Math.max(1, last.run - warm.run);
  return {
    runs: used.length,
    liveAfterWarm: warm.liveAfter,
    liveAfterLast: last.liveAfter,
    perRunLiveGrowthPostWarm: Math.round((last.liveAfter - warm.liveAfter) / span),
    rustInflightLast: last.rustInflightSpillBytes,
    sqliteWriteBytesLast: last.sqliteWriteBytes,
    mergeScanBeginsMax: Math.max(...used.map((row) => row.mergeScanBegins)),
  };
}

const source = path.join(ROOT, "data/hits_subset.parquet");
const parquet = path.join(os.tmpdir(), `wcol-hits-prefix-${process.pid}.parquet`);
const duck = await (await DuckDBInstance.create(":memory:")).connect();
await duck.run(
  `COPY (
    SELECT UserID FROM read_parquet('${source.replaceAll("'", "''")}') LIMIT ${ROWS}
  ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 122880)`,
);
const card = await duck.runAndReadAll(
  `SELECT count(*) AS n, count(DISTINCT UserID) AS users FROM read_parquet('${parquet.replaceAll("'", "''")}')`,
);
console.log(JSON.stringify({ event: "spill_hits_prefix_data", rows: ROWS, targetGroupBytes: TARGET, cap: CAP, card: card.getRowObjectsJson(), bytes: fs.statSync(parquet).size }));

if (MODE !== "do") {
const readAt = async (offset: number, length: number) => {
  const fh = await fs.promises.open(parquet, "r");
  try {
    const out = new Uint8Array(length);
    await fh.read(out, 0, length, offset);
    return out;
  } finally {
    await fh.close();
  }
};

const node = await loadNodeWasm();
const coordinator = await loadCoordinatorWasm();
const fileSize = fs.statSync(parquet).size;
const footer = await readParquetFooter(readAt, fileSize);
const planned = planJob(coordinator, {
  body: JSON.stringify({
    sources: { hits: { key: "http://example.invalid/hits.parquet" } },
    sql: SQL,
    output: { format: "json" },
    hashReducers: 2,
    groupSpill: true,
    targetGroupBytes: TARGET,
  }),
  footers: [{ fileSize, footer }],
  maxNodes: 32,
  batchRows: 8192,
  minBytesPerNode: 1,
  hashReducers: 2,
});
if (planned.kind !== "json" || planned.graph.kind !== "graph") {
  throw new Error("hits prefix did not plan a hash graph");
}

const spillR2 = createMemoryR2SpillBucket();
const byTask = new Map<number, SpillBoundary[]>();
const started = Date.now();
let capped = false;
try {
  await runLocalHashGraph({ node, coordinator }, planned.graph as Extract<ExecutionPlan, { kind: "graph" }>, readAt, {
    spillR2Bucket: spillR2,
    isolatedNodeWasm: true,
    scheduling: { maxActiveScans: 1, openingConcurrency: 1, maxActiveIngress: 2 },
    onReducerProgress: (info) => {
      const rows = info.spillBoundaries ?? [];
      const prev = byTask.get(info.taskId) ?? [];
      if (rows.length <= prev.length) return;
      byTask.set(info.taskId, rows);
      const last = rows[rows.length - 1]!;
      console.log(JSON.stringify({
        event: "spill_hits_prefix_run",
        elapsedMs: Date.now() - started,
        taskId: info.taskId,
        run: last.run,
        liveBefore: last.liveBefore,
        liveAfter: last.liveAfter,
        sqliteWriteBytes: last.sqliteWriteBytes,
        rustInflight: last.rustInflightSpillBytes,
        mergeScanBegins: last.mergeScanBegins,
        r2BytesUploaded: spillR2.bytesUploaded,
        r2Puts: spillR2.puts,
      }));
      if (rows.filter((row) => row.mergeScanBegins === 0).length >= CAP) {
        capped = true;
        throw new Error(`spill hits prefix cap ${CAP}`);
      }
    },
  });
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  if (!capped && !message.includes("spill hits prefix cap")) throw error;
}

const reducers = [...byTask.entries()].map(([taskId, rows]) => ({
  taskId,
  ...slope(rows),
  r2BytesUploaded: spillR2.bytesUploaded,
  r2Puts: spillR2.puts,
}));
console.log(JSON.stringify({
  event: "spill_hits_prefix_done",
  capped,
  elapsedMs: Date.now() - started,
  r2BytesUploaded: spillR2.bytesUploaded,
  r2Puts: spillR2.puts,
  reducers,
  climbed: reducers.some((row) => (row.perRunLiveGrowthPostWarm ?? 0) > 1024 * 1024),
}));
}

if (MODE !== "local") {
  const mf = createSpillMiniflare({ WCOL_SPILL_RUN_CAP: String(CAP) });
  const startedDo = Date.now();
  try {
    const output = await mf.getR2Bucket("OUTPUT");
    await output.put("hits.parquet", fs.readFileSync(parquet));
    const response = await mf.dispatchFetch("http://wcol.local/jobs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        sources: { hits: { bucket: "output", key: "hits.parquet" } },
        sql: SQL,
        output: { format: "json" },
        hashReducers: 2,
        groupSpill: true,
        targetGroupBytes: TARGET,
        maxNodes: 32,
        idempotencyKey: `spill-hits-prefix-${Date.now()}`,
      }),
    });
    const posted = (await response.json()) as { jobId?: string; error?: string };
    if (!posted.jobId) throw new Error(`job POST ${response.status}: ${JSON.stringify(posted).slice(0, 400)}`);
    const deadline = Date.now() + 90_000;
    let state = "";
    while (Date.now() < deadline) {
      const status = await mf.dispatchFetch(`http://wcol.local/jobs/${posted.jobId}`);
      const record = (await status.json()) as { state?: string; error?: string };
      state = String(record.state ?? "");
      const snaps = await loadReducerProgressSnapshots(output, posted.jobId, 1);
      console.log(JSON.stringify({
        event: "spill_hits_prefix_do",
        elapsedMs: Date.now() - startedDo,
        state,
        error: record.error,
        reducers: snaps.map((row) => ({ taskId: row.taskId, ...slope(row.spillBoundaries as SpillBoundary[]) })),
      }));
      const runs = Math.max(0, ...snaps.map((row) => row.spillBoundaries.length));
      if (state === "completed" || state === "failed" || runs >= CAP) break;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  } finally {
    await mf.dispose();
  }
}

await fs.promises.unlink(parquet).catch(() => undefined);
