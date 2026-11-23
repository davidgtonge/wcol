/**
 * Closed CF spill proof: DuckDB match **and** R2 sorted-run spill on node reducers.
 *
 * Uses 8k distinct keys vs a 64 KiB reducer budget (engine floor), 2 hash reducers.
 *
 *   npm run build:node-wasm && npm run build:coordinator-wasm && npm run test:spill-cf
 *   npm run test:cf-spill-quick   # after workers are in cloudflare/dist
 */

import fs from "node:fs";
import path from "node:path";
import { distDir, ROOT } from "./build-workers.ts";
import {
  createSpillMiniflare,
  listJobObjectKeys,
  runGroupBySpillJob,
  spillReport,
  verifyNodeWasmExports,
} from "./cf-spill-shared.ts";

const DEADLINE_MS = Number(process.env.WCOL_SPILL_DEADLINE_MS ?? 30_000);
const DEFAULT_ROWS = 8_000;
/** Engine floors combined budget at 64 KiB; 8k groups cannot fit. */
const DEFAULT_TARGET_BYTES = 64 * 1024;
const DEFAULT_REDUCERS = 2;

const rowLimit = Number(process.env.WCOL_SPILL_ROWS ?? DEFAULT_ROWS);
const targetGroupBytes = Number(process.env.WCOL_SPILL_TARGET_BYTES ?? DEFAULT_TARGET_BYTES);
const hashReducers = Number(process.env.WCOL_SPILL_REDUCERS ?? DEFAULT_REDUCERS);

/** Highest keys are flushed first; skipping scan/end cannot match DuckDB. */
const SPILL_SQL =
  "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY UserID DESC LIMIT 10";

const wasmPath = path.join(distDir, "wcol_node.wasm");
if (!fs.existsSync(wasmPath)) {
  throw new Error("missing cloudflare/dist — run: npm run build:workers");
}
verifyNodeWasmExports();

const wallStart = performance.now();

const { DuckDBInstance } = await import("@duckdb/node-api");
const duck = await (await DuckDBInstance.create(":memory:")).connect();
const quoted = (file: string) => `'${file.replaceAll("'", "''")}'`;

const tmpParquet = path.join(ROOT, "data", `.spill-quick-${rowLimit}.parquet`);
await duck.run(
  `COPY (SELECT i AS UserID FROM range(${rowLimit}) t(i)) TO ${quoted(tmpParquet)} (FORMAT PARQUET)`,
);
const duckExpected = (
  await duck.runAndReadAll(SPILL_SQL.replace("FROM hits", `FROM ${quoted(tmpParquet)}`))
).getRows() as unknown[][];
const duckText = duckExpected.map((row) => row.map((v) => (v === null ? "null" : String(v))));

const mf = createSpillMiniflare();
try {
  const output = await mf.getR2Bucket("OUTPUT");
  await output.put("hits.parquet", fs.readFileSync(tmpParquet));

  const { timing, jobId } = await runGroupBySpillJob(mf, output, {
    sql: SPILL_SQL,
    duckExpected: duckText,
    hashReducers,
    groupSpill: true,
    targetGroupBytes,
    jobTimeoutMs: DEADLINE_MS,
  });
  const d = timing.spillDiag;
  const spillObserved =
    d.spillExecByOp.begin >= 1 &&
    d.spillExecByOp.append >= 1 &&
    d.spillExecByOp.scan >= 1 &&
    d.spillExecByOp.end >= 1;
  if (!spillObserved) {
    const keys = await listJobObjectKeys(output, jobId);
    throw new Error(
      `CF spill not observed (need R2 spillExec begin/append/scan/end). telemetry=${JSON.stringify(timing.spill)} host=${JSON.stringify(timing.spillDiag)} taskSlots=${timing.taskSlots} objects=${JSON.stringify(keys)}`,
    );
  }

  const wallMs = Math.round(performance.now() - wallStart);
  if (wallMs > DEADLINE_MS) {
    throw new Error(`exceeded ${DEADLINE_MS}ms deadline (took ${wallMs}ms)`);
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "quick",
        proof: "duckdb-parity+r2-sorted-runs",
        deadlineMs: DEADLINE_MS,
        wallMs,
        spillObserved: true,
        inputRows: rowLimit,
        distinctKeys: rowLimit,
        hashReducers,
        ...spillReport(timing.spillDiag, timing.wallMs, targetGroupBytes),
        wasmMemoryBytes: timing.wasmMemoryBytes,
        taskSlots: timing.taskSlots,
      },
      null,
      2,
    ),
  );
} finally {
  await mf.dispose();
  if (fs.existsSync(tmpParquet)) fs.unlinkSync(tmpParquet);
}
