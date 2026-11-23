/**
 * 90s Miniflare probe. Same fixture as the local CF-signature run.
 * Poll reducer progress and exit. Do not wait for the job to finish.
 *
 *   npx tsx scripts/spill-do-probe.mts
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";
import {
  createSpillMiniflare,
  loadReducerProgressSnapshots,
  verifyNodeWasmExports,
} from "./cf-spill-shared.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ROWS = Number(process.env.SPILL_DO_PROBE_ROWS ?? 220_000);
const TARGET = Number(process.env.SPILL_DO_PROBE_TARGET_BYTES ?? 4 * 1024 * 1024);
const PROBE_MS = Number(process.env.SPILL_DO_PROBE_MS ?? 90_000);
const SQL = "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10";

function slope(rows: Array<{ run: number; liveAfter: number; sqliteAppendBytes?: number }>) {
  if (rows.length < 2) return null;
  const warm = rows[Math.min(2, rows.length - 1)]!;
  const last = rows[rows.length - 1]!;
  const span = Math.max(1, last.run - warm.run);
  return {
    runs: rows.length,
    liveAfterWarm: warm.liveAfter,
    liveAfterLast: last.liveAfter,
    perRunLiveGrowthPostWarm: Math.round((last.liveAfter - warm.liveAfter) / span),
    sqliteAppendBytesLast: last.sqliteAppendBytes ?? 0,
    rustInflightLast: (last as { rustInflightSpillBytes?: number }).rustInflightSpillBytes ?? 0,
  };
}

verifyNodeWasmExports();

const parquet = path.join(os.tmpdir(), `wcol-spill-do-probe-${process.pid}.parquet`);
const duck = await (await DuckDBInstance.create(":memory:")).connect();
await duck.run(
  `COPY (
    SELECT (abs(hash(i)) % 9000000000000000000)::UBIGINT AS UserID
    FROM range(${ROWS}) t(i)
  ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 1000000)`,
);

const spillCap = process.env.SPILL_DO_PROBE_CAP ?? "8";
const mf = createSpillMiniflare({
  WCOL_SPILL_RUN_CAP: spillCap,
  WCOL_NODE_WASM_BUILD: process.env.WCOL_NODE_WASM_BUILD ?? "local",
});
const started = Date.now();
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
      maxNodes: 1,
      idempotencyKey: `spill-do-probe-${Date.now()}`,
    }),
  });
  const posted = (await response.json()) as { jobId?: string; error?: string };
  if (!posted.jobId) {
    throw new Error(`job POST ${response.status}: ${JSON.stringify(posted).slice(0, 400)}`);
  }
  const jobId = posted.jobId;
  console.log(JSON.stringify({ event: "spill_do_probe_start", jobId, rows: ROWS, targetGroupBytes: TARGET, probeMs: PROBE_MS }));

  const deadline = Date.now() + PROBE_MS;
  let lastPrint = "";
  let finalState = "";
  while (Date.now() < deadline) {
    const status = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await status.json()) as { state?: string; error?: string };
    finalState = String(record.state ?? "");
    const snaps = await loadReducerProgressSnapshots(output, jobId, 1);
    const top = snaps[0];
    const sample = {
      event: "spill_do_probe_sample",
      elapsedMs: Date.now() - started,
      state: finalState,
      error: record.error,
      reducers: snaps.map((row) => ({
        taskId: row.taskId,
        ...slope(row.spillBoundaries),
      })),
    };
    const line = JSON.stringify(sample);
    if (line !== lastPrint) {
      console.log(line);
      lastPrint = line;
    }
    if (finalState === "completed" || finalState === "failed") break;
    if ((top?.spillBoundaries.length ?? 0) >= 8) break;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  const snaps = await loadReducerProgressSnapshots(output, jobId, 1);
  const top = snaps[0];
  const topSlope = top ? slope(top.spillBoundaries) : null;
  console.log(
    JSON.stringify({
      event: "spill_do_probe_done",
      jobId,
      state: finalState,
      elapsedMs: Date.now() - started,
      top: topSlope,
      comparedWithLocal: {
        localPerRunLiveGrowthPostWarm: -3201,
        localAckedPerRun: 1_665_714,
        climbed: topSlope != null && topSlope.perRunLiveGrowthPostWarm > 1024 * 1024,
      },
    }),
  );
} finally {
  await mf.dispose();
  await fs.promises.unlink(parquet).catch(() => undefined);
}
