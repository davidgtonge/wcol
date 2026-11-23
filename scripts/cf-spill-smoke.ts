/**
 * Cloudflare workerd smoke: GROUP BY spill through R2 sorted runs + Wasm host.
 *
 * Default spill budget is 2 MiB (less write amplification than 256 KiB).
 *
 * Miniflare persists simulated R2 + DO SQLite on disk. Full 500k runs need
 * several GiB free; "No space left on device" from workerd is local disk, not CF.
 *
 * Full distributed smoke (slow): npm run test:cf-spill-smoke
 * Fast iteration: npm run test:cf-spill-quick
 */

import fs from "node:fs";
import path from "node:path";
import { Miniflare } from "miniflare";
import { buildWorkers, distDir, ROOT } from "./build-workers.ts";
import { loadJobSpillTiming, spillReport, type SpillDiag } from "./cf-spill-shared.ts";

/** Reducer combined group budget when groupSpill is enabled. */
const SPILL_TARGET_GROUP_BYTES = 2 * 1024 * 1024;

const hitsPath = process.argv[2] ?? path.join(ROOT, "data/hits_subset_500k.parquet");
const rowLimitRaw = process.env.WCOL_SPILL_SMOKE_ROW_LIMIT;
const rowLimit = rowLimitRaw != null && rowLimitRaw !== "" ? Number(rowLimitRaw) : undefined;
const skipControl = process.env.WCOL_SPILL_SMOKE_SKIP_CONTROL === "1";
const hashReducers = Number(process.env.WCOL_SPILL_SMOKE_REDUCERS ?? "4");

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function availableDiskBytes(): number {
  const { bavail, bsize } = fs.statfsSync(ROOT);
  return Number(bavail) * Number(bsize);
}

function assertDiskHeadroom(): void {
  const minGiB =
    rowLimit != null && rowLimit <= 50_000 ? 2 : skipControl ? 4 : 6;
  const need = minGiB * 1024 ** 3;
  const avail = availableDiskBytes();
  if (avail >= need) return;
  throw new Error(
    `Need ~${minGiB} GiB free on the data volume for this smoke (have ${(avail / 1024 ** 3).toFixed(2)} GiB). ` +
      `workerd/Miniflare simulates R2 under OUTPUT/blobs and DO SQLite spill — not a spill logic bug. ` +
      `Free disk space, or run lighter: WCOL_SPILL_SMOKE_ROW_LIMIT=50000 WCOL_SPILL_SMOKE_SKIP_CONTROL=1`,
  );
}

function miniflare(): Miniflare {
  return new Miniflare({
    workers: [
      {
        name: "wcol-coordinator",
        modulesRoot: distDir,
        modules: [
          { type: "ESModule", path: path.join(distDir, "coordinator-worker.js") },
          { type: "CompiledWasm", path: path.join(distDir, "wcol_coordinator.wasm") },
        ],
        compatibilityDate: "2026-01-01",
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
          { type: "ESModule", path: path.join(distDir, "node-worker.js") },
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

async function post(mf: Miniflare, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: JSON.parse(text) as Record<string, unknown> };
}

async function waitFor(mf: Miniflare, jobId: string): Promise<Record<string, unknown>> {
  const started = Date.now();
  for (;;) {
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const record = (await response.json()) as Record<string, unknown>;
    if (record.state === "completed") return record;
    if (record.state === "failed") throw new Error(`job ${jobId} failed: ${record.error}`);
    if (Date.now() - started > 600_000) throw new Error(`job ${jobId} timed out in ${record.state}`);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

function verifyNodeWasmExports(): void {
  const wasmPath = path.join(distDir, "wcol_node.wasm");
  const bytes = fs.readFileSync(wasmPath);
  const module = new WebAssembly.Module(bytes);
  const names = new Set(WebAssembly.Module.exports(module).map((e) => e.name));
  for (const sym of [
    "wcs_sp_open_graph",
    "wcs_sp_spill_op",
    "wcs_sp_spill_complete",
    "wcs_sp_spill_append_ptr",
  ]) {
    assert(names.has(sym), `node wasm missing export ${sym}`);
  }
}

const { DuckDBInstance } = await import("@duckdb/node-api");
const duck = await (await DuckDBInstance.create(":memory:")).connect();
const quoted = (file: string) => `'${file.replaceAll("'", "''")}'`;

async function duckRows(sql: string): Promise<string[][]> {
  const result = await duck.runAndReadAll(sql);
  return (result.getRows() as unknown[][]).map((row) => row.map((v) => (v === null ? "null" : String(v))));
}

const asText = (rows: unknown[][]) => rows.map((row) => row.map((v) => (v === null ? "null" : String(v))));

const SPILL_SQL =
  "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC, UserID LIMIT 10";

await buildWorkers();
verifyNodeWasmExports();
assertDiskHeadroom();

let parquetForJob = hitsPath;
let tmpParquet: string | undefined;
if (rowLimit != null) {
  assert(Number.isFinite(rowLimit) && rowLimit > 0, `invalid WCOL_SPILL_SMOKE_ROW_LIMIT=${rowLimitRaw}`);
  tmpParquet = path.join(ROOT, "data", `.spill-smoke-${rowLimit}.parquet`);
  await duck.run(
    `COPY (SELECT UserID FROM ${quoted(hitsPath)} LIMIT ${rowLimit}) TO ${quoted(tmpParquet)} (FORMAT PARQUET)`,
  );
  parquetForJob = tmpParquet;
}

const mf = miniflare();
try {
  assert(fs.existsSync(parquetForJob), `missing parquet: ${parquetForJob}`);
  const output = await mf.getR2Bucket("OUTPUT");
  await output.put("hits.parquet", fs.readFileSync(parquetForJob));

  const duckExpected = await duckRows(
    SPILL_SQL.replace("FROM hits", `FROM ${quoted(parquetForJob)}`),
  );

  async function runCase(label: string, extra: Record<string, unknown>) {
    const body = {
      sources: { hits: { bucket: "output", key: "hits.parquet" } },
      sql: SPILL_SQL,
      output: { format: "json" },
      ...extra,
    };
    const started = performance.now();
    const res = await post(mf, body);
    assert(res.status === 200, `${label}: ${res.status} ${JSON.stringify(res.body).slice(0, 400)}`);
    const jobId = String(res.body.jobId);
    await waitFor(mf, jobId);
    const syncRows = asText(res.body.rows as unknown[][]);
    assert(
      JSON.stringify(syncRows) === JSON.stringify(duckExpected),
      `${label}: rows differ from DuckDB`,
    );
    const timing = await loadJobSpillTiming(output, jobId, 1);
    let wallMs = performance.now() - started;
    if (timing.wallMs > 0) wallMs = Math.max(wallMs, timing.wallMs);
    return {
      label,
      wallMs,
      wasmMemoryBytes: timing.wasmMemoryBytes,
      spill: timing.spill,
      spillDiag: timing.spillDiag,
      slots: timing.taskSlots,
    };
  }

  let control:
    | {
        label: string;
        wallMs: number;
        wasmMemoryBytes: number;
        spill: SpillTelemetry;
        spillDiag: SpillDiag;
        slots: number;
      }
    | undefined;
  if (!skipControl) {
    control = await runCase("control", {
      hashReducers,
      groupSpill: false,
      targetGroupBytes: 64 * 1024 * 1024,
    });
    assert(
      control.spill.begin === 0,
      `control run spilled: ${JSON.stringify(control.spill)}`,
    );
  }

  const spillRun = await runCase("spill", {
    hashReducers,
    groupSpill: true,
    targetGroupBytes: SPILL_TARGET_GROUP_BYTES,
  });
  const diag = spillRun.spillDiag;
  assert(diag.spillExecByOp.begin >= 1, `no spill run put: ${JSON.stringify(diag.spillExecByOp)}`);
  assert(diag.spillExecByOp.append >= 1, `no spill append chunks: ${JSON.stringify(diag.spillExecByOp)}`);
  assert(diag.spillExecByOp.scan >= 1, `no spill run read: ${JSON.stringify(diag.spillExecByOp)}`);
  assert(diag.spillExecByOp.end >= 1, `no spill run close: ${JSON.stringify(diag.spillExecByOp)}`);

  const report = spillReport(diag, spillRun.wallMs, SPILL_TARGET_GROUP_BYTES);

  console.log(
    JSON.stringify(
      {
        ok: true,
        inputRows: rowLimit ?? "full",
        hashReducers,
        skipControl,
        freeDiskGiB: Math.round((availableDiskBytes() / 1024 ** 3) * 100) / 100,
        duckRows: duckExpected.length,
        control: control
          ? {
              wallMs: Math.round(control.wallMs),
              wasmMemoryBytes: control.wasmMemoryBytes,
              taskSlots: control.slots,
            }
          : null,
        spill: {
          wasmMemoryBytes: spillRun.wasmMemoryBytes,
          taskSlots: spillRun.slots,
          ...report,
        },
        optimizationNotes: [
          "Group spill uses R2 sorted runs (stream-pool driver); spillDiag.byOp tracks put/append/scan/end.",
          "Lower targetGroupBytes increases spill run count; latency is usually dominated by merge reads and upload size.",
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await mf.dispose();
  if (tmpParquet && fs.existsSync(tmpParquet)) fs.unlinkSync(tmpParquet);
}
