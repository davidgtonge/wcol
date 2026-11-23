/**
 * Where reducer Wasm grows, at a pinned reducer count of 2.
 *
 *   npx tsx scripts/wasm-growth-bench.ts
 *
 * Q15 on the 2M file checks the rebuilt spill-run counter.
 * Q33 runs on row slices of that file, which stores URL as plain strings.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import type { MemGrowthSample } from "../src/runtime/host/host-types.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const sourceParquet = path.resolve(process.cwd(), "data/hits_subset_2m.parquet");
const MiB = 1024 * 1024;
const REDUCERS = 2;

function loadSql(label: string): string {
  const line = fs
    .readFileSync(path.join(process.cwd(), "scripts/clickbench-queries.tsv"), "utf8")
    .split("\n")
    .find((row) => row.startsWith(`${label}\t`));
  if (!line) throw new Error(`missing ${label}`);
  return line.split("\t")[1]!.trim().replace(/;$/, "");
}

function mib(bytes: number): number {
  return Number((bytes / MiB).toFixed(2));
}

function summarize(samples: MemGrowthSample[] | undefined) {
  return (samples ?? []).map((sample) => ({
    phase: sample.phase,
    wasmMiB: mib(sample.wasmLinearBytes),
    liveMiB: mib(sample.rustLiveBytes),
    peakLiveMiB: mib(sample.rustPeakBytes),
    groupMiB: mib(sample.groupTableBytes),
    internerMiB: mib(sample.stringInternerBytes),
    spillRuns: sample.spillRunsCommitted,
    spillPendingMiB: mib(sample.spillPendingWriteBytes),
    spillMergeMiB: mib(sample.spillMergeBufferBytes),
    wcsPendingMiB: mib(sample.wcsPendingBytes),
    activeInputs: sample.activeInputStreams,
    otherLiveMiB: mib(
      sample.rustLiveBytes -
        sample.groupTableBytes -
        sample.stringInternerBytes -
        sample.spillPendingWriteBytes -
        sample.spillMergeBufferBytes -
        sample.wcsPendingBytes,
    ),
  }));
}

async function readAt(file: string, offset: number, length: number): Promise<Uint8Array> {
  const fh = await fs.promises.open(file, "r");
  try {
    const out = new Uint8Array(length);
    await fh.read(out, 0, length, offset);
    return out;
  } finally {
    await fh.close();
  }
}

async function runQuery(
  label: string,
  file: string,
  sql: string,
  node: Awaited<ReturnType<typeof loadNodeWasm>>,
  coordinator: Awaited<ReturnType<typeof loadCoordinatorWasm>>,
): Promise<void> {
  const size = fs.statSync(file).size;
  const footer = await readParquetFooter((offset, length) => readAt(file, offset, length), size);
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { hits: { key: "http://example.invalid/hits.parquet" } },
      sql,
      output: { format: "json", rowCap: 20 },
      hashReducers: REDUCERS,
      groupSpill: true,
      targetGroupBytes: 16 * MiB,
      hardGroupBytes: 32 * MiB,
    }),
    footers: [{ fileSize: size, footer }],
    maxNodes: 4,
    minBytesPerNode: 1,
    batchRows: 2048,
    hashReducers: REDUCERS,
  });
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error(`${label} planned ${planned.kind}`);
  }
  const reducers = new Map<number, { wasm: number; spillRuns: number; group: number; growth?: MemGrowthSample[] }>();
  let scanWasm = 0;
  const scanGrowth: MemGrowthSample[][] = [];
  const started = performance.now();
  await runLocalHashGraph(
    { node, coordinator },
    planned.graph,
    (offset, length) => readAt(file, offset, length),
    {
      onScanProgress: (info) => {
        scanWasm = Math.max(scanWasm, info.wasmMemoryBytes);
        if (info.memGrowth && info.memGrowth.length > 0) scanGrowth.push(info.memGrowth);
      },
      onReducerProgress: (info) => {
        const previous = reducers.get(info.taskId);
        reducers.set(info.taskId, {
          wasm: Math.max(previous?.wasm ?? 0, info.wasmMemoryBytes),
          spillRuns: Math.max(previous?.spillRuns ?? 0, info.spillRuns ?? 0),
          group: Math.max(previous?.group ?? 0, info.spillPeakLiveBytes ?? 0),
          growth: info.memGrowth ?? previous?.growth,
        });
      },
    },
  );
  const spillRuns = [...reducers.values()].reduce((sum, row) => sum + row.spillRuns, 0);
  const peakGroup = Math.max(0, ...[...reducers.values()].map((row) => row.group));
  const peakReducerWasm = Math.max(0, ...[...reducers.values()].map((row) => row.wasm));
  console.log(
    JSON.stringify({
      label,
      file: path.basename(file),
      reducers: REDUCERS,
      wallMs: Math.round(performance.now() - started),
      spillRuns,
      peakGroupMiB: mib(peakGroup),
      scanWasmMiB: mib(scanWasm),
      reducerWasmMiB: mib(peakReducerWasm),
      scanGrowth: scanGrowth.map(summarize),
      reducersDetail: [...reducers.entries()].map(([taskId, row]) => ({
        taskId,
        wasmMiB: mib(row.wasm),
        groupMiB: mib(row.group),
        spillRuns: row.spillRuns,
        growth: summarize(row.growth),
      })),
    }),
  );
}

async function sliceParquet(duck: Awaited<ReturnType<Awaited<ReturnType<typeof DuckDBInstance.create>>["connect"]>>, rows: number): Promise<string> {
  const dest = path.join(os.tmpdir(), `wcol-q33-${rows}.parquet`);
  const escaped = sourceParquet.replaceAll("'", "''");
  await duck.run(`COPY (SELECT * FROM read_parquet('${escaped}') LIMIT ${rows}) TO '${dest}' (FORMAT PARQUET)`);
  return dest;
}

async function main(): Promise<void> {
  const node = await loadNodeWasm();
  const coordinator = await loadCoordinatorWasm();
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await runQuery("Q15", sourceParquet, loadSql("Q15"), node, coordinator);
  for (const rows of [400_000, 500_000]) {
    const file = await sliceParquet(duck, rows);
    await runQuery(`Q33-${rows}`, file, loadSql("Q33"), node, coordinator);
  }
  await runQuery("Q33", sourceParquet, loadSql("Q33"), node, coordinator);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
