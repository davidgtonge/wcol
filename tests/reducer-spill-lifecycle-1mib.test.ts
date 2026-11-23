/**
 * Isolated 1 MiB lifecycle (same helpers as reducer-spill-lifecycle.test.ts).
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "./helpers/memory-r2-spill-bucket.ts";
import type { SpillBoundary } from "../src/runtime/host/host-types.ts";
import type { ExecutionPlan } from "../src/runtime/coordinator/execution-graph.ts";

const WARMUP = 5;
const PLATEAU_SLACK = 2 * 1024 * 1024;
const GAP_SLACK = 512 * 1024;

type LifecycleCase = {
  label: string;
  groups: number;
  targetGroupBytes: number;
  minRuns: number;
  hashReducers: number;
  batchRows: number;
};

function gapToNextFlushEnter(rows: SpillBoundary[], warmup: number): number[] {
  const gaps: number[] = [];
  for (let i = warmup; i + 1 < rows.length; i += 1) {
    gaps.push(rows[i + 1]!.liveBefore - rows[i]!.liveAfter);
  }
  return gaps;
}

function summarizeBoundaries(
  label: string,
  targetGroupBytes: number,
  rows: SpillBoundary[],
  bytesAcked: number,
) {
  const first = rows[0]!;
  const last = rows[rows.length - 1]!;
  const gaps = gapToNextFlushEnter(rows, WARMUP);
  const liveTail = rows.slice(WARMUP).map((row) => row.liveAfter);
  const warmBaseline = liveTail[0] ?? 0;
  return {
    label,
    targetGroupBytes,
    spillRuns: rows.length,
    bytesAcked,
    liveAfterTailPeakMinusWarm: Math.max(...liveTail) - warmBaseline,
    maxCrossRunGap: gaps.length ? Math.max(...gaps) : 0,
  };
}

function assertPlateau(summary: ReturnType<typeof summarizeBoundaries>, boundaries: SpillBoundary[]): void {
  assert.ok(
    boundaries.every((row) => row.mergeScanBegins === 0 && row.mergeSteps === 0),
    "merge/finalize must not run during ingest flushes",
  );
  assert.ok(
    summary.liveAfterTailPeakMinusWarm < PLATEAU_SLACK,
    `${summary.label}: liveAfter grew ${summary.liveAfterTailPeakMinusWarm} after warm-up`,
  );
  assert.ok(
    summary.maxCrossRunGap < GAP_SLACK,
    `${summary.label}: GROUP_RESET→NEXT_FLUSH_ENTER max gap ${summary.maxCrossRunGap}`,
  );
}

async function ensureParquet(caseDef: LifecycleCase, outPath: string): Promise<void> {
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  const col = "k";
  await duck.run(
    `COPY (
      SELECT printf('user-%d', i) AS ${col} FROM range(${caseDef.groups}) t(i)
    ) TO '${outPath.replaceAll("'", "''")}' (FORMAT PARQUET)`,
  );
}

describe("1 MiB spill lifecycle", () => {
  it("plateaus with discard-on-ack", { timeout: 600_000 }, async () => {
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", "start\n");
    const caseDef: LifecycleCase = {
      label: "1mib",
      groups: 800_000,
      targetGroupBytes: 1024 * 1024,
      minRuns: 20,
      hashReducers: 2,
      batchRows: 4096,
    };
    const parquetPath = path.join(os.tmpdir(), `wcol-spill-1mib-${process.pid}.parquet`);
    await ensureParquet(caseDef, parquetPath);
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", "parquet\n");
    const readFileAt = async (offset: number, length: number): Promise<Uint8Array> => {
      const fh = await fs.promises.open(parquetPath, "r");
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
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", "wasm\n");
    const size = fs.statSync(parquetPath).size;
    const footer = await readParquetFooter(readFileAt, size);
    const planned = planJob(coordinator, {
      body: JSON.stringify({
        sources: { t: { key: "http://example.invalid/data.parquet" } },
        sql: "SELECT k, COUNT(*) AS c FROM t GROUP BY k",
        output: { format: "json" },
        hashReducers: caseDef.hashReducers,
        groupSpill: true,
        targetGroupBytes: caseDef.targetGroupBytes,
      }),
      footers: [{ fileSize: size, footer }],
      maxNodes: 1,
      batchRows: caseDef.batchRows,
      minBytesPerNode: 1,
      hashReducers: caseDef.hashReducers,
    });
    assert.equal(planned.kind, "json");
    if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("expected graph");
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", "plan\n");
    const spillR2 = createMemoryR2SpillBucket();
    let boundaries: SpillBoundary[] = [];
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", "graph-start\n");
    await runLocalHashGraph(
      { node, coordinator },
      planned.graph as Extract<ExecutionPlan, { kind: "graph" }>,
      readFileAt,
      {
        spillR2Bucket: spillR2,
        onReducerProgress: (info) => {
          if ((info.spillBoundaries?.length ?? 0) > boundaries.length) {
            boundaries = info.spillBoundaries ?? boundaries;
          }
        },
      },
    );
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", `after graph boundaries=${boundaries.length}\n`);
    assert.ok(boundaries.length >= caseDef.minRuns, `saw ${boundaries.length} spill runs`);
    const summary = summarizeBoundaries("1mib-discard", caseDef.targetGroupBytes, boundaries, spillR2.bytesUploaded);
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", `before plateau tail=${summary.liveAfterTailPeakMinusWarm}\n`);
    assertPlateau(summary, boundaries);
    fs.appendFileSync("/tmp/wcol-1mib-trace.log", `done\n`);
    await fs.promises.unlink(parquetPath).catch(() => undefined);
  });
});
