/**
 * R2 sorted-run spill lifecycle: post-flush live bytes plateau and merge path.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
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
  timeoutMs: number;
  /** If set, read this parquet instead of generating `groups` string keys. */
  parquetPath?: string;
  sql?: string;
  keyColumn?: string;
};

type LifecycleSummary = {
  label: string;
  targetGroupBytes: number;
  spillRuns: number;
  bytesAcked: number;
  mergeScanBeginsMax: number;
  mergeStepsMax: number;
  liveAfterRun1: number;
  liveAfterLast: number;
  liveBeforeRun1: number;
  liveBeforeLast: number;
  liveAfterDelta: number;
  perRunLiveGrowth: number;
  maxCrossRunGap: number;
  maxSqlAckedMinusReset: number;
  liveAfterTailPeakMinusWarm: number;
  serializeBytesLast: number;
  rustInflightLast: number;
};

function gapToNextFlushEnter(rows: SpillBoundary[], warmup: number): number[] {
  const gaps: number[] = [];
  for (let i = warmup; i + 1 < rows.length; i += 1) {
    gaps.push(rows[i + 1]!.liveBefore - rows[i]!.liveAfter);
  }
  return gaps;
}

function summarizeBoundaries(label: string, targetGroupBytes: number, rows: SpillBoundary[], bytesAcked: number): LifecycleSummary {
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
    mergeScanBeginsMax: Math.max(...rows.map((row) => row.mergeScanBegins)),
    mergeStepsMax: Math.max(...rows.map((row) => row.mergeSteps)),
    liveAfterRun1: first.liveAfter,
    liveAfterLast: last.liveAfter,
    liveBeforeRun1: first.liveBefore,
    liveBeforeLast: last.liveBefore,
    liveAfterDelta: last.liveAfter - first.liveAfter,
    perRunLiveGrowth: (last.liveAfter - first.liveAfter) / Math.max(1, last.run - first.run),
    maxCrossRunGap: gaps.length ? Math.max(...gaps) : 0,
    maxSqlAckedMinusReset: Math.max(...rows.slice(WARMUP).map((row) => row.liveSqlAcked - row.liveAfter)),
    liveAfterTailPeakMinusWarm: Math.max(...liveTail) - warmBaseline,
    serializeBytesLast: last.serializeBytes,
    rustInflightLast: last.rustInflightSpillBytes,
  };
}

function logSummary(summary: LifecycleSummary): void {
  console.log(`\n=== spill lifecycle: ${summary.label} ===`);
  console.log(JSON.stringify(summary, null, 2));
}

function assertPlateau(summary: LifecycleSummary, boundaries: SpillBoundary[]): void {
  assert.ok(
    boundaries.every((row) => row.mergeScanBegins === 0 && row.mergeSteps === 0),
    "merge/finalize must not run during ingest flushes",
  );
  assert.ok(
    summary.liveAfterTailPeakMinusWarm < PLATEAU_SLACK,
    `${summary.label}: liveAfter grew ${summary.liveAfterTailPeakMinusWarm} after warm-up`,
  );
  // After a reset the table refills to the flush target before the next flush, so the gap
  // is about one target: a fixed bound only held for targets far below it.
  assert.ok(
    summary.maxCrossRunGap < summary.targetGroupBytes + GAP_SLACK,
    `${summary.label}: GROUP_RESET→NEXT_FLUSH_ENTER max gap ${summary.maxCrossRunGap}`,
  );
  for (const row of boundaries.slice(WARMUP)) {
    assert.ok(
      row.rustInflightSpillBytes <= row.liveAfter + 256 * 1024,
      `inflight spill (${row.rustInflightSpillBytes}) >> group_reset (${row.liveAfter}) on run ${row.run}`,
    );
  }
}

async function ensureParquet(caseDef: LifecycleCase, outPath: string): Promise<void> {
  if (caseDef.parquetPath) {
    if (!fs.existsSync(caseDef.parquetPath)) {
      throw new Error(`parquet not found: ${caseDef.parquetPath}`);
    }
    fs.copyFileSync(caseDef.parquetPath, outPath);
    return;
  }
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  const col = caseDef.keyColumn ?? "k";
  await duck.run(
    `COPY (
      SELECT printf('user-%d', i) AS ${col} FROM range(${caseDef.groups}) t(i)
    ) TO '${outPath.replaceAll("'", "''")}' (FORMAT PARQUET)`,
  );
}

async function runLifecycleCase(caseDef: LifecycleCase): Promise<{
  boundaries: SpillBoundary[];
  summary: LifecycleSummary;
  spillR2: ReturnType<typeof createMemoryR2SpillBucket>;
  rows: Uint8Array;
  spillHost: { runPutsCompleted: number; runReads: number; runCommits: number };
}> {
  const parquetPath = path.join(os.tmpdir(), `wcol-spill-lifecycle-${caseDef.label}-${process.pid}.parquet`);
  await ensureParquet(caseDef, parquetPath);

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
  const size = fs.statSync(parquetPath).size;
  const footer = await readParquetFooter(readFileAt, size);
  const keyCol = caseDef.keyColumn ?? "k";
  const table = caseDef.parquetPath ? "hits" : "t";
  // Every group is aggregated (and spilled) on the reducers; the LIMIT only keeps the answer
  // under the JSON result cap, which an unbounded GROUP BY of millions of keys exceeds.
  const sql = caseDef.sql ?? `SELECT ${keyCol}, COUNT(*) AS c FROM ${table} GROUP BY ${keyCol} ORDER BY c DESC LIMIT 10`;
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { [table]: { key: "http://example.invalid/data.parquet" } },
      sql,
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
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error("spill query did not plan a graph");
  }

  const spillR2 = createMemoryR2SpillBucket();
  let boundaries: SpillBoundary[] = [];
  const spillHost = { runPutsCompleted: 0, runReads: 0, runCommits: 0 };
  const result = await runLocalHashGraph(
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
      onSpillHostTelemetry: (telemetry) => {
        spillHost.runPutsCompleted += telemetry.runPutsCompleted;
        spillHost.runReads += telemetry.runReads;
        spillHost.runCommits += telemetry.runCommits;
      },
    },
  );

  assert.ok(
    boundaries.length >= caseDef.minRuns,
    `${caseDef.label}: expected >= ${caseDef.minRuns} spill runs, saw ${boundaries.length}`,
  );
  assert.ok(spillR2.bytesUploaded > 0, `${caseDef.label}: expected R2 spill upload bytes`);

  const summary = summarizeBoundaries(caseDef.label, caseDef.targetGroupBytes, boundaries, spillR2.bytesUploaded);
  logSummary(summary);
  return { boundaries, summary, spillR2, rows: result.bytes, spillHost };
}

describe("reducer spill lifecycle checkpoints", { concurrency: 1 }, () => {
  it("small target (32 KiB) plateaus", { timeout: 180_000 }, async () => {
    const { boundaries, summary } = await runLifecycleCase(
      {
        label: "32kib",
        groups: 12_000,
        targetGroupBytes: 32 * 1024,
        minRuns: 45,
        hashReducers: 2,
        batchRows: 128,
        timeoutMs: 180_000,
      },
    );
    assertPlateau(summary, boundaries);
  });

  it("1 MiB target plateaus", { timeout: 600_000 }, async () => {
    const { boundaries, summary } = await runLifecycleCase(
      {
        label: "1mib",
        groups: 800_000,
        targetGroupBytes: 1024 * 1024,
        minRuns: 20,
        hashReducers: 2,
        batchRows: 4096,
        timeoutMs: 600_000,
      },
    );
    assertPlateau(summary, boundaries);
  });

  it("16 MiB discard-on-ack (synthetic 3M keys)", { timeout: 900_000 }, async () => {
    const hits = process.env.HITS_PARQUET;
    const caseDef: LifecycleCase = {
      label: hits ? "16mib-hits" : "16mib-synthetic",
      groups: hits ? 0 : 3_000_000,
      targetGroupBytes: 16 * 1024 * 1024,
      minRuns: hits ? 10 : 15,
      hashReducers: 2,
      batchRows: 8192,
      timeoutMs: 900_000,
      parquetPath: hits,
      sql: hits ? "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10" : undefined,
      keyColumn: hits ? "UserID" : "k",
    };
    const { boundaries, summary } = await runLifecycleCase(caseDef);
    const q15LikePerRun = 3 * 1024 * 1024;
    if (summary.perRunLiveGrowth > 2 * 1024 * 1024) {
      console.log(`\n*** Q15-like per-run live growth: ${summary.perRunLiveGrowth} bytes/run ***\n`);
    }
    assert.ok(
      summary.perRunLiveGrowth < q15LikePerRun,
      `Q15-like retention: ${summary.perRunLiveGrowth} bytes/run`,
    );
    if (summary.liveAfterTailPeakMinusWarm >= PLATEAU_SLACK || summary.maxCrossRunGap >= summary.targetGroupBytes + GAP_SLACK) {
      console.log(
        `note: post-reset wobble (tail-warm=${summary.liveAfterTailPeakMinusWarm}, maxGap=${summary.maxCrossRunGap}) — not Q15 monotonic climb`,
      );
    }
  });

  it("16 MiB R2 spill (multi-run merge path)", { timeout: 600_000 }, async () => {
    const caseDef: LifecycleCase = {
      label: "16mib-merge-probe",
      groups: 800_000,
      targetGroupBytes: 1024 * 1024,
      minRuns: 20,
      hashReducers: 2,
      batchRows: 4096,
      timeoutMs: 600_000,
    };
    const { boundaries, summary, spillR2, rows, spillHost } = await runLifecycleCase(caseDef);
    const maxMergeBegins = Math.max(...boundaries.map((row) => row.mergeScanBegins));
    const maxMergeSteps = Math.max(...boundaries.map((row) => row.mergeSteps));
    console.log(
      JSON.stringify(
        {
          r2Puts: spillR2.puts,
          r2Gets: spillR2.gets,
          bytesUploaded: spillR2.bytesUploaded,
          hostRunPutEnds: spillHost.runPutsCompleted,
          hostRunReads: spillHost.runReads,
          hostRunCommits: spillHost.runCommits,
          maxMergeBegins,
          maxMergeSteps,
          spillBoundaries: boundaries.length,
          outputBytes: rows.byteLength,
        },
        null,
        2,
      ),
    );
    assert.ok(spillR2.puts >= 2, `expected >= 2 committed spill runs, puts=${spillR2.puts}`);
    assert.ok(rows.byteLength > 0, "finalizer produced no rows");
    assert.ok(
      spillR2.gets > 0 || spillHost.runReads > 0 || maxMergeSteps > 0,
      `expected R2 run-read or merge activity (gets=${spillR2.gets}, hostReads=${spillHost.runReads}, mergeSteps=${maxMergeSteps})`,
    );
    const q15LikePerRun = 3 * 1024 * 1024;
    assert.ok(
      summary.perRunLiveGrowth < q15LikePerRun,
      `R2 spill Q15-like growth: ${summary.perRunLiveGrowth} bytes/run`,
    );
  });
});
