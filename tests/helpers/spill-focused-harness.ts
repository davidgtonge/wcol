/**
 * Fast Q15-shaped spill repro: one scan → one reducer, UserID u64, isolated Wasm.
 * Targets 40+ spill boundaries in ~1–2 minutes (not full hits, not multi-scan Miniflare).
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, type ReadAt } from "../../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../../src/runtime/host/wasm.ts";
import type { SpillBoundary } from "../../src/runtime/host/host-types.ts";
import { createMemoryR2SpillBucket } from ".././helpers/memory-r2-spill-bucket.ts";

export const Q15_USERID_SQL =
  "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10";

export type SpillOwnershipSample = {
  run: number;
  liveBefore: number;
  liveSqlAcked: number;
  liveAfter: number;
  sqliteWriteBytes: number;
  rustInflightSpillBytes: number;
  rustQueuedSpillBytes: number;
  hostPendingSqlBytes: number;
  serializeBytes: number;
};

export function spillOwnershipFromBoundaries(rows: SpillBoundary[]): SpillOwnershipSample[] {
  return rows.map((row) => ({
    run: row.run,
    liveBefore: row.liveBefore,
    liveSqlAcked: row.liveSqlAcked,
    liveAfter: row.liveAfter,
    sqliteWriteBytes: row.sqliteWriteBytes,
    rustInflightSpillBytes: row.rustInflightSpillBytes,
    rustQueuedSpillBytes: row.rustQueuedSpillBytes,
    hostPendingSqlBytes: row.hostPendingSqlBytes,
    serializeBytes: row.serializeBytes,
  }));
}

export type FastSpillPlateauSummary = {
  spillRuns: number;
  /** Sum of per-boundary `sqliteWriteBytes` (last page of each run, not the whole run). */
  totalSqliteWriteBytes: number;
  avgSqliteWriteBytesPerRun: number;
  /** Host R2 bytes uploaded for spill runs. */
  r2BytesUploaded: number;
  /** @deprecated use r2BytesUploaded */
  sqliteBytesAcked: number;
  liveAfterRun1: number;
  liveAfterLast: number;
  perRunLiveGrowthPostWarm: number;
  tailLiveAfterWarm: number;
  wasmInstanceId: number;
  boundaries: SpillBoundary[];
  ownership: SpillOwnershipSample[];
};

export async function runFastUserIdSpillPlateau(options?: {
  groups?: number;
  targetGroupBytes?: number;
  warmup?: number;
  /** Planned scanners. Use >1 with rowGroupSize to fan partials into each reducer. */
  maxNodes?: number;
  hashReducers?: number;
  rowGroupSize?: number;
}): Promise<FastSpillPlateauSummary> {
  const groups = options?.groups ?? Number(process.env.SPILL_FAST_GROUPS ?? 14_000);
  const targetGroupBytes =
    options?.targetGroupBytes ?? Number(process.env.SPILL_FAST_TARGET_BYTES ?? 32 * 1024);
  const warmup = options?.warmup ?? 5;
  const maxNodes = options?.maxNodes ?? 1;
  const hashReducers = options?.hashReducers ?? 2;
  const rowGroupSize = options?.rowGroupSize;
  const parquet = path.join(os.tmpdir(), `wcol-spill-fast-${process.pid}.parquet`);

  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await duck.run(
    `COPY (
      SELECT (abs(hash(i)) % 9000000000000000000)::UBIGINT AS UserID
      FROM range(${groups}) t(i)
    ) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET${
      rowGroupSize ? `, ROW_GROUP_SIZE ${rowGroupSize}` : ""
    })`,
  );

  const readAt: ReadAt = async (offset, length) => {
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
  const size = fs.statSync(parquet).size;
  const footer = await readParquetFooter(readAt, size);
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { hits: { key: "http://example.invalid/hits.parquet" } },
      sql: Q15_USERID_SQL,
      output: { format: "json" },
      hashReducers,
      groupSpill: true,
      targetGroupBytes,
    }),
    footers: [{ fileSize: size, footer }],
    maxNodes,
    batchRows: 8192,
    minBytesPerNode: 1,
    hashReducers,
  });
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error("fast spill job did not plan a hash graph");
  }

  const boundariesByTask = new Map<number, SpillBoundary[]>();
  let wasmInstanceId = 0;
  const spillR2 = createMemoryR2SpillBucket();
  try {
    await runLocalHashGraph(
      { node, coordinator },
      planned.graph,
      readAt,
      {
        isolatedNodeWasm: true,
        spillR2Bucket: spillR2,
        onReducerProgress: (info) => {
          wasmInstanceId = info.wasmInstanceId;
          const prev = boundariesByTask.get(info.taskId) ?? [];
          if ((info.spillBoundaries?.length ?? 0) > prev.length) {
            const rows = info.spillBoundaries ?? prev;
            boundariesByTask.set(info.taskId, rows);
            if (process.env.SPILL_CF_LOG === "1") {
              const last = rows[rows.length - 1];
              if (last) {
                console.log(
                  JSON.stringify({
                    event: "spill",
                    taskId: info.taskId,
                    run: last.run,
                    liveAfter: last.liveAfter,
                    sqliteWriteBytes: last.sqliteWriteBytes,
                    rustInflight: last.rustInflightSpillBytes,
                  }),
                );
              }
            }
          }
        },
      },
    );
  } finally {
    await fs.promises.unlink(parquet).catch(() => undefined);
  }

  let boundaries: SpillBoundary[] = [];
  for (const rows of boundariesByTask.values()) {
    if (rows.length > boundaries.length) boundaries = rows;
  }

  if (boundaries.length === 0) {
    throw new Error("no spill boundaries recorded");
  }
  const first = boundaries[0]!;
  const last = boundaries[boundaries.length - 1]!;
  const warm = boundaries[Math.min(warmup, boundaries.length - 1)]!;
  const tail = boundaries.slice(Math.min(warmup, boundaries.length - 1));
  const totalSqliteWrite = boundaries.reduce((sum, row) => sum + row.sqliteWriteBytes, 0);
  const ownership = spillOwnershipFromBoundaries(boundaries);
  const r2BytesUploaded = spillR2.bytesUploaded;

  return {
    spillRuns: boundaries.length,
    totalSqliteWriteBytes: totalSqliteWrite,
    avgSqliteWriteBytesPerRun: totalSqliteWrite / boundaries.length,
    r2BytesUploaded,
    sqliteBytesAcked: r2BytesUploaded,
    liveAfterRun1: first.liveAfter,
    liveAfterLast: last.liveAfter,
    perRunLiveGrowthPostWarm: (last.liveAfter - warm.liveAfter) / Math.max(1, last.run - warm.run),
    tailLiveAfterWarm: Math.max(...tail.map((row) => row.liveAfter)) - warm.liveAfter,
    wasmInstanceId,
    boundaries,
    ownership,
  };
}
