/**
 * Hosted ClickBench Parquet → Rust ingest chain over HTTP Range reads.
 * Host fetches column chunks (or footer-only for metadata COUNT); Wasm emits PWCL partials on the WCS edge.
 */

import { HttpRangeSource } from "../ts-query-engine/io/byte-source.ts";
import { ParquetFile, planParquetSql } from "../ts-query-engine/parquet.ts";
import {
  columnsFromPlan,
  rowGroups,
  type ParquetColumn,
  type ParquetScanGroup,
} from "../ts-query-engine/parquet-job.ts";
import { memoryAwareConcurrentJobs } from "./coordinator-tasks.ts";
import { readParquetFooter, runPlannedQuery } from "../../src/runtime/coordinator/plan-dispatch.ts";
import { scanColumnsForStreamSql } from "./stream-sql-plan.ts";
import { loadStreamWasm } from "../../src/runtime/host/wasm.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

export { partialsEquivalent } from "../ts-query-engine/partial-oracle.ts";

export const DEFAULT_HITS_URL =
  "https://datasets.clickhouse.com/hits_compatible/hits.parquet";

export type HitsExecGroupOptions = {
  url: string;
  sql: string;
  maxRowGroups?: number;
  batchRows?: number;
  scratchBytes?: number;
  /** HTTP ReadAt pool concurrency (default 4). */
  readAtConcurrency?: number;
};

export type DistributedHitsConfig = {
  /** Node slots for the physical planner. Default: one per `rowGroupsPerJob` row groups. */
  nodeCount?: number;
  /** Sizes the default `nodeCount`; the planner, not this, decides which node reads what. */
  rowGroupsPerJob: number;
  /** How many worker jobs run at once. */
  concurrentJobs: number;
  readAtConcurrency: number;
  /** Cap summed in-flight host ReadAt depth across all workers (Rust depth 2 each). */
  maxTotalOutstandingReads: number;
  /** Stable source id for task keys (default: exec `url`). */
  sourceKey?: string;
  /** When set with `peakMemoryPerJobBytes`, caps `concurrentJobs` via [`memoryAwareConcurrentJobs`]. */
  availableMemoryBytes?: number;
  /** Observed or estimated peak Wasm bytes per worker job (default 13 MiB from Q8 sweep). */
  peakMemoryPerJobBytes?: number;
  /** Per-shard worker retries before the coordinator marks the job failed (default 3). */
  maxAttemptsPerTask?: number;
};

export const DEFAULT_DISTRIBUTED_HITS: DistributedHitsConfig = {
  rowGroupsPerJob: 7,
  concurrentJobs: 4,
  readAtConcurrency: 4,
  maxTotalOutstandingReads: 16,
};

class GlobalReadAtBudget {
  private inFlight = 0;
  constructor(private readonly max: number) {}

  async acquire(): Promise<void> {
    while (this.inFlight >= this.max) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    this.inFlight += 1;
  }

  release(): void {
    this.inFlight = Math.max(0, this.inFlight - 1);
  }
}

export type HitsExecGroupPlan = {
  columns: ParquetColumn[];
  /** Column indexes the SQL reads (empty → metadata-only count, fused ingest plan). */
  scanColumns: number[];
  groups: ParquetScanGroup[];
  rangeReads: number;
  compressedBytes: number;
};

export async function planHitsExecGroup(
  options: HitsExecGroupOptions,
): Promise<HitsExecGroupPlan> {
  const file = await ParquetFile.open(options.url);
  try {
    const columns = columnsFromPlan(file.plan);
    const wasm = await loadStreamWasm();
    let scanColumns: number[];
    if (wasm.wcol_columns_for_sql) {
      scanColumns = await scanColumnsForStreamSql(options.sql, columns);
    } else {
      const planned = await planParquetSql(options.sql, columns);
      scanColumns = planned.columns;
    }
    for (const index of scanColumns) {
      const column = columns[index];
      if (!column) throw new Error(`query references missing column ${index}`);
      if (column.physical === 4 || column.physical === 5) {
        throw new Error(`float columns are not supported in the stream slice (${column.name})`);
      }
    }
    let groups = rowGroups(file.plan, scanColumns);
    if (options.maxRowGroups !== undefined) {
      groups = groups.slice(0, options.maxRowGroups);
    }
    const rangeReads = groups.reduce((sum, group) => sum + group.chunks.length, 0);
    const compressedBytes = groups.reduce(
      (sum, group) => sum + group.chunks.reduce((inner, chunk) => inner + chunk.compressedSize, 0),
      0,
    );
    return { columns, scanColumns, groups, rangeReads, compressedBytes };
  } finally {
    await file.close();
  }
}

/** One node of the physical plan. `plan` supplies the column preview; the planner owns the reads. */
export async function runHitsExecGroup(
  wasm: WcolStreamWasm,
  _plan: HitsExecGroupPlan,
  options: HitsExecGroupOptions,
): Promise<{
  partial: Uint8Array;
  inputBytes: number;
  outputBytes: number;
  readAtWaitMs?: number;
  readAtCalls?: number;
  maxWasmMemoryBytes?: number;
}> {
  const source = new HttpRangeSource(options.url, { rangeOnly: true });
  const readAt = (offset: number, length: number) => source.read(offset, length);
  const footer = await readParquetFooter(readAt, await source.contentLength());
  const result = await runPlannedQuery(wasm, {
    sql: options.sql,
    sourceKey: options.url,
    footer,
    readAt,
    nodeCount: options.nodeCount ?? [true].length,
    batchRows: options.batchRows,
    maxRowGroups: options.maxRowGroups,
    scratchBytes: options.scratchBytes,
    readAtConcurrency: options.readAtConcurrency,
  });
  return {
    partial: result.partial,
    inputBytes: result.plannedBytes,
    outputBytes: result.partial.byteLength,
    readAtWaitMs: result.readAtWaitMs,
    readAtCalls: result.readAtCalls,
    maxWasmMemoryBytes: result.maxWasmMemoryBytes,
  };
}

/**
 * Distributed run from the WCOL physical plan: Rust plans the query over the file's footer
 * (placement, reads, operators), each node runs its program over HTTP range reads, and the
 * coordinator merges node partials with `ReduceOp`.
 */
export async function runDistributedHitsExecGroup(
  wasm: WcolStreamWasm,
  plan: HitsExecGroupPlan,
  options: HitsExecGroupOptions,
  distributed: DistributedHitsConfig = DEFAULT_DISTRIBUTED_HITS,
): Promise<{
  partial: Uint8Array;
  workerCount: number;
  /** After memory-aware cap (may be less than config `concurrentJobs`). */
  concurrentJobsUsed?: number;
  readAtWaitMs: number;
  readAtCalls: number;
  pipelineMs: number;
  maxWasmMemoryBytes: number;
  reduceMs: number;
  workerWallMsSum: number;
  failedTaskAttempts: number;
}> {
  const nodeCount =
    distributed.nodeCount ?? Math.max(1, Math.ceil(plan.groups.length / Math.max(1, distributed.rowGroupsPerJob)));
  const peakPerJob = distributed.peakMemoryPerJobBytes ?? 13 * 1024 * 1024;
  let concurrentJobs = distributed.concurrentJobs;
  if (distributed.availableMemoryBytes !== undefined) {
    concurrentJobs = memoryAwareConcurrentJobs({
      requestedConcurrency: distributed.concurrentJobs,
      availableMemoryBytes: distributed.availableMemoryBytes,
      peakMemoryPerJobBytes: peakPerJob,
    });
  }
  const globalReadBudget =
    distributed.maxTotalOutstandingReads > 0
      ? new GlobalReadAtBudget(distributed.maxTotalOutstandingReads)
      : null;
  const source = new HttpRangeSource(options.url, { rangeOnly: true });
  const readAt = async (offset: number, length: number): Promise<Uint8Array> => {
    if (globalReadBudget) await globalReadBudget.acquire();
    try {
      return await source.read(offset, length);
    } finally {
      globalReadBudget?.release();
    }
  };
  const footer = await readParquetFooter(readAt, await source.contentLength());
  const started = Date.now();
  const result = await runPlannedQuery(wasm, {
    sql: options.sql,
    sourceKey: distributed.sourceKey ?? options.url,
    footer,
    readAt,
    nodeCount,
    concurrentJobs,
    maxAttemptsPerTask: distributed.maxAttemptsPerTask,
    batchRows: options.batchRows,
    maxRowGroups: options.maxRowGroups,
    scratchBytes: options.scratchBytes,
    readAtConcurrency: distributed.readAtConcurrency,
  });
  return {
    partial: result.partial,
    workerCount: result.nodeCount,
    concurrentJobsUsed: Math.min(concurrentJobs, Math.max(1, result.nodeCount)),
    readAtWaitMs: result.readAtWaitMs,
    readAtCalls: result.readAtCalls,
    pipelineMs: Date.now() - started,
    maxWasmMemoryBytes: result.maxWasmMemoryBytes,
    reduceMs: result.reduceMs,
    workerWallMsSum: 0,
    failedTaskAttempts: result.failedTaskAttempts,
  };
}

