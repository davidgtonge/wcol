/**
 * Controlled fan-in spill experiment: real node wasm, TS host loop, R2 sorted-run spill,
 * Q15 UserID (u64) decode path — not synthetic string keys.
 */
import fs from "node:fs";
import path from "node:path";
import { planJob, readParquetFooter, resultJson, type ReadAt } from "../../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph, type LocalHashScheduling } from "./run-local-hash-graph.ts";
import type { ExecutionPlan } from "../../src/runtime/coordinator/execution-graph.ts";
import { HttpRangeSource } from "../../src/runtime/host/ports/http-range.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../../src/runtime/host/wasm.ts";
import type { SpillBoundary } from "../../src/runtime/host/host-types.ts";
import { minSpillRunsForDataset } from "./spill-coverage.ts";

export {
  assertSpillCoverageOrThrow,
  formatSpillCoverageNote,
  minSpillRunsForDataset,
  spillCoverageVerdict,
} from "./spill-coverage.ts";

/** ClickBench full file (same default as `cf-production-jobs.ts` stage C). */
export const DEFAULT_CLICKBENCH_HITS_URL =
  "https://datasets.clickhouse.com/hits_compatible/hits.parquet";

/** Same shape as hosted Q15 (limit after aggregate; spill still runs on full groups). */
export const Q15_SQL =
  "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10";
export const DEFAULT_TARGET_GROUP_BYTES = 16 * 1024 * 1024;

export const Q15_TOP_USER = "1313338681122956954";
export const Q15_TOP_COUNT = "29097";
/** Cloudflare reference reducer task id (slot often 32 on hosted graphs). */
export const CF_REF_REDUCER_TASK_ID = 32;
export const CF_REF_REDUCER_SLOT = 32;
export const CF_PLANNED_SCANNERS = 32;
export const CF_ACTIVE_SCANS = 2;
export const CF_DISPATCH_CONCURRENCY = 2;

const WARMUP = 5;

export type FanInTopology = {
  id: "A" | "FULL";
  label: string;
  maxNodes: number;
  hashReducers: number;
  activeScanners: number;
  dispatchConcurrency: number;
  /** Scan start window (`maxActiveIngress`); defaults to planned scan count. */
  maxActiveIngress?: number;
};

export const FANIN_TOPOLOGIES: FanInTopology[] = [
  {
    id: "A",
    label: "1 planned scan, hashReducers=2 (baseline u64 path)",
    maxNodes: 1,
    hashReducers: 2,
    activeScanners: 1,
    dispatchConcurrency: 1,
  },
  {
    id: "FULL",
    label: "32 planned scans → 16 reducers, dispatch=2 (CF-shaped single run)",
    maxNodes: CF_PLANNED_SCANNERS,
    hashReducers: 16,
    activeScanners: CF_ACTIVE_SCANS,
    dispatchConcurrency: CF_DISPATCH_CONCURRENCY,
    maxActiveIngress: CF_PLANNED_SCANNERS,
  },
];

export type ScanIngressSummary = {
  scanTasks: number;
  maxEgressPeakPendingBytes: number;
  /** Largest Wasm linear memory seen on any scan progress sample (per-scan when isolated). */
  maxScanWasmBytes: number;
  /** Max in-run Wasm peak minus Wasm size at scan start (egress reclamation check). */
  maxScanWasmPeakOverStartBytes: number;
  totalOutputBytes: number;
  maxOutstandingReads: number;
  /** Distinct node Wasm instances used by scanners in this job. */
  distinctScanWasmInstances: number;
};

export type LocalMetricValidation = {
  nodeWasmScope: "shared_singleton" | "per_task_isolate";
  spillBoundariesScope: "per_wasm_module_global";
  reducerWasmInstanceIds: Record<string, number>;
  distinctReducerWasmInstances: number;
  /** True when every reducer summary fingerprint matches but WCS inputs differ. */
  suspiciousIdenticalReducerSummaries: boolean;
  reducerBoundaryFingerprints: Record<string, string>;
};

export type ReducerSpillSummary = {
  taskId: number;
  slot: number;
  partition: number;
  wasmInstanceId: number;
  spillRuns: number;
  liveAfterRun1: number;
  liveAfterLast: number;
  liveSqlAckedLast: number;
  cumulativeWcsInputBytes: number;
  cumulativeSqliteAppendBytes: number;
  ingressRetainedPeakBytes: number;
  perRunLiveGrowth: number;
  perRunLiveGrowthPostWarm: number;
  maxCrossRunGap: number;
  liveAfterTailPeakMinusWarm: number;
  avgSqliteAppendBytesPerRun: number;
  mergeScanBeginsMax: number;
  mergeStepsMax: number;
  rustInflightLast: number;
  /** liveAfter growth per spill run vs cumulative WCS input (detect ingress correlation). */
  liveGrowthPerMiBInput: number;
  /** liveAfter growth per spill run vs cumulative SQLite append bytes. */
  liveGrowthPerMiBSpill: number;
};

export type FanInRunResult = {
  topology: FanInTopology;
  hitsLocation: string;
  fileSize: number;
  plannedScans: number;
  plannedReducers: number;
  scheduling: LocalHashScheduling & { peakSimultaneousScans: number };
  scanIngress: ScanIngressSummary;
  reducers: ReducerSpillSummary[];
  /** CF reference slot (default 32), when present in the plan. */
  refReducer: ReducerSpillSummary | null;
  worstReducer: ReducerSpillSummary;
  topRows: string[][];
  ledgerPeakAdapterBytes: number;
  metricValidation: LocalMetricValidation;
};

export type ParquetByteSource = {
  label: string;
  fileSize: number;
  readAt: ReadAt;
  isRemote: boolean;
  isFullClickBench: boolean;
};

export function isHttpHitsLocation(location: string): boolean {
  return /^https?:\/\//i.test(location.trim());
}

/** Resolve local path, `HITS_PARQUET`, `HITS_URL`, or ClickBench HTTP (not R2 — use Miniflare for bucket reads). */
export function resolveHitsLocation(options: { forFullGraph: boolean; repoRoot: string }): string {
  const fromEnv = process.env.HITS_PARQUET ?? process.env.HITS_URL;
  if (fromEnv) return fromEnv.trim();
  if (options.forFullGraph) {
    const localFull = path.join(options.repoRoot, "data/hits.parquet");
    if (fs.existsSync(localFull)) return localFull;
    return DEFAULT_CLICKBENCH_HITS_URL;
  }
  return path.join(options.repoRoot, "data/hits_subset_2m.parquet");
}

export async function openParquetByteSource(location: string): Promise<ParquetByteSource> {
  const label = location.trim();
  if (isHttpHitsLocation(label)) {
    const source = new HttpRangeSource(label, { rangeOnly: true });
    const fileSize = await source.contentLength();
    const isFullClickBench =
      label.includes("hits_compatible/hits.parquet") ||
      /\/hits\.parquet(?:\?|$)/.test(label);
    return {
      label,
      fileSize,
      readAt: (offset, length) => source.read(offset, length),
      isRemote: true,
      isFullClickBench,
    };
  }
  if (!fs.existsSync(label)) {
    throw new Error(`missing hits parquet: ${label} (set HITS_PARQUET or HITS_URL to http(s)://… or R2-backed URL)`);
  }
  const fileSize = fs.statSync(label).size;
  const readAt: ReadAt = async (offset, length) => {
    const fh = await fs.promises.open(label, "r");
    try {
      const out = new Uint8Array(length);
      await fh.read(out, 0, length, offset);
      return out;
    } finally {
      await fh.close();
    }
  };
  const base = path.basename(label).toLowerCase();
  const isFullClickBench = base === "hits.parquet";
  return { label, fileSize, readAt, isRemote: false, isFullClickBench };
}

function reducerMeta(graph: Extract<ExecutionPlan, { kind: "graph" }>): Map<
  number,
  { slot: number; partition: number }
> {
  const meta = new Map<number, { slot: number; partition: number }>();
  for (const edge of graph.edges) {
    if (edge.partition === undefined) continue;
    const task = graph.tasks.find((item) => item.id === edge.to);
    if (!task) continue;
    meta.set(edge.to, { slot: task.slot, partition: edge.partition });
  }
  return meta;
}

function summarizeReducer(
  taskId: number,
  slot: number,
  partition: number,
  wasmInstanceId: number,
  rows: SpillBoundary[],
  cumulativeWcsInputBytes: number,
  ingressRetainedPeakBytes: number,
): ReducerSpillSummary {
  const first = rows[0]!;
  const last = rows[rows.length - 1]!;
  const warmIndex = Math.min(WARMUP, rows.length - 1);
  const warm = rows[warmIndex]!;
  const tail = rows.slice(warmIndex);
  const gaps: number[] = [];
  for (let i = 0; i + 1 < rows.length; i += 1) {
    gaps.push(rows[i + 1]!.liveBefore - rows[i]!.liveAfter);
  }
  const appendSamples = rows.map((row) => row.sqliteWriteBytes).filter((n) => n > 0);
  const cumulativeSqliteAppendBytes = appendSamples.reduce((sum, n) => sum + n, 0);
  const avgAppend =
    appendSamples.length > 0 ? cumulativeSqliteAppendBytes / appendSamples.length : 0;
  const runSpan = Math.max(1, last.run - first.run);
  const postWarmSpan = Math.max(1, last.run - warm.run);
  const perRunLiveGrowth = (last.liveAfter - first.liveAfter) / runSpan;
  const perRunLiveGrowthPostWarm = (last.liveAfter - warm.liveAfter) / postWarmSpan;
  const mibInput = cumulativeWcsInputBytes / (1024 * 1024);
  const mibSpill = cumulativeSqliteAppendBytes / (1024 * 1024);
  return {
    taskId,
    slot,
    partition,
    wasmInstanceId,
    spillRuns: rows.length,
    liveAfterRun1: first.liveAfter,
    liveAfterLast: last.liveAfter,
    liveSqlAckedLast: last.liveSqlAcked,
    cumulativeWcsInputBytes,
    cumulativeSqliteAppendBytes,
    ingressRetainedPeakBytes,
    perRunLiveGrowth,
    perRunLiveGrowthPostWarm,
    maxCrossRunGap: gaps.length ? Math.max(...gaps) : 0,
    liveAfterTailPeakMinusWarm: Math.max(...tail.map((row) => row.liveAfter)) - warm.liveAfter,
    avgSqliteAppendBytesPerRun: avgAppend,
    mergeScanBeginsMax: Math.max(...rows.map((row) => row.mergeScanBegins)),
    mergeStepsMax: Math.max(...rows.map((row) => row.mergeSteps)),
    rustInflightLast: last.rustInflightSpillBytes,
    liveGrowthPerMiBInput: mibInput > 0 ? perRunLiveGrowthPostWarm / mibInput : 0,
    liveGrowthPerMiBSpill: mibSpill > 0 ? perRunLiveGrowthPostWarm / mibSpill : 0,
  };
}

function countHashTopology(graph: Extract<ExecutionPlan, { kind: "graph" }>): {
  plannedScans: number;
  plannedReducers: number;
} {
  const partitioned = graph.edges.filter((edge) => edge.partition !== undefined);
  return {
    plannedScans: [...new Set(partitioned.map((edge) => edge.from))].length,
    plannedReducers: [...new Set(partitioned.map((edge) => edge.to))].length,
  };
}

function schedulingFromEnv(topology: FanInTopology): LocalHashScheduling {
  const activeScanners = Number(process.env.SPILL_FANIN_ACTIVE_SCANS ?? topology.activeScanners);
  const dispatchConcurrency = Number(
    process.env.SPILL_FANIN_DISPATCH ?? topology.dispatchConcurrency,
  );
  const maxActiveIngress = Number(
    process.env.SPILL_FANIN_INGRESS_WINDOW ??
      topology.maxActiveIngress ??
      topology.maxNodes,
  );
  return {
    openingConcurrency: dispatchConcurrency,
    maxActiveScans: activeScanners,
    maxActiveIngress,
  };
}

export async function runFanInSpillCase(options: {
  topology: FanInTopology;
  hitsLocation: string;
  targetGroupBytes?: number;
  batchRows?: number;
  plannedScanners?: number;
  refReducerSlot?: number;
  refReducerTaskId?: number;
}): Promise<FanInRunResult> {
  const byteSource = await openParquetByteSource(options.hitsLocation);
  const { readAt, fileSize, label: hitsLocation } = byteSource;

  const node = await loadNodeWasm();
  const coordinator = await loadCoordinatorWasm();
  const footer = await readParquetFooter(readAt, fileSize);
  const planSourceKey = isHttpHitsLocation(hitsLocation)
    ? hitsLocation
    : "http://example.invalid/hits.parquet";
  const maxNodes =
    options.plannedScanners ??
    (process.env.SPILL_FANIN_SCANNERS ? Number(process.env.SPILL_FANIN_SCANNERS) : undefined) ??
    options.topology.maxNodes;
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { hits: { key: planSourceKey } },
      sql: Q15_SQL,
      output: { format: "json" },
      hashReducers: options.topology.hashReducers,
      groupSpill: true,
      targetGroupBytes: options.targetGroupBytes ?? DEFAULT_TARGET_GROUP_BYTES,
    }),
    footers: [{ fileSize, footer }],
    maxNodes,
    batchRows: options.batchRows ?? 8192,
    minBytesPerNode: 1,
    hashReducers: options.topology.hashReducers,
  });
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error(`${options.topology.id}: job did not plan a hash graph`);
  }
  const graph = planned.graph as Extract<ExecutionPlan, { kind: "graph" }>;
  const { plannedScans, plannedReducers } = countHashTopology(graph);
  const taskMeta = reducerMeta(graph);
  const refTaskId = options.refReducerTaskId ?? CF_REF_REDUCER_TASK_ID;
  const refSlot = options.refReducerSlot ?? CF_REF_REDUCER_SLOT;

  const boundariesByTask = new Map<number, SpillBoundary[]>();
  const reducerWasmIds = new Map<number, number>();
  const scanStats = new Map<
    number,
    {
      egressPeak: number;
      wasmMax: number;
      wasmPeakOverStart: number;
      outputBytes: number;
      outstandingReads: number;
      wasmInstanceIds: Set<number>;
    }
  >();
  const scanRunPeaks = new Map<string, { start: number; peak: number; instanceId: number }>();
  const scheduling = schedulingFromEnv(options.topology);
  const defaultHostBudget =
    options.topology.id === "FULL" ? 512 * 1024 * 1024 : 32 * 1024 * 1024;
  const maxHostBytes = Number(process.env.SPILL_FANIN_HOST_BYTES ?? defaultHostBudget);
  const isolatedNodeWasm = process.env.SPILL_FANIN_SHARED_WASM === "1" ? false : true;

  const run = await runLocalHashGraph(
    { node, coordinator },
    graph,
    readAt,
    {
      scheduling,
      maxHostBytes,
      isolatedNodeWasm,
      onScanProgress: (info) => {
        const key = `${info.taskId}:${info.scanIndex}`;
        const prev = scanRunPeaks.get(key);
        const start = prev?.start ?? info.wasmMemoryBytes;
        const peak = Math.max(prev?.peak ?? info.wasmMemoryBytes, info.wasmMemoryBytes);
        scanRunPeaks.set(key, { start, peak, instanceId: info.wasmInstanceId });

        const cur = scanStats.get(info.taskId) ?? {
          egressPeak: 0,
          wasmMax: 0,
          wasmPeakOverStart: 0,
          outputBytes: 0,
          outstandingReads: 0,
          wasmInstanceIds: new Set<number>(),
        };
        cur.egressPeak = Math.max(cur.egressPeak, info.egressPeakPendingBytes ?? 0);
        cur.wasmMax = Math.max(cur.wasmMax, info.wasmMemoryBytes);
        cur.wasmPeakOverStart = Math.max(cur.wasmPeakOverStart, peak - start);
        cur.outputBytes = info.outputBytes;
        cur.outstandingReads = Math.max(cur.outstandingReads, info.outstandingReads ?? 0);
        cur.wasmInstanceIds.add(info.wasmInstanceId);
        scanStats.set(info.taskId, cur);
      },
      onReducerProgress: (info) => {
        reducerWasmIds.set(info.taskId, info.wasmInstanceId);
        if ((info.spillBoundaries?.length ?? 0) >= (boundariesByTask.get(info.taskId)?.length ?? 0)) {
          boundariesByTask.set(info.taskId, info.spillBoundaries ?? []);
        }
      },
    },
  );

  const parsed = JSON.parse(resultJson(coordinator, planned.graph, { rows: run.bytes }, planned.rowCap)) as {
    rows?: unknown[][];
  };
  const topRows = (parsed.rows ?? []).map((row) =>
    row.map((value) => (value === null ? "null" : String(value))),
  );

  const reducers = [...boundariesByTask.entries()]
    .filter(([, rows]) => rows.length > 0)
    .map(([taskId, rows]) => {
      const meta = taskMeta.get(taskId) ?? { slot: -1, partition: -1 };
      return summarizeReducer(
        taskId,
        meta.slot,
        meta.partition,
        reducerWasmIds.get(taskId) ?? 0,
        rows,
        run.wcsInputBytesByReducer.get(taskId) ?? 0,
        run.ingressRetainedPeakByReducer.get(taskId) ?? 0,
      );
    })
    .sort((left, right) => right.spillRuns - left.spillRuns);

  if (reducers.length === 0) {
    throw new Error(`${options.topology.id}: no reducer spill boundaries recorded`);
  }

  const refReducer =
    reducers.find((row) => row.taskId === refTaskId) ??
    reducers.find((row) => row.slot === refSlot) ??
    null;
  const worstReducer = reducers.reduce((best, row) =>
    row.perRunLiveGrowthPostWarm > best.perRunLiveGrowthPostWarm ? row : best,
  );

  let maxEgressPeak = 0;
  let maxScanWasm = 0;
  let maxScanWasmPeakOverStart = 0;
  let totalOutput = 0;
  let maxOutstanding = 0;
  const scanWasmInstances = new Set<number>();
  for (const stats of scanStats.values()) {
    maxEgressPeak = Math.max(maxEgressPeak, stats.egressPeak);
    maxScanWasm = Math.max(maxScanWasm, stats.wasmMax);
    maxScanWasmPeakOverStart = Math.max(maxScanWasmPeakOverStart, stats.wasmPeakOverStart);
    totalOutput += stats.outputBytes;
    maxOutstanding = Math.max(maxOutstanding, stats.outstandingReads);
    for (const id of stats.wasmInstanceIds) scanWasmInstances.add(id);
  }

  const reducerBoundaryFingerprints: Record<string, string> = {};
  for (const reducer of reducers) {
    reducerBoundaryFingerprints[String(reducer.taskId)] =
      `${reducer.liveAfterRun1}:${reducer.liveAfterLast}:${reducer.spillRuns}:${reducer.cumulativeWcsInputBytes}`;
  }
  const fingerprints = new Set(Object.values(reducerBoundaryFingerprints));
  const wcsInputs = new Set(reducers.map((row) => row.cumulativeWcsInputBytes));
  const suspiciousIdenticalReducerSummaries =
    reducers.length > 1 && fingerprints.size === 1 && wcsInputs.size > 1;

  const metricValidation: LocalMetricValidation = {
    nodeWasmScope: run.nodeWasmScope,
    spillBoundariesScope: "per_wasm_module_global",
    reducerWasmInstanceIds: Object.fromEntries(
      [...reducerWasmIds.entries()].map(([taskId, id]) => [String(taskId), id]),
    ),
    distinctReducerWasmInstances: new Set(reducerWasmIds.values()).size,
    suspiciousIdenticalReducerSummaries,
    reducerBoundaryFingerprints,
  };

  return {
    topology: options.topology,
    hitsLocation,
    fileSize,
    plannedScans,
    plannedReducers,
    scheduling: {
      ...scheduling,
      peakSimultaneousScans: run.peakSimultaneousScans,
    },
    scanIngress: {
      scanTasks: scanStats.size,
      maxEgressPeakPendingBytes: maxEgressPeak,
      maxScanWasmBytes: maxScanWasm,
      maxScanWasmPeakOverStartBytes: maxScanWasmPeakOverStart,
      totalOutputBytes: totalOutput,
      maxOutstandingReads: maxOutstanding,
      distinctScanWasmInstances: scanWasmInstances.size,
    },
    reducers,
    refReducer,
    worstReducer,
    topRows,
    ledgerPeakAdapterBytes: run.ledgerPeakAdapterBytes,
    metricValidation,
  };
}

export function fanInNeedsFullHits(topology: FanInTopology, location: string): boolean {
  if (topology.id === "A") return false;
  if (isHttpHitsLocation(location)) return false;
  const name = path.basename(location.split("?")[0]!).toLowerCase();
  if (name === "hits.parquet") return false;
  return name.includes("subset") || name.includes("_2m.") || name.includes("500k");
}

export function assertQ15TopRow(topRows: string[][]): void {
  const first = topRows[0];
  if (!first || first.length < 2) {
    throw new Error(`Q15 result missing top row: ${JSON.stringify(topRows.slice(0, 3))}`);
  }
  if (first[0] !== Q15_TOP_USER || first[1] !== Q15_TOP_COUNT) {
    throw new Error(
      `Q15 top row mismatch: got ${JSON.stringify(first)}, ` +
        `expected [${Q15_TOP_USER}, ${Q15_TOP_COUNT}]`,
    );
  }
}
