/**
 * Direct-RPC hash job. Rust opens a stream per partition that has rows.
 * This module only starts reducers and scans, reports scanners that produced
 * nothing, then runs the finalizer over the reducer bodies.
 */

import type { GraphPlan } from "./job-replay.ts";
import { concatBytes } from "../host/bytes.ts";
import { FINALIZER_INPUT_BYTES, finalizerInputGuard, isFatalTaskError } from "./task-errors.ts";
import { hashShardedParquetExport, localExportProgram, rangeSortParquetExport } from "./parquet-shard-graph.ts";
import type { ReadAt } from "./plan-dispatch.ts";
import { runWcolEngine } from "../host/run-wcol-engine.ts";
import { createPoolInputLatch, type PoolInputLatch } from "../host/pool-streams.ts";
import { createStageTimeline, type StageTimeline } from "./stage-timeline.ts";
import { createLimiter } from "../concurrency.ts";
import type { GraphWasm } from "../host/wasm-stream.ts";
import type { SourceReadAt } from "../host/stream-pool.ts";

const DEFAULT_MAX_ACTIVE_INGRESS = 8;
const DEFAULT_MAX_ACTIVE_SCANS = 16;
const DEFAULT_OPENING_CONCURRENCY = 6;
/** How long an abort may take before the job reports its failure anyway. */
const ABORT_BOUND_MS = 5_000;

function failureText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Stall and overload are the cause. Transport EOF and RPC cancel are what the abort does afterwards. */
export function preferHashFailure(current: unknown, next: unknown): unknown {
  if (!current) return next;
  const rank = (error: unknown) => {
    const message = failureText(error);
    if (isFatalTaskError(message)) return 0;
    if (message.includes("stalled:")) return 0;
    if (message.includes("Durable Object is overloaded")) return 0;
    if (
      message.includes("transport EOF")
      || message.includes("disconnected prematurely")
      || message.includes("closed bounded pipe")
    ) {
      return 2;
    }
    return 1;
  };
  return rank(next) < rank(current) ? next : current;
}

/** Wait for `work` to settle, at most `ms`; never rejects. A stuck isolate must not hold the job. */
function settleWithin(work: Promise<unknown>, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    void work.then(
      () => undefined,
      () => undefined,
    ).finally(() => {
      clearTimeout(timer);
      resolve();
    });
  });
}

export type RunnableTask = {
  taskId: number;
  slot: number;
  program: Uint8Array;
};

export type HashScanDestination = { partition: number; slot: number; taskId: number };

export type ObjectPart = {
  objectKey: string;
  /** Encoded `partitionBy` values in the object's Hive path; empty when unpartitioned. */
  partitionValues: string[];
  bytes: number;
  /** Rows in the object, when the writer counted them. */
  rows?: number;
  parts: number;
  localBytes?: Uint8Array;
  taskId?: number;
};

export type PrepareReducerContext = {
  /** Wire reducer WCS egress into the concurrently running finalizer. */
  attachFinalizerInput(body: ReadableStream<Uint8Array>): void;
};

export type PreparedHashReducer = {
  waitConsumeReady: (scanIndex: number) => Promise<void>;
  accept: (scanIndex: number, stream: ReadableStream<Uint8Array>) => Promise<void>;
  completeScan: (scanIndex: number) => Promise<void>;
  seal: () => Promise<void>;
  abort: (error: unknown) => Promise<void>;
  invokedSlots?: number[];
  parquetPart?: ObjectPart;
};

/** Bounds RPCs being opened. A scan opens inside it and runs outside it. */
export type OpenLimit = <T>(open: () => Promise<T>) => Promise<T>;

export type HashRemote = {
  prepareReducer: (
    task: RunnableTask,
    expectedScans: number,
    finalizer: PrepareReducerContext,
    exportProgram?: Uint8Array,
  ) => Promise<PreparedHashReducer>;
  /**
   * Run one scan to its end. Its opening RPC goes through `open`, which bounds how many
   * open at once; `maxActiveScans` bounds how many run.
   */
  runScan: (
    task: RunnableTask,
    scanIndex: number,
    destinations: HashScanDestination[],
    open: OpenLimit,
  ) => Promise<{ openedPartitions: number[]; bytes: number; invokedSlots?: number[] }>;
  /** Best-effort. The caller bounds the wait; a stuck isolate must not hold the job. */
  abortTask?: (task: RunnableTask, error: unknown) => Promise<void>;
};

export type HashJobTiming = {
  reducerOpenMs: number;
  scanPhaseMs: number;
  peakSimultaneousScans: number;
  openedEdges: number;
  finalizerMs: number;
  /** Partial-state bytes the finalizer received from every reducer. */
  finalizerInputBytes: number;
};

export type HashJobResult = {
  rows?: Uint8Array;
  partial?: Uint8Array;
  parquet?: Uint8Array;
  outputs: ObjectPart[];
  invokedSlots: number[];
  timing: HashJobTiming;
};

export type HashJobOptions = {
  wasm: GraphWasm;
  plan: GraphPlan;
  /** The finalizer's reads of a single object; `readSources` reads any planned source. */
  readAt?: ReadAt;
  readSources?: SourceReadAt;
  hashRemote: HashRemote;
  /** Scan RPCs being opened at once. */
  openingConcurrency?: number;
  /** Scan start window: a scan starts only within this many of the oldest unfinished one. */
  maxActiveIngress?: number;
  /** Scans running at once. */
  maxActiveScans?: number;
  /** Resolves when the attempt has had no task progress. Independent of the scan promises. */
  stalled?: Promise<string>;
  onStall?: (reason: string) => Promise<void>;
  /** Stops the job: it aborts the attempt and rejects with the signal's reason. */
  signal?: AbortSignal;
  /** Macrotask yield for the coordinator finalizer. */
  yieldToEventLoop?: () => Promise<void>;
  /** Accepted until the Cloudflare caller stops passing it; there is no finalizer test hook. */
  simulateFinalizerError?: undefined;
  /** When set, root Parquet bytes upload to R2 instead of returning in memory. */
  uploadParquetRoot?: (bytes: Uint8Array, rootTaskId: number) => Promise<ObjectPart>;
  /** Stage spans for this attempt; the caller persists them. */
  timeline?: StageTimeline;
};

type Reducer = { id: number; partition: number; program: Uint8Array; slot: number };

/** Where reducer output goes: the coordinator finalizer, or one Parquet writer per partition. */
type OutputMode = { kind: "finalizer" } | { kind: "sharded_hash" } | { kind: "range_sort" };

/** The hash job's shape, read once from the plan. Pure. */
export function hashJobShape(plan: GraphPlan): {
  scans: RunnableTask[];
  reducers: Reducer[];
  destinations: HashScanDestination[];
  mode: OutputMode;
  rootProgram: Uint8Array;
} {
  const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
  if (partitioned.length === 0) throw new Error("runHashJob requires hash-partition edges");
  const taskOf = (id: number) => {
    const task = plan.tasks.find((item) => item.id === id);
    if (!task) throw new Error(`graph missing task ${id}`);
    return { taskId: id, slot: task.slot, program: task.program };
  };
  const scanIds = [...new Set(partitioned.map((edge) => edge.from))].sort((a, b) => a - b);
  const byPartition = new Map<number, number>();
  for (const edge of partitioned) byPartition.set(edge.partition!, edge.to);
  const reducers = [...byPartition.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([partition, id]) => {
      const task = taskOf(id);
      return { id, partition, program: task.program, slot: task.slot };
    });
  const mode: OutputMode = hashShardedParquetExport(plan, reducers.map((reducer) => reducer.id))
    ? { kind: "sharded_hash" }
    : rangeSortParquetExport(plan)
      ? { kind: "range_sort" }
      : { kind: "finalizer" };
  return {
    scans: scanIds.map(taskOf),
    reducers,
    destinations: reducers.map((reducer) => ({ partition: reducer.partition, slot: reducer.slot, taskId: reducer.id })),
    mode,
    rootProgram: taskOf(plan.rootTaskId).program,
  };
}

/** The program a reducer's partition is written with, or none when the finalizer takes it. */
function exportProgramFor(plan: GraphPlan, mode: OutputMode, reducer: Reducer): Uint8Array | undefined {
  switch (mode.kind) {
    case "sharded_hash":
      return localExportProgram(plan, reducer.id);
    case "range_sort":
      return reducer.program;
    case "finalizer":
      return undefined;
  }
}

/** The best failure seen so far (see `preferHashFailure`), and a promise that wakes on the first. */
function failureLatch(): { note(error: unknown): void; current(): unknown; failed: Promise<void> } {
  let failure: unknown = null;
  let wake!: () => void;
  const failed = new Promise<void>((resolve) => {
    wake = resolve;
  });
  return {
    note(error) {
      failure = preferHashFailure(failure, error ?? new Error("hash job failed"));
      wake();
    },
    current: () => failure,
    failed,
  };
}

/** The coordinator finalizer over every reducer's body, running while the scans do. */
function startFinalizer(
  options: HashJobOptions,
  program: Uint8Array,
  latch: PoolInputLatch,
  inputs: number,
): { work: Promise<Uint8Array>; startedAt: number } {
  const parts: Uint8Array[] = [];
  const { readAt, readSources } = options;
  const work = runWcolEngine({
    wasm: options.wasm.coordinator,
    plan: program,
    scratchBytes: 1024 * 1024,
    layout: { inputs, outputs: 1 },
    inputLatch: latch,
    host: {
      readAt: readSources ?? (readAt ? (_source, offset, length) => readAt(offset, length) : undefined),
      writePort: (_port, chunk) => {
        parts.push(chunk);
      },
      yield: options.yieldToEventLoop ?? (() => new Promise((resolve) => setTimeout(resolve, 0))),
    },
  }).then(() => concatBytes(parts));
  return { work, startedAt: performance.now() };
}

/** Shape the client result from the root's bytes or the partition writers' objects. Pure but for the upload. */
async function hashJobResult(
  options: HashJobOptions,
  bytes: Uint8Array,
  shardOutputs: ObjectPart[],
  invokedSlots: number[],
  timing: HashJobTiming,
): Promise<HashJobResult> {
  const { plan } = options;
  const base = { outputs: [], invokedSlots, timing };
  switch (plan.rootOutput) {
    case "parquet":
      if (shardOutputs.length > 0) return { ...base, outputs: shardOutputs };
      if (options.uploadParquetRoot) return { ...base, outputs: [await options.uploadParquetRoot(bytes, plan.rootTaskId)] };
      return { ...base, parquet: bytes };
    case "partial":
      return { ...base, partial: bytes };
    case "rows":
      return { ...base, rows: bytes };
  }
}

/**
 * Run a hash-partitioned graph. Scanners and reducers are `hashRemote`; the root task runs
 * here on `wasm.coordinator`. Any failure (a scan, a reducer, the finalizer, a stall or
 * `signal`) aborts the whole attempt, bounded, before the best failure is thrown.
 */
export async function runHashJob(options: HashJobOptions): Promise<HashJobResult> {
  const { plan, hashRemote } = options;
  const timeline = options.timeline ?? createStageTimeline();
  const { scans, reducers, destinations, mode, rootProgram } = hashJobShape(plan);
  const openLimit = createLimiter(options.openingConcurrency ?? DEFAULT_OPENING_CONCURRENCY);
  const scanWindow = Math.max(1, options.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS);
  const maxActiveScans = Math.max(1, options.maxActiveScans ?? DEFAULT_MAX_ACTIVE_SCANS);
  const failures = failureLatch();
  const invoked = new Set<number>();
  const finalizerLatch = createPoolInputLatch();
  const guardFinalizerInput = finalizerInputGuard(FINALIZER_INPUT_BYTES);
  /** Reducers prepared so far, by task id: the ones an abort reaches through their handle. */
  const preparedHandles = new Map<number, PreparedHashReducer>();
  const activeScans = new Set<Promise<void>>();

  const onAbort = () => failures.note(options.signal?.reason ?? new Error("hash job aborted"));
  if (options.signal?.aborted) onAbort();
  options.signal?.addEventListener("abort", onAbort, { once: true });

  /** `work`, unless the attempt fails or stalls first: then the attempt's failure. */
  const gate = async <T>(work: Promise<T>): Promise<T> => {
    const winner = await Promise.race([
      work.then(
        (value) => ({ kind: "value" as const, value }),
        (error: unknown) => ({ kind: "error" as const, error }),
      ),
      failures.failed.then(() => ({ kind: "failed" as const })),
      ...(options.stalled ? [options.stalled.then((reason) => ({ kind: "stall" as const, reason }))] : []),
    ]);
    switch (winner.kind) {
      case "value":
        return winner.value;
      case "error":
        throw winner.error;
      case "failed":
        throw failures.current();
      case "stall":
        await options.onStall?.(winner.reason);
        throw new Error(winner.reason);
    }
  };

  const finalizer = mode.kind === "finalizer" ? startFinalizer(options, rootProgram, finalizerLatch, reducers.length) : undefined;
  void finalizer?.work.catch((error: unknown) => failures.note(error));

  // Every reducer at once (the plan bounds the count, at most 64), right after the first
  // wave of scans is sent: scans are the critical path, and a reducer holds an early edge
  // until its session is ready. Sent first, 64 prepares delayed the scans' arrival.
  let reducerOpenMs = 0;
  let preparing: Promise<PreparedHashReducer[]> | undefined;
  const prepareReducers = (): Promise<PreparedHashReducer[]> => {
    if (preparing) return preparing;
    const openStarted = performance.now();
    preparing = Promise.all(reducers.map(async (reducer, index) => {
      const exportProgram = exportProgramFor(plan, mode, reducer);
      const handle = await timeline.span("prepareReducer", { taskId: reducer.id }, () =>
        hashRemote.prepareReducer(
          { taskId: reducer.id, slot: reducer.slot, program: reducer.program },
          scans.length,
          {
            attachFinalizerInput: exportProgram
              ? () => undefined
              : (body) => finalizerLatch.attachInput(index, guardFinalizerInput.wrap(body, `reducer ${reducer.id} (partition ${reducer.partition})`)),
          },
          exportProgram,
        ),
      );
      for (const slot of handle.invokedSlots ?? []) invoked.add(slot);
      // Abort reaches every reducer that exists, also before all are ready.
      preparedHandles.set(reducer.id, handle);
      // Came up after the attempt failed: abort it here, before the failure is reported.
      const failure = failures.current();
      if (failure) await settleWithin(handle.abort(failure), ABORT_BOUND_MS);
      return handle;
    }));
    preparing.then(
      () => {
        reducerOpenMs = performance.now() - openStarted;
      },
      (error: unknown) => failures.note(error),
    );
    return preparing;
  };

  /** Reducers through their handles; scans, and reducers with no handle yet, through `abortTask`. */
  const abortAttempt = async (error: unknown): Promise<void> => {
    finalizerLatch.abort(error);
    const abortTask = hashRemote.abortTask;
    const unprepared = reducers
      .filter((reducer) => !preparedHandles.has(reducer.id))
      .map((reducer) => ({ taskId: reducer.id, slot: reducer.slot, program: reducer.program }));
    await Promise.all([
      ...[...preparedHandles.values()].map((handle) => settleWithin(handle.abort(error), ABORT_BOUND_MS)),
      ...(abortTask ? [...scans, ...unprepared].map((task) => settleWithin(abortTask(task, error), ABORT_BOUND_MS)) : []),
    ]);
    // Reducers still preparing abort themselves as they come up; scans on a stuck isolate
    // may never settle. Wait for both, bounded, so the original failure still surfaces.
    if (preparing) await settleWithin(preparing, ABORT_BOUND_MS);
    await settleWithin(Promise.allSettled(activeScans), ABORT_BOUND_MS);
    if (finalizer) await settleWithin(finalizer.work, ABORT_BOUND_MS);
  };

  /** One scan: run it, then complete the reducers it sent nothing to. Failures are noted, not thrown. */
  const runScan = async (scanIndex: number): Promise<{ openedEdges: number }> => {
    const scan = scans[scanIndex];
    const ids = { taskId: scan.taskId, scanIndex };
    const queuedAt = timeline.now();
    // Time between entering the scan window and getting an opening permit.
    const open: OpenLimit = (work) => openLimit(() => {
      timeline.record({ stage: "scanWait", ...ids, startMs: queuedAt, endMs: timeline.now(), outcome: "ok" });
      return work();
    });
    try {
      // prepareReducer already marked every slot consume-ready for this attempt.
      // Another waitConsumeReady per scan is an RPC into a reducer that may be spilling.
      const pushed = await timeline.span("scan", ids, () => hashRemote.runScan(scan, scanIndex, destinations, open));
      for (const slot of pushed.invokedSlots ?? []) invoked.add(slot);
      const opened = new Set(pushed.openedPartitions);
      const empty = reducers.flatMap((reducer, index) => (opened.has(reducer.partition) ? [] : [index]));
      if (empty.length > 0) {
        const handles = await prepareReducers();
        await timeline.span("completeScan", ids, () =>
          Promise.all(empty.map((index) => handles[index].completeScan(scanIndex))),
        );
      }
      return { openedEdges: pushed.openedPartitions.length };
    } catch (error) {
      if (failureText(error).includes("stalled:")) await options.onStall?.(failureText(error));
      failures.note(error);
      return { openedEdges: 0 };
    }
  };

  /**
   * Scans in order: each starts within `scanWindow` of the oldest unfinished one, at most
   * `maxActiveScans` at once. Returns when all finished; a failure throws through `gate`.
   */
  const scheduleScans = async (): Promise<{ peakSimultaneous: number; openedEdges: number }> => {
    const closed = new Array<boolean>(scans.length).fill(false);
    let oldest = 0;
    let next = 0;
    let peakSimultaneous = 0;
    let openedEdges = 0;
    while (oldest < scans.length && !failures.current()) {
      while (next < scans.length && next < oldest + scanWindow && activeScans.size < maxActiveScans) {
        const scanIndex = next;
        next += 1;
        const task = runScan(scanIndex).then((result) => {
          openedEdges += result.openedEdges;
          closed[scanIndex] = true;
          while (oldest < scans.length && closed[oldest]) oldest += 1;
          activeScans.delete(task);
        });
        activeScans.add(task);
        peakSimultaneous = Math.max(peakSimultaneous, activeScans.size);
      }
      void prepareReducers();
      if (activeScans.size === 0) break;
      await gate(Promise.race(activeScans));
    }
    const failure = failures.current();
    if (failure) throw failure;
    return { peakSimultaneous, openedEdges };
  };

  try {
    const scanStartedAt = performance.now();
    const scheduled = await scheduleScans();
    const prepared = await gate(prepareReducers());
    await gate(timeline.span("seal", {}, () => Promise.all(prepared.map((handle) => handle.seal()))));
    let bytes: Uint8Array = new Uint8Array();
    if (finalizer) {
      for (let index = 0; index < reducers.length; index++) finalizerLatch.sealInput(index);
      // The finalizer ran alongside the scans; this is the drain after the last seal.
      bytes = await gate(timeline.span("finalize", { taskId: plan.rootTaskId }, () => finalizer.work));
    }
    const timing: HashJobTiming = {
      reducerOpenMs,
      scanPhaseMs: performance.now() - scanStartedAt,
      peakSimultaneousScans: scheduled.peakSimultaneous,
      openedEdges: scheduled.openedEdges,
      finalizerMs: finalizer ? performance.now() - finalizer.startedAt : 0,
      finalizerInputBytes: guardFinalizerInput.bytes(),
    };
    const shardOutputs = prepared.flatMap((handle) => (handle.parquetPart ? [handle.parquetPart] : []));
    return await hashJobResult(options, bytes, shardOutputs, [...invoked], timing);
  } catch (error) {
    failures.note(error);
    const failure = failures.current();
    await abortAttempt(failure);
    throw failure;
  } finally {
    options.signal?.removeEventListener("abort", onAbort);
  }
}
