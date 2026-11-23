/**
 * Direct-RPC hash job. Rust opens a stream per partition that has rows.
 * This module only starts reducers and scans, reports scanners that produced
 * nothing, then runs the finalizer over the reducer bodies.
 */

import type { GraphPlan } from "./job-replay.ts";
import { concatBytes } from "../host/bytes.ts";
import { FINALIZER_INPUT_BYTES, finalizerInputGuard, isFatalTaskError } from "./task-errors.ts";
import {
  hashShardedParquetExport,
  localExportProgram,
  rangeSortParquetExport,
} from "./parquet-shard-graph.ts";
import type { ReadAt } from "./plan-dispatch.ts";
import { runWcolEngine } from "../host/run-wcol-engine.ts";
import { createPoolInputLatch } from "../host/pool-streams.ts";
import { createStageTimeline, type StageTimeline } from "./stage-timeline.ts";
import { createLimiter } from "../concurrency.ts";
import {
  asGraphWasm,
  type GraphWasm,
  type StreamRunStats,
  type WcolStreamWasm,
} from "../host/wasm-stream.ts";

const DEFAULT_MAX_ACTIVE_INGRESS = 8;
const DEFAULT_MAX_ACTIVE_SCANS = 16;
const DEFAULT_OPENING_CONCURRENCY = 6;

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

function withTimeout(work: Promise<unknown>, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(), ms);
    work.then(
      () => {
        clearTimeout(timer);
        resolve();
      },
      () => {
        clearTimeout(timer);
        resolve();
      },
    );
  });
}

function settleBounded(active: Set<Promise<void>>, ms: number): Promise<void> {
  if (active.size === 0) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(), ms);
    Promise.allSettled(active).then(() => {
      clearTimeout(timer);
      resolve();
    });
  });
}

export type RunnableTask = {
  taskId: number;
  slot: number;
  program: Uint8Array;
  producers: RunnableTask[];
};

export type HashScanDestination = { partition: number; slot: number; taskId: number };

export type ObjectPart = {
  objectKey: string;
  partitionValues: string[];
  bytes: number;
  parts: number;
  localBytes?: Uint8Array;
  taskId?: number;
};

export type PrepareReducerContext = {
  /** Finalizer input port index (0 .. reducer count - 1). */
  finalizerPort: number;
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

export type HashRemote = {
  prepareReducer: (
    task: RunnableTask,
    expectedScans: number,
    finalizer: PrepareReducerContext,
    exportProgram?: Uint8Array,
  ) => Promise<PreparedHashReducer>;
  runScan: (
    task: RunnableTask,
    scanIndex: number,
    destinations: HashScanDestination[],
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
  stats: StreamRunStats[];
  timing?: HashJobTiming;
};

/**
 * Run a hash-partitioned graph. Scanners and reducers are `hashRemote`.
 * The root task runs here on `wasm.coordinator`.
 */
export async function runHashJob(options: {
  wasm: WcolStreamWasm | GraphWasm;
  plan: GraphPlan;
  readAt?: ReadAt;
  readSources?: (source: number, offset: number, length: number) => Promise<Uint8Array>;
  hashRemote: HashRemote;
  openingConcurrency?: number;
  maxActiveIngress?: number;
  maxActiveScans?: number;
  maxHostBytes?: number;
  /** Resolves when the attempt has had no task progress. Independent of the scan promises. */
  stalled?: Promise<string>;
  onStall?: (reason: string) => Promise<void>;
  /** Macrotask yield for the coordinator finalizer. celld needs I/O, not setTimeout. */
  yieldToEventLoop?: () => Promise<void>;
  /** Stress: delay each finalizer egress chunk. */
  finalizerWriteDelayMs?: number;
  /** Test hook: fail finalizer on first output write. */
  simulateFinalizerError?: Error;
  /** When set, root Parquet bytes upload to R2 instead of returning in memory. */
  uploadParquetRoot?: (bytes: Uint8Array, rootTaskId: number) => Promise<ObjectPart>;
  /** Per-partition Parquet shard upload. The body is the shard byte stream. */
  uploadParquetShard?: (body: ReadableStream<Uint8Array>, taskId: number) => Promise<ObjectPart>;
  /** Stage spans for this attempt; the caller persists them. */
  timeline?: StageTimeline;
}): Promise<HashJobResult> {
  const plan = options.plan;
  const timeline = options.timeline ?? createStageTimeline();
  const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
  if (partitioned.length === 0) {
    throw new Error("runHashJob requires hash-partition edges");
  }
  const scanIds = [...new Set(partitioned.map((edge) => edge.from))].sort((a, b) => a - b);
  const byPartition = new Map<number, number>();
  for (const edge of partitioned) byPartition.set(edge.partition!, edge.to);
  const reducers = [...byPartition.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([partition, id]) => ({ id, partition }));
  const taskOf = (id: number) => {
    const task = plan.tasks.find((item) => item.id === id);
    if (!task) throw new Error(`graph missing task ${id}`);
    return task;
  };
  const runnable = (id: number): RunnableTask => {
    const task = taskOf(id);
    return { taskId: id, slot: task.slot, program: task.program, producers: [] };
  };
  const destinations = reducers.map((reducer) => ({
    partition: reducer.partition,
    slot: taskOf(reducer.id).slot,
    taskId: reducer.id,
  }));
  const invoked = new Set<number>();
  const limit = createLimiter(options.openingConcurrency ?? DEFAULT_OPENING_CONCURRENCY);
  const scanWindow = Math.max(1, options.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS);
  const maxActiveScans = Math.max(1, options.maxActiveScans ?? DEFAULT_MAX_ACTIVE_SCANS);
  const abortBoundMs = 5_000;
  const declareStall = async (reason: string): Promise<never> => {
    await options.onStall?.(reason);
    const error = new Error(reason);
    const abort = options.hashRemote.abortTask;
    if (abort) {
      const tasks = [...scanIds, ...reducers.map((reducer) => reducer.id)];
      await Promise.all(tasks.map((id) => withTimeout(abort(runnable(id), error), abortBoundMs)));
    }
    throw error;
  };
  const gate = async <T>(work: Promise<T>): Promise<T> => {
    if (!options.stalled) return work;
    const winner = await Promise.race([
      work.then(
        (value) => ({ kind: "value" as const, value }),
        (error: unknown) => ({ kind: "error" as const, error }),
      ),
      options.stalled.then((reason) => ({ kind: "stall" as const, reason })),
    ]);
    if (winner.kind === "value") return winner.value;
    if (winner.kind === "error") throw winner.error;
    return declareStall(winner.reason);
  };

  const reducerIds = reducers.map((reducer) => reducer.id);
  const shardedHash = hashShardedParquetExport(plan, reducerIds);
  const rangeSort = rangeSortParquetExport(plan);
  const partitionParquet = shardedHash || rangeSort;

  const wasm = asGraphWasm(options.wasm);
  const root = taskOf(plan.rootTaskId);
  const finalizerLatch = createPoolInputLatch();
  const guardFinalizerInput = finalizerInputGuard(FINALIZER_INPUT_BYTES);
  const finalizerParts: Uint8Array[] = [];
  const finalizerStarted = performance.now();
  const preparedHandles: PreparedHashReducer[] = [];
  let failure: unknown = null;
  let signalFailure!: () => void;
  // Wakes the scan loop on any failure. A finalizer failure is not a scan
  // promise, so racing `active` alone would wait for scans that never end.
  const failed = new Promise<void>((resolve) => {
    signalFailure = resolve;
  });
  const noteFailure = (error: unknown): void => {
    failure = preferHashFailure(failure, error);
    signalFailure();
  };
  const shardOutputs: ObjectPart[] = [];

  const abortHashAttempt = async (error: unknown): Promise<void> => {
    if (!partitionParquet) finalizerLatch.abort(error);
    await Promise.all(
      preparedHandles.map((handle) => withTimeout(handle.abort(error), abortBoundMs)),
    );
  };
  /** Reducers via their handles; remote scans via `abortTask` (an aborted reducer does not reach them). */
  const abortAttempt = async (error: unknown): Promise<void> => {
    const abortTask = options.hashRemote.abortTask;
    await Promise.all([
      abortHashAttempt(error),
      ...(abortTask ? scanIds.map((id) => withTimeout(abortTask(runnable(id), error), abortBoundMs)) : []),
    ]);
  };

  const finalizerDelayMs = options.finalizerWriteDelayMs ?? 0;
  const finalizerWork = partitionParquet
    ? Promise.resolve()
    : runWcolEngine({
    wasm: wasm.coordinator,
    plan: root.program,
    scratchBytes: 1024 * 1024,
    layout: { inputs: reducers.length, outputs: 1 },
    inputLatch: finalizerLatch,
    host: {
      readAt: options.readSources
        ? (source, offset, length) => options.readSources!(source, offset, length)
        : options.readAt
          ? (_source, offset, length) => options.readAt!(offset, length)
          : undefined,
      writePort: async (_port, chunk) => {
        if (options.simulateFinalizerError) throw options.simulateFinalizerError;
        if (finalizerDelayMs > 0) {
          await new Promise<void>((resolve) => setTimeout(resolve, finalizerDelayMs));
        }
        finalizerParts.push(chunk);
      },
      yield: options.yieldToEventLoop ?? (() => new Promise((resolve) => setTimeout(resolve, 0))),
    },
  });
  if (!partitionParquet) {
    void finalizerWork.catch((error: unknown) => {
      noteFailure(error);
    });
  }

  const exportProgramFor = (reducerId: number, reducerProgram: Uint8Array): Uint8Array | undefined => {
    if (shardedHash) return localExportProgram(plan, reducerId);
    if (rangeSort) return reducerProgram;
    return undefined;
  };

  const openStarted = performance.now();
  let reducerOpenMs = 0;
  // Every reducer at once (the plan bounds the count, at most 64), right after the first
  // wave of scans is sent: scans are the critical path, and a reducer holds an early edge
  // until its session is ready. Sent first, 64 prepares delayed the scans' arrival.
  // Scans keep the dispatch limiter.
  const prepareAll = createLimiter(Math.max(1, reducers.length));
  let startPrepares!: () => void;
  const preparesReleased = new Promise<void>((resolve) => {
    startPrepares = resolve;
  });
  const preparing = preparesReleased.then(() => Promise.all(
      reducers.map((reducer, index) =>
        prepareAll(async () => {
          const exportProgram = exportProgramFor(reducer.id, taskOf(reducer.id).program);
          const handle = await timeline.span("prepareReducer", { taskId: reducer.id }, () =>
            options.hashRemote.prepareReducer(
              runnable(reducer.id),
              scanIds.length,
              {
                finalizerPort: index,
                attachFinalizerInput: exportProgram
                  ? () => undefined
                  : (body) => finalizerLatch.attachInput(index, guardFinalizerInput.wrap(body, `reducer ${reducer.id} (partition ${reducer.partition})`)),
              },
              exportProgram,
            ),
          );
          for (const slot of handle.invokedSlots ?? []) invoked.add(slot);
          // Abort reaches every reducer that exists, also before all are ready.
          preparedHandles.push(handle);
          // Came up after the attempt failed: abort it here, before the failure is reported.
          if (failure) await withTimeout(handle.abort(failure), abortBoundMs);
          return handle;
        }),
      ),
    ));
  void preparing.then(
    () => {
      reducerOpenMs = performance.now() - openStarted;
    },
    (error: unknown) => noteFailure(error),
  );
  const closed = new Array<boolean>(scanIds.length).fill(false);
  let oldest = 0;
  let next = 0;
  let inflight = 0;
  let peakSimultaneous = 0;
  let openedEdges = 0;
  const active = new Set<Promise<void>>();
  const scanStartedAt = performance.now();

  while (oldest < scanIds.length && !failure) {
    while (
      next < scanIds.length &&
      next < oldest + scanWindow &&
      inflight < maxActiveScans
    ) {
      const scanIndex = next;
      next += 1;
      inflight += 1;
      peakSimultaneous = Math.max(peakSimultaneous, inflight);
      const scanTask = { taskId: scanIds[scanIndex]!, scanIndex };
      const queuedAt = timeline.now();
      const task = limit(async () => {
        // Time between entering the scan window and getting a dispatch permit.
        timeline.record({ stage: "scanWait", ...scanTask, startMs: queuedAt, endMs: timeline.now(), outcome: "ok" });
        try {
          // prepareReducer already marked every slot consume-ready for this attempt.
          // Another waitConsumeReady per scan is an RPC into a reducer that may be spilling.
          const pushed = await timeline.span("scan", scanTask, () =>
            options.hashRemote.runScan(runnable(scanTask.taskId), scanIndex, destinations),
          );
          for (const slot of pushed.invokedSlots ?? []) invoked.add(slot);
          openedEdges += pushed.openedPartitions.length;
          const opened = new Set(pushed.openedPartitions);
          const emptyIndexes = reducers.flatMap((reducer, index) => (opened.has(reducer.partition) ? [] : [index]));
          if (emptyIndexes.length > 0) {
            const handles = await preparing;
            await timeline.span("completeScan", scanTask, () =>
              Promise.all(emptyIndexes.map((index) => handles[index]!.completeScan(scanIndex))),
            );
          }
        } catch (error) {
          if (failureText(error).includes("stalled:")) await options.onStall?.(failureText(error));
          noteFailure(error);
        } finally {
          inflight -= 1;
          closed[scanIndex] = true;
          while (oldest < scanIds.length && closed[oldest]) oldest += 1;
        }
      });
      active.add(task);
      void task.finally(() => active.delete(task));
    }
    startPrepares();
    if (active.size === 0) break;
    await gate(Promise.race([...active, failed]));
  }
  startPrepares();
  if (failure) {
    await abortAttempt(failure);
    // Reducers still preparing abort themselves as they come up (see above); wait for
    // them, bounded, so every reducer is aborted before the failure is reported.
    await settleBounded(new Set([preparing.then(() => undefined, () => undefined)]), abortBoundMs);
    // Scans on a stuck isolate may never settle; the original failure must still surface.
    await settleBounded(active, abortBoundMs);
    await finalizerWork.catch(() => undefined);
    throw failure;
  }
  await Promise.allSettled([...active]);
  const prepared = await gate(preparing);
  await gate(timeline.span("seal", {}, () => Promise.all(prepared.map((handle) => handle.seal()))));
  if (partitionParquet) {
    for (const handle of prepared) {
      if (handle.parquetPart) shardOutputs.push(handle.parquetPart);
    }
  }
  if (!partitionParquet) {
    for (let index = 0; index < reducers.length; index++) {
      finalizerLatch.sealInput(index);
    }
    try {
      // Finalizer runs concurrently with scans; this is the drain after the last seal.
      await timeline.span("finalize", { taskId: plan.rootTaskId }, () => finalizerWork.then(() => undefined));
    } catch (error) {
      await abortHashAttempt(error);
      await finalizerWork.catch(() => undefined);
      throw error;
    }
  }
  const bytes = partitionParquet ? new Uint8Array() : concatBytes(finalizerParts);
  const timing: HashJobTiming = {
    reducerOpenMs,
    scanPhaseMs: performance.now() - scanStartedAt,
    peakSimultaneousScans: peakSimultaneous,
    openedEdges,
    finalizerMs: partitionParquet ? 0 : performance.now() - finalizerStarted,
    finalizerInputBytes: guardFinalizerInput.bytes(),
  };
  const invokedSlots = [...invoked];
  if (plan.rootOutput === "parquet") {
    if (partitionParquet && shardOutputs.length > 0) {
      return { outputs: shardOutputs, invokedSlots, stats: [], timing };
    }
    if (options.uploadParquetRoot) {
      const part = await options.uploadParquetRoot(bytes, plan.rootTaskId);
      return { outputs: [part], invokedSlots, stats: [], timing };
    }
    return { parquet: bytes, outputs: [], invokedSlots, stats: [], timing };
  }
  if (plan.rootOutput === "partial") return { partial: bytes, outputs: [], invokedSlots, stats: [], timing };
  return { rows: bytes, outputs: [], invokedSlots, stats: [], timing };
}
