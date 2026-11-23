import type { ExecutionPlan, ExecutionTask } from "../../src/runtime/coordinator/execution-graph.ts";
import { COORDINATOR_SLOT, edgesFrom, edgesInto } from "../../src/runtime/coordinator/execution-graph.ts";
import { startInProcessEdge, type DoRpcEdgeOpener } from "./graph-edge-transport.ts";
import {
  runHashExchangeSchedule,
  type HashScheduleTiming,
  type PreparedHashReducer,
  type PushPartitionedScanResult,
} from "./hash-exchange.ts";
import {
  createOpeningLimiter,
  DEFAULT_DEMUX_BYTES,
  DEFAULT_EDGE_BYTES,
  DEFAULT_INGRESS_BYTES,
  DEFAULT_MAX_ACTIVE_EGRESS,
  DEFAULT_MAX_ACTIVE_INGRESS,
  DEFAULT_MAX_ACTIVE_SCANS,
  IngressScheduler,
  type OpenedProducer,
  type OpeningLimiter,
} from "./dispatch-limits.ts";
import { PortIngress } from "../../src/runtime/host/ports/ingress.ts";
import type { SharedBytePool } from "./shared-byte-pool.ts";
import { isFullyColocated, sinkTaskIds, type GraphPlan } from "../../src/runtime/coordinator/job-replay.ts";
import type { ReadAt } from "../../src/runtime/coordinator/plan-dispatch.ts";
import { runWcolEngine } from "../../src/runtime/host/run.ts";
import {
  asGraphWasm,
  type GraphWasm,
  type StreamRunStats,
  type WcolStreamWasm,
} from "../../src/runtime/host/wasm-stream.ts";
import { lastPartialPayload } from "./wcs-transport.ts";
import type { TaskIdentity } from "./task-identity.ts";

export type GraphRunHooks = {
  onTaskStart?: (identity: TaskIdentity) => void;
  onTaskComplete?: (identity: TaskIdentity) => void;
  onTaskFailed?: (identity: TaskIdentity, error: unknown, dirty: boolean) => void;
};

export type LocalGraphRunOptions = {
  wasm: WcolStreamWasm | GraphWasm;
  plan: Extract<ExecutionPlan, { kind: "graph" }>;
  queryId: string;
  readAt: ReadAt;
  concurrentTasks?: number;
  maxAttempts?: number;
  scratchBytes?: number;
  readAtConcurrency?: number;
  /** When set, ReadAt source `i` is distinct. Otherwise every source uses `readAt`. */
  readSources?: (source: number, offset: number, length: number) => Promise<Uint8Array>;
  rpc?: DoRpcEdgeOpener;
  hooks?: GraphRunHooks;
};

async function runScanTask(
  wasm: WcolStreamWasm,
  task: ExecutionTask,
  readAt: ReadAt,
  writable: WritableStream<Uint8Array>,
  options: Pick<LocalGraphRunOptions, "scratchBytes" | "readAtConcurrency" | "readSources">,
): Promise<{ stats: StreamRunStats; dirty: boolean }> {
  let dirty = false;
  const writer = writable.getWriter();
  try {
    const stats = await runWcolEngine({
      wasm,
      plan: task.program,
      scratchBytes: options.scratchBytes ?? 256 * 1024,
      readAtConcurrency: options.readAtConcurrency,
      host: {
        readAt: (source, offset, length) =>
          options.readSources ? options.readSources(source, offset, length) : readAt(offset, length),
        writePort: async (_port, chunk) => {
          dirty = true;
          await writer.write(chunk);
        },
        yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
      },
    });
    return { stats, dirty };
  } finally {
    await writer.close();
  }
}

async function runMergeTask(
  wasm: WcolStreamWasm,
  task: ExecutionTask,
  openAt: (index: number) => Promise<OpenedProducer>,
  count: number,
  maxActive: number,
  options: Pick<LocalGraphRunOptions, "scratchBytes">,
  rootOutput: "partial" | "rows" | "parquet",
  _pool?: SharedBytePool,
): Promise<{ result: Uint8Array; stats: StreamRunStats }> {
  const parts: Uint8Array[] = [];
  const ports = count === 0 ? null : new PortIngress(count, openAt);
  try {
    const stats = await runWcolEngine({
      wasm,
      plan: task.program,
      scratchBytes: options.scratchBytes ?? 1024 * 1024,
      maxActiveIngress: maxActive,
      host: {
        readPort: ports ? (port, maxLength) => ports.read(port, maxLength) : undefined,
        abortInputs: ports ? () => ports.abort() : undefined,
        writePort: async (_port, chunk) => {
          parts.push(chunk);
        },
        yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
      },
    });
    const wire = concat(parts);
    if (rootOutput === "partial") {
      const partial = lastPartialPayload(wire);
      if (!partial) throw new Error(`task ${task.id} produced no partial`);
      return { result: partial, stats };
    }
    return { result: wire, stats };
  } catch (error) {
    await ports?.abort(error);
    throw error;
  }
}

/**
 * Run an execution graph locally. Scan tasks and the root merge task execute concurrently;
 * `in_process` edges use bounded pipes. Several producers are decoded independently; the
 * merge consumes any ready port.
 */
export async function runExecutionGraphLocal(
  options: LocalGraphRunOptions,
): Promise<{ partial?: Uint8Array; rows?: Uint8Array; parquet?: Uint8Array; stats: StreamRunStats[] }> {
  const { plan, queryId, readAt } = options;
  const wasm = asGraphWasm(options.wasm);
  const root = plan.tasks.find((t) => t.id === plan.rootTaskId);
  if (!root) throw new Error("graph missing root task");
  const workers = plan.tasks.filter(
    (t) => t.id !== plan.rootTaskId && edgesInto(t.id, plan.edges).length === 0,
  );
  const stats: StreamRunStats[] = [];
  const maxAttempts = options.maxAttempts ?? 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const identity = (taskId: number): TaskIdentity => ({ queryId, taskId, attempt });
    let dirty = false;
    try {
      const pipes = workers.map(() => startInProcessEdge(DEFAULT_EDGE_BYTES, undefined));
      const scheduler = new IngressScheduler();
      options.hooks?.onTaskStart?.(identity(root.id));
      const mergePromise = runMergeTask(
        wasm.coordinator,
        root,
        async (index) => {
          const reserved = await scheduler.reserve();
          return {
            stream: pipes[index]!.readable,
            release: () => scheduler.release(reserved),
          };
        },
        workers.length,
        scheduler.maxActive,
        options,
        plan.rootOutput,
        scheduler.pool,
      ).then((out) => {
        stats.push(out.stats);
        options.hooks?.onTaskComplete?.(identity(root.id));
        return out.result;
      });

      const slots = Math.max(1, options.concurrentTasks ?? workers.length);
      let next = 0;
      const runSlot = async () => {
        for (;;) {
          const worker = workers[next++];
          if (!worker) return;
          const edgeIndex = workers.indexOf(worker);
          options.hooks?.onTaskStart?.(identity(worker.id));
          const { stats: workerStats, dirty: workerDirty } = await runScanTask(
            wasm.node,
            worker,
            readAt,
            pipes[edgeIndex]!.writable,
            options,
          );
          dirty = dirty || workerDirty;
          stats.push(workerStats);
          options.hooks?.onTaskComplete?.(identity(worker.id));
        }
      };
      const result = await Promise.all([
        mergePromise,
        ...Array.from({ length: Math.min(slots, workers.length) }, () => runSlot()),
      ]).then(([merged]) => merged);
      if (plan.rootOutput === "parquet") {
        return { parquet: result, stats };
      }
      if (plan.rootOutput === "rows") {
        return { rows: result, stats };
      }
      return { partial: result, stats };
    } catch (error) {
      options.hooks?.onTaskFailed?.(identity(root.id), error, dirty);
      if (attempt >= maxAttempts) throw error;
    }
  }
  throw new Error("graph execution exhausted attempts");
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.byteLength;
  }
  return out;
}

/** Deferred: insert intermediate reduce tasks when fan-in exceeds planner threshold. */
export function maybeInsertReductionTree(
  plan: Extract<ExecutionPlan, { kind: "graph" }>,
  estimatedPartialBytes: number,
  thresholdBytes: number,
): Extract<ExecutionPlan, { kind: "graph" }> {
  if (estimatedPartialBytes <= thresholdBytes) return plan;
  return plan;
}

export { openEdgeWcsStream } from "./graph-edge-transport.ts";

export type ObjectPart = {
  objectKey: string;
  partitionValues: string[];
  bytes: number;
  parts: number;
  /** Set when this isolate produced the bytes and the caller still has to store them. */
  localBytes?: Uint8Array;
  taskId?: number;
};

/** One computation. `slot` is only where it runs. Producers are the incoming edges, in order. */
export type RunnableTask = {
  taskId: number;
  slot: number;
  program: Uint8Array;
  producers: RunnableTask[];
};

export type RemoteTaskResult = {
  invokedSlots: number[];
  /** WCS bytes, or a one-shot JSON receipt when this task wrote an object. */
  body: ReadableStream<Uint8Array>;
};

export type RunRemoteTask = (
  task: RunnableTask,
  objectKey: string | undefined,
  /** Set for a hash reducer. The task must not pull `producers`; these streams are its input ports. */
  ingress?: ReadableStream<Uint8Array>[],
) => Promise<RemoteTaskResult>;

export type HashScanDestination = { partition: number; slot: number; taskId: number };

export type HashRemote = {
  prepareReducer: (task: RunnableTask, expectedScans: number) => Promise<PreparedHashReducer & { invokedSlots?: number[] }>;
  runScan: (
    task: RunnableTask,
    scanIndex: number,
    destinations: HashScanDestination[],
  ) => Promise<PushPartitionedScanResult & { invokedSlots?: number[] }>;
};

export type HashJobTiming = HashScheduleTiming & { finalizerMs: number };

export type ExecutionGraphResult = {
  rows?: Uint8Array;
  partial?: Uint8Array;
  parquet?: Uint8Array;
  outputs: ObjectPart[];
  invokedSlots: number[];
  stats: StreamRunStats[];
  /** Set for a hash exchange. Coordinator clock only; node clocks are stored beside the job. */
  timing?: HashJobTiming;
};

export function runnableTree(plan: GraphPlan, taskId: number): RunnableTask {
  const task = plan.tasks.find((item) => item.id === taskId);
  if (!task) throw new Error(`graph missing task ${taskId}`);
  return {
    taskId: task.id,
    slot: task.slot,
    program: task.program,
    producers: edgesInto(task.id, plan.edges).map((edge) => runnableTree(plan, edge.from)),
  };
}

async function readStream(stream: ReadableStream<Uint8Array>, maxBytes?: number): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let total = 0;
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (maxBytes != null && total > maxBytes) {
      throw new Error(`result exceeds ${maxBytes} bytes`);
    }
    parts.push(value);
  }
  return concat(parts);
}

async function runLocalBytes(
  wasm: GraphWasm,
  task: ExecutionTask,
  openAt: (index: number) => Promise<OpenedProducer>,
  count: number,
  maxActive: number,
  options: LocalGraphRunOptions,
  output: "partial" | "rows" | "parquet",
  pool?: SharedBytePool,
): Promise<Uint8Array> {
  if (taskHasProducers(options.plan, task.id) || count > 0) {
    const merged = await runMergeTask(
      wasm.coordinator,
      task,
      openAt,
      count,
      maxActive,
      options,
      output === "partial" ? "partial" : output === "parquet" ? "parquet" : "rows",
      pool,
    );
    return merged.result;
  }
  const { readable, writable } = startInProcessEdge();
  const done = runScanTask(wasm.node, task, options.readAt, writable, options);
  const bytes = await readStream(readable);
  await done;
  return bytes;
}

function taskHasProducers(plan: GraphPlan, taskId: number): boolean {
  return edgesInto(taskId, plan.edges).length > 0;
}

/**
 * Run `plan`. Fully colocated graphs stay in this isolate. Any other placement
 * dispatches remote slots through `remote` and runs a coordinator-slot task here.
 * `onlyTaskIds` limits which sinks start; omitted sinks are already durable.
 */
export async function runExecutionGraph(options: {
  wasm?: WcolStreamWasm | GraphWasm;
  plan: GraphPlan;
  readAt?: ReadAt;
  readSources?: LocalGraphRunOptions["readSources"];
  remote: RunRemoteTask;
  hashRemote?: HashRemote;
  objectKey?: (taskId: number) => string | undefined;
  onlyTaskIds?: ReadonlySet<number>;
  openingConcurrency?: number;
  maxActiveIngress?: number;
  maxActiveScans?: number;
  maxActiveEgress?: number;
  ingressBytes?: number;
  demuxBytes?: number;
  edgeBytes?: number;
  maxResultBytes?: number;
  /** After a remote stream closes, the rest of the slots that task opened. */
  onStreamDone?: (taskId: number) => Promise<number[]>;
}): Promise<ExecutionGraphResult> {
  const plan = options.plan;
  const sinks = sinkTaskIds(plan).filter((id) => !options.onlyTaskIds || options.onlyTaskIds.has(id));
  const invoked = new Set<number>();
  const outputs: ObjectPart[] = [];
  const stats: StreamRunStats[] = [];
  const limit = createOpeningLimiter(options.openingConcurrency);
  const scheduler = new IngressScheduler({
    maxActive: options.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS,
    maxBytes: options.ingressBytes ?? DEFAULT_INGRESS_BYTES,
    ...(options.edgeBytes != null ? { edgeBytes: options.edgeBytes, legacyReservation: true } : {}),
  });

  if (sinks.length === 0) {
    return { outputs, invokedSlots: [], stats };
  }

  if (plan.edges.some((edge) => edge.partition !== undefined)) {
    return runPartitionedAggregate(options, invoked);
  }

  if (isFullyColocated(plan) && options.onlyTaskIds == null) {
    const wasm = asGraphWasm(options.wasm ?? (() => { throw new Error("colocated graph needs wasm"); })());
    if (!options.readAt && !options.readSources) throw new Error("colocated graph needs readAt");
    if (plan.edges.length === 0) {
      for (const taskId of sinks) {
        const task = plan.tasks.find((item) => item.id === taskId)!;
        const bytes = await runLocalBytes(
          wasm,
          task,
          async () => {
            throw new Error("scan has no input ports");
          },
          0,
          1,
          {
            wasm,
            plan,
            queryId: "local",
            readAt: options.readAt ?? (async () => new Uint8Array()),
            readSources: options.readSources,
          },
          plan.rootOutput,
        );
        const key = options.objectKey?.(taskId);
        if (key) {
          outputs.push({
            objectKey: key,
            partitionValues: [],
            bytes: bytes.byteLength,
            parts: 1,
            localBytes: bytes,
            taskId,
          });
        }
        else if (plan.rootOutput === "rows") return { rows: bytes, outputs, invokedSlots: [], stats };
        else if (plan.rootOutput === "parquet") return { parquet: bytes, outputs, invokedSlots: [], stats };
        else return { partial: bytes, outputs, invokedSlots: [], stats };
      }
      return { outputs, invokedSlots: [], stats };
    }
    const local = await runExecutionGraphLocal({
      wasm,
      plan,
      queryId: "local",
      readAt: options.readAt ?? (async () => new Uint8Array()),
      readSources: options.readSources,
    });
    stats.push(...local.stats);
    const rootKey = options.objectKey?.(plan.rootTaskId);
    if (rootKey && local.parquet) {
      outputs.push({
        objectKey: rootKey,
        partitionValues: [],
        bytes: local.parquet.byteLength,
        parts: 1,
        localBytes: local.parquet,
        taskId: plan.rootTaskId,
      });
    }
    return { rows: local.rows, partial: local.partial, parquet: local.parquet, outputs, invokedSlots: [], stats };
  }

  const produced = await Promise.all(
    sinks.map((taskId) =>
      runTree({
        tree: runnableTree(plan, taskId),
        plan,
        wasm: options.wasm,
        readAt: options.readAt,
        readSources: options.readSources,
        remote: options.remote,
        objectKey: options.objectKey?.(taskId),
        limit,
        scheduler,
        invoked,
        maxResultBytes: options.maxResultBytes,
        onStreamDone: options.onStreamDone,
      }),
    ),
  );
  let rows: Uint8Array | undefined;
  let partial: Uint8Array | undefined;
  let parquet: Uint8Array | undefined;
  for (const item of produced) {
    if (item.outputs) outputs.push(...item.outputs);
    if (item.rows) rows = item.rows;
    if (item.partial) partial = item.partial;
    if (item.parquet) parquet = item.parquet;
  }
  return { rows, partial, parquet, outputs, invokedSlots: [...invoked], stats };
}

/** Hash exchange: open every reducer, then each scan once, then the finalizer. */
async function runPartitionedAggregate(
  options: {
    wasm?: WcolStreamWasm | GraphWasm;
    plan: GraphPlan;
    readAt?: ReadAt;
    readSources?: LocalGraphRunOptions["readSources"];
    remote: RunRemoteTask;
    hashRemote?: HashRemote;
    openingConcurrency?: number;
    maxActiveIngress?: number;
    maxActiveScans?: number;
    maxActiveEgress?: number;
    ingressBytes?: number;
    demuxBytes?: number;
    edgeBytes?: number;
  },
  invoked: Set<number>,
): Promise<ExecutionGraphResult> {
  const plan = options.plan;
  const partitioned = plan.edges.filter((edge) => edge.partition !== undefined);
  const scanIds = [...new Set(partitioned.map((edge) => edge.from))].sort((a, b) => a - b);
  const byPartition = new Map<number, number>();
  for (const edge of partitioned) byPartition.set(edge.partition!, edge.to);
  const reducers = [...byPartition.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([partition, id]) => ({ id, partition }));
  const taskOf = (id: number): ExecutionTask => {
    const task = plan.tasks.find((item) => item.id === id);
    if (!task) throw new Error(`graph missing task ${id}`);
    return task;
  };
  const runnable = (id: number): RunnableTask => {
    const task = taskOf(id);
    return { taskId: id, slot: task.slot, program: task.program, producers: [] };
  };
  const timing: HashScheduleTiming = {
    reducerOpenMs: 0,
    reducerOpens: [],
    scans: [],
    scanPhaseMs: 0,
    peakSimultaneousScans: 0,
    peakBufferedBytes: 0,
    writeBlockedMs: 0,
  };
  const destinations = reducers.map((reducer) => ({
    partition: reducer.partition,
    slot: taskOf(reducer.id).slot,
    taskId: reducer.id,
  }));
  const hashRemote = options.hashRemote;
  const bodies = await runHashExchangeSchedule({
    scans: scanIds.map((id) => ({ id })),
    reducers,
    openingConcurrency: options.openingConcurrency,
    maxActiveIngress: options.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS,
    maxActiveScans: options.maxActiveScans ?? DEFAULT_MAX_ACTIVE_SCANS,
    maxActiveEgress: options.maxActiveEgress ?? DEFAULT_MAX_ACTIVE_EGRESS,
    maxPendingBytes: options.demuxBytes ?? options.ingressBytes ?? DEFAULT_DEMUX_BYTES,
    timing,
    ...(hashRemote
      ? {
          exchange: {
            prepareReducer: async (reducer, expectedScans) => {
              const opened = await hashRemote.prepareReducer(runnable(reducer.id), expectedScans);
              for (const slot of opened.invokedSlots ?? []) invoked.add(slot);
              return opened;
            },
            runScan: async (scan, scanIndex) => {
              const opened = await hashRemote.runScan(runnable(scan.id), scanIndex, destinations);
              for (const slot of opened.invokedSlots ?? []) invoked.add(slot);
              return opened;
            },
          },
        }
      : {
          runScan: async (id) => {
            const opened = await options.remote(runnable(id), undefined);
            for (const slot of opened.invokedSlots) invoked.add(slot);
            return opened.body;
          },
          runReducer: async (id, ingress) => {
            const opened = await options.remote(runnable(id), undefined, ingress);
            for (const slot of opened.invokedSlots) invoked.add(slot);
            return opened.body;
          },
        }),
  });
  const wasm = asGraphWasm(options.wasm ?? (() => {
    throw new Error("hash finalizer needs wasm");
  })());
  const localOptions: LocalGraphRunOptions = {
    wasm,
    plan,
    queryId: "hash",
    readAt: options.readAt ?? (async () => new Uint8Array()),
    readSources: options.readSources,
  };
  const output = plan.rootOutput === "parquet" ? "parquet" : plan.rootOutput === "partial" ? "partial" : "rows";
  const fanIn = new IngressScheduler({
    maxActive: options.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS,
    maxBytes: options.ingressBytes ?? DEFAULT_INGRESS_BYTES,
    ...(options.edgeBytes != null ? { edgeBytes: options.edgeBytes, legacyReservation: true } : {}),
  });
  const finalizerStarted = performance.now();
  const bytes = await runLocalBytes(
    wasm,
    taskOf(plan.rootTaskId),
    async (index) => {
      const reserved = await fanIn.reserve();
      return { stream: bodies[index]!, release: () => fanIn.release(reserved) };
    },
    bodies.length,
    fanIn.maxActive,
    localOptions,
    output,
    fanIn.pool,
  );
  const schedule = { ...timing, finalizerMs: performance.now() - finalizerStarted };
  const invokedSlots = [...invoked];
  if (output === "parquet") return { parquet: bytes, outputs: [], invokedSlots, stats: [], timing: schedule };
  if (output === "partial") return { partial: bytes, outputs: [], invokedSlots, stats: [], timing: schedule };
  return { rows: bytes, outputs: [], invokedSlots, stats: [], timing: schedule };
}

async function runTree(args: {
  tree: RunnableTask;
  plan: GraphPlan;
  wasm?: WcolStreamWasm | GraphWasm;
  readAt?: ReadAt;
  readSources?: LocalGraphRunOptions["readSources"];
  remote: RunRemoteTask;
  objectKey?: string;
  limit: OpeningLimiter;
  scheduler: IngressScheduler;
  invoked: Set<number>;
  maxResultBytes?: number;
  onStreamDone?: (taskId: number) => Promise<number[]>;
}): Promise<{ rows?: Uint8Array; partial?: Uint8Array; parquet?: Uint8Array; outputs?: ObjectPart[] }> {
  const { tree } = args;
  if (tree.slot !== COORDINATOR_SLOT) {
    // The permit covers the RPC until it returns a stream, not the time spent reading it.
    const opened = await args.limit(() => args.remote(tree, args.objectKey));
    for (const slot of opened.invokedSlots) args.invoked.add(slot);
    const bytes = await readStream(opened.body, args.objectKey ? undefined : args.maxResultBytes);
    if (args.objectKey) {
      const receipt = JSON.parse(new TextDecoder().decode(bytes)) as { outputs: ObjectPart[]; invokedSlots?: number[] };
      for (const slot of receipt.invokedSlots ?? []) args.invoked.add(slot);
      return { outputs: receipt.outputs };
    }
    if (args.onStreamDone) {
      for (const slot of await args.onStreamDone(tree.taskId)) args.invoked.add(slot);
    }
    if (args.plan.rootOutput === "parquet") return { parquet: bytes };
    if (args.plan.rootOutput === "partial") return { partial: bytes };
    return { rows: bytes };
  }
  const wasm = asGraphWasm(args.wasm ?? (() => { throw new Error("coordinator task needs wasm"); })());
  const localOptions: LocalGraphRunOptions = {
    wasm,
    plan: args.plan,
    queryId: "local",
    readAt: args.readAt ?? (async () => new Uint8Array()),
    readSources: args.readSources,
  };
  const task = args.plan.tasks.find((item) => item.id === tree.taskId)!;
  const bytes = await runLocalBytes(
    wasm,
    task,
    (index) => openChild(tree.producers[index]!, args),
    tree.producers.length,
    args.scheduler.maxActive,
    localOptions,
    args.plan.rootOutput,
  );
  if (args.objectKey) {
    return {
      outputs: [{
        objectKey: args.objectKey,
        partitionValues: [],
        bytes: bytes.byteLength,
        parts: 1,
        localBytes: bytes,
        taskId: tree.taskId,
      }],
    };
  }
  if (args.plan.rootOutput === "parquet") return { parquet: bytes };
  if (args.plan.rootOutput === "partial") return { partial: bytes };
  return { rows: bytes };
}

async function openChild(
  child: RunnableTask,
  args: {
    plan: GraphPlan;
    wasm?: WcolStreamWasm | GraphWasm;
    readAt?: ReadAt;
    readSources?: LocalGraphRunOptions["readSources"];
    remote: RunRemoteTask;
    limit: OpeningLimiter;
    scheduler: IngressScheduler;
    invoked: Set<number>;
    maxResultBytes?: number;
  },
): Promise<{ stream: ReadableStream<Uint8Array>; release: () => void }> {
  const pipe = await args.scheduler.reserve();
  try {
    if (child.slot === COORDINATOR_SLOT) {
      const wasm = asGraphWasm(args.wasm ?? (() => { throw new Error("coordinator task needs wasm"); })());
      const localOptions: LocalGraphRunOptions = {
        wasm,
        plan: args.plan,
        queryId: "local",
        readAt: args.readAt ?? (async () => new Uint8Array()),
        readSources: args.readSources,
      };
      const task = args.plan.tasks.find((item) => item.id === child.taskId)!;
      const { readable, writable } = startInProcessEdge(
        pipe > 0 ? pipe : DEFAULT_EDGE_BYTES,
        args.scheduler.pool,
      );
      if (child.producers.length === 0) {
        void runScanTask(wasm.node, task, localOptions.readAt, writable, localOptions).catch(async (error) => {
          await writable.abort(error).catch(() => {});
        });
      } else {
        void runMergeTask(
          wasm.coordinator,
          task,
          (index) => openChild(child.producers[index]!, args),
          child.producers.length,
          args.scheduler.maxActive,
          localOptions,
          "rows",
          args.scheduler.pool,
        ).then(
          async (out) => {
            const writer = writable.getWriter();
            await writer.write(out.result);
            await writer.close();
          },
          async (error) => {
            await writable.abort(error).catch(() => {});
          },
        );
      }
      return { stream: readable, release: () => args.scheduler.release(pipe) };
    }
    const opened = await args.limit(() => args.remote(child, undefined));
    for (const slot of opened.invokedSlots) args.invoked.add(slot);
    return { stream: opened.body, release: () => args.scheduler.release(pipe) };
  } catch (error) {
    args.scheduler.release(pipe);
    throw error;
  }
}
