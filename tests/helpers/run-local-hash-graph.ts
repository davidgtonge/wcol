/**
 * Test harness: run a hash-partitioned graph in this process (scans, reducers and the
 * finalizer) with the same edges and pipes as the direct-RPC job on Cloudflare.
 */

import { type ExecutionPlan } from "../../src/runtime/coordinator/execution-graph.ts";
import {
  runHashJob,
  type HashJobTiming,
  type HashScanDestination,
  type RunnableTask,
} from "../../src/runtime/coordinator/run-hash-job.ts";
import {
  readDictTraffic,
  readMemGrowth,
  runWcolEngine,
  type DictTraffic,
  type MemGrowthSample,
  type SpillBoundary,
} from "../../src/runtime/host/run-wcol-engine.ts";
import type { R2OutputBucket } from "../../src/runtime/host/ports/r2-bucket.ts";
import { createSharedBytePool } from "../../src/runtime/host/shared-byte-pool.ts";
import { concatBytes } from "../../src/runtime/host/bytes.ts";
import { runScanStreamPool } from "../../src/runtime/host/run-scan-stream-pool.ts";
import { createBoundedBytePipe } from "../../src/runtime/host/ports/rpc.ts";
import { createPoolInputLatch, streamFromBytes } from "../../src/runtime/host/pool-streams.ts";
import { createReducerIngressSession } from "../../src/runtime/host/scan-edge-session.ts";
import { type ReadAt } from "../../src/runtime/coordinator/plan-dispatch.ts";
import { claimingReadAt, localWork } from "../../src/runtime/coordinator/unit-claims.ts";
import { instantiateNodeWasm, wasmInstanceId } from "../../src/runtime/host/wasm.ts";
import type { GraphWasm, WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";
import { createMemoryR2SpillBucket } from "./memory-r2-spill-bucket.ts";

/** Shared host byte budget (pipes, adapters). */
const LOCAL_HOST_BUDGET = 32 * 1024 * 1024;
/** Reducer → finalizer egress pipe (finalizer consumes concurrently). */
const DEFAULT_REDUCER_EGRESS_PIPE_BYTES = 1024 * 1024;

function reducerEgressPipeBytes(hooks?: LocalHashGraphHooks): number {
  if (hooks?.reducerEgressPipeBytes != null) return hooks.reducerEgressPipeBytes;
  const fromEnv = Number(process.env.WCOL_REDUCER_EGRESS_PIPE_BYTES ?? 0);
  if (Number.isFinite(fromEnv) && fromEnv > 0) return fromEnv;
  return DEFAULT_REDUCER_EGRESS_PIPE_BYTES;
}

/** Matches Cloudflare `WCOL_DISPATCH_CONCURRENCY` / scan caps (independent of planned scan count). */
export type LocalHashScheduling = {
  openingConcurrency?: number;
  maxActiveScans?: number;
  maxActiveIngress?: number;
};

export type LocalHashGraphHooks = {
  scheduling?: LocalHashScheduling;
  /** Wrap one scan's reads, eg. to simulate R2 latency and bandwidth per scan node. */
  scanReadAt?: (taskId: number, read: ReadAt) => ReadAt;
  /** Shared fan-in byte pool (default 32 MiB). */
  maxHostBytes?: number;
  /** Reducer → finalizer bounded pipe (default 1 MiB). Finalizer consumes concurrently. */
  reducerEgressPipeBytes?: number;
  /** Fail the coordinator finalizer when it takes its first output. */
  simulateFinalizerError?: Error;
  onScanProgress?: (info: {
    taskId: number;
    scanIndex: number;
    outputBytes: number;
    wasmMemoryBytes: number;
    wasmInstanceId: number;
    egressPendingBytes?: number;
    egressPeakPendingBytes?: number;
    outstandingReads?: number;
    dictTraffic?: DictTraffic;
    memGrowth?: MemGrowthSample[];
  }) => void;
  onReducerProgress?: (info: {
    taskId: number;
    wasmMemoryBytes: number;
    wasmInstanceId: number;
    spillRuns?: number;
    spillPeakLiveBytes?: number;
    spillPeakInternerBytes?: number;
    spillBoundaries?: SpillBoundary[];
    dictTraffic?: DictTraffic;
    memGrowth?: MemGrowthSample[];
  }) => void;
  /**
   * One node Wasm instance per reducer and per scan (matches one DO isolate each).
   * Defaults to true; pass false only for tests that intentionally share one module.
   */
  isolatedNodeWasm?: boolean;
  /** In-memory R2 for sorted spill runs. Default: fresh memory bucket per job. */
  spillR2Bucket?: R2OutputBucket;
  /** Called while reducer egress is backpressured into the finalizer pipe. */
  onReducerOutputMetrics?: (info: {
    taskId: number;
    pipeBufferedBytes: number;
    pipeCapacity: number;
    pipeBlockedWriters: number;
  }) => void;
  /** Spill host I/O totals (local memory R2 or real bucket). */
  onSpillHostTelemetry?: (info: {
    runPutsCompleted: number;
    runReads: number;
    runCommits: number;
  }) => void;
};

export type RunLocalHashGraphResult = {
  bytes: Uint8Array;
  timing?: HashJobTiming;
  peakSimultaneousScans: number;
  wcsInputBytesByReducer: ReadonlyMap<number, number>;
  ingressRetainedPeakByReducer: ReadonlyMap<number, number>;
  ledgerPeakAdapterBytes: number;
  /** How node Wasm was wired for this run. */
  nodeWasmScope: "shared_singleton" | "per_task_isolate";
};

/** The coordinator module, except that its first output take throws `error`. */
function failingFinalizerWasm(wasm: WcolStreamWasm, error: Error): WcolStreamWasm {
  return {
    ...wasm,
    wcs_sp_out_ptr: () => {
      throw error;
    },
  };
}

/** Scanners open a pipe only for partitions that receive rows. */
export async function runLocalHashGraph(
  wasm: GraphWasm,
  plan: Extract<ExecutionPlan, { kind: "graph" }>,
  readAt: ReadAt,
  hooks?: LocalHashGraphHooks,
): Promise<RunLocalHashGraphResult> {
  const spillR2Bucket = hooks?.spillR2Bucket ?? createMemoryR2SpillBucket();
  const onScanProgress = hooks?.onScanProgress;
  const onReducerProgress = hooks?.onReducerProgress;
  const onReducerOutputMetrics = hooks?.onReducerOutputMetrics;
  const onSpillHostTelemetry = hooks?.onSpillHostTelemetry;
  const discardReducerOutput = process.env.WCOL_DISCARD_REDUCER_OUTPUT === "1";
  const scheduling = hooks?.scheduling;
  const maxHostBytes = hooks?.maxHostBytes ?? LOCAL_HOST_BUDGET;
  const egressPipeBytes = reducerEgressPipeBytes(hooks);
  const isolatedNodeWasm = hooks?.isolatedNodeWasm !== false;
  const wcsInputBytesByReducer = new Map<number, number>();
  const ingressRetainedPeakByReducer = new Map<number, number>();
  // One set of work queues per run: scans of a split object balance their units here.
  const nextUnit = localWork(plan.tasks);
  const sessions = new Map<number, ReturnType<typeof createReducerIngressSession>>();
  const latches = new Map<number, ReturnType<typeof createPoolInputLatch>>();
  // Scans start alongside reducer setup (as on Cloudflare): an edge waits for its reducer.
  const reducerReady = new Map<number, { promise: Promise<void>; resolve: () => void }>();
  const readyFor = (taskId: number) => {
    let entry = reducerReady.get(taskId);
    if (!entry) {
      let resolve!: () => void;
      const promise = new Promise<void>((done) => {
        resolve = done;
      });
      entry = { promise, resolve };
      reducerReady.set(taskId, entry);
    }
    return entry;
  };
  const bytePool = createSharedBytePool(maxHostBytes);
  const coordinator = hooks?.simulateFinalizerError
    ? failingFinalizerWasm(wasm.coordinator, hooks.simulateFinalizerError)
    : wasm.coordinator;
  const result = await runHashJob({
    wasm: { node: wasm.node, coordinator },
    plan,
    readAt,
    openingConcurrency: scheduling?.openingConcurrency,
    maxActiveScans: scheduling?.maxActiveScans,
    maxActiveIngress: scheduling?.maxActiveIngress,
    hashRemote: {
      prepareReducer: async (
        task: RunnableTask,
        expectedScans: number,
        finalizer,
        exportProgram?: Uint8Array,
      ) => {
        const reducerWasm = isolatedNodeWasm ? await instantiateNodeWasm() : wasm.node;
        const session = createReducerIngressSession({
          jobId: "local",
          executionAttempt: 1,
          expectedScans,
        });
        sessions.set(task.taskId, session);
        const inputLatch = createPoolInputLatch();
        latches.set(task.taskId, inputLatch);
        session.readyToAccept();
        readyFor(task.taskId).resolve();
        const shardExport = exportProgram != null && !discardReducerOutput;
        const pipe = discardReducerOutput || shardExport
          ? null
          : createBoundedBytePipe({
              maxBufferedBytes: egressPipeBytes,
              pool: bytePool,
              owner: `reducer-${task.taskId}`,
            });
        if (pipe) {
          finalizer.attachFinalizerInput(pipe.readable);
        } else if (!shardExport) {
          finalizer.attachFinalizerInput(streamFromBytes(new Uint8Array(0)));
        }
        const writer = pipe ? pipe.writable.getWriter() : null;
        if (writer) void writer.closed.catch(() => undefined);
        const shardChunks: Uint8Array[] = [];
        let exportWriter: WritableStreamDefaultWriter<Uint8Array> | null = null;
        let exportDone: Promise<unknown> = Promise.resolve();
        let exportLatch: ReturnType<typeof createPoolInputLatch> | null = null;
        if (shardExport && exportProgram && exportProgram !== task.program) {
          exportLatch = createPoolInputLatch();
          const exportPipe = createBoundedBytePipe({
            maxBufferedBytes: egressPipeBytes,
            pool: bytePool,
            owner: `export-${task.taskId}`,
          });
          exportLatch.attachInput(0, exportPipe.readable);
          exportWriter = exportPipe.writable.getWriter();
          void exportWriter.closed.catch(() => undefined);
          const exportWasm = isolatedNodeWasm ? await instantiateNodeWasm() : wasm.node;
          exportDone = runWcolEngine({
            wasm: exportWasm,
            plan: exportProgram,
            scratchBytes: 1024 * 1024,
            layout: { inputs: 1, outputs: 1 },
            inputLatch: exportLatch,
            host: {
              writePort: async (_port, chunk) => {
                shardChunks.push(chunk);
              },
              yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
            },
          });
        }
        let metricsTimer: ReturnType<typeof setInterval> | undefined;
        if (pipe && onReducerOutputMetrics) {
          metricsTimer = setInterval(() => {
            const m = pipe.metrics();
            onReducerOutputMetrics({
              taskId: task.taskId,
              pipeBufferedBytes: m.bufferedBytes,
              pipeCapacity: m.capacity,
              pipeBlockedWriters: m.blockedWriters,
            });
          }, 2000);
        }
        const running = runWcolEngine({
          wasm: reducerWasm,
          plan: task.program,
          scratchBytes: 1024 * 1024,
          layout: { inputs: expectedScans, outputs: 1 },
          inputLatch,
          readAt: (_source, offset, length) => readAt(offset, length),
          onProgress: (info) => {
            onReducerProgress?.({
              taskId: task.taskId,
              wasmMemoryBytes: info.wasmMemoryBytes,
              wasmInstanceId: wasmInstanceId(reducerWasm),
              spillRuns: info.spillRuns,
              spillPeakInternerBytes: info.spillPeakInternerBytes,
              spillBoundaries: info.spillBoundaries,
            });
          },
          host: {
            writePort: async (_port, chunk) => {
              if (discardReducerOutput) return;
              if (exportWriter) {
                await exportWriter.write(chunk);
                return;
              }
              if (shardExport) {
                shardChunks.push(chunk);
                return;
              }
              if (!writer) return;
              await writer.write(chunk);
            },
            yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
            spillR2Bucket,
          },
        });
        const done = running.then(
          async (stats) => {
            onReducerProgress?.({
              taskId: task.taskId,
              wasmMemoryBytes: Math.max(stats.maxWasmMemoryBytes, reducerWasm.memory.buffer.byteLength),
              wasmInstanceId: wasmInstanceId(reducerWasm),
              spillRuns: stats.spillRuns,
              spillPeakLiveBytes: stats.spillPeakLiveBytes,
              spillPeakInternerBytes: stats.spillPeakInternerBytes,
              spillBoundaries: stats.spillBoundaries,
              dictTraffic: stats.dictTraffic,
              memGrowth: stats.memGrowth,
            });
            if (writer) await writer.close();
            if (onSpillHostTelemetry && stats.spillDiag) {
              onSpillHostTelemetry({
                runPutsCompleted: stats.spillDiag.spillExecByOp.end,
                runReads: stats.spillDiag.spillExecByOp.scan,
                runCommits: stats.spillDiag.spillExecByOp.commit,
              });
            }
          },
          async (error: unknown) => {
            session.abort(error);
            if (writer) await writer.abort(error);
            throw error;
          },
        );
        void done.catch(() => undefined);
        const handle: {
          invokedSlots: number[];
          waitConsumeReady: (scanIndex: number) => Promise<void>;
          accept: (scanIndex: number, stream: ReadableStream<Uint8Array>) => Promise<void>;
          completeScan: (scanIndex: number) => Promise<void>;
          seal: () => Promise<void>;
          abort: (error: unknown) => Promise<void>;
          parquetPart?: {
            objectKey: string;
            partitionValues: string[];
            bytes: number;
            parts: number;
            taskId: number;
            localBytes: Uint8Array;
          };
        } = {
          invokedSlots: [task.slot],
          waitConsumeReady: (scanIndex: number) => session.waitConsumeReady(scanIndex),
          accept: async (scanIndex: number, stream: ReadableStream<Uint8Array>) => {
            session.acceptScanEdge(scanIndex, stream);
            inputLatch.attachInput(scanIndex, stream);
          },
          completeScan: async (scanIndex: number) => {
            session.completeScan(scanIndex);
            inputLatch.sealInput(scanIndex);
          },
          seal: async () => {
            await done;
            if (exportWriter) {
              await exportWriter.close();
              exportLatch?.sealInput(0);
              await exportDone;
            }
            if (metricsTimer) clearInterval(metricsTimer);
            session.seal();
            for (let index = 0; index < expectedScans; index++) inputLatch.sealInput(index);
            if (shardExport) {
              const bytes = shardChunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
              handle.parquetPart = {
                objectKey: `local-shard-${task.taskId}`,
                partitionValues: [],
                bytes,
                parts: 1,
                taskId: task.taskId,
                localBytes: concatBytes(shardChunks),
              };
            }
          },
          abort: async (error: unknown) => {
            if (metricsTimer) clearInterval(metricsTimer);
            session.abort(error);
            inputLatch.abort(error);
            if (exportWriter) await exportWriter.abort(error).catch(() => undefined);
            exportLatch?.abort(error);
            await exportDone.catch(() => undefined);
            await done.catch(() => undefined);
          },
        };
        return handle;
      },
      runScan: async (task: RunnableTask, scanIndex: number, destinations: HashScanDestination[], open) => {
        const scanRead = hooks?.scanReadAt ? hooks.scanReadAt(task.taskId, readAt) : readAt;
        // Opening a scan here is taking its isolate; it then runs outside the opening limit.
        const scanWasm = await open(async () => (isolatedNodeWasm ? instantiateNodeWasm() : wasm.node));
        const wasmAtScanStart = scanWasm.memory.buffer.byteLength;
        const openedPartitions: number[] = [];
        const partitionCount = Math.max(...destinations.map((d) => d.partition)) + 1;
        const stats = await runScanStreamPool({
          wasm: scanWasm,
          program: task.program,
          scratchBytes: 1024 * 1024,
          partitionCount,
          readAt: claimingReadAt(
            (_source, offset, length) => scanRead(offset, length),
            () => nextUnit(task.taskId),
          ),
          onPartitionStream: async (partition, body) => {
            const dest = destinations.find((item) => item.partition === partition);
            if (!dest) throw new Error(`scan missing destination ${partition}`);
            await readyFor(dest.taskId).promise;
            const session = sessions.get(dest.taskId);
            if (!session) throw new Error(`reducer ${dest.taskId} has no session`);
            const inputLatch = latches.get(dest.taskId);
            if (!inputLatch) throw new Error(`reducer ${dest.taskId} has no input latch`);
            openedPartitions.push(partition);
            session.acceptScanEdge(scanIndex, body);
            inputLatch.attachInput(scanIndex, body);
          },
        });
        // Bytes the scan handed to its partition streams, as the pool counted them.
        const bytes = stats.profile.outputBytes;
        for (const partition of openedPartitions) {
          const dest = destinations.find((item) => item.partition === partition);
          if (dest) {
            wcsInputBytesByReducer.set(
              dest.taskId,
              (wcsInputBytesByReducer.get(dest.taskId) ?? 0) + bytes,
            );
          }
        }
        if (onScanProgress) {
          const wasmAtScanEnd = scanWasm.memory.buffer.byteLength;
          onScanProgress({
            taskId: task.taskId,
            scanIndex,
            outputBytes: bytes,
            wasmMemoryBytes: Math.max(wasmAtScanStart, wasmAtScanEnd),
            wasmInstanceId: wasmInstanceId(scanWasm),
            egressPeakPendingBytes: 0,
            dictTraffic: readDictTraffic(scanWasm),
            memGrowth: readMemGrowth(scanWasm),
          });
        }
        return { openedPartitions, bytes, invokedSlots: [task.slot] };
      },
    },
  });
  const shardBytes = result.outputs
    ?.map((part) => part.localBytes)
    .filter((part): part is Uint8Array => part != null);
  const bytes = result.rows
    ?? result.partial
    ?? result.parquet
    ?? (shardBytes && shardBytes.length > 0 ? concatBytes(shardBytes) : undefined);
  if (!bytes) throw new Error("hash job produced no bytes");
  return {
    bytes,
    timing: result.timing,
    peakSimultaneousScans: result.timing?.peakSimultaneousScans ?? 0,
    wcsInputBytesByReducer,
    ingressRetainedPeakByReducer,
    ledgerPeakAdapterBytes: bytePool.peakUsed,
    nodeWasmScope: isolatedNodeWasm ? "per_task_isolate" : "shared_singleton",
  };
}
