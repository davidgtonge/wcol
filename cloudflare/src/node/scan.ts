/** Hash scans: read sources, partition rows, and push each partition to its reducer. */
import { readMemGrowth } from "../../../src/runtime/host/run-wcol-engine.ts";
import { claimingReadAt } from "../../../src/runtime/coordinator/unit-claims.ts";
import { coloOnce, isolateId } from "../colo.ts";
import { sha256Hex } from "../digest.ts";
import { runScanStreamPool } from "../../../src/runtime/host/run-scan-stream-pool.ts";
import { createIsolatePeak, joinIsolate } from "./isolate-load.ts";
import { type BoundedBytePipe, createBoundedBytePipe } from "../../../src/runtime/host/ports/rpc.ts";
import { createSharedBytePool } from "../../../src/runtime/host/shared-byte-pool.ts";
import { pumpReadableToWriter } from "../../../src/runtime/host/pool-streams.ts";
import { type PoolStats } from "../../../src/runtime/host/stream-pool.ts";
import { reducerStub, type ReducerRuntime } from "./session.ts";
import { type GraphTaskEnv, type RunTaskHandle, type RunTaskOnNodeOptions } from "./types.ts";
import { traceObjectKey } from "./records.ts";
import { eventLoopYield, nextUnitFromCoordinator, sourceReader, streamed } from "./program.ts";

/** Partition id is the port. An array index is never a fallback. */
export function destinationForPort<T extends { partition: number }>(destinations: readonly T[], port: number): T {
  const dest = destinations.find((item) => item.partition === port);
  if (!dest) throw new Error(`hash scan missing destination ${port}`);
  return dest;
}

/**
 * JS buffering one hash scan may hold across all its partition pipes, shared rather than
 * split. Split evenly, 64 partitions got 128 KiB each: whenever one reducer stopped reading
 * (a sync spill), its pipe filled at once and the whole scan stalled on that one port while
 * the other reducers sat idle. On Q33 that was most of each scan's time. Shared, a stalled
 * partition absorbs up to `SCAN_PIPE_CAP_BYTES` while the others keep flowing, and the
 * pool bounds the total. (A fixed 2 MiB per pipe was once 128 MiB, the whole isolate.)
 */
const SCAN_PIPE_BUDGET_BYTES = 16 * 1024 * 1024;
const SCAN_PIPE_CAP_BYTES = 2 * 1024 * 1024;

async function acceptHashPartition(
  env: GraphTaskEnv,
  params: {
    jobId: string;
    executionAttempt: number;
    scanTaskId: number;
    scanIndex: number;
    dest: { slot: number };
    partition: number;
    stream: ReadableStream<Uint8Array>;
    reducerRuntime: ReducerRuntime;
  },
): Promise<void> {
  const stub = reducerStub(env, params.jobId, params.executionAttempt, params.dest.slot, params.reducerRuntime);
  await stub.acceptScanEdge({
    jobId: params.jobId,
    executionAttempt: params.executionAttempt,
    scanTaskId: params.scanTaskId,
    scanIndex: params.scanIndex,
    partition: params.partition,
    stream: params.stream,
  });
}

/** One hash scan: Rust opens a pipe only for partitions that receive rows. */
export async function runTaskOnNode(options: RunTaskOnNodeOptions): Promise<RunTaskHandle> {
  const { env, params, durableObjectId, waitUntil, hooks, timing } = options;
  const destinations = [...(params.hashDestinations ?? [])].sort((a, b) => a.partition - b.partition);
  if (destinations.length === 0) {
    throw new Error("node runTask requires hash destinations");
  }
  const openedPartitions: number[] = [];
  // Asked now so the answer is in hand when the scan reports; one fetch per isolate.
  const colo = coloOnce();
  const pushing: Promise<void>[] = [];
  const admits: { partition: number; waitMs: number; sentAt: number }[] = [];
  let bytes = 0;
  const owns = hooks?.ownsSession ?? (() => true);
  // A refused or broken edge must fail the scan now. Otherwise its pipe fills, the scan
  // pool blocks on that port, and the failure in `pushing` is never reached.
  const scanAbort = new AbortController();
  hooks?.signal?.addEventListener("abort", () => scanAbort.abort(hooks.signal?.reason), { once: true });
  const watch = (work: Promise<void>): void => {
    pushing.push(
      work.catch((error: unknown) => {
        scanAbort.abort(error);
        throw error;
      }),
    );
  };
  const pushPartition = (port: number, stream: ReadableStream<Uint8Array>): void => {
    const dest = destinationForPort(destinations, port);
    const sentAt = Date.now();
    watch(
      acceptHashPartition(env, {
        jobId: params.jobId,
        executionAttempt: params.executionAttempt,
        scanTaskId: params.taskId,
        scanIndex: params.hashScanIndex ?? 0,
        dest,
        partition: port,
        stream,
        reducerRuntime: params.reducerRuntime ?? "node",
      }).then(
        () => {
          admits.push({ partition: port, waitMs: Date.now() - sentAt, sentAt });
        },
        (error) => {
          admits.push({ partition: port, waitMs: Date.now() - sentAt, sentAt });
          throw error;
        },
      ),
    );
  };
  const runScan = async (write: (chunk: Uint8Array) => Promise<void>): Promise<void> => {
    const rpcAt = Date.now();
    if (params.trace) {
      if (!env.OUTPUT) throw new Error("R2 output binding is missing for a dispatch trace");
      const programSha256 = await sha256Hex(params.program);
      await env.OUTPUT.put(
        traceObjectKey(params.jobId, params.taskId),
        JSON.stringify({ durableObjectId, slot: params.slot, taskId: params.taskId, hashScan: true, programSha256 }),
      );
    }
    const wasm = await options.wasm.load();
    const wasmAt = Date.now();
    const partitionCount = Math.max(...destinations.map((d) => d.partition)) + 1;
    const pipePool = createSharedBytePool(SCAN_PIPE_BUDGET_BYTES);
    const leaveIsolate = joinIsolate("scan", params.taskId, () => ({
      wasmBytes: wasm.memory.buffer.byteLength,
      hostBytes: pipePool.used,
    }));
    const isolatePeak = createIsolatePeak();
    const pipes: BoundedBytePipe[] = [];
    // A pump waiting for pool bytes held by a dead edge would never wake. Every edge ends
    // too: a reducer holds each accept open until its stream ends, so a pipe left open
    // keeps that call, and the reducer's isolate, alive after the attempt is gone.
    scanAbort.signal.addEventListener(
      "abort",
      () => {
        pipePool.abort(scanAbort.signal.reason);
        for (const pipe of pipes) pipe.abort(scanAbort.signal.reason);
      },
      { once: true },
    );
    const readers = params.sources.map((source, index) => sourceReader(env, source, params.etags?.[index]));
    const claims = { asked: 0, granted: 0 };
    let pool: PoolStats | undefined;
    let poolDoneAt = 0;
    try {
      pool = await runScanStreamPool({
        signal: scanAbort.signal,
        wasm,
        program: params.program,
        scratchBytes: 1024 * 1024,
        partitionCount,
        readAt: claimingReadAt(
          async (source, offset, length) => {
            const reader = readers[source];
            if (!reader) throw new Error(`ReadAt source ${source} out of range`);
            return reader.read(Number(offset), length);
          },
          async () => {
            claims.asked += 1;
            const unit = await nextUnitFromCoordinator(env, params.jobId, params.executionAttempt, params.taskId);
            if (unit !== null) claims.granted += 1;
            return unit;
          },
        ),
        yieldToEventLoop: () => {
          isolatePeak.observe();
          return eventLoopYield();
        },
        onPartitionStream: (partition, body) => {
          openedPartitions.push(partition);
          // Decouple scan production from acceptScanEdge. A direct streamed RPC
          // body deadlocks when the reducer is inside sync Wasm (accept queued,
          // scan blocked on the unread body). The local pipe draws on the scan's
          // shared buffer pool while accept is in flight or the reducer is busy.
          const pipe = createBoundedBytePipe({
            maxBufferedBytes: SCAN_PIPE_CAP_BYTES,
            pool: pipePool,
            owner: `scan-${params.taskId}-p${partition}`,
          });
          pipes.push(pipe);
          pushPartition(partition, pipe.readable);
          watch(pumpReadableToWriter(body, pipe.writable.getWriter()));
        },
      }).catch((error: unknown) => {
        scanAbort.abort(error);
        throw error;
      });
      poolDoneAt = Date.now();
      bytes = pool.profile.outputBytes;
      await Promise.all(pushing);
    } finally {
      leaveIsolate();
      // Observe every push, also when the scan failed first.
      void Promise.allSettled(pushing);
    }
    // PROFILE: one line per scan. Wasm finished producing at `poolDoneAt`; reducers had
    // consumed every partition at `endAt`. Logs only: no billed writes.
    const fields = {
      event: "scan_profile",
      jobId: params.jobId,
      attempt: params.executionAttempt,
      scanIndex: params.hashScanIndex ?? 0,
      ...timing,
      rpcAt,
      wasmAt,
      poolDoneAt,
      endAt: Date.now(),
      partitions: openedPartitions.length,
      claims,
      profile: pool?.profile,
      steps: pool?.steps,
      maxStepMs: pool?.maxStepMs,
      maxSyncMs: pool?.maxSyncMs,
      // Linear memory never shrinks, so this is the scan's peak.
      wasmMemoryBytes: wasm.memory.buffer.byteLength,
      // Edge bytes buffered in this scan's pipes: its host bytes outside Wasm.
      pipePeakBytes: pipePool.peakUsed,
      isolatePeak: isolatePeak.peak(),
      // A sample per 8 MiB of growth: what was live when the scan's memory grew.
      memGrowth: readMemGrowth(wasm),
    };
    // Off the critical path: the colo lookup is one fetch per isolate.
    void coloOnce().then((colo) => console.log(JSON.stringify({ ...fields, colo, isolate: isolateId() })));
    await write(
      new TextEncoder().encode(
        JSON.stringify({
          openedPartitions,
          bytes,
          // Bytes each opened partition sent to its reducer, by partition.
          partitionBytes: Object.fromEntries(openedPartitions.map((partition, i) => [partition, pipes[i]?.metrics().bytesWritten ?? 0])),
          colo: await colo,
          invokedSlots: [params.slot],
          admits,
          sourceReads: pool?.profile.reads,
          sourceReadBytes: pool?.profile.readBytes,
        }),
      ),
    );
  };
  const body = streamed(
    waitUntil,
    { env, wasm: options.wasm, jobId: params.jobId, attempt: params.executionAttempt, taskId: params.taskId, slot: params.slot },
    runScan,
    undefined,
    owns,
    true,
  );
  return { invokedSlots: [params.slot], body };
}
