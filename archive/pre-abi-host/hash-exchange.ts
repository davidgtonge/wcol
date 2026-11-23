/**
 * Hash shuffle: scanners fan out tagged WCS; reducers consume per-scan edges.
 *
 * The coordinator admits scans in a contiguous ingress window. A scan starts only
 * when destinations are consume-ready (ingress waiting on that slot), not merely
 * because the reducer exists. Unopened edges are completed with completeScan/seal,
 * not by the scanner sending empty streams.
 */

import { createBoundedBytePipe, type BoundedBytePipe } from "../../src/runtime/host/ports/rpc.ts";
import {
  createOpeningLimiter,
  DEFAULT_DEMUX_BYTES,
  DEFAULT_EDGE_BYTES,
  DEFAULT_MAX_ACTIVE_EGRESS,
  DEFAULT_MAX_ACTIVE_INGRESS,
  DEFAULT_MAX_ACTIVE_SCANS,
} from "./dispatch-limits.ts";
import { OwnedByteReader } from "../../src/runtime/host/ports/readable.ts";
import { createReducerIngressSession } from "../../src/runtime/host/scan-edge-session.ts";
import { createSharedBytePool, type SharedBytePool } from "./shared-byte-pool.ts";

const WCS_HEADER_BYTES = 32;
const WCS_MAGIC = 0x31534357;

export type HashScan = { id: number };
export type HashReducer = { id: number; partition: number };

/** @deprecated Use SharedBytePool. Kept as an alias for demux tests. */
export type DemuxBudget = SharedBytePool;

export function createDemuxBudget(maxPendingBytes: number): SharedBytePool {
  return createSharedBytePool(maxPendingBytes);
}

export type HashScanPhase = { id: number; openMs: number; drainMs: number; bytes: number };

/** Coordinator-side clock for one hash exchange. Filled when the caller passes it. */
export type HashReducerOpen = { partition: number; openMs: number };

export type HashScheduleTiming = {
  reducerOpenMs: number;
  reducerOpens: HashReducerOpen[];
  scans: HashScanPhase[];
  /** First scan start to last scan writers closed. */
  scanPhaseMs: number;
  peakSimultaneousScans: number;
  peakBufferedBytes: number;
  /** pool.used + demux pending assembly. Pipe queues are in the pool. */
  peakRetainedBytes?: number;
  streamObjects?: number;
  lazyPipesCreated?: number;
  peakPendingWrites?: number;
  /** Wall time with at least one partition write() in flight. */
  writeBlockedMs: number;
  /** WCS shuffle bytes observed on the coordinator. Scanner-push keeps this 0. */
  coordinatorShuffleBytes?: number;
  openedEdges?: number;
};

export type DemuxOptions = {
  /** When false, writers (bounded pipes) own the pool; demux does not double-charge. */
  accountFrames?: boolean;
  /** Max in-flight partition writes. Does not cap open destination streams. */
  maxPendingWrites?: number;
  onPendingBytes?: (bytes: number) => void;
};

function waiter(): { wait: () => Promise<void>; wake: () => void } {
  const waiters: Array<() => void> = [];
  return {
    wait: () => new Promise((resolve) => waiters.push(resolve)),
    wake: () => {
      const pending = waiters.splice(0);
      for (const resolve of pending) resolve();
    },
  };
}

export async function demuxPartitionStream(
  input: ReadableStream<Uint8Array>,
  writers: WritableStreamDefaultWriter<Uint8Array>[],
  partitions: number,
  budget: SharedBytePool = createSharedBytePool(DEFAULT_DEMUX_BYTES),
  abort?: AbortSignal,
  writes?: { begin: () => void; end: () => void },
  options?: DemuxOptions,
): Promise<number> {
  const account = options?.accountFrames !== false;
  const owned = new OwnedByteReader(input);
  let pending = new Uint8Array(0);
  let bytes = 0;
  let inputDone = false;
  const writing: Array<Promise<void> | null> = Array.from({ length: partitions }, () => null);
  const gate = waiter();
  let failure: unknown = null;

  const fail = (error: unknown) => {
    if (failure == null) failure = error;
    gate.wake();
    budget.abort(error);
  };

  const startWrite = (partition: number, frame: Uint8Array) => {
    const writer = writers[partition];
    if (!writer) throw new Error(`hash exchange has no reducer for partition ${partition}`);
    writes?.begin();
    const done = writer.write(frame).then(
      () => {
        writing[partition] = null;
        if (account) budget.release(frame.byteLength);
        writes?.end();
        gate.wake();
      },
      (error: unknown) => {
        writing[partition] = null;
        if (account) budget.release(frame.byteLength);
        writes?.end();
        fail(error);
      },
    );
    writing[partition] = done;
  };

  const peekFrame = (): { partition: number; total: number } | null => {
    if (pending.byteLength < WCS_HEADER_BYTES) return null;
    const view = new DataView(pending.buffer, pending.byteOffset, pending.byteLength);
    if (view.getUint32(0, true) !== WCS_MAGIC) {
      throw new Error("hash exchange lost a WCS frame boundary");
    }
    const length = view.getUint32(8, true);
    const total = WCS_HEADER_BYTES + length;
    if (pending.byteLength < total) return null;
    const partition = view.getUint32(12, true);
    if (partition >= partitions) {
      throw new Error(`hash exchange partition ${partition} is outside 0..${partitions}`);
    }
    return { partition, total };
  };

  const takeFrame = (total: number): Uint8Array => {
    const frame = pending.slice(0, total);
    pending = pending.subarray(total);
    options?.onPendingBytes?.(pending.byteLength);
    return frame;
  };

  const readOnce = (): Promise<ReadableStreamReadResult<Uint8Array>> => owned.read();

  const waitForWrite = (partition?: number) => {
    const inflight =
      partition != null && writing[partition]
        ? [writing[partition]!]
        : (writing.filter(Boolean) as Promise<void>[]);
    return Promise.race([
      gate.wait(),
      budget.wait(),
      ...inflight.map((done) => done.then(() => undefined, () => undefined)),
    ]);
  };

  const onAbort = () => {
    fail(abort?.reason ?? new Error("hash exchange aborted"));
    void owned.close(failure);
  };
  abort?.addEventListener("abort", onAbort);

  try {
    for (;;) {
      if (abort?.aborted) throw abort.reason ?? new Error("hash exchange aborted");
      if (failure) throw failure;

      const peeked = peekFrame();
      if (peeked) {
        if (writing[peeked.partition]) {
          await waitForWrite(peeked.partition);
          continue;
        }
        const maxWrites = options?.maxPendingWrites;
        if (maxWrites != null && writing.filter(Boolean).length >= maxWrites) {
          await waitForWrite();
          continue;
        }
        if (account && !budget.tryAcquire(peeked.total)) {
          await waitForWrite();
          continue;
        }
        const frame = takeFrame(peeked.total);
        bytes += frame.byteLength;
        startWrite(peeked.partition, frame);
        continue;
      }

      if (!inputDone) {
        const read = readOnce().then(
          (chunk) => ({ kind: "read" as const, chunk }),
          (error: unknown) => {
            fail(error);
            return { kind: "read" as const, chunk: { done: true, value: undefined } };
          },
        );
        const raced = await Promise.race([
          read,
          gate.wait().then(() => ({ kind: "wake" as const })),
          budget.wait().then(() => ({ kind: "wake" as const })),
          ...(writing.filter(Boolean) as Promise<void>[]).map((done) =>
            done.then(() => ({ kind: "wake" as const }), () => ({ kind: "wake" as const })),
          ),
        ]);
        if (raced.kind === "read") {
          owned.consume();
          const { done, value } = raced.chunk;
          if (value && value.byteLength > 0) pending = concat(pending, value);
          options?.onPendingBytes?.(pending.byteLength);
          if (done) inputDone = true;
        }
        continue;
      }

      if (pending.byteLength !== 0) throw new Error("truncated hash exchange frame");
      if (writing.some(Boolean)) {
        await waitForWrite();
        continue;
      }
      break;
    }
  } finally {
    abort?.removeEventListener("abort", onAbort);
    await owned.close(failure);
  }
  return bytes;
}

export type PartitionAccept = (
  partition: number,
  stream: ReadableStream<Uint8Array>,
) => Promise<void>;

export type PushPartitionedScanArgs = {
  input: ReadableStream<Uint8Array>;
  partitions: number;
  accept: PartitionAccept;
  pool: SharedBytePool;
  maxPendingWrites?: number;
  pipeBytes?: number;
  openingConcurrency?: number;
  abort?: AbortSignal;
  writes?: { begin: () => void; end: () => void };
  onPendingBytes?: (bytes: number) => void;
};

export type PushPartitionedScanResult = {
  bytes: number;
  openedPartitions: number[];
};

type LazyPushEdge = {
  write(chunk: Uint8Array): Promise<void>;
  close(): Promise<void>;
  abort(reason: unknown): Promise<void>;
  created: boolean;
};

function createLazyPushEdge(
  onCreate: (readable: ReadableStream<Uint8Array>) => Promise<void>,
  pool: SharedBytePool,
  localCap: number,
): LazyPushEdge {
  let pipe: BoundedBytePipe | null = null;
  let writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  let opening: Promise<void> | null = null;
  let created = false;
  let closed = false;
  let failed: unknown = null;
  const signal = waiter();

  const ensure = (): Promise<void> => {
    if (opening) return opening;
    opening = (async () => {
      if (failed) throw failed;
      pipe = createBoundedBytePipe({ maxBufferedBytes: localCap, pool });
      writer = pipe.writable.getWriter();
      created = true;
      signal.wake();
      await onCreate(pipe.readable);
    })().catch((error: unknown) => {
      failed ??= error;
      closed = true;
      signal.wake();
      throw error;
    });
    return opening;
  };

  return {
    get created() {
      return created;
    },
    async write(chunk) {
      if (failed) throw failed;
      if (closed && !pipe) throw new Error("write to closed partition edge");
      await ensure();
      if (failed) throw failed;
      await writer!.write(chunk);
    },
    async close() {
      closed = true;
      signal.wake();
      if (!opening) return;
      await opening.catch(() => undefined);
      if (writer) await writer.close();
    },
    async abort(reason) {
      failed ??= reason;
      closed = true;
      signal.wake();
      if (opening) await opening.catch(() => undefined);
      if (writer) await writer.abort(reason).catch(() => undefined);
    },
  };
}

function lazyWriter(edge: LazyPushEdge): WritableStreamDefaultWriter<Uint8Array> {
  return {
    write: (chunk: Uint8Array) => edge.write(chunk),
    close: () => edge.close(),
    abort: (reason?: unknown) => edge.abort(reason ?? new Error("aborted")),
    get closed() {
      return Promise.resolve();
    },
    get ready() {
      return Promise.resolve();
    },
    desiredSize: 1,
    releaseLock() {},
  } as WritableStreamDefaultWriter<Uint8Array>;
}

/** Split tagged WCS on the scanner and lazily accept each partition stream. */
export async function pushPartitionedScan(args: PushPartitionedScanArgs): Promise<PushPartitionedScanResult> {
  const partitions = args.partitions;
  if (partitions < 1) throw new Error("pushPartitionedScan needs partitions");
  const localCap = args.pipeBytes ?? DEFAULT_EDGE_BYTES;
  const maxPendingWrites = args.maxPendingWrites ?? DEFAULT_MAX_ACTIVE_EGRESS;
  const limit = createOpeningLimiter(args.openingConcurrency);
  const edges: LazyPushEdge[] = [];
  for (let partition = 0; partition < partitions; partition++) {
    const index = partition;
    edges.push(
      createLazyPushEdge(
        (readable) => limit(() => args.accept(index, readable)),
        args.pool,
        localCap,
      ),
    );
  }
  const writers = edges.map(lazyWriter);
  try {
    const bytes = await demuxPartitionStream(
      args.input,
      writers,
      partitions,
      args.pool,
      args.abort,
      args.writes,
      {
        accountFrames: false,
        maxPendingWrites,
        onPendingBytes: args.onPendingBytes,
      },
    );
    if (args.abort?.aborted) throw args.abort.reason;
    for (let partition = 0; partition < partitions; partition++) {
      if (edges[partition]!.created) await writers[partition]!.close();
    }
    return {
      bytes,
      openedPartitions: edges.flatMap((edge, partition) => (edge.created ? [partition] : [])),
    };
  } catch (error) {
    for (const edge of edges) await edge.abort(error).catch(() => undefined);
    throw error;
  }
}

export type PreparedHashReducer = {
  body: ReadableStream<Uint8Array>;
  waitConsumeReady: (scanIndex: number) => Promise<void>;
  accept: (scanIndex: number, stream: ReadableStream<Uint8Array>) => Promise<void>;
  completeScan: (scanIndex: number) => Promise<void>;
  seal: () => Promise<void>;
  abort: (error: unknown) => Promise<void>;
};

export type HashScanPushContext = {
  partitions: number;
  accept: PartitionAccept;
  pool: SharedBytePool;
  maxPendingWrites: number;
  pipeBytes: number;
  openingConcurrency?: number;
  abort: AbortSignal;
  writes: { begin: () => void; end: () => void };
  onPendingBytes: (bytes: number) => void;
};

export type HashExchangeOps = {
  prepareReducer: (reducer: HashReducer, expectedScans: number) => Promise<PreparedHashReducer>;
  runScan: (scan: HashScan, scanIndex: number, ctx: HashScanPushContext) => Promise<PushPartitionedScanResult>;
};

/**
 * Keep a pending read on `stream` so an RPC disconnect surfaces before the
 * coordinator later pulls reducer output. The first chunk is preserved for
 * the real consumer.
 */
export function watchReadableStreamFailure(
  stream: ReadableStream<Uint8Array>,
  onFailure: (error: unknown) => void,
): ReadableStream<Uint8Array> {
  const reader = stream.getReader();
  let pending: Promise<ReadableStreamReadResult<Uint8Array>> | undefined = reader.read();
  void pending.catch((error) => onFailure(error));
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        try {
          const chunk = await (pending ?? reader.read());
          pending = undefined;
          if (chunk.done) {
            controller.close();
            return;
          }
          if (chunk.value && chunk.value.byteLength > 0) controller.enqueue(chunk.value);
        } catch (error) {
          onFailure(error);
          controller.error(error);
        }
      },
      cancel(reason) {
        pending = undefined;
        return reader.cancel(reason).catch(() => undefined);
      },
    },
    { highWaterMark: 0 },
  );
}

/**
 * Open every reducer, then run eligible scans. A scan is admitted only in the
 * contiguous window and after every reducer is consume-ready for that scan index.
 */
export async function runHashExchangeSchedule(args: {
  scans: HashScan[];
  reducers: HashReducer[];
  openingConcurrency?: number;
  /** Contiguous scan window tied to reducer consume-ready. Default 8. */
  maxActiveIngress?: number;
  /** Extra cap on scans running inside the window. Default 16. */
  maxActiveScans?: number;
  /** Concurrent outstanding partition writes per scan. Default 32. */
  maxActiveEgress?: number;
  maxPendingBytes?: number;
  pipeBytes?: number;
  runScan?: (id: number) => Promise<ReadableStream<Uint8Array>>;
  runReducer?: (id: number, ingress: ReadableStream<Uint8Array>[]) => Promise<ReadableStream<Uint8Array>>;
  exchange?: HashExchangeOps;
  timing?: HashScheduleTiming;
}): Promise<ReadableStream<Uint8Array>[]> {
  if (args.reducers.length === 0) throw new Error("hash exchange has no reducers");
  const partitions = Math.max(...args.reducers.map((reducer) => reducer.partition)) + 1;
  if (args.reducers.length !== partitions) {
    throw new Error(`hash exchange has ${args.reducers.length} reducers for ${partitions} partitions`);
  }
  const ordered = [...args.reducers].sort((a, b) => a.partition - b.partition);
  for (let index = 0; index < ordered.length; index++) {
    if (ordered[index]!.partition !== index) {
      throw new Error("hash exchange partitions are not 0..n-1");
    }
  }
  const scanCount = args.scans.length;
  if (scanCount < 1) throw new Error("hash exchange has no scans");
  const scanWindow = Math.max(1, args.maxActiveIngress ?? DEFAULT_MAX_ACTIVE_INGRESS);
  const maxActiveScans = Math.max(1, args.maxActiveScans ?? DEFAULT_MAX_ACTIVE_SCANS);
  const maxActiveEgress = Math.max(1, args.maxActiveEgress ?? DEFAULT_MAX_ACTIVE_EGRESS);
  const pool = createSharedBytePool(args.maxPendingBytes ?? DEFAULT_DEMUX_BYTES);
  const localCap = args.pipeBytes ?? DEFAULT_EDGE_BYTES;
  const ops = args.exchange ?? inProcessExchange(args);

  const limit = createOpeningLimiter(args.openingConcurrency);
  const openingCap =
    args.openingConcurrency != null && Number.isFinite(args.openingConcurrency)
      ? args.openingConcurrency
      : null;
  const openStarted = performance.now();
  const reducerOpens: HashReducerOpen[] = [];
  const preparedHandles = await Promise.all(
    ordered.map((reducer) =>
      limit(async () => {
        const started = performance.now();
        const handle = await ops.prepareReducer(reducer, scanCount);
        reducerOpens.push({ partition: reducer.partition, openMs: performance.now() - started });
        return handle;
      }),
    ),
  );
  const byPartition: PreparedHashReducer[] = new Array(partitions);
  const watched: ReadableStream<Uint8Array>[] = new Array(partitions);
  const started = new Array<boolean>(scanCount).fill(false);
  const closed = new Array<boolean>(scanCount).fill(false);
  const running = new Array<Promise<void>>(scanCount);
  let oldest = 0;
  let inflight = 0;
  let simultaneous = 0;
  let peakSimultaneous = 0;
  let scanPhaseStart = 0;
  let scanPhaseEnd = 0;
  let writeInFlight = 0;
  let peakPendingWrites = 0;
  let writeBlockStart = 0;
  let writeBlockedMs = 0;
  let peakRetainedBytes = 0;
  let openedEdges = 0;
  let failure: unknown = null;
  const abort = new AbortController();
  const phases: HashScanPhase[] = [];
  const signal = waiter();
  let epoch = 0;
  const wake = () => {
    epoch += 1;
    signal.wake();
  };

  const noteRetained = (pendingBytes: number) => {
    peakRetainedBytes = Math.max(peakRetainedBytes, pool.used + pendingBytes);
  };

  const markClosed = (index: number) => {
    if (closed[index]) return;
    closed[index] = true;
    while (oldest < scanCount && closed[oldest]) oldest += 1;
    wake();
  };

  const abortAll = (error: unknown) => {
    if (abort.signal.aborted) return;
    failure ??= error;
    abort.abort(error);
    pool.abort(error);
    for (const stream of watched) {
      void stream?.cancel(error).catch(() => undefined);
    }
    for (const handle of byPartition) {
      void handle?.abort(error).catch(() => undefined);
    }
    wake();
  };

  const bodies = preparedHandles.map((handle, index) => {
    const partition = ordered[index]!.partition;
    byPartition[partition] = handle;
    const watchedBody = watchReadableStreamFailure(handle.body, abortAll);
    watched[partition] = watchedBody;
    return watchedBody;
  });
  if (args.timing) {
    args.timing.reducerOpenMs = performance.now() - openStarted;
    args.timing.reducerOpens.push(...reducerOpens);
  }

  const writes = {
    begin: () => {
      if (writeInFlight === 0) writeBlockStart = performance.now();
      writeInFlight += 1;
      if (writeInFlight > peakPendingWrites) peakPendingWrites = writeInFlight;
    },
    end: () => {
      writeInFlight = Math.max(0, writeInFlight - 1);
      if (writeInFlight === 0) writeBlockedMs += performance.now() - writeBlockStart;
    },
  };

  const runOneScan = async (scanIndex: number): Promise<void> => {
    const scan = args.scans[scanIndex]!;
    const scanStarted = performance.now();
    if (scanPhaseStart === 0) scanPhaseStart = scanStarted;
    simultaneous += 1;
    if (simultaneous > peakSimultaneous) peakSimultaneous = simultaneous;
    try {
      if (abort.signal.aborted) throw abort.signal.reason;
      const openMs = performance.now() - scanStarted;
      const drainStarted = performance.now();
      const pushed = await ops.runScan(scan, scanIndex, {
        partitions,
        accept: (partition, stream) => byPartition[partition]!.accept(scanIndex, stream),
        pool,
        maxPendingWrites: maxActiveEgress,
        pipeBytes: localCap,
        openingConcurrency: args.openingConcurrency,
        abort: abort.signal,
        writes,
        onPendingBytes: noteRetained,
      });
      if (abort.signal.aborted) throw abort.signal.reason;
      openedEdges += pushed.openedPartitions.length;
      const opened = new Set(pushed.openedPartitions);
      await Promise.all(
        byPartition.map((handle, partition) =>
          opened.has(partition) ? Promise.resolve() : handle.completeScan(scanIndex),
        ),
      );
      phases.push({ id: scan.id, openMs, drainMs: performance.now() - drainStarted, bytes: pushed.bytes });
    } catch (error) {
      abortAll(error);
      throw error;
    } finally {
      simultaneous = Math.max(0, simultaneous - 1);
      scanPhaseEnd = performance.now();
      markClosed(scanIndex);
    }
  };

  try {
    while (oldest < scanCount) {
      if (failure) throw failure;
      const snapshot = epoch;
      for (let index = 0; index < scanCount; index++) {
        if (started[index] || closed[index]) continue;
        if (index < oldest || index >= oldest + scanWindow) continue;
        if (inflight >= maxActiveScans) break;
        if (openingCap != null && inflight >= openingCap) break;
        started[index] = true;
        inflight += 1;
        const scanIndex = index;
        running[index] = limit(async () => {
          try {
            await Promise.all(byPartition.map((handle) => handle.waitConsumeReady(scanIndex)));
            if (abort.signal.aborted) throw abort.signal.reason;
            await runOneScan(scanIndex);
          } catch (error) {
            abortAll(error);
          } finally {
            inflight -= 1;
            wake();
          }
        });
      }
      if (oldest >= scanCount) break;
      if (epoch === snapshot) await signal.wait();
    }
    await Promise.all(running.filter((task): task is Promise<void> => task != null));
    if (failure) throw failure;
    await Promise.all(byPartition.map((handle) => handle.seal()));
  } catch (error) {
    abortAll(error);
    await Promise.allSettled(running.filter((task): task is Promise<void> => task != null));
    throw error;
  }

  if (args.timing) {
    args.timing.scans.push(...phases);
    args.timing.scanPhaseMs = scanPhaseStart === 0 ? 0 : scanPhaseEnd - scanPhaseStart;
    args.timing.peakSimultaneousScans = peakSimultaneous;
    args.timing.peakBufferedBytes = pool.peakUsed;
    args.timing.peakRetainedBytes = Math.max(peakRetainedBytes, pool.peakUsed);
    args.timing.streamObjects = scanCount * partitions;
    args.timing.lazyPipesCreated = openedEdges;
    args.timing.peakPendingWrites = peakPendingWrites;
    args.timing.writeBlockedMs = writeBlockedMs + (writeInFlight > 0 ? performance.now() - writeBlockStart : 0);
    args.timing.coordinatorShuffleBytes = 0;
    args.timing.openedEdges = openedEdges;
  }
  return bodies;
}

function inProcessExchange(args: {
  runScan?: (id: number) => Promise<ReadableStream<Uint8Array>>;
  runReducer?: (id: number, ingress: ReadableStream<Uint8Array>[]) => Promise<ReadableStream<Uint8Array>>;
}): HashExchangeOps {
  const runScanStream = args.runScan;
  const runReducer = args.runReducer;
  if (!runScanStream || !runReducer) {
    throw new Error("hash exchange needs runScan/runReducer or exchange ops");
  }
  return {
    async prepareReducer(reducer, expectedScans) {
      const session = createReducerIngressSession({
        jobId: "in-process",
        executionAttempt: 1,
        expectedScans,
      });
      const body = await runReducer(reducer.id, session.ingresses());
      return {
        body,
        waitConsumeReady: (scanIndex) => session.waitConsumeReady(scanIndex),
        accept: async (scanIndex, stream) => {
          session.acceptScanEdge(scanIndex, stream);
        },
        completeScan: async (scanIndex) => {
          session.completeScan(scanIndex);
        },
        seal: async () => {
          session.seal();
        },
        abort: async (error) => {
          session.abort(error);
        },
      };
    },
    async runScan(scan, _scanIndex, ctx) {
      const mixed = await runScanStream(scan.id);
      return pushPartitionedScan({
        input: mixed,
        partitions: ctx.partitions,
        accept: ctx.accept,
        pool: ctx.pool,
        maxPendingWrites: ctx.maxPendingWrites,
        pipeBytes: ctx.pipeBytes,
        openingConcurrency: ctx.openingConcurrency,
        abort: ctx.abort,
        writes: ctx.writes,
        onPendingBytes: ctx.onPendingBytes,
      });
    },
  };
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right;
  const out = new Uint8Array(left.byteLength + right.byteLength);
  out.set(left, 0);
  out.set(right, left.byteLength);
  return out;
}
