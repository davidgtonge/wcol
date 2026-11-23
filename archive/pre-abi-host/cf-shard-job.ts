/**
 * Shard lifecycle shared by the Node HTTP coordinator and Cloudflare Durable Objects.
 * Placement differs; Rust GraphHost + ReduceOp do not.
 */

import { HttpRangeSource } from "../ts-query-engine/io/byte-source.ts";
import {
  CoordinatorTaskRegistry,
  runTaskWithRetries,
} from "./coordinator-tasks.ts";
import { CoordinatorReducer, coordinatorReducePartials } from "./coordinator-reduce.ts";
import { runWcolEngine, type WcolHost } from "../../src/runtime/host/run-wcol-engine.ts";
import { lastPartialPayload, WcsPartialFrameReader } from "./wcs-transport.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

export type TaskPartialTransport = "json" | "wcs_stream" | "wcs_reduce_stage";

export type ShardTask = {
  taskId: string;
  wireProgram: Uint8Array;
};

export type ShardRunResult = {
  partial: Uint8Array;
  /** Full WCS output. Set when the caller asked for the wire, including row fragments. */
  output?: Uint8Array;
  wasmMemoryBytes: number;
  readAtCalls: number;
  /** `FRAME_PARTIAL` records pushed into `ReduceOp` while reading the node stream. */
  frameCount?: number;
};

export type ShardJobResult = {
  partial: Uint8Array;
  failedTaskAttempts: number;
  duplicateRejected: boolean;
  /** Largest Wasm linear memory seen in one worker isolate. */
  peakIsolateWasmBytes: number;
  /** Peak sum of Wasm linear memory across shards that were in flight together. */
  aggregateInFlightWasmBytes: number;
  shardPeaks: number[];
};

export function bytesToBase64(bytes: Uint8Array): string {
  const parts: string[] = [];
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    parts.push(String.fromCharCode(...bytes.subarray(i, i + chunk)));
  }
  return btoa(parts.join(""));
}

export function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

export type WorkerShardInput =
  | { transport: "http"; url: string }
  | { transport: "r2"; read: (offset: number, length: number) => Promise<Uint8Array> };

/** One WorkerShard wire program inside an isolate that already loaded stream Wasm. */
export async function runWorkerShard(
  wasm: WcolStreamWasm,
  input: WorkerShardInput,
  wireProgram: Uint8Array,
  hooks?: { onYield?: () => void | Promise<void>; requirePartial?: boolean },
): Promise<ShardRunResult> {
  let readAt: NonNullable<WcolHost["readAt"]>;
  if (input.transport === "http") {
    const source = new HttpRangeSource(input.url, { rangeOnly: true });
    readAt = (_source, offset, length) => source.read(offset, length);
  } else {
    readAt = (_source, offset, length) => input.read(offset, length);
  }
  const parts: Uint8Array[] = [];
  const stats = await runWcolEngine({
    wasm,
    plan: wireProgram,
    scratchBytes: 1024 * 1024,
    layout: { inputs: 0, outputs: 1 },
    readAtConcurrency: 2,
    host: {
      readAt,
      writePort: async (_port, chunk) => {
        parts.push(chunk);
      },
      yield: async () => {
        await new Promise((resolve) => setTimeout(resolve, 0));
        await hooks?.onYield?.();
      },
    },
  });
  const wire = concat(parts);
  const partial = lastPartialPayload(wire);
  if (!partial && hooks?.requirePartial !== false) throw new Error("worker shard produced no partial");
  return {
    partial: partial ?? new Uint8Array(),
    output: wire,
    wasmMemoryBytes: stats.maxWasmMemoryBytes,
    readAtCalls: stats.readAtCalls ?? 0,
  };
}

/**
 * Coordinator: one successful partial per task, retries, duplicate rejection, Rust Reduce.
 * `runAttempt` is the placement hook (in-process or a WcolNode fetch).
 */
export async function executeShardJob(options: {
  tasks: readonly ShardTask[];
  runAttempt: (task: ShardTask, attempt: number) => Promise<ShardRunResult>;
  reduce: (partials: Uint8Array[]) => Uint8Array;
  concurrentJobs?: number;
  maxAttempts?: number;
  /** After every task succeeds, submit the first partial again. */
  replayDuplicate?: boolean;
  registry?: CoordinatorTaskRegistry;
  onCompleted?: (
    taskId: string,
    attempt: number,
    partial: Uint8Array,
    frameCount?: number,
  ) => Promise<void>;
}): Promise<ShardJobResult> {
  const registry = options.registry ?? new CoordinatorTaskRegistry();
  const concurrency = Math.max(1, options.concurrentJobs ?? 4);
  const maxAttempts = options.maxAttempts ?? 3;
  const activeWasm = new Map<string, number>();
  let peakIsolateWasmBytes = 0;
  let aggregateInFlightWasmBytes = 0;
  const shardPeaks: number[] = [];
  let next = 0;

  const noteWasm = (taskId: string, bytes: number) => {
    activeWasm.set(taskId, bytes);
    peakIsolateWasmBytes = Math.max(peakIsolateWasmBytes, bytes);
    let sum = 0;
    for (const value of activeWasm.values()) sum += value;
    aggregateInFlightWasmBytes = Math.max(aggregateInFlightWasmBytes, sum);
  };

  const runSlot = async (): Promise<void> => {
    for (;;) {
      const index = next;
      next += 1;
      if (index >= options.tasks.length) return;
      const task = options.tasks[index]!;
      if (registry.has(task.taskId)) continue;
      activeWasm.set(task.taskId, 0);
      try {
        await runTaskWithRetries(registry, task.taskId, maxAttempts, async (attempt) => {
          const result = await options.runAttempt(task, attempt);
          noteWasm(task.taskId, result.wasmMemoryBytes);
          shardPeaks.push(result.wasmMemoryBytes);
          await options.onCompleted?.(task.taskId, attempt, result.partial, result.frameCount);
          return { partial: result.partial };
        });
      } finally {
        activeWasm.delete(task.taskId);
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(concurrency, options.tasks.length) }, () => runSlot()));

  const ranked = [...shardPeaks].sort((a, b) => b - a);
  let topSum = 0;
  for (let i = 0; i < Math.min(concurrency, ranked.length); i++) topSum += ranked[i]!;
  aggregateInFlightWasmBytes = Math.max(aggregateInFlightWasmBytes, topSum);

  let duplicateRejected = false;
  if (options.replayDuplicate && options.tasks.length > 0) {
    const taskId = options.tasks[0]!.taskId;
    const finalized = registry.finalizeForReduce(options.tasks.map((task) => task.taskId));
    if (!finalized.ok) {
      throw new Error(`cannot replay duplicate: ${finalized.error.kind}`);
    }
    const again = registry.registerCompletedAttempt(taskId, 99, finalized.partials[0]!);
    duplicateRejected = !again.ok && again.reason === "duplicate_completed";
  }

  const finalized = registry.finalizeForReduce(options.tasks.map((task) => task.taskId));
  if (!finalized.ok) {
    throw new Error(`coordinator finalize failed: ${finalized.error.kind} ${finalized.error.taskIds.join(",")}`);
  }
  let failedTaskAttempts = 0;
  for (const task of options.tasks) {
    failedTaskAttempts += registry.failedAttemptCount(task.taskId);
  }
  return {
    partial: options.reduce(finalized.partials),
    failedTaskAttempts,
    duplicateRejected,
    peakIsolateWasmBytes,
    aggregateInFlightWasmBytes,
    shardPeaks,
  };
}

export function reduceShardPartials(wasm: WcolStreamWasm, partials: Uint8Array[]): Uint8Array {
  return coordinatorReducePartials(wasm, partials);
}

function concat(parts: Uint8Array[]): Uint8Array {
  const wire = new Uint8Array(parts.reduce((sum, part) => sum + part.byteLength, 0));
  let at = 0;
  for (const part of parts) {
    wire.set(part, at);
    at += part.byteLength;
  }
  return wire;
}

/** Decode the last FRAME_PARTIAL payload from a GraphHost WCS byte stream. */
export async function collectPartialFromWcsStream(source: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = source.getReader();
  const chunks: Uint8Array[] = [];
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value?.byteLength) chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const wire = concat(chunks);
  const partial = lastPartialPayload(wire);
  if (!partial) throw new Error("WCS stream contained no partial frame");
  return partial;
}

/**
 * Merge a worker WCS stream with `ReduceOp`, one `FRAME_PARTIAL` at a time as frames arrive.
 * Memory stays at one frame plus the merged state, not the whole stream.
 */
export async function reduceWcsStream(
  wasm: WcolStreamWasm,
  source: ReadableStream<Uint8Array>,
): Promise<{ partial: Uint8Array; frameCount: number }> {
  const frames = new WcsPartialFrameReader();
  const reducer = new CoordinatorReducer(wasm);
  const reader = source.getReader();
  let frameCount = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value?.byteLength) continue;
      for (const partial of frames.push(value)) {
        reducer.push(partial);
        frameCount += 1;
      }
    }
    frames.finish();
    return { partial: reducer.finish(), frameCount };
  } finally {
    reader.releaseLock();
    reducer.close();
  }
}

export async function reducePartialFromWcsStream(
  wasm: WcolStreamWasm,
  source: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  return (await reduceWcsStream(wasm, source)).partial;
}

/** Stream GraphHost WCS bytes to a host sink (inter-DO transport). */
export async function pipeWorkerShardWcs(
  wasm: WcolStreamWasm,
  input: WorkerShardInput,
  wireProgram: Uint8Array,
  write: (chunk: Uint8Array) => Promise<void>,
  hooks?: { onYield?: () => void | Promise<void> },
): Promise<ShardRunResult & { wireBytes: number }> {
  let readAt: NonNullable<WcolHost["readAt"]>;
  if (input.transport === "http") {
    const source = new HttpRangeSource(input.url, { rangeOnly: true });
    readAt = (_source, offset, length) => source.read(offset, length);
  } else {
    readAt = (_source, offset, length) => input.read(offset, length);
  }
  const parts: Uint8Array[] = [];
  const stats = await runWcolEngine({
    wasm,
    plan: wireProgram,
    scratchBytes: 1024 * 1024,
    layout: { inputs: 0, outputs: 1 },
    readAtConcurrency: 2,
    host: {
      readAt,
      writePort: async (_port, chunk) => {
        parts.push(chunk);
        await write(chunk);
      },
      yield: async () => {
        await new Promise((resolve) => setTimeout(resolve, 0));
        await hooks?.onYield?.();
      },
    },
  });
  const wire = concat(parts);
  const partial = lastPartialPayload(wire);
  if (!partial) throw new Error("worker shard produced no partial");
  return {
    partial,
    wasmMemoryBytes: stats.maxWasmMemoryBytes,
    readAtCalls: stats.readAtCalls ?? 0,
    wireBytes: wire.byteLength,
  };
}

/** Pump GraphHost WCS into a bounded pipe with backpressure (`WritableStreamDefaultWriter.write`). */
export async function pipeWorkerShardWcsToWritable(
  wasm: WcolStreamWasm,
  input: WorkerShardInput,
  wireProgram: Uint8Array,
  writable: WritableStream<Uint8Array>,
  hooks?: { onYield?: () => void | Promise<void> },
): Promise<ShardRunResult & { wireBytes: number }> {
  const writer = writable.getWriter();
  try {
    const result = await pipeWorkerShardWcs(
      wasm,
      input,
      wireProgram,
      async (chunk) => {
        await writer.write(chunk);
      },
      hooks,
    );
    await writer.close();
    return result;
  } catch (error) {
    await writer.abort(error);
    throw error;
  } finally {
    writer.releaseLock();
  }
}
