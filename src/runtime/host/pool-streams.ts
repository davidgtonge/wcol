/** Streams into and out of a stream pool's ports. */
import type { StreamPool } from "./stream-pool.ts";

/** Queue attach/seal until `bind(pool)` (scan edges may arrive before `runWcolEngine` opens the pool). */
export type PoolInputLatch = {
  attachInput(port: number, stream: ReadableStream<Uint8Array>): void;
  sealInput(port: number): void;
  abort(reason?: unknown): void;
  bind(pool: StreamPool): void;
};

export function streamFromBytes(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      if (bytes.byteLength > 0) controller.enqueue(bytes);
      controller.close();
    },
  });
}

/** `onPoolReady` for a task whose inputs are whole buffers, one per ingress port; none without inputs. */
export function attachBytesInputs(inputs: readonly Uint8Array[]): ((pool: StreamPool) => void) | undefined {
  if (inputs.length === 0) return undefined;
  return (pool) => inputs.forEach((bytes, port) => pool.attachInput(port, streamFromBytes(bytes)));
}

/**
 * An abort reaches the pool whenever it comes: before `bind` it is held and applied on bind,
 * so a pool opened after its attempt failed starts failed instead of waiting for inputs.
 */
export function createPoolInputLatch(): PoolInputLatch {
  type Pending = { kind: "attach"; port: number; stream: ReadableStream<Uint8Array> } | { kind: "seal"; port: number };
  const pending: Pending[] = [];
  let pool: StreamPool | null = null;
  let aborted: { reason: unknown } | undefined;

  const dropPending = (reason: unknown): void => {
    for (const op of pending.splice(0)) {
      if (op.kind === "attach") void op.stream.cancel(reason).catch(() => undefined);
    }
  };

  return {
    attachInput(port, stream) {
      if (aborted) throw aborted.reason;
      if (pool) pool.attachInput(port, stream);
      else pending.push({ kind: "attach", port, stream });
    },
    sealInput(port) {
      if (aborted) return;
      if (pool) pool.sealInput(port);
      else pending.push({ kind: "seal", port });
    },
    abort(reason) {
      if (aborted) return;
      aborted = { reason: reason ?? new Error("pool input latch aborted") };
      dropPending(aborted.reason);
      pool?.abort(aborted.reason);
    },
    bind(next) {
      if (pool) throw new Error("pool input latch is already bound");
      pool = next;
      if (aborted) {
        next.abort(aborted.reason);
        return;
      }
      for (const op of pending.splice(0)) {
        if (op.kind === "attach") next.attachInput(op.port, op.stream);
        else next.sealInput(op.port);
      }
    },
  };
}

/** A promise settled from outside, observed so an unawaited rejection is never unhandled. */
function settleable(): { promise: Promise<void>; resolve: () => void; reject: (error: unknown) => void } {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

/**
 * Forward a pool output to a hash partition RPC once its first byte arrives; a partition
 * with no rows opens nothing. Resolves when the consumer has drained the source; rejects
 * when the source fails, the consumer cancels, or `onFirstByte` fails (the source is then
 * cancelled, which fails the pool).
 */
export async function hashScanOutputStream(
  source: ReadableStream<Uint8Array>,
  onFirstByte: (body: ReadableStream<Uint8Array>) => void | Promise<void>,
): Promise<void> {
  const reader = source.getReader();
  try {
    let lead: Uint8Array | undefined;
    while (!lead) {
      const first = await reader.read();
      if (first.done) return;
      if (first.value.byteLength > 0) lead = first.value;
    }
    const drained = settleable();
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (lead) {
          controller.enqueue(lead);
          lead = undefined;
          return;
        }
        try {
          const next = await reader.read();
          if (next.done) {
            controller.close();
            drained.resolve();
          } else if (next.value.byteLength > 0) {
            controller.enqueue(next.value);
          }
        } catch (error) {
          controller.error(error);
          drained.reject(error);
        }
      },
      cancel(reason) {
        drained.reject(reason ?? new Error("hash scan output cancelled"));
        return reader.cancel(reason).catch(() => undefined);
      },
    });
    try {
      await onFirstByte(body);
      await drained.promise;
    } catch (error) {
      await reader.cancel(error).catch(() => undefined);
      throw error;
    }
  } finally {
    reader.releaseLock();
  }
}

/** Pump a producer stream into one writer, chunk by chunk, awaiting each write. */
export async function pumpReadableToWriter(
  source: ReadableStream<Uint8Array>,
  writer: WritableStreamDefaultWriter<Uint8Array>,
  signal?: AbortSignal,
  trackReaders?: Set<ReadableStreamDefaultReader<Uint8Array>>,
): Promise<void> {
  const reader = source.getReader();
  trackReaders?.add(reader);
  try {
    for (;;) {
      if (signal?.aborted) throw signal.reason ?? new Error("pumpReadableToWriter aborted");
      const result = await reader.read();
      if (result.done) break;
      if (result.value.byteLength > 0) await writer.write(result.value);
    }
    await writer.close();
  } catch (error) {
    await writer.abort(error).catch(() => undefined);
    throw error;
  } finally {
    trackReaders?.delete(reader);
    reader.releaseLock();
  }
}
