/** Streams into and out of a stream pool's ports. */
import type { StreamPool } from "./stream-pool.ts";

/** Minimum bytes requested from BYOB reads; buffer stays 64 KiB. */
export const READ_AT_LEAST_BYTES = 64 * 1024;

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

export function createPoolInputLatch(): PoolInputLatch {
  type Pending = { kind: "attach"; port: number; stream: ReadableStream<Uint8Array> } | { kind: "seal"; port: number };
  const pending: Pending[] = [];
  let pool: StreamPool | null = null;
  let aborted: unknown = null;

  const flush = (): void => {
    if (!pool) return;
    for (const op of pending.splice(0)) {
      if (op.kind === "attach") pool.attachInput(op.port, op.stream);
      else pool.sealInput(op.port);
    }
  };

  return {
    attachInput(port, stream) {
      if (aborted) throw aborted;
      if (pool) pool.attachInput(port, stream);
      else pending.push({ kind: "attach", port, stream });
    },
    sealInput(port) {
      if (aborted) return;
      if (pool) pool.sealInput(port);
      else pending.push({ kind: "seal", port });
    },
    abort(reason) {
      aborted = reason ?? new Error("pool input latch aborted");
      pool?.abort(reason);
    },
    bind(next) {
      pool = next;
      flush();
    },
  };
}

/** Forward a pool output to a hash partition RPC once the first byte arrives. Resolves when the source is fully drained. */
export function hashScanOutputStream(
  source: ReadableStream<Uint8Array>,
  onFirstByte: (body: ReadableStream<Uint8Array>) => void | Promise<void>,
): Promise<void> {
  return (async () => {
    const reader = source.getReader();
    let drainResolve!: () => void;
    let drainReject!: (error: unknown) => void;
    const drained = new Promise<void>((resolve, reject) => {
      drainResolve = resolve;
      drainReject = reject;
    });
    try {
      const first = await reader.read();
      if (first.done || !first.value?.byteLength) {
        drainResolve();
        return;
      }
      const lead = first.value;
      let sentLead = false;
      const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            if (!sentLead) {
              sentLead = true;
              controller.enqueue(lead);
              return;
            }
            const next = await reader.read();
            if (next.done) {
              controller.close();
              drainResolve();
              return;
            }
            if (next.value.byteLength) controller.enqueue(next.value);
          } catch (error) {
            drainReject(error);
            controller.error(error);
          }
        },
        cancel(reason) {
          drainReject(reason ?? new Error("hash scan output cancelled"));
          void reader.cancel(reason);
        },
      });
      await onFirstByte(body);
      await drained;
    } catch (error) {
      drainReject(error);
      throw error;
    } finally {
      reader.releaseLock();
    }
  })();
}

/** Pump a producer stream into one pool ingress mailbox (BYOB read when supported; no readAtLeast). */
export async function pumpReadableToWriter(
  source: ReadableStream<Uint8Array>,
  writer: WritableStreamDefaultWriter<Uint8Array>,
  signal?: AbortSignal,
  trackReaders?: Set<ReadableStreamDefaultReader<Uint8Array> | ReadableStreamBYOBReader>,
): Promise<void> {
  let buffer = new Uint8Array(READ_AT_LEAST_BYTES);
  let reader: ReadableStreamDefaultReader<Uint8Array> | ReadableStreamBYOBReader;
  let byob = false;
  try {
    reader = source.getReader({ mode: "byob" });
    byob = true;
  } catch {
    reader = source.getReader();
  }
  trackReaders?.add(reader);
  try {
    for (;;) {
      if (signal?.aborted) {
        throw signal.reason ?? new Error("pumpReadableToWriter aborted");
      }
      if (byob) {
        const result = await (reader as ReadableStreamBYOBReader).read(buffer);
        if (result.done && (!result.value || result.value.byteLength === 0)) break;
        const view = result.value;
        if (view && view.byteLength > 0) {
          await writer.write(view);
          if (view.buffer !== buffer.buffer || view.byteLength !== buffer.byteLength) {
            buffer = new Uint8Array(READ_AT_LEAST_BYTES);
          }
        }
        if (result.done) break;
        continue;
      }
      const result = await (reader as ReadableStreamDefaultReader<Uint8Array>).read();
      if (result.done) break;
      const value = result.value;
      if (value && value.byteLength > 0) await writer.write(value);
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
