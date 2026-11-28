/**
 * Bounded byte pipe for WCS1 transport.
 * Producer `write` blocks when local buffered bytes exceed the cap, or when a
 * shared {@link SharedBytePool} is full. Pool bytes are released when the
 * consumer dequeues, or on cancel/abort.
 */

type SharedBytePool = {
  acquire(bytes: number): Promise<void>;
  release(bytes: number): void;
};

type PipeCloseReason = {
  action: "close" | "abort" | "cancel";
  message: string;
};

export type BoundedBytePipeMetrics = {
  peakBufferedBytes: number;
  bytesWritten: number;
  bytesRead: number;
  bufferedBytes: number;
  capacity: number;
  blockedWriters: number;
};

export type BoundedBytePipe = {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  /**
   * Fail the pipe now, also while a write is blocked on the cap. `writable.abort()` waits
   * for that write, which only a reader can release. Queued bytes are dropped and the
   * readable errors with `reason`: a reader must not take a cut edge for a complete one.
   */
  abort: (reason?: unknown) => void;
  metrics: () => BoundedBytePipeMetrics;
};

export type BoundedBytePipeOptions = {
  maxBufferedBytes: number;
  pool?: SharedBytePool;
  /** Names the pipe in its errors, eg. `scan-3-p1`. */
  owner?: string;
};

function reasonText(reason: unknown, fallback: string): string {
  return reason instanceof Error ? reason.message : String(reason ?? fallback);
}

export function createBoundedBytePipe(
  maxBufferedBytes: number | BoundedBytePipeOptions,
  pool?: SharedBytePool,
): BoundedBytePipe {
  const options: BoundedBytePipeOptions =
    typeof maxBufferedBytes === "number"
      ? { maxBufferedBytes, pool }
      : maxBufferedBytes;
  const cap = options.maxBufferedBytes;
  const shared = options.pool;
  const owner = options.owner ?? "bounded byte pipe";
  if (cap < 1) throw new Error("maxBufferedBytes must be >= 1");
  let buffered = 0;
  let acquired = 0;
  let peakBuffered = 0;
  let bytesWritten = 0;
  let bytesRead = 0;
  const queue: Uint8Array[] = [];
  let closed = false;
  let readWait: (() => void) | null = null;
  let writeWait: (() => void) | null = null;
  let blockedWriters = 0;
  let closeReason: PipeCloseReason | undefined;
  let readController: ReadableStreamDefaultController<Uint8Array> | undefined;

  const closedError = (): Error => {
    const detail = closeReason ? ` (${closeReason.action}: ${closeReason.message})` : "";
    return new Error(`write to closed bounded pipe${detail} [${owner}]`);
  };

  const notifyRead = () => {
    readWait?.();
    readWait = null;
  };
  const notifyWrite = () => {
    writeWait?.();
    writeWait = null;
  };

  const dequeue = (): Uint8Array | undefined => {
    const chunk = queue.shift();
    if (!chunk) return undefined;
    buffered -= chunk.byteLength;
    bytesRead += chunk.byteLength;
    if (shared && chunk.byteLength > 0) {
      shared.release(chunk.byteLength);
      acquired = Math.max(0, acquired - chunk.byteLength);
    }
    notifyWrite();
    return chunk;
  };

  const releaseHeld = () => {
    if (shared && acquired > 0) shared.release(acquired);
    acquired = 0;
    buffered = 0;
    queue.length = 0;
  };

  const abort = (reason?: unknown) => {
    // Already failed, or closed by the writer with every byte taken: nothing is cut.
    if (closeReason && (closeReason.action !== "close" || queue.length === 0)) return;
    closed = true;
    closeReason = { action: "abort", message: reasonText(reason, "abort") };
    releaseHeld();
    readWait = null;
    try {
      readController?.error(reason ?? new Error(`${owner} aborted`));
    } catch {
      // The readable had already ended; there is no reader left to tell.
    }
    notifyWrite();
  };

  const readable = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        readController = controller;
      },
      pull(controller) {
        const chunk = dequeue();
        if (chunk) {
          controller.enqueue(chunk);
          return;
        }
        if (closed) {
          controller.close();
          return;
        }
        readWait = () => {
          const next = dequeue();
          if (next) controller.enqueue(next);
          else if (closed) controller.close();
        };
      },
      cancel(reason) {
        closed = true;
        closeReason = { action: "cancel", message: reasonText(reason, "cancel") };
        releaseHeld();
        notifyRead();
        notifyWrite();
      },
    },
    { highWaterMark: 0 },
  );

  const writable = new WritableStream<Uint8Array>({
    async write(chunk) {
      if (closed) throw closedError();
      if (shared && chunk.byteLength > 0) {
        await shared.acquire(chunk.byteLength);
        if (closed) {
          shared.release(chunk.byteLength);
          throw closedError();
        }
        acquired += chunk.byteLength;
      }
      bytesWritten += chunk.byteLength;
      queue.push(chunk);
      buffered += chunk.byteLength;
      peakBuffered = Math.max(peakBuffered, buffered);
      notifyRead();
      if (buffered > cap && !closed) {
        blockedWriters += 1;
        try {
          while (buffered > cap && !closed) {
            await new Promise<void>((resolve) => {
              writeWait = resolve;
            });
          }
        } finally {
          blockedWriters = Math.max(0, blockedWriters - 1);
        }
      }
    },
    close() {
      closed = true;
      closeReason = { action: "close", message: "writable.close" };
      notifyRead();
    },
    abort,
  });

  return {
    readable,
    writable,
    abort,
    metrics: () => ({
      peakBufferedBytes: peakBuffered,
      bytesWritten,
      bytesRead,
      bufferedBytes: buffered,
      capacity: cap,
      blockedWriters,
    }),
  };
}

export async function drainReadable(
  readable: ReadableStream<Uint8Array>,
  options?: { delayMs?: number },
): Promise<number> {
  const reader = readable.getReader();
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (options?.delayMs) {
        await new Promise((r) => setTimeout(r, options.delayMs));
      }
    }
  } finally {
    reader.releaseLock();
  }
  return total;
}
