/**
 * Byte streams transported through immutable object-store chunks.
 *
 * Wire protocol (UTF-8 NDJSON over the supplied signal stream):
 *   {"type":"open","prefix":"wcol-streams/<random-id>"}
 *   {"type":"chunk","seq":0,"bytes":5242880}
 *   ...
 *   {"type":"end","count":N}
 *
 * A chunk signal is emitted only after its object PUT succeeds.
 * The receiver records descriptors as signals arrive, GETs up to `prefetch`
 * bodies at once, and yields bytes strictly in sequence order.
 */

export interface ChunkStore {
  put(key: string, data: Uint8Array, signal: AbortSignal): Promise<void>;
  get(key: string, signal: AbortSignal): Promise<Uint8Array>;
}

export interface WritableChunkOptions {
  /** Object size in bytes; defaults to 5 MiB. */
  chunkSize?: number;
  /** Maximum number of outstanding uploads/notifications; defaults to 8. */
  maxInFlight?: number;
  /** New random stream ID is appended to this prefix. */
  keyPrefix?: string;
}

export interface ReadableChunkOptions {
  /** Maximum GETs / fully buffered chunks in flight; defaults to 8. */
  prefetch?: number;
  /** Hard cap on declared chunk size; defaults to 64 MiB. */
  maxChunkSize?: number;
}

type ChunkSignal =
  | { type: "open"; prefix: string }
  | { type: "chunk"; seq: number; bytes: number }
  | { type: "end"; count: number };

type FetchResult =
  | { ok: true; data: Uint8Array }
  | { ok: false; error: Error };

type PendingChunk = { seq: number; bytes: number; key: string };

const encoder = new TextEncoder();
const MAX_SIGNAL_LINE = 4096;

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function objectKey(prefix: string, seq: number): string {
  return `${prefix}/${String(seq).padStart(12, "0")}`;
}

function encodeSignal(signal: ChunkSignal): Uint8Array {
  return encoder.encode(`${JSON.stringify(signal)}\n`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseSignal(line: string): ChunkSignal {
  const value: unknown = JSON.parse(line);
  if (!isRecord(value)) throw new Error("Invalid stream signal");

  if (value.type === "open" && typeof value.prefix === "string" && value.prefix.length > 0) {
    return { type: "open", prefix: value.prefix };
  }
  if (
    value.type === "chunk" &&
    typeof value.seq === "number" &&
    Number.isSafeInteger(value.seq) &&
    value.seq >= 0 &&
    typeof value.bytes === "number" &&
    Number.isSafeInteger(value.bytes) &&
    value.bytes > 0
  ) {
    return { type: "chunk", seq: value.seq, bytes: value.bytes };
  }
  if (
    value.type === "end" &&
    typeof value.count === "number" &&
    Number.isSafeInteger(value.count) &&
    value.count >= 0
  ) {
    return { type: "end", count: value.count };
  }
  throw new Error("Invalid stream signal");
}

/** Supports arbitrary signal transport framing (split or joined NDJSON lines). */
async function* decodeSignals(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): AsyncGenerator<ChunkSignal> {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let pending = "";

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        pending += decoder.decode();
        if (pending.length !== 0) throw new Error("Truncated signal frame");
        return;
      }
      pending += decoder.decode(value, { stream: true });
      let newline: number;
      while ((newline = pending.indexOf("\n")) !== -1) {
        const line = pending.slice(0, newline);
        pending = pending.slice(newline + 1);
        if (line.length > MAX_SIGNAL_LINE) throw new Error("Signal frame too large");
        yield parseSignal(line);
      }
      if (pending.length > MAX_SIGNAL_LINE) throw new Error("Signal frame too large");
    }
  } finally {
    reader.releaseLock();
  }
}

/** One waiter per event: producer/consumer each have one asynchronous loop. */
function notification() {
  let wake: (() => void) | undefined;
  return {
    wait: () =>
      new Promise<void>((resolve) => {
        wake = resolve;
      }),
    notify: () => {
      const resume = wake;
      wake = undefined;
      resume?.();
    },
  };
}

/**
 * Exposes a byte WritableStream. Ownership of `signals` passes to this stream:
 * closing the data writer writes END and closes the signal writer as well.
 */
export function chunkWritable(
  store: ChunkStore,
  signals: WritableStream<Uint8Array>,
  options: WritableChunkOptions = {},
): WritableStream<Uint8Array> {
  const chunkSize = positiveInteger(options.chunkSize ?? 5 * 1024 * 1024, "chunkSize");
  const maxInFlight = positiveInteger(options.maxInFlight ?? 8, "maxInFlight");
  const base = (options.keyPrefix ?? "wcol-streams").replace(/\/+$/, "");
  if (!base) throw new Error("keyPrefix cannot be empty");
  const prefix = `${base}/${crypto.randomUUID()}`;

  const sender = signals.getWriter();
  const abort = new AbortController();
  const active = new Set<Promise<void>>();
  let controller: WritableStreamDefaultController | undefined;
  let failure: Error | undefined;
  let buffer: Uint8Array | undefined;
  let length = 0;
  let nextSeq = 0;
  let opened = false;
  let publishTail = Promise.resolve();

  function fail(cause: unknown): void {
    if (failure) return;
    failure = asError(cause);
    abort.abort(failure);
    controller?.error(failure);
    void sender.abort(failure).catch(() => {});
  }

  function open(): void {
    if (opened) return;
    opened = true;
    publishTail = publishTail.then(() => sender.write(encodeSignal({ type: "open", prefix })));
    void publishTail.catch(fail);
  }

  function uploadChunk(): void {
    if (!buffer || !length) return;
    const data = buffer.subarray(0, length);
    buffer = undefined;
    length = 0;
    const seq = nextSeq++;
    open();

    const put = Promise.resolve().then(() => store.put(objectKey(prefix, seq), data, abort.signal));
    void put.catch(fail);

    const publish = publishTail.then(async () => {
      await put;
      if (failure) throw failure;
      await sender.write(encodeSignal({ type: "chunk", seq, bytes: data.byteLength }));
    });
    publishTail = publish;

    const tracked = publish.then(
      () => {
        active.delete(tracked);
      },
      (error: unknown) => {
        active.delete(tracked);
        fail(error);
      },
    );
    active.add(tracked);
  }

  async function waitForCapacity(): Promise<void> {
    while (active.size >= maxInFlight) {
      await Promise.race(active);
      if (failure) throw failure;
    }
  }

  return new WritableStream<Uint8Array>({
    start(sink) {
      controller = sink;
    },

    async write(input) {
      try {
        if (failure) throw failure;
        if (!(input instanceof Uint8Array)) throw new TypeError("Expected Uint8Array");
        let offset = 0;
        while (offset < input.byteLength) {
          buffer ??= new Uint8Array(chunkSize);
          const size = Math.min(chunkSize - length, input.byteLength - offset);
          buffer.set(input.subarray(offset, offset + size), length);
          length += size;
          offset += size;
          if (length === chunkSize) {
            uploadChunk();
            await waitForCapacity();
          }
        }
        if (failure) throw failure;
      } catch (error) {
        fail(error);
        throw error;
      }
    },

    async close() {
      try {
        if (failure) throw failure;
        uploadChunk();
        open();
        await Promise.all(active);
        if (failure) throw failure;
        await publishTail;
        await sender.write(encodeSignal({ type: "end", count: nextSeq }));
        await sender.close();
      } catch (error) {
        fail(error);
        throw error;
      }
    },

    abort(reason) {
      fail(reason ?? new Error("Byte stream aborted"));
    },
  });
}

/**
 * Exposes a byte ReadableStream, transparently GETting committed chunks.
 * Signal lines are drained as they arrive. GET bodies are bounded to `prefetch`
 * full chunks in memory and emitted in sequence order.
 */
export function chunkReadable(
  store: ChunkStore,
  signals: ReadableStream<Uint8Array>,
  options: ReadableChunkOptions = {},
): ReadableStream<Uint8Array> {
  const prefetch = positiveInteger(options.prefetch ?? 8, "prefetch");
  const maxChunkSize = positiveInteger(options.maxChunkSize ?? 64 * 1024 * 1024, "maxChunkSize");
  const signalReader = signals.getReader();
  const abort = new AbortController();
  const pending: PendingChunk[] = [];
  const queued: Array<Promise<FetchResult>> = [];
  const available = notification();
  let prefix: string | undefined;
  let nextSeq = 0;
  let ended = false;
  let cancelled = false;
  let failure: Error | undefined;

  function fail(cause: unknown): void {
    if (failure || cancelled) return;
    failure = asError(cause);
    abort.abort(failure);
    available.notify();
    void signalReader.cancel(failure).catch(() => {});
  }

  function startGets(): void {
    while (queued.length < prefetch && pending.length > 0 && !cancelled && !failure) {
      const chunk = pending.shift()!;
      const get: Promise<FetchResult> = Promise.resolve()
        .then(() => store.get(chunk.key, abort.signal))
        .then((data): FetchResult => {
          if (data.byteLength !== chunk.bytes) {
            throw new Error(`Incorrect chunk length for ${chunk.key}`);
          }
          return { ok: true, data };
        })
        .catch((error: unknown): FetchResult => ({ ok: false, error: asError(error) }));
      queued.push(get);
      available.notify();
    }
  }

  async function receiveSignals(): Promise<void> {
    try {
      for await (const signal of decodeSignals(signalReader)) {
        if (cancelled) return;
        if (signal.type === "open") {
          if (prefix !== undefined) throw new Error("Duplicate OPEN signal");
          prefix = signal.prefix;
          continue;
        }
        if (prefix === undefined) throw new Error("Missing OPEN signal");
        if (signal.type === "end") {
          if (signal.count !== nextSeq) throw new Error("Incorrect END chunk count");
          ended = true;
          startGets();
          available.notify();
          return;
        }
        if (signal.seq !== nextSeq++) throw new Error("Out-of-order chunk signal");
        if (signal.bytes > maxChunkSize) throw new Error("Chunk exceeds maxChunkSize");
        pending.push({ seq: signal.seq, bytes: signal.bytes, key: objectKey(prefix, signal.seq) });
        startGets();
        available.notify();
      }
      if (!cancelled) throw new Error("Signal stream closed without END");
    } catch (error) {
      fail(error);
    }
  }

  return new ReadableStream<Uint8Array>(
    {
      start() {
        void receiveSignals();
      },

      async pull(controller) {
        startGets();
        while (queued.length === 0 && (!ended || pending.length > 0) && !failure) {
          await available.wait();
          startGets();
        }
        if (failure) throw failure;
        if (queued.length === 0) {
          controller.close();
          return;
        }

        const result = await queued[0];
        queued.shift();
        startGets();
        if (result.ok) {
          controller.enqueue(result.data);
          return;
        }
        fail(result.error);
        throw result.error;
      },

      async cancel(reason) {
        cancelled = true;
        abort.abort(reason);
        available.notify();
        try {
          await signalReader.cancel(reason);
        } catch {
          // Already closed/errored.
        }
      },
    },
    { highWaterMark: 0 },
  );
}
