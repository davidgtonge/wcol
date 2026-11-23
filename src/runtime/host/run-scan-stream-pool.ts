import type { WcolStreamWasm } from "./wasm-stream.ts";
import { OUTPUT_SLOTS, portKiB } from "./port-budget.ts";
import { createStreamPool, type PoolStats, type ReadAtCallback } from "./stream-pool.ts";
import { hashScanOutputStream } from "./pool-streams.ts";

export type RunScanStreamPoolOptions = {
  wasm: WcolStreamWasm;
  program: Uint8Array;
  scratchBytes: number;
  partitionCount: number;
  inputSizeKiB?: number;
  outputSizeKiB?: number;
  readAt: ReadAtCallback;
  yieldToEventLoop?: () => Promise<void>;
  onPartitionStream: (partition: number, body: ReadableStream<Uint8Array>) => void | Promise<void>;
  /** Abort the scan, eg. when a consumer refused one of its edges. The pool fails with the reason. */
  signal?: AbortSignal;
};

/** Hash scan driver: Parquet ReadAt + hash egress on fixed output slots (no ABI v1 loop). */
export async function runScanStreamPool(options: RunScanStreamPoolOptions): Promise<PoolStats> {
  const pool = createStreamPool({
    inputs: 0,
    outputs: options.partitionCount,
    inputSize: options.inputSizeKiB ?? 64,
    outputSize: options.outputSizeKiB ?? portKiB(OUTPUT_SLOTS, options.partitionCount),
    wasm: options.wasm,
    graph: { program: options.program, scratchBytes: options.scratchBytes },
    readAt: options.readAt,
    // Prefer scheduler.wait/setTimeout so queued DO RPCs can enter between steps.
    yieldToEventLoop: options.yieldToEventLoop,
  });
  const onAbort = () => pool.abort(options.signal?.reason ?? new Error("scan aborted"));
  if (options.signal?.aborted) onAbort();
  options.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const forwards = Array.from({ length: options.partitionCount }, (_, partition) =>
      hashScanOutputStream(pool.outputStreams[partition]!, (body) => options.onPartitionStream(partition, body)),
    );
    // A scan whose pool has finished still waits for each edge to be taken. If one is
    // refused, the abort is the only news of it: fail now rather than wait for a reader
    // that will never come (the job's stall timeout).
    await Promise.race([Promise.all([pool.finished, ...forwards]), rejectOnAbort(options.signal)]);
    return pool.stats();
  } finally {
    options.signal?.removeEventListener("abort", onAbort);
    await pool.close();
  }
}

/** Rejects with the signal's reason once it aborts; never settles without one. */
function rejectOnAbort(signal: AbortSignal | undefined): Promise<never> {
  return new Promise<never>((_, reject) => {
    if (!signal) return;
    const abort = () => reject(signal.reason ?? new Error("scan aborted"));
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
  });
}
