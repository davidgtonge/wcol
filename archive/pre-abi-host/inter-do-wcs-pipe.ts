/**
 * Milestone B / F: bounded WCS1 byte stream between Durable Objects via `fetch` + `ReadableStream`.
 * Future: swap consumer `fetch(body)` for Workers RPC native byte streams without changing GraphHost.
 */

import { createBoundedBytePipe, type BoundedBytePipeMetrics } from "../../src/runtime/host/ports/rpc.ts";
import { runWcolStream, type StreamRunStats, type WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

export type InterDoBoundedWcsSession = {
  runId: string;
  bufferCap: number;
  /** HTTP response body for the consumer DO `fetch`. */
  response: Response;
  /** Resolves when the producer finished pumping (or the consumer aborted). */
  finished: Promise<BoundedBytePipeMetrics>;
};

/**
 * Start a producer-side bounded pipe. The caller should `waitUntil(finished)` and return `response`.
 */
export function startInterDoBoundedWcsPipe(options: {
  bufferCap: number;
  runId?: string;
  /** Pump WCS1 (or raw) bytes into the bounded writable side. */
  produce: (writable: WritableStream<Uint8Array>) => Promise<void>;
  responseHeaders?: Record<string, string>;
}): InterDoBoundedWcsSession {
  const runId = options.runId ?? crypto.randomUUID();
  const pipe = createBoundedBytePipe(options.bufferCap);
  const finished = (async () => {
    try {
      await options.produce(pipe.writable);
    } catch {
      /* consumer may cancel the readable side */
    }
    return pipe.metrics();
  })();
  const response = new Response(pipe.readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-run-id": runId,
      "x-wcol-buffer-cap": String(options.bufferCap),
      ...options.responseHeaders,
    },
  });
  return { runId, bufferCap: options.bufferCap, response, finished };
}

/** Consumer helper: pull an inter-DO producer URL and return the response body stream. */
export async function fetchInterDoWcsBody(
  producer: { fetch(input: string, init?: RequestInit): Promise<Response> },
  url: string,
  init?: RequestInit,
): Promise<{ runId: string | null; body: ReadableStream<Uint8Array> }> {
  const response = await producer.fetch(url, init);
  if (!response.ok || !response.body) {
    throw new Error(`inter-DO WCS fetch ${response.status}: ${await response.text()}`);
  }
  return {
    runId: response.headers.get("x-wcol-run-id"),
    body: response.body,
  };
}

export type InterDoWcsConsumerResult = StreamRunStats & {
  runId: string | null;
};

/** Run a byte-oriented Wasm plan over an inter-DO WCS (or raw) readable stream. */
export async function runInterDoWcsConsumer(
  wasm: WcolStreamWasm,
  plan: Uint8Array,
  source: ReadableStream<Uint8Array>,
  options?: { scratchBytes?: number; sink?: WritableStream<Uint8Array> },
): Promise<StreamRunStats> {
  const sink = options?.sink ?? new WritableStream<Uint8Array>({ write() {} });
  return runWcolStream(wasm, plan, source, sink, options?.scratchBytes ?? 256 * 1024);
}

/** Consumer: `fetch` producer DO, then `runInterDoWcsConsumer`. */
export async function consumeInterDoFetchWcs(
  wasm: WcolStreamWasm,
  plan: Uint8Array,
  producer: { fetch(input: string, init?: RequestInit): Promise<Response> },
  url: string,
  init?: RequestInit,
  options?: { scratchBytes?: number },
): Promise<InterDoWcsConsumerResult> {
  const { runId, body } = await fetchInterDoWcsBody(producer, url, init);
  const stats = await runInterDoWcsConsumer(wasm, plan, body, options);
  return { ...stats, runId };
}

export function interDoRunIdFromResponse(response: Response, fallback?: string): string {
  return response.headers.get("x-wcol-run-id") ?? fallback ?? "";
}

/** Params for GraphHost WorkerShard WCS producer (RPC or fetch egress). */
export type InterDoShardWcsEgressParams = {
  url: string;
  wireProgram: string;
  bufferCap?: number;
  runId?: string;
};

/** Native RPC return: bounded WCS byte stream + run metadata (no HTTP response wrapper). */
export type InterDoShardWcsEgressHandle = {
  runId: string;
  bufferCap: number;
  body: ReadableStream<Uint8Array>;
};

export type InterDoShardWcsProducerRpc = {
  openInterDoShardWcsEgress(params: InterDoShardWcsEgressParams): Promise<InterDoShardWcsEgressHandle>;
};

export type InterDoRawByteEgressParams = {
  totalBytes: number;
  bufferCap?: number;
  chunk?: number;
  runId?: string;
};

export type InterDoRawByteProducerRpc = {
  openInterDoRawByteEgress(params: InterDoRawByteEgressParams): Promise<InterDoShardWcsEgressHandle>;
};

/** Consumer: Workers RPC producer handle → `runInterDoWcsConsumer`. */
export async function consumeInterDoRpcShardWcs(
  wasm: WcolStreamWasm,
  plan: Uint8Array,
  producer: InterDoShardWcsProducerRpc,
  params: InterDoShardWcsEgressParams,
  options?: { scratchBytes?: number },
): Promise<InterDoWcsConsumerResult> {
  const handle = await producer.openInterDoShardWcsEgress(params);
  const stats = await runInterDoWcsConsumer(wasm, plan, handle.body, options);
  return { ...stats, runId: handle.runId };
}
