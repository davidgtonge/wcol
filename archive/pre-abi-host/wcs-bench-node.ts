/**
 * Milestone B: inter-DO byte stream with bounded buffering (WCS1 transport layer).
 */

import {
  createBoundedBytePipe,
  drainReadable,
  pumpSyntheticBytes,
  type BoundedBytePipeMetrics,
} from "../../src/runtime/host/ports/rpc.ts";
import { startInterDoBoundedWcsPipe, runInterDoWcsConsumer, interDoRunIdFromResponse } from "./inter-do-wcs-pipe.ts";
import {
  startWcs1BoundedPipe,
  startWcs1PartialBoundedPipe,
  WCS_VALIDATE_AND_COPY_PLAN,
} from "./wcs-frame-codec.ts";
import { base64ToBytes, pipeWorkerShardWcsToWritable } from "./cf-shard-job.ts";
import {
  WCS_EGRESS_METRICS,
  WCS_EGRESS_METRICS_BENCH,
  WCS_EGRESS_SHARD,
  WCS_INGRESS_SHARD_RPC,
} from "./inter-do-wcs-paths.ts";
import {
  consumeInterDoRpcShardWcs,
  type InterDoShardWcsProducerRpc,
  type InterDoRawByteProducerRpc,
} from "./inter-do-wcs-pipe.ts";
import { loadNodeWasm } from "../../cloudflare/src/wasm.ts";

type DurableObjectNamespace = {
  idFromName(name: string): unknown;
  get(id: unknown): { fetch(input: string, init?: RequestInit): Promise<Response> };
};

export type WcsBenchEnv = {
  WCOL_NODE: DurableObjectNamespace;
};

export type WcsBenchProducerCtx = {
  storage: {
    get(key: string): Promise<unknown>;
    put(key: string, value: unknown): Promise<unknown>;
  };
  waitUntil(p: Promise<unknown>): void;
};

export type StoredWcsBenchMetrics = BoundedBytePipeMetrics & {
  runId: string;
  bufferCap: number;
  chunkBytes: number;
  kind: "raw" | "wcs1";
};

export function wcsBenchMetricsStorageKey(runId: string): string {
  return `wcsBenchMetrics:${runId}`;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function persistBenchMetrics(ctx: WcsBenchProducerCtx, metrics: StoredWcsBenchMetrics): void {
  ctx.waitUntil(ctx.storage.put(wcsBenchMetricsStorageKey(metrics.runId), metrics));
}

/** Producer: GET /wcs/bench/source?bytes=&bufferCap=&chunk=&runId= */
export async function handleWcsBenchSource(
  request: Request,
  ctx?: WcsBenchProducerCtx,
): Promise<Response> {
  const url = new URL(request.url);
  const totalBytes = Number(url.searchParams.get("bytes") ?? 4 * 1024 * 1024);
  const bufferCap = Number(url.searchParams.get("bufferCap") ?? 256 * 1024);
  const chunkSize = Number(url.searchParams.get("chunk") ?? 64 * 1024);
  const runId = url.searchParams.get("runId") ?? crypto.randomUUID();
  if (!Number.isFinite(totalBytes) || totalBytes < 1 || totalBytes > 512 * 1024 * 1024) {
    return json({ error: "invalid bytes" }, 400);
  }
  const pipe = createBoundedBytePipe(bufferCap);
  const pump = (async () => {
    try {
      await pumpSyntheticBytes(pipe.writable, totalBytes, chunkSize);
    } catch {
      /* consumer may abort */
    }
    if (ctx) {
      const m = pipe.metrics();
      persistBenchMetrics(ctx, {
        runId,
        bufferCap,
        chunkBytes: chunkSize,
        kind: "raw",
        ...m,
      });
    }
  })();
  if (ctx) ctx.waitUntil(pump);
  else void pump;
  return new Response(pipe.readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-bench-bytes": String(totalBytes),
      "x-wcol-buffer-cap": String(bufferCap),
      "x-wcol-run-id": runId,
    },
  });
}

/** Producer: GET /wcs/bench/metrics?runId= */
export async function handleWcsBenchMetrics(
  request: Request,
  ctx: WcsBenchProducerCtx,
): Promise<Response> {
  const runId = new URL(request.url).searchParams.get("runId");
  if (!runId) return json({ error: "runId required" }, 400);
  const stored = (await ctx.storage.get(wcsBenchMetricsStorageKey(runId))) as StoredWcsBenchMetrics | undefined;
  if (!stored) return json({ error: "metrics not ready" }, 404);
  return json(stored);
}

type DrainBody = {
  producerNode?: string;
  bytes?: number;
  bufferCap?: number;
  chunk?: number;
  delayMs?: number;
};

export function producerPeakWithinCap(
  peakBufferedBytes: number,
  bufferCap: number,
  chunkBytes: number,
): boolean {
  return peakBufferedBytes <= bufferCap + chunkBytes;
}

async function readProducerMetrics(
  producer: ReturnType<DurableObjectNamespace["get"]>,
  runId: string,
): Promise<StoredWcsBenchMetrics | null> {
  for (let attempt = 0; attempt < 120; attempt++) {
    const response = await producer.fetch(
      `http://do${WCS_EGRESS_METRICS}?runId=${encodeURIComponent(runId)}`,
    );
    if (response.status === 404) {
      const legacy = await producer.fetch(
        `http://do${WCS_EGRESS_METRICS_BENCH}?runId=${encodeURIComponent(runId)}`,
      );
      if (legacy.status === 404) {
        await new Promise((r) => setTimeout(r, 25));
        continue;
      }
      if (!legacy.ok) return null;
      return (await legacy.json()) as StoredWcsBenchMetrics;
    }
    if (!response.ok) return null;
    return (await response.json()) as StoredWcsBenchMetrics;
  }
  return null;
}

/** Consumer: POST /wcs/bench/drain — pulls from another WcolNode source stream. */
export async function handleWcsBenchDrain(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as DrainBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bytes = body.bytes ?? 4 * 1024 * 1024;
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const chunk = body.chunk ?? 64 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = crypto.randomUUID();
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const started = Date.now();
  const response = await producer.fetch(
    `http://do/wcs/bench/source?bytes=${bytes}&bufferCap=${bufferCap}&chunk=${chunk}&runId=${encodeURIComponent(runId)}`,
  );
  if (!response.ok || !response.body) {
    return json({ error: await response.text() }, 502);
  }
  const received = await drainReadable(response.body, { delayMs: delayMs > 0 ? delayMs : undefined });
  const producerMetrics = await readProducerMetrics(producer, runId);
  const peakBufferedBytes = producerMetrics?.peakBufferedBytes ?? null;
  const backpressureOk =
    peakBufferedBytes == null
      ? false
      : producerPeakWithinCap(peakBufferedBytes, bufferCap, chunk);
  return json({
    producerNode,
    runId,
    expectedBytes: bytes,
    receivedBytes: received,
    ok: received === bytes && backpressureOk,
    wallMs: Date.now() - started,
    bufferCap,
    chunkBytes: chunk,
    delayMs,
    producerPeakBufferedBytes: peakBufferedBytes,
    producerBytesWritten: producerMetrics?.bytesWritten ?? null,
    backpressureOk,
  });
}

/** Producer: GET /wcs/bench/wcs-source — valid WCS1 frames through bounded pipe. */
export async function handleWcsBenchSourceWcs(
  request: Request,
  ctx?: WcsBenchProducerCtx,
): Promise<Response> {
  const url = new URL(request.url);
  const totalBytes = Number(url.searchParams.get("bytes") ?? 4 * 1024 * 1024);
  const bufferCap = Number(url.searchParams.get("bufferCap") ?? 256 * 1024);
  const batch = Number(url.searchParams.get("batch") ?? 64 * 1024);
  const runId = url.searchParams.get("runId") ?? crypto.randomUUID();
  if (!Number.isFinite(totalBytes) || totalBytes < 1 || totalBytes > 512 * 1024 * 1024) {
    return json({ error: "invalid bytes" }, 400);
  }
  const { readable, whenDone } = startWcs1BoundedPipe(totalBytes, bufferCap, batch);
  if (ctx) {
    ctx.waitUntil(
      whenDone.then((m) =>
        persistBenchMetrics(ctx, {
          runId,
          bufferCap,
          chunkBytes: batch,
          kind: "wcs1",
          ...m,
        }),
      ),
    );
  }
  return new Response(readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-wcs1-bytes": String(totalBytes),
      "x-wcol-run-id": runId,
    },
  });
}

/** Consumer: POST /wcs/bench/consume-wcs — WCS1 guard + copy in Rust (`PLAN_BYTE_PIPE`). */
export async function handleWcsBenchConsumeWcs(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as DrainBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bytes = body.bytes ?? 4 * 1024 * 1024;
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const batch = body.chunk ?? 64 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = crypto.randomUUID();
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const started = Date.now();
  const response = await producer.fetch(
    `http://do/wcs/bench/wcs-source?bytes=${bytes}&bufferCap=${bufferCap}&batch=${batch}&runId=${encodeURIComponent(runId)}`,
  );
  if (!response.ok || !response.body) {
    return json({ error: await response.text() }, 502);
  }
  let bodyStream = response.body;
  if (delayMs > 0) {
    bodyStream = slowReadable(response.body, delayMs);
  }
  const wasm = await loadNodeWasm();
  const stats = await runInterDoWcsConsumer(wasm, WCS_VALIDATE_AND_COPY_PLAN, bodyStream);
  const producerMetrics = await readProducerMetrics(producer, interDoRunIdFromResponse(response, runId));
  const peakBufferedBytes = producerMetrics?.peakBufferedBytes ?? null;
  const backpressureOk =
    peakBufferedBytes == null
      ? false
      : producerPeakWithinCap(peakBufferedBytes, bufferCap, batch);
  const wireBytes = stats.inputBytes;
  const copiedBytes = stats.outputBytes;
  return json({
    producerNode,
    runId,
    payloadBytes: bytes,
    wireBytes,
    copiedBytes,
    inputBytes: stats.inputBytes,
    outputBytes: stats.outputBytes,
    ok: copiedBytes === wireBytes && wireBytes > bytes && backpressureOk,
    wallMs: Date.now() - started,
    bufferCap,
    chunkBytes: batch,
    delayMs,
    producerPeakBufferedBytes: peakBufferedBytes,
    backpressureOk,
    validator: "rust-wcs-guard-copy",
  });
}

type ShardWcsBody = DrainBody & {
  url?: string;
  wireProgram?: string;
  runId?: string;
};

export type ShardWcsEgressParams = {
  url: string;
  wireProgram: string;
  bufferCap?: number;
  runId?: string;
};

/** Shared producer: bounded pipe + GraphHost WorkerShard WCS bytes. */
export function beginShardWcsInterDoSession(
  body: ShardWcsEgressParams,
  ctx?: WcsBenchProducerCtx,
) {
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const runId = body.runId ?? crypto.randomUUID();
  const wasmPromise = loadNodeWasm();
  const session = startInterDoBoundedWcsPipe({
    bufferCap,
    runId,
    responseHeaders: { "x-wcol-graphhost-wcs": "1" },
    produce: async (writable) => {
      const wasm = await wasmPromise;
      await pipeWorkerShardWcsToWritable(
        wasm,
        { transport: "http", url: body.url },
        base64ToBytes(body.wireProgram),
        writable,
      );
    },
  });
  if (ctx) {
    ctx.waitUntil(
      session.finished.then((m) =>
        persistBenchMetrics(ctx, {
          runId: session.runId,
          bufferCap,
          chunkBytes: 64 * 1024,
          kind: "wcs1",
          ...m,
        }),
      ),
    );
  }
  return session;
}

/** Milestone F: RPC producer — same bytes as `POST` `WCS_EGRESS_SHARD`. */
export async function openShardWcsEgressRpc(
  ctx: WcsBenchProducerCtx,
  params: ShardWcsEgressParams,
): Promise<{ runId: string; bufferCap: number; body: ReadableStream<Uint8Array> }> {
  const session = beginShardWcsInterDoSession(params, ctx);
  const body = session.response.body;
  if (!body) throw new Error("shard WCS egress missing body");
  return { runId: session.runId, bufferCap: session.bufferCap, body };
}

export type RawByteEgressParams = {
  totalBytes: number;
  bufferCap?: number;
  chunk?: number;
  runId?: string;
};

/** Milestone F: RPC producer for raw byte soak (no GraphHost). */
export async function openRawByteEgressRpc(
  ctx: WcsBenchProducerCtx,
  params: RawByteEgressParams,
): Promise<{ runId: string; bufferCap: number; body: ReadableStream<Uint8Array> }> {
  const totalBytes = params.totalBytes;
  const bufferCap = params.bufferCap ?? 256 * 1024;
  const chunkSize = params.chunk ?? 64 * 1024;
  const runId = params.runId ?? crypto.randomUUID();
  if (!Number.isFinite(totalBytes) || totalBytes < 1 || totalBytes > 512 * 1024 * 1024) {
    throw new Error("invalid totalBytes");
  }
  const session = startInterDoBoundedWcsPipe({
    bufferCap,
    runId,
    produce: (writable) => pumpSyntheticBytes(writable, totalBytes, chunkSize),
  });
  ctx.waitUntil(
    session.finished.then((m) =>
      persistBenchMetrics(ctx, {
        runId: session.runId,
        bufferCap,
        chunkBytes: chunkSize,
        kind: "raw",
        ...m,
      }),
    ),
  );
  const body = session.response.body;
  if (!body) throw new Error("raw byte egress missing body");
  return { runId: session.runId, bufferCap: session.bufferCap, body };
}

/** Producer: POST /wcs/bench/shard-wcs-source — real GraphHost WCS from one WorkerShard plan. */
export async function handleWcsBenchShardWcsSource(
  request: Request,
  ctx?: WcsBenchProducerCtx,
): Promise<Response> {
  const body = (await request.json()) as ShardWcsBody;
  const url = body.url;
  const wireProgram = body.wireProgram;
  if (!url || !wireProgram) return json({ error: "url and wireProgram required" }, 400);
  const session = beginShardWcsInterDoSession(
    {
      url,
      wireProgram,
      bufferCap: body.bufferCap,
      runId: body.runId,
    },
    ctx,
  );
  return session.response;
}

/** Producer: GET /wcs/bench/partial-wcs-source — synthetic FRAME_PARTIAL stream. */
export async function handleWcsBenchPartialWcsSource(
  request: Request,
  ctx?: WcsBenchProducerCtx,
): Promise<Response> {
  const url = new URL(request.url);
  const totalBytes = Number(url.searchParams.get("bytes") ?? 256 * 1024);
  const bufferCap = Number(url.searchParams.get("bufferCap") ?? 256 * 1024);
  const batch = Number(url.searchParams.get("batch") ?? 8 * 1024);
  const runId = url.searchParams.get("runId") ?? crypto.randomUUID();
  if (!Number.isFinite(totalBytes) || totalBytes < 1) return json({ error: "invalid bytes" }, 400);
  const { readable, whenDone } = startWcs1PartialBoundedPipe(totalBytes, bufferCap, batch);
  if (ctx) {
    ctx.waitUntil(
      whenDone.then((m) =>
        persistBenchMetrics(ctx, {
          runId,
          bufferCap,
          chunkBytes: batch,
          kind: "wcs1",
          ...m,
        }),
      ),
    );
  }
  return new Response(readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-wcs1-partial-bytes": String(totalBytes),
      "x-wcol-run-id": runId,
    },
  });
}

/** Consumer: POST /wcs/bench/consume-shard-wcs — GraphHost wire from remote shard. */
export async function handleWcsBenchConsumeShardWcs(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as ShardWcsBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = crypto.randomUUID();
  if (!body.url || !body.wireProgram) return json({ error: "url and wireProgram required" }, 400);
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const started = Date.now();
  const response = await producer.fetch(`http://do${WCS_EGRESS_SHARD}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ...body, runId, bufferCap }),
  });
  return consumeWcsThroughRust(
    producer,
    runId,
    response,
    started,
    bufferCap,
    64 * 1024,
    delayMs,
    "graphhost-shard",
    WCS_VALIDATE_AND_COPY_PLAN,
  );
}

/** Consumer: POST `/wcs/ingress/shard-rpc` — same as fetch ingress, producer via Workers RPC. */
export async function handleWcsIngressShardRpc(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as ShardWcsBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = body.runId ?? crypto.randomUUID();
  if (!body.url || !body.wireProgram) return json({ error: "url and wireProgram required" }, 400);
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode)) as unknown as InterDoShardWcsProducerRpc;
  const started = Date.now();
  const wasm = await loadNodeWasm();
  const stats = await consumeInterDoRpcShardWcs(wasm, WCS_VALIDATE_AND_COPY_PLAN, producer, {
    url: body.url,
    wireProgram: body.wireProgram,
    bufferCap,
    runId,
  });
  let peakBufferedBytes: number | null = null;
  const metricsProducer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const producerMetrics = await readProducerMetrics(metricsProducer, stats.runId ?? runId);
  peakBufferedBytes = producerMetrics?.peakBufferedBytes ?? null;
  const backpressureOk =
    peakBufferedBytes == null
      ? stats.inputBytes <= bufferCap + 64 * 1024
      : producerPeakWithinCap(peakBufferedBytes, bufferCap, 64 * 1024);
  return json({
    runId: stats.runId ?? runId,
    wireBytes: stats.inputBytes,
    copiedBytes: stats.outputBytes,
    ok: stats.outputBytes === stats.inputBytes && stats.inputBytes > 0 && backpressureOk,
    wallMs: Date.now() - started,
    bufferCap,
    chunkBytes: 64 * 1024,
    delayMs,
    producerPeakBufferedBytes: peakBufferedBytes,
    backpressureOk,
    validator: "graphhost-shard-rpc",
    transport: "native_rpc_wcs",
  });
}

type RpcPipeBody = DrainBody & { producerNode?: string };

/** Consumer: POST `/wcs/bench/rpc-pipe` — drain raw bytes from RPC producer (CI smoke). */
export async function handleWcsBenchRpcPipe(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as RpcPipeBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bytes = body.bytes ?? 512 * 1024;
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const chunk = body.chunk ?? 64 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = crypto.randomUUID();
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode)) as unknown as InterDoRawByteProducerRpc;
  const started = Date.now();
  const handle = await producer.openInterDoRawByteEgress({
    totalBytes: bytes,
    bufferCap,
    chunk,
    runId,
  });
  let stream = handle.body;
  if (delayMs > 0) stream = slowReadable(stream, delayMs);
  const received = await drainReadable(stream);
  const metricsProducer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const producerMetrics = await readProducerMetrics(metricsProducer, handle.runId);
  const peakBufferedBytes = producerMetrics?.peakBufferedBytes ?? null;
  const backpressureOk =
    peakBufferedBytes == null
      ? false
      : producerPeakWithinCap(peakBufferedBytes, bufferCap, chunk);
  return json({
    producerNode,
    runId: handle.runId,
    expectedBytes: bytes,
    receivedBytes: received,
    ok: received === bytes && backpressureOk,
    wallMs: Date.now() - started,
    bufferCap,
    chunkBytes: chunk,
    delayMs,
    producerPeakBufferedBytes: peakBufferedBytes,
    backpressureOk,
    transport: "native_rpc_wcs",
  });
}

/** Consumer: POST /wcs/bench/consume-partial-wcs — synthetic PARTIAL frames. */
export async function handleWcsBenchConsumePartialWcs(request: Request, env: WcsBenchEnv): Promise<Response> {
  const body = (await request.json()) as DrainBody;
  const producerNode = body.producerNode ?? "wcs-producer";
  const bytes = body.bytes ?? 256 * 1024;
  const bufferCap = body.bufferCap ?? 256 * 1024;
  const batch = body.chunk ?? 8 * 1024;
  const delayMs = body.delayMs ?? 0;
  const runId = crypto.randomUUID();
  const producer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName(producerNode));
  const started = Date.now();
  const response = await producer.fetch(
    `http://do/wcs/bench/partial-wcs-source?bytes=${bytes}&bufferCap=${bufferCap}&batch=${batch}&runId=${encodeURIComponent(runId)}`,
  );
  return consumeWcsThroughRust(
    producer,
    runId,
    response,
    started,
    bufferCap,
    batch,
    delayMs,
    "synthetic-partial",
    WCS_VALIDATE_AND_COPY_PLAN,
  );
}

async function consumeWcsThroughRust(
  producer: ReturnType<DurableObjectNamespace["get"]>,
  runId: string,
  response: Response,
  started: number,
  bufferCap: number,
  chunkBytes: number,
  delayMs: number,
  validator: string,
  plan: Uint8Array,
): Promise<Response> {
  if (!response.ok || !response.body) {
    return json({ error: await response.text() }, 502);
  }
  const metricsRunId = interDoRunIdFromResponse(response, runId);
  let bodyStream = response.body;
  if (delayMs > 0) bodyStream = slowReadable(response.body, delayMs);
  const wasm = await loadNodeWasm();
  const stats = await runInterDoWcsConsumer(wasm, plan, bodyStream);
  const producerMetrics = await readProducerMetrics(producer, metricsRunId);
  const peakBufferedBytes = producerMetrics?.peakBufferedBytes ?? null;
  const backpressureOk =
    peakBufferedBytes == null
      ? stats.inputBytes <= bufferCap + chunkBytes
      : producerPeakWithinCap(peakBufferedBytes, bufferCap, chunkBytes);
  return json({
    runId: metricsRunId || runId,
    wireBytes: stats.inputBytes,
    copiedBytes: stats.outputBytes,
    ok: stats.outputBytes === stats.inputBytes && stats.inputBytes > 0 && backpressureOk,
    wallMs: Date.now() - started,
    bufferCap,
    chunkBytes,
    delayMs,
    producerPeakBufferedBytes: peakBufferedBytes,
    backpressureOk,
    validator,
  });
}

function slowReadable(source: ReadableStream<Uint8Array>, delayMs: number): ReadableStream<Uint8Array> {
  const reader = source.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) {
        controller.close();
        return;
      }
      await new Promise((r) => setTimeout(r, delayMs));
      controller.enqueue(value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}
