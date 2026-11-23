import { DurableObject } from "cloudflare:workers";
import nodeModule from "../dist/wcol_node.wasm";
import { acceptScanEdgeOnNode, eventLoopYield } from "../src/node/run.ts";
import { instantiateWorkerWasm } from "../src/wasm.ts";
import { createBoundedBytePipe } from "../../src/runtime/host/ports/rpc.ts";
import { createReducerIngressSession } from "../../src/runtime/host/scan-edge-session.ts";
import { createPoolInputLatch, createStreamPool, type StreamPool } from "../../src/runtime/host/stream-pool.ts";
import type { AcceptScanEdgeParams, HashIngressHolder } from "../src/node/types.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

type Variant = "A" | "B" | "C" | "D";

type Env = {
  RELAY_SCAN: DurableObjectNamespace<RelayScan>;
  RELAY_REDUCE: DurableObjectNamespace<RelayReduce>;
};

const JOB_ID = "reducer-relay";
const ATTEMPT = 1;
const SCANNERS = 4;

type EdgeTrace = {
  instanceBootId: string;
  variant: Variant;
  scanIndex: number;
  session: string;
  wasmBytes: number;
  handlerEntered: number;
  registered?: number;
  firstRead?: number;
  firstBytes?: number;
  handlerReturned?: number;
  handlerRejected?: string;
  streamEnded?: number;
  streamError?: string | null;
  bytes: number;
  stepsAtEnd?: number;
};

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function observe(source: ReadableStream<Uint8Array>, trace: EdgeTrace): ReadableStream<Uint8Array> {
  return source.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      if (!trace.firstBytes) trace.firstBytes = Date.now();
      trace.bytes += chunk.byteLength;
      controller.enqueue(chunk);
    },
  }));
}

export class RelayReduce extends DurableObject<Env> {
  private readonly instanceBootId = crypto.randomUUID();
  private variant: Variant = "A";
  private wasm: WcolStreamWasm | null = null;
  private holder: HashIngressHolder = { session: null };
  private pool: StreamPool | null = null;
  private readonly traces: EdgeTrace[] = [];

  private sessionName(): string {
    return `${JOB_ID}@${ATTEMPT}`;
  }

  private wasmBytes(): number {
    return this.wasm?.memory.buffer.byteLength ?? 0;
  }

  async prepare(variant: Variant): Promise<Record<string, unknown>> {
    this.variant = variant;
    this.holder.session = createReducerIngressSession({
      jobId: JOB_ID,
      executionAttempt: ATTEMPT,
      expectedScans: SCANNERS,
    });
    this.holder.session.readyToAccept();
    if (variant === "B" || variant === "C" || variant === "D") {
      this.wasm = await instantiateWorkerWasm(nodeModule as WebAssembly.Module, "node");
    }
    if (variant === "C" || variant === "D") {
      const latch = createPoolInputLatch();
      this.holder.stream = latch;
      this.pool = createStreamPool({
        inputs: SCANNERS,
        outputs: 1,
        inputSize: 64,
        outputSize: 64,
        wasm: this.wasm!,
        yieldToEventLoop: eventLoopYield,
      });
      latch.bind(this.pool);
      const output = this.pool.outputStreams[0]!;
      this.ctx.waitUntil((async () => {
        const reader = output.getReader();
        try {
          for (;;) {
            const next = await reader.read();
            if (next.done) break;
          }
        } catch {
          // The pool reports its own failure on the edge that fed it.
        }
      })());
      await eventLoopYield();
    }
    return {
      instanceBootId: this.instanceBootId,
      variant,
      wasmBytes: this.wasmBytes(),
      steps: this.pool?.diagnostics().steps ?? 0,
      session: this.sessionName(),
    };
  }

  async accept(params: AcceptScanEdgeParams): Promise<EdgeTrace> {
    const trace: EdgeTrace = {
      instanceBootId: this.instanceBootId,
      variant: this.variant,
      scanIndex: params.scanIndex,
      session: this.sessionName(),
      wasmBytes: this.wasmBytes(),
      handlerEntered: Date.now(),
      bytes: 0,
    };
    this.traces.push(trace);
    const stream = observe(params.stream, trace);
    try {
      if (this.variant === "C") {
        trace.registered = Date.now();
        trace.firstRead = Date.now();
        await acceptScanEdgeOnNode(this.holder, { ...params, stream });
        trace.handlerReturned = Date.now();
        trace.streamEnded = trace.handlerReturned;
      } else if (this.variant === "D") {
        await this.registerAndReturn(params, stream, trace);
      } else {
        await this.holdWithoutPool(params, stream, trace);
      }
      trace.stepsAtEnd = Number(this.pool?.diagnostics().steps ?? 0);
      trace.wasmBytes = this.wasmBytes();
      return trace;
    } catch (error) {
      trace.handlerRejected = messageOf(error);
      trace.streamError = trace.handlerRejected;
      trace.streamEnded = Date.now();
      trace.stepsAtEnd = Number(this.pool?.diagnostics().steps ?? 0);
      trace.wasmBytes = this.wasmBytes();
      throw error;
    }
  }

  /** A and B: same relay as production, but nothing is attached to a Wasm pool. */
  private async holdWithoutPool(params: AcceptScanEdgeParams, stream: ReadableStream<Uint8Array>, trace: EdgeTrace): Promise<void> {
    const session = this.holder.session;
    if (!session) throw new Error("reducer ingress session is missing");
    const relay = new TransformStream<Uint8Array, Uint8Array>();
    const consumed = stream.pipeTo(relay.writable);
    session.acceptScanEdge(params.scanIndex, relay.readable);
    trace.registered = Date.now();
    const reader = relay.readable.getReader();
    trace.firstRead = Date.now();
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
      }
      await consumed;
      trace.handlerReturned = Date.now();
      trace.streamEnded = trace.handlerReturned;
    } catch (error) {
      trace.streamError = messageOf(error);
      throw error;
    }
  }

  /**
   * D: register the stream with the live pool, then return. Consumption stays
   * owned by this object through waitUntil, not by the RPC handler.
   */
  private async registerAndReturn(params: AcceptScanEdgeParams, stream: ReadableStream<Uint8Array>, trace: EdgeTrace): Promise<void> {
    const session = this.holder.session;
    if (!session) throw new Error("reducer ingress session is missing");
    const relay = new TransformStream<Uint8Array, Uint8Array>();
    const edge = new AbortController();
    this.holder.edges ??= new Set();
    this.holder.edges.add(edge);
    const consumed = stream.pipeTo(relay.writable, { signal: edge.signal });
    session.acceptScanEdge(params.scanIndex, relay.readable);
    this.holder.stream?.attachInput(params.scanIndex, relay.readable);
    trace.registered = Date.now();
    trace.firstRead = Date.now();
    const owned = consumed.then(
      () => {
        trace.streamEnded = Date.now();
        trace.stepsAtEnd = Number(this.pool?.diagnostics().steps ?? 0);
        trace.wasmBytes = this.wasmBytes();
      },
      (error: unknown) => {
        trace.streamError = messageOf(error);
        trace.streamEnded = Date.now();
        trace.stepsAtEnd = Number(this.pool?.diagnostics().steps ?? 0);
        trace.wasmBytes = this.wasmBytes();
      },
    ).finally(() => {
      this.holder.edges?.delete(edge);
    });
    this.ctx.waitUntil(owned);
    trace.handlerReturned = Date.now();
  }

  async report(): Promise<Record<string, unknown>> {
    const deadline = Date.now() + 5_000;
    while (this.traces.some((trace) => !trace.streamEnded) && Date.now() < deadline) {
      await eventLoopYield();
    }
    return {
      instanceBootId: this.instanceBootId,
      variant: this.variant,
      session: this.sessionName(),
      wasmBytes: this.wasmBytes(),
      steps: this.pool?.diagnostics().steps ?? 0,
      traces: this.traces,
    };
  }
}

export class RelayScan extends DurableObject<Env> {
  async run(params: { reducerNames: string[]; scanIndex: number; chunks: number; chunkBytes: number }): Promise<Record<string, unknown>> {
    const edges = await Promise.all(params.reducerNames.map((name, partition) => this.one(name, partition, params)));
    return { edges };
  }

  private async one(
    name: string,
    partition: number,
    params: { scanIndex: number; chunks: number; chunkBytes: number },
  ): Promise<Record<string, unknown>> {
    const stub = this.env.RELAY_REDUCE.get(this.env.RELAY_REDUCE.idFromName(name));
    const pipe = createBoundedBytePipe({ maxBufferedBytes: 128 * 1024, owner: `relay-p${partition}` });
    const writer = pipe.writable.getWriter();
    const acceptPromise = stub.accept({
      jobId: JOB_ID,
      executionAttempt: ATTEMPT,
      scanTaskId: params.scanIndex,
      scanIndex: params.scanIndex,
      partition,
      stream: pipe.readable,
    });
    let produceError: string | null = null;
    try {
      for (let index = 0; index < params.chunks; index += 1) {
        await writer.write(new Uint8Array(params.chunkBytes));
      }
      await writer.close();
    } catch (error) {
      produceError = messageOf(error);
      await writer.abort(error).catch(() => undefined);
    }
    let acceptError: string | null = null;
    let trace: EdgeTrace | null = null;
    try {
      trace = await acceptPromise;
    } catch (error) {
      acceptError = messageOf(error);
    }
    return { partition, scanIndex: params.scanIndex, produceError, acceptError, trace };
  }
}

function clamp(value: string | null, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return Math.min(parsed, max);
}

function variantOf(value: string | null): Variant {
  const upper = (value ?? "A").toUpperCase();
  if (upper === "B" || upper === "C" || upper === "D") return upper;
  return "A";
}

function summarize(
  variant: Variant,
  scans: Array<{ edges?: Array<Record<string, unknown>> }>,
  reports: Array<Record<string, unknown>>,
  prepareBoots: string[],
): Record<string, unknown> {
  const errors = new Map<string, number>();
  let produceFailed = 0;
  for (const scan of scans) {
    for (const edge of scan.edges ?? []) {
      const problem = (edge.produceError as string | null) ?? (edge.acceptError as string | null);
      if (!problem) continue;
      produceFailed += 1;
      errors.set(problem, (errors.get(problem) ?? 0) + 1);
    }
  }
  let ok = 0;
  let failed = 0;
  let bootMismatch = 0;
  let bytes = 0;
  let unfinished = 0;
  let edges = 0;
  let sample: Record<string, unknown> | null = null;
  reports.forEach((report, index) => {
    const boot = String(report.instanceBootId ?? "");
    if (prepareBoots[index] && boot && prepareBoots[index] !== boot) bootMismatch += 1;
    for (const trace of (report.traces as EdgeTrace[] | undefined) ?? []) {
      edges += 1;
      bytes += trace.bytes;
      if (trace.instanceBootId && boot && trace.instanceBootId !== boot) bootMismatch += 1;
      const problem = trace.handlerRejected ?? trace.streamError ?? null;
      if (problem) {
        failed += 1;
        errors.set(problem, (errors.get(problem) ?? 0) + 1);
        sample ??= { problem, trace, reportBoot: boot };
      } else if (!trace.streamEnded) unfinished += 1;
      else ok += 1;
    }
  });
  return {
    variant,
    edges,
    ok,
    failed,
    produceFailed,
    unfinished,
    bootMismatch,
    bytes,
    errors: Object.fromEntries(errors),
    sample,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/run") return new Response("wcol reducer relay\n");
    const variant = variantOf(url.searchParams.get("variant"));
    const reducers = clamp(url.searchParams.get("reducers"), 64, 64);
    const scanners = clamp(url.searchParams.get("scanners"), SCANNERS, SCANNERS);
    const chunks = clamp(url.searchParams.get("chunks"), 2, 4);
    const chunkBytes = clamp(url.searchParams.get("chunkBytes"), 65536, 65536);
    const run = crypto.randomUUID().slice(0, 8);
    const names = Array.from({ length: reducers }, (_, index) => `${run}-r${index}`);
    const started = Date.now();
    const prepared = await Promise.all(names.map(async (name) => {
      const stub = env.RELAY_REDUCE.get(env.RELAY_REDUCE.idFromName(name));
      return stub.prepare(variant);
    }));
    const scans = await Promise.all(Array.from({ length: scanners }, (_, index) => {
      const stub = env.RELAY_SCAN.get(env.RELAY_SCAN.idFromName(`${run}-s${index}`));
      return stub.run({ reducerNames: names, scanIndex: index, chunks, chunkBytes });
    }));
    const reports = await Promise.all(names.map((name) => env.RELAY_REDUCE.get(env.RELAY_REDUCE.idFromName(name)).report()));
    const summary = summarize(variant, scans, reports, prepared.map((item) => String(item.instanceBootId ?? "")));
    const prepareSteps = prepared.map((item) => Number(item.steps ?? 0));
    return Response.json({
      ...summary,
      wallMs: Date.now() - started,
      prepareWasmBytes: prepared[0]?.wasmBytes ?? 0,
      prepareStepsMin: Math.min(...prepareSteps),
      prepareStepsMax: Math.max(...prepareSteps),
      prepareBoot: prepared[0]?.instanceBootId ?? "",
    });
  },
};
