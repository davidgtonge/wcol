import { DurableObject } from "cloudflare:workers";
import { createBoundedBytePipe } from "../../src/runtime/host/ports/rpc.ts";

type Mode = "immediate" | "delay" | "held" | "pipe" | "pipe-delay" | "relay";

type AcceptOptions = {
  mode: Mode;
  delayMs: number;
};

type TraceEvent = { event: string; t: number; [key: string]: unknown };

type Env = {
  PROBE_SCAN: DurableObjectNamespace<ProbeScan>;
  PROBE_REDUCE: DurableObjectNamespace<ProbeReduce>;
};

function wait(ms: number): Promise<void> {
  const scheduler = (globalThis as { scheduler?: { wait?: (delay: number) => Promise<void> } }).scheduler;
  if (typeof scheduler?.wait === "function") return scheduler.wait(ms);
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class ProbeReduce extends DurableObject<Env> {
  private readonly incarnation = crypto.randomUUID().slice(0, 8);
  private trace: TraceEvent[] = [];
  private done: Promise<void> | null = null;

  /** Sync slices on this isolate while other requests are queued. Models a Wasm step loop. */
  async busy(ms: number, sliceMs: number): Promise<{ ok: true }> {
    const end = Date.now() + ms;
    const slice = Math.max(1, sliceMs);
    while (Date.now() < end) {
      const until = Math.min(end, Date.now() + slice);
      while (Date.now() < until) {}
      await wait(0);
    }
    return { ok: true };
  }

  async accept(stream: ReadableStream<Uint8Array>, options: AcceptOptions): Promise<Record<string, unknown>> {
    const entered = Date.now();
    this.trace = [{ event: "handlerEntered", t: entered, incarnation: this.incarnation, mode: options.mode }];
    if (options.mode === "relay") {
      const relay = new TransformStream<Uint8Array, Uint8Array>();
      const consumed = stream.pipeTo(relay.writable);
      const reader = relay.readable.getReader();
      let bytes = 0;
      let chunks = 0;
      let sourceCancelled: string | null = null;
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          chunks += 1;
          bytes += next.value.byteLength;
        }
        await consumed;
      } catch (error) {
        sourceCancelled = messageOf(error);
      } finally {
        this.trace.push({ event: "consumerClosed", t: Date.now(), bytes, chunks, sourceCancelled });
        await this.ctx.storage.put("last", { incarnation: this.incarnation, trace: this.trace });
      }
      return { accepted: true, held: true, incarnation: this.incarnation, trace: this.trace };
    }
    const reader = stream.getReader();
    const consume = async (): Promise<void> => {
      if (options.delayMs > 0) await wait(options.delayMs);
      const readStarted = Date.now();
      this.trace.push({ event: "readStarted", t: readStarted });
      let bytes = 0;
      let chunks = 0;
      let firstByte = 0;
      let sourceCancelled: string | null = null;
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          if (!firstByte) firstByte = Date.now();
          chunks += 1;
          bytes += next.value.byteLength;
        }
      } catch (error) {
        sourceCancelled = messageOf(error);
      } finally {
        const consumerClosed = Date.now();
        this.trace.push({ event: "consumerClosed", t: consumerClosed, bytes, chunks, firstByte, sourceCancelled });
        await this.ctx.storage.put("last", { incarnation: this.incarnation, trace: this.trace });
      }
    };
    if (options.mode === "held") {
      await consume();
      this.trace.push({ event: "handlerReturned", t: Date.now() });
      return { accepted: true, held: true, incarnation: this.incarnation, trace: this.trace };
    }
    this.done = consume();
    this.ctx.waitUntil(this.done);
    this.trace.push({ event: "handlerReturned", t: Date.now() });
    return { accepted: true, held: false, incarnation: this.incarnation };
  }

  async report(): Promise<Record<string, unknown>> {
    if (this.done) await this.done;
    const stored = await this.ctx.storage.get("last");
    return { incarnation: this.incarnation, memory: this.trace, stored };
  }
}

export class ProbeScan extends DurableObject<Env> {
  async run(params: {
    reducerNames: string[];
    mode: Mode;
    delayMs: number;
    chunks: number;
    chunkBytes: number;
    gapMs: number;
  }): Promise<Record<string, unknown>> {
    const edges = await Promise.all(params.reducerNames.map((name) => this.one(name, params)));
    return { scan: this.ctx.id.toString().slice(0, 8), edges };
  }

  private async one(name: string, params: {
    mode: Mode;
    delayMs: number;
    chunks: number;
    chunkBytes: number;
    gapMs: number;
  }): Promise<Record<string, unknown>> {
    const stub = this.env.PROBE_REDUCE.get(this.env.PROBE_REDUCE.idFromName(name));
    const piped = params.mode === "pipe" || params.mode === "pipe-delay" || params.mode === "relay";
    const pipe = piped ? createBoundedBytePipe({ maxBufferedBytes: 128 * 1024, owner: "probe" }) : null;
    const plain = piped ? null : new TransformStream<Uint8Array, Uint8Array>();
    const readable = pipe ? pipe.readable : plain!.readable;
    const writer = pipe ? pipe.writable.getWriter() : plain!.writable.getWriter();
    const acceptMode: Mode = params.mode === "pipe" ? "held" : params.mode === "pipe-delay" ? "delay" : params.mode;
    const acceptPromise = stub.accept(readable, { mode: acceptMode, delayMs: params.delayMs });
    const produce: TraceEvent[] = [{ event: "produceStarted", t: Date.now() }];
    let produceError: string | null = null;
    try {
      for (let index = 0; index < params.chunks; index += 1) {
        await writer.write(new Uint8Array(params.chunkBytes).fill(index + 1));
        produce.push({ event: "chunk", t: Date.now(), index });
        if (params.gapMs > 0) await wait(params.gapMs);
      }
      await writer.close();
      produce.push({ event: "produceClosed", t: Date.now() });
    } catch (error) {
      produceError = messageOf(error);
      produce.push({ event: "produceFailed", t: Date.now(), error: produceError });
      await writer.abort(error).catch(() => undefined);
    }
    let acceptError: string | null = null;
    let acceptResult: unknown = null;
    try {
      acceptResult = await acceptPromise;
    } catch (error) {
      acceptError = messageOf(error);
    }
    produce.push({ event: "acceptSettled", t: Date.now(), acceptError });
    let report: unknown = null;
    let reportError: string | null = null;
    try {
      report = await stub.report();
    } catch (error) {
      reportError = messageOf(error);
    }
    return { reducer: name, produceError, acceptError, acceptResult, reportError, report, produce };
  }
}

function clamp(value: string | null, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return Math.min(parsed, max);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/run") return new Response("wcol stream probe\n");
    const modeValue = url.searchParams.get("mode") ?? "immediate";
    const mode: Mode =
      modeValue === "delay" || modeValue === "held" || modeValue === "pipe" || modeValue === "pipe-delay" || modeValue === "relay"
        ? modeValue
        : "immediate";
    const scanners = clamp(url.searchParams.get("scanners"), 1, 4);
    const reducers = clamp(url.searchParams.get("reducers"), 1, 64);
    const delayMs = mode === "delay" || mode === "pipe-delay" ? clamp(url.searchParams.get("delayMs"), 2000, 10_000) : 0;
    const run = crypto.randomUUID().slice(0, 8);
    const reducerNames = Array.from({ length: reducers }, (_, index) => `${run}-r${index}`);
    const blockMs = clamp(url.searchParams.get("blockMs"), 0, 8_000);
    const sliceMs = clamp(url.searchParams.get("sliceMs"), 50, 2_000);
    const started = Date.now();
    const busy = blockMs
      ? Promise.all(reducerNames.map((name) => env.PROBE_REDUCE.get(env.PROBE_REDUCE.idFromName(name)).busy(blockMs, sliceMs)))
      : null;
    if (busy) await wait(100);
    const scans = await Promise.all(
      Array.from({ length: scanners }, (_, index) => {
        const stub = env.PROBE_SCAN.get(env.PROBE_SCAN.idFromName(`${run}-s${index}`));
        return stub.run({
          reducerNames,
          mode,
          delayMs,
          chunks: clamp(url.searchParams.get("chunks"), 4, 16),
          chunkBytes: clamp(url.searchParams.get("chunkBytes"), 1024, 65_536),
          gapMs: clamp(url.searchParams.get("gapMs"), 0, 1000),
        });
      }),
    );
    if (busy) await busy;
    return Response.json({ mode, delayMs, scanners, reducers, blockMs, sliceMs, wallMs: Date.now() - started, scans });
  },
};
