export type BenchMode = "parallel" | "sequential";
export type BenchTransport = "fetch" | "rpc";

/** Passed to `DurableObjectNamespace.get()` on first creation only. */
export type DoLocationHint = "wnam" | "enam" | "sam" | "weur" | "eeur" | "apac" | "oc" | "afr" | "me";

export const HINT_CANDIDATES: ReadonlyArray<DoLocationHint | "default"> = [
  "default",
  "weur",
  "eeur",
  "enam",
  "wnam",
];

export type TouchBreakdown = {
  name: string;
  totalMs: number;
  idFromNameMs: number;
  getStubMs: number;
  invokeMs: number;
  doId: string;
  /** Time from DO constructor to handler (DO isolate clock). */
  doHandlerMs: number | null;
  constructInstanceId: string;
};

export type CountBench = {
  count: number;
  cold: boolean;
  mode: BenchMode;
  transport: BenchTransport;
  totalMs: number;
  perDoMs: { min: number; max: number; p50: number };
  touches: TouchBreakdown[];
};

export type IntraBenchOptions = {
  counts?: number[];
  modes?: BenchMode[];
  transport?: BenchTransport;
  runId?: string;
  /** Repeat count=1 with fresh DO names; report all samples plus min. */
  samples?: number;
  /** Touch one throwaway DO before timed runs (warm worker isolate, still cold target DOs). */
  warmWorker?: boolean;
  locationHint?: DoLocationHint;
};

export type HintTuneResult = {
  runId: string;
  target: string;
  warmWorker: boolean;
  samplesPerHint: number;
  arms: Array<{
    locationHint: DoLocationHint | "default";
    p50Ms: number;
    minMs: number;
    maxMs: number;
    invokeP50Ms: number;
  }>;
  best: { locationHint: DoLocationHint | "default"; p50Ms: number };
};

export type IntraBenchResult = {
  runId: string;
  measuredOn: "worker";
  transport: BenchTransport;
  warmWorker: boolean;
  results: CountBench[];
  singleSamples?: TouchBreakdown[];
  singleBestMs?: number;
  singleP50Ms?: number;
  locationHint?: DoLocationHint;
};

export type PingPayload = {
  ok: true;
  doId: string;
  constructedAt: number;
  handledAt: number;
  /** New on every DO constructor run (hibernate wake counts as a new instance). */
  constructInstanceId: string;
};

export type TouchWithConstruct = TouchBreakdown & {
  constructInstanceId: string;
};

export type IdentityReuseTrial = {
  idleSeconds: number;
  trial: number;
  name: string;
  prime: TouchWithConstruct;
  reuse: TouchWithConstruct;
  /** True when reuse ran in a new DO constructor (hibernate wake or eviction). */
  constructorRerun: boolean;
  simulatePoolRpcMs: number;
};

export type IdentityReuseBenchOptions = {
  idleSeconds?: number[];
  freshSamples?: number;
  trialsPerIdle?: number;
  transport?: BenchTransport;
  runId?: string;
  warmWorker?: boolean;
  locationHint?: DoLocationHint;
  /** Added before each invoke to model Pool DO lease RPC in end-to-end latency. */
  simulatePoolRpcMs?: number;
};

export type IdentityReuseBenchResult = {
  runId: string;
  measuredOn: "worker";
  transport: BenchTransport;
  warmWorker: boolean;
  locationHint?: DoLocationHint;
  simulatePoolRpcMs: number;
  fresh: {
    samples: TouchWithConstruct[];
    perDoMs: { min: number; max: number; p50: number };
  };
  /** Second ping on same name with no idle (hot path). */
  immediateReuse: {
    samples: Array<{ prime: TouchWithConstruct; reuse: TouchWithConstruct; constructorRerun: boolean }>;
    reusePerDoMs: { min: number; max: number; p50: number };
  };
  idleArms: Array<{
    idleSeconds: number;
    trials: IdentityReuseTrial[];
    primePerDoMs: { min: number; max: number; p50: number };
    reusePerDoMs: { min: number; max: number; p50: number };
    constructorRerunRate: number;
  }>;
};

export interface PingableStub {
  ping(): Promise<PingPayload>;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DoBinding {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId, options?: { locationHint?: DoLocationHint }): PingableStub;
}

export interface ProbeEnv {
  START_PROBE: DoBinding;
}

function summarizeMs(values: number[]): { min: number; max: number; p50: number } {
  if (values.length === 0) return { min: 0, max: 0, p50: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const p50 = sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
  return { min: sorted[0]!, max: sorted[sorted.length - 1]!, p50 };
}

async function invokeFetch(stub: PingableStub): Promise<PingPayload> {
  const response = await stub.fetch("http://do/ping");
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`fetch ping failed (${response.status}): ${text.slice(0, 200)}`);
  }
  return (await response.json()) as PingPayload;
}

async function invokeRpc(stub: PingableStub): Promise<PingPayload> {
  return stub.ping();
}

export async function touchColdDo(
  binding: DoBinding,
  name: string,
  transport: BenchTransport,
  locationHint?: DoLocationHint,
  simulatePoolRpcMs = 0,
): Promise<TouchBreakdown> {
  const t0 = performance.now();
  const id = binding.idFromName(name);
  const t1 = performance.now();
  const stub = locationHint ? binding.get(id, { locationHint }) : binding.get(id);
  const t2 = performance.now();
  if (simulatePoolRpcMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, simulatePoolRpcMs));
  }
  const body = transport === "rpc" ? await invokeRpc(stub) : await invokeFetch(stub);
  const t3 = performance.now();
  return {
    name,
    totalMs: t3 - t0,
    idFromNameMs: t1 - t0,
    getStubMs: t2 - t1,
    invokeMs: t3 - t2,
    doId: body.doId,
    doHandlerMs: body.handledAt - body.constructedAt,
    constructInstanceId: body.constructInstanceId,
  };
}

export async function touchColdDoFromProbe(
  env: ProbeEnv,
  name: string,
  transport: BenchTransport,
  locationHint?: DoLocationHint,
  simulatePoolRpcMs = 0,
): Promise<TouchBreakdown> {
  return touchColdDo(env.START_PROBE, name, transport, locationHint, simulatePoolRpcMs);
}

export async function benchCount(
  env: ProbeEnv,
  count: number,
  mode: BenchMode,
  transport: BenchTransport,
  namePrefix: string,
  locationHint?: DoLocationHint,
): Promise<CountBench> {
  const names = Array.from({ length: count }, (_, i) => `${namePrefix}${i}`);
  const t0 = performance.now();
  let touches: TouchBreakdown[];
  if (mode === "parallel") {
    touches = await Promise.all(names.map((name) => touchColdDoFromProbe(env, name, transport, locationHint)));
  } else {
    touches = [];
    for (const name of names) touches.push(await touchColdDoFromProbe(env, name, transport, locationHint));
  }
  return {
    count,
    cold: true,
    mode,
    transport,
    totalMs: performance.now() - t0,
    perDoMs: summarizeMs(touches.map((t) => t.totalMs)),
    touches,
  };
}

function parseCounts(value: unknown): number[] | undefined {
  if (!Array.isArray(value)) return undefined;
  if (value.length === 0) return [];
  return value.filter((n): n is number => typeof n === "number" && Number.isInteger(n) && n > 0);
}

function parseModes(value: unknown): BenchMode[] | undefined {
  if (!Array.isArray(value)) return undefined;
  return value.filter((m): m is BenchMode => m === "parallel" || m === "sequential");
}

export async function runIntraBench(env: ProbeEnv, raw: IntraBenchOptions): Promise<IntraBenchResult> {
  const counts = raw.counts ?? [1, 10, 100];
  const modes = raw.modes ?? ["parallel"];
  const transport: BenchTransport = raw.transport === "fetch" ? "fetch" : "rpc";
  const runId = raw.runId && raw.runId.length > 0 ? raw.runId : crypto.randomUUID();
  const samples = Math.min(Math.max(raw.samples ?? 1, 1), 20);
  const warmWorker = raw.warmWorker === true;
  const locationHint = raw.locationHint;

  if (counts.length === 0 && samples <= 1) {
    throw new Error("counts must be non-empty unless samples > 1");
  }
  if (modes.length === 0 && counts.length > 0) {
    throw new Error("modes must include parallel and/or sequential");
  }

  if (warmWorker) {
    await touchColdDoFromProbe(env, `${runId}:warmup`, transport, locationHint);
  }

  const results: CountBench[] = [];
  for (const count of counts) {
    for (const mode of modes) {
      results.push(await benchCount(env, count, mode, transport, `${runId}:c${count}:${mode}:`, locationHint));
    }
  }
  let singleSamples: TouchBreakdown[] | undefined;
  let singleBestMs: number | undefined;
  let singleP50Ms: number | undefined;
  if (samples > 1 || counts.includes(1)) {
    singleSamples = [];
    for (let i = 0; i < samples; i++) {
      singleSamples.push(await touchColdDoFromProbe(env, `${runId}:sample:${i}`, transport, locationHint));
    }
    singleBestMs = Math.min(...singleSamples.map((s) => s.totalMs));
    singleP50Ms = summarizeMs(singleSamples.map((s) => s.totalMs)).p50;
  }

  return {
    runId,
    measuredOn: "worker",
    transport,
    warmWorker,
    locationHint,
    results,
    singleSamples,
    singleBestMs,
    singleP50Ms,
  };
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asTouchWithConstruct(t: TouchBreakdown): TouchWithConstruct {
  return t;
}

export async function runIdentityReuseBench(env: ProbeEnv, raw: IdentityReuseBenchOptions): Promise<IdentityReuseBenchResult> {
  const idleSeconds = raw.idleSeconds ?? [1, 5, 15, 60];
  const freshSamples = Math.min(Math.max(raw.freshSamples ?? 8, 1), 30);
  const trialsPerIdle = Math.min(Math.max(raw.trialsPerIdle ?? 3, 1), 10);
  const transport: BenchTransport = raw.transport === "fetch" ? "fetch" : "rpc";
  const runId = raw.runId && raw.runId.length > 0 ? raw.runId : crypto.randomUUID();
  const warmWorker = raw.warmWorker === true;
  const locationHint = raw.locationHint;
  const simulatePoolRpcMs = Math.max(0, raw.simulatePoolRpcMs ?? 0);

  if (warmWorker) {
    await touchColdDoFromProbe(env, `${runId}:warmup`, transport, locationHint, simulatePoolRpcMs);
  }

  const freshTouches: TouchWithConstruct[] = [];
  for (let i = 0; i < freshSamples; i++) {
    freshTouches.push(
      asTouchWithConstruct(
        await touchColdDoFromProbe(env, `${runId}:fresh:${i}`, transport, locationHint, simulatePoolRpcMs),
      ),
    );
  }

  const immediateSamples: IdentityReuseBenchResult["immediateReuse"]["samples"] = [];
  for (let i = 0; i < freshSamples; i++) {
    const name = `${runId}:immediate:${i}`;
    const prime = asTouchWithConstruct(
      await touchColdDoFromProbe(env, name, transport, locationHint, simulatePoolRpcMs),
    );
    const reuse = asTouchWithConstruct(
      await touchColdDoFromProbe(env, name, transport, locationHint, simulatePoolRpcMs),
    );
    immediateSamples.push({
      prime,
      reuse,
      constructorRerun: prime.constructInstanceId !== reuse.constructInstanceId,
    });
  }

  const idleArms: IdentityReuseBenchResult["idleArms"] = [];
  for (const idle of idleSeconds) {
    const trials: IdentityReuseTrial[] = [];
    for (let trial = 0; trial < trialsPerIdle; trial++) {
      const name = `${runId}:idle${idle}s:t${trial}`;
      const prime = asTouchWithConstruct(
        await touchColdDoFromProbe(env, name, transport, locationHint, simulatePoolRpcMs),
      );
      await sleepMs(idle * 1000);
      const reuse = asTouchWithConstruct(
        await touchColdDoFromProbe(env, name, transport, locationHint, simulatePoolRpcMs),
      );
      trials.push({
        idleSeconds: idle,
        trial,
        name,
        prime,
        reuse,
        constructorRerun: prime.constructInstanceId !== reuse.constructInstanceId,
        simulatePoolRpcMs,
      });
    }
    const reruns = trials.filter((t) => t.constructorRerun).length;
    idleArms.push({
      idleSeconds: idle,
      trials,
      primePerDoMs: summarizeMs(trials.map((t) => t.prime.totalMs)),
      reusePerDoMs: summarizeMs(trials.map((t) => t.reuse.totalMs)),
      constructorRerunRate: trials.length === 0 ? 0 : reruns / trials.length,
    });
  }

  return {
    runId,
    measuredOn: "worker",
    transport,
    warmWorker,
    locationHint,
    simulatePoolRpcMs,
    fresh: {
      samples: freshTouches,
      perDoMs: summarizeMs(freshTouches.map((t) => t.totalMs)),
    },
    immediateReuse: {
      samples: immediateSamples,
      reusePerDoMs: summarizeMs(immediateSamples.map((s) => s.reuse.totalMs)),
    },
    idleArms,
  };
}

export function parseIdentityReuseBody(body: unknown): IdentityReuseBenchOptions {
  const payload = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  const idleSeconds = Array.isArray(payload.idleSeconds)
    ? payload.idleSeconds.filter((n): n is number => typeof n === "number" && Number.isFinite(n) && n >= 0)
    : undefined;
  return {
    idleSeconds,
    freshSamples: typeof payload.freshSamples === "number" ? payload.freshSamples : undefined,
    trialsPerIdle: typeof payload.trialsPerIdle === "number" ? payload.trialsPerIdle : undefined,
    transport: payload.transport === "fetch" ? "fetch" : payload.transport === "rpc" ? "rpc" : undefined,
    runId: typeof payload.runId === "string" ? payload.runId : undefined,
    warmWorker: payload.warmWorker === true,
    locationHint: typeof payload.locationHint === "string" ? (payload.locationHint as DoLocationHint) : undefined,
    simulatePoolRpcMs: typeof payload.simulatePoolRpcMs === "number" ? payload.simulatePoolRpcMs : undefined,
  };
}

export function compactIdentityReuse(payload: IdentityReuseBenchResult): unknown {
  const mapTouch = (t: TouchWithConstruct) => ({
    totalMs: roundMs(t.totalMs),
    invokeMs: roundMs(t.invokeMs),
    doHandlerMs: t.doHandlerMs === null ? null : roundMs(t.doHandlerMs),
    doId: t.doId,
    constructInstanceId: t.constructInstanceId,
  });
  return {
    runId: payload.runId,
    measuredOn: payload.measuredOn,
    transport: payload.transport,
    warmWorker: payload.warmWorker,
    locationHint: payload.locationHint,
    simulatePoolRpcMs: payload.simulatePoolRpcMs,
    fresh: {
      perDoMs: {
        min: roundMs(payload.fresh.perDoMs.min),
        max: roundMs(payload.fresh.perDoMs.max),
        p50: roundMs(payload.fresh.perDoMs.p50),
      },
      sampleCount: payload.fresh.samples.length,
    },
    immediateReuse: {
      reusePerDoMs: {
        min: roundMs(payload.immediateReuse.reusePerDoMs.min),
        max: roundMs(payload.immediateReuse.reusePerDoMs.max),
        p50: roundMs(payload.immediateReuse.reusePerDoMs.p50),
      },
      constructorRerunRate:
        payload.immediateReuse.samples.length === 0
          ? 0
          : payload.immediateReuse.samples.filter((s) => s.constructorRerun).length / payload.immediateReuse.samples.length,
    },
    idleArms: payload.idleArms.map((arm) => ({
      idleSeconds: arm.idleSeconds,
      primePerDoMs: {
        min: roundMs(arm.primePerDoMs.min),
        max: roundMs(arm.primePerDoMs.max),
        p50: roundMs(arm.primePerDoMs.p50),
      },
      reusePerDoMs: {
        min: roundMs(arm.reusePerDoMs.min),
        max: roundMs(arm.reusePerDoMs.max),
        p50: roundMs(arm.reusePerDoMs.p50),
      },
      constructorRerunRate: roundMs(arm.constructorRerunRate * 1000) / 1000,
      trials: arm.trials.map((t) => ({
        trial: t.trial,
        constructorRerun: t.constructorRerun,
        prime: mapTouch(t.prime),
        reuse: mapTouch(t.reuse),
      })),
    })),
  };
}

export async function runHintTune(
  binding: DoBinding,
  target: string,
  options?: { samplesPerHint?: number; warmWorker?: boolean; runId?: string },
): Promise<HintTuneResult> {
  const samplesPerHint = Math.min(Math.max(options?.samplesPerHint ?? 12, 3), 30);
  const warmWorker = options?.warmWorker !== false;
  const runId = options?.runId && options.runId.length > 0 ? options.runId : crypto.randomUUID();
  if (warmWorker) {
    await touchColdDo(binding, `${runId}:warmup`, "rpc");
  }
  const arms: HintTuneResult["arms"] = [];
  for (const hint of HINT_CANDIDATES) {
    const touches: TouchBreakdown[] = [];
    for (let i = 0; i < samplesPerHint; i++) {
      const resolved = hint === "default" ? undefined : hint;
      touches.push(await touchColdDo(binding, `${runId}:hint:${hint}:${i}`, "rpc", resolved));
    }
    const totals = summarizeMs(touches.map((t) => t.totalMs));
    const invokes = summarizeMs(touches.map((t) => t.invokeMs));
    arms.push({
      locationHint: hint,
      p50Ms: totals.p50,
      minMs: totals.min,
      maxMs: totals.max,
      invokeP50Ms: invokes.p50,
    });
  }
  const best = arms.reduce((a, b) => (b.p50Ms < a.p50Ms ? b : a));
  return { runId, target, warmWorker, samplesPerHint, arms, best };
}

export function parseIntraBenchBody(body: unknown): IntraBenchOptions {
  const payload = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  return {
    counts: parseCounts(payload.counts),
    modes: parseModes(payload.modes),
    transport: payload.transport === "fetch" ? "fetch" : payload.transport === "rpc" ? "rpc" : undefined,
    runId: typeof payload.runId === "string" ? payload.runId : undefined,
    samples: typeof payload.samples === "number" ? payload.samples : undefined,
    warmWorker: payload.warmWorker === true,
    locationHint: typeof payload.locationHint === "string" ? (payload.locationHint as DoLocationHint) : undefined,
  };
}

export function compactHintTune(payload: HintTuneResult): unknown {
  return {
    runId: payload.runId,
    target: payload.target,
    warmWorker: payload.warmWorker,
    samplesPerHint: payload.samplesPerHint,
    best: {
      locationHint: payload.best.locationHint,
      p50Ms: roundMs(payload.best.p50Ms),
    },
    arms: payload.arms.map((a) => ({
      locationHint: a.locationHint,
      p50Ms: roundMs(a.p50Ms),
      minMs: roundMs(a.minMs),
      maxMs: roundMs(a.maxMs),
      invokeP50Ms: roundMs(a.invokeP50Ms),
    })),
  };
}

export function roundMs(n: number): number {
  return Math.round(n * 100) / 100;
}

export function compactIntra(payload: IntraBenchResult): unknown {
  const mapTouch = (t: TouchBreakdown) => ({
    totalMs: roundMs(t.totalMs),
    invokeMs: roundMs(t.invokeMs),
    getStubMs: roundMs(t.getStubMs),
    doHandlerMs: t.doHandlerMs === null ? null : roundMs(t.doHandlerMs),
    doId: t.doId,
  });
  return {
    runId: payload.runId,
    measuredOn: payload.measuredOn,
    transport: payload.transport,
    warmWorker: payload.warmWorker,
    singleBestMs: payload.singleBestMs === undefined ? undefined : roundMs(payload.singleBestMs),
    singleP50Ms: payload.singleP50Ms === undefined ? undefined : roundMs(payload.singleP50Ms),
    locationHint: payload.locationHint,
    singleSamples: payload.singleSamples?.map(mapTouch),
    results: payload.results.map((r) => ({
      count: r.count,
      mode: r.mode,
      transport: r.transport,
      totalMs: roundMs(r.totalMs),
      perDoMs: {
        min: roundMs(r.perDoMs.min),
        max: roundMs(r.perDoMs.max),
        p50: roundMs(r.perDoMs.p50),
      },
      touches: r.count <= 3 ? r.touches.map(mapTouch) : undefined,
    })),
  };
}
