import {
  type JobPlan,
  type JobPlanRequest,
  type JobSourceLocation,
  jobSources,
  planJob,
} from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { decodeExecutionPlanWire, type ExecutionPlan } from "../../../src/runtime/coordinator/execution-graph.ts";
import { assertNoStreamFanOut, isFullyColocated, type GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { HttpRangeSource } from "../../../src/runtime/host/ports/http-range.ts";
import { R2RangeSource } from "../../../src/runtime/host/ports/r2-read.ts";
import { r2Bucket, type GraphJobSource } from "../node/node.ts";
import type { R2OutputBucket } from "../r2.ts";
import {
  byteCap,
  descriptorKey,
  type GraphJobContext,
  type GraphJobEnv,
  type GraphJobRecord,
  graphObjectKey,
  type JobDescriptor,
  jobKey,
  type PinnedSource,
  sha256Hex,
  toTaskSource,
} from "./record.ts";

/**
 * Pin each source's etag and size, and keep the immutable graph in R2.
 * Planning stays in `planJob`.
 */


export type Prepared = {
  plan: JobPlan;
  graph: GraphPlan | null;
  graphWire: Uint8Array;
  descriptor: JobDescriptor;
  sources: GraphJobSource[];
};

export function outputBucket(env: GraphJobEnv): R2OutputBucket {
  if (!env.OUTPUT) throw new Error("R2 output binding is missing");
  return env.OUTPUT;
}

export async function loadWasm() {
  const { loadCoordinatorWasm } = await import("../wasm.ts");
  return loadCoordinatorWasm();
}

/** Footer thrift, object size, and the etag that later reads must match. */
export async function readFooter(
  env: GraphJobEnv,
  location: JobSourceLocation,
): Promise<{ fileSize: number; footer: Uint8Array; etag: string }> {
  if ("url" in location) {
    const http = new HttpRangeSource(location.url, { rangeOnly: true });
    const fileSize = await http.contentLength();
    if (fileSize < 12) throw new Error("file is too small to be Parquet");
    const tail = await http.read(fileSize - 8, 8);
    const footer = await footerFromTail(fileSize, tail, (offset, length) => http.read(offset, length));
    return { fileSize, footer, etag: http.validator() ?? "" };
  }
  const bucket = r2Bucket(env, location.bucket);
  const headed = typeof bucket.head === "function";
  const head = headed ? await bucket.head!(location.key) : await bucket.get(location.key);
  if (!head || (!headed && head.body === null)) throw new Error(`object not found: ${location.bucket}/${location.key}`);
  const fileSize = head.size ?? 0;
  const etag = head.etag || String(head.httpEtag ?? "").replaceAll('"', "");
  if (!fileSize) throw new Error(`object has no size: ${location.bucket}/${location.key}`);
  // An etag names one immutable version, so its footer can be reused across jobs
  // (and fetched by tooling: R2 has no ranged reads from outside a Worker).
  const cacheKey = etag ? footerCacheKey(location.bucket, location.key, etag) : null;
  const cached = cacheKey && env.OUTPUT ? await env.OUTPUT.get(cacheKey) : null;
  if (cached && cached.body !== null) {
    return { fileSize, footer: new Uint8Array(await cached.arrayBuffer()), etag };
  }
  const range = new R2RangeSource(bucket, location.key, etag || undefined);
  const tail = await range.read(fileSize - 8, 8);
  const footer = await footerFromTail(fileSize, tail, (offset, length) => range.read(offset, length));
  if (cacheKey && env.OUTPUT) await env.OUTPUT.put(cacheKey, footer);
  return { fileSize, footer, etag };
}

/** `_footers/{bucket}/{key}/{etag}.bin`: the Parquet footer of one object version. */
export function footerCacheKey(bucket: string, key: string, etag: string): string {
  return `_footers/${bucket}/${key}/${etag}.bin`;
}

async function footerFromTail(
  fileSize: number,
  tail: Uint8Array,
  read: (offset: number, length: number) => Promise<Uint8Array>,
): Promise<Uint8Array> {
  if (fileSize < 12) throw new Error("file is too small to be Parquet");
  if (String.fromCharCode(...tail.subarray(4)) !== "PAR1") throw new Error("missing Parquet footer magic");
  const len = new DataView(tail.buffer, tail.byteOffset, 8).getUint32(0, true);
  if (len === 0 || len > fileSize - 12) throw new Error("invalid Parquet footer length");
  return read(fileSize - 8 - len, len);
}

export async function prepare(ctx: GraphJobContext, body: string, fingerprint: string): Promise<Prepared> {
  const request = jobSources(await loadWasm(), body);
  const pinned = await Promise.all(
    request.sources.map(async (location) => {
      const footer = await readFooter(ctx.env, location);
      return { location, etag: footer.etag, size: footer.fileSize, footer: footer.footer };
    }),
  );
  const plan = planJob(
    await loadWasm(),
    planRequest(ctx.env, body, pinned.map((item) => ({ fileSize: item.size, footer: item.footer }))),
  );
  const graphWire =
    plan.kind === "concat"
      ? (plan.programs[0] ?? new Uint8Array())
      : plan.graphWire;
  const graph =
    plan.kind === "concat" ? null : plan.graph.kind === "graph" ? plan.graph : null;
  if (graph) assertNoStreamFanOut(graph);
  const descriptor: JobDescriptor = {
    fingerprint,
    sources: pinned.map(({ location, etag, size }) => ({ location, etag, size })),
    rowCap: plan.kind === "json" ? plan.rowCap : 0,
    byteCap: byteCap(ctx.env),
    kind: plan.kind,
    bucket: plan.kind === "parquet" ? plan.bucket : undefined,
    prefix: plan.kind === "parquet" ? plan.prefix : undefined,
    key: plan.kind === "concat" ? plan.key : undefined,
    modelName: plan.kind === "model" ? plan.name : undefined,
    modelOptions: plan.kind === "model" ? plan.options : undefined,
    lateBody: plan.kind === "json" && plan.late ? body : undefined,
  };
  return { plan, graph, graphWire, descriptor, sources: request.sources.map(toTaskSource) };
}

export function unpinned(descriptor: JobDescriptor): boolean {
  return descriptor.sources.some((source) => !source.etag);
}

export function allowUnpinned(plan: JobPlan, env: GraphJobEnv): boolean {
  if (plan.kind !== "json" || plan.graph.kind !== "graph") return false;
  if (!isFullyColocated(plan.graph)) return false;
  return Number(env.WCOL_JOB_SYNC_MS ?? 10_000) > 0;
}

/** Reject plans this host cannot run, before they are claimed or stored. */
export function rejectUnexecutable(prepared: Prepared): string | null {
  if (prepared.plan.kind === "concat") {
    return "concat output cannot execute in the graph coordinator";
  }
  return null;
}

async function putImmutable(bucket: R2OutputBucket, key: string, bytes: Uint8Array, digest: string): Promise<void> {
  const created = await bucket.put(key, bytes, { onlyIf: { etagDoesNotMatch: "*" } });
  if (created == null) {
    const existing = await bucket.get(key);
    if (!existing || existing.body == null) throw new Error(`immutable object missing after conditional put: ${key}`);
    const stored = new Uint8Array(await existing.arrayBuffer());
    if ((await sha256Hex(stored)) !== digest) throw new Error(`immutable object ${key} does not match ${digest}`);
    return;
  }
  const verified = await bucket.get(key);
  if (!verified || verified.body == null) throw new Error(`immutable object missing: ${key}`);
  if ((await sha256Hex(new Uint8Array(await verified.arrayBuffer()))) !== digest) {
    throw new Error(`immutable object ${key} failed verification`);
  }
}

export async function writeImmutable(env: GraphJobEnv, jobId: string, prepared: Prepared, digest: string): Promise<void> {
  const bucket = outputBucket(env);
  await putImmutable(bucket, graphObjectKey(jobId, digest), prepared.graphWire, digest);
  const descriptorBytes = new TextEncoder().encode(JSON.stringify(prepared.descriptor));
  await putImmutable(bucket, descriptorKey(jobId, digest), descriptorBytes, await sha256Hex(descriptorBytes));
  if (env.WCOL_DISPATCH_TRACE === "1" && prepared.graph) {
    const tasks = await Promise.all(
      prepared.graph.tasks.map(async (task) => ({
        taskId: task.id,
        slot: task.slot,
        programSha256: await sha256Hex(task.program),
        kind: task.id === prepared.graph!.rootTaskId && prepared.graph!.edges.length > 0 ? "finalizer" : "scan",
      })),
    );
    await bucket.put(`_jobs/${jobId}/planned-${digest}.json`, JSON.stringify({ tasks }), {
      onlyIf: { etagDoesNotMatch: "*" },
    });
  }
}

async function objectBytes(bucket: R2OutputBucket, key: string): Promise<Uint8Array | null> {
  const object = await bucket.get(key);
  if (!object || object.body == null) return null;
  return new Uint8Array(await object.arrayBuffer());
}

export async function loadDescriptor(ctx: GraphJobContext, jobId: string, digest?: string): Promise<JobDescriptor> {
  const bucket = outputBucket(ctx.env);
  const bytes = (digest ? await objectBytes(bucket, descriptorKey(jobId, digest)) : null) ?? (await objectBytes(bucket, descriptorKey(jobId)));
  if (!bytes) throw new Error("job descriptor missing");
  return JSON.parse(new TextDecoder().decode(bytes)) as JobDescriptor;
}

export async function loadGraph(ctx: GraphJobContext, jobId: string, descriptor: JobDescriptor, digest?: string): Promise<GraphPlan | null> {
  const bucket = outputBucket(ctx.env);
  const bytes = (digest ? await objectBytes(bucket, graphObjectKey(jobId, digest)) : null) ?? (await objectBytes(bucket, graphObjectKey(jobId)));
  if (!bytes) throw new Error("job graph missing");
  if (descriptor.kind === "concat") {
    return null;
  }
  const plan = decodeExecutionPlanWire(bytes);
  return plan.kind === "graph" ? plan : null;
}

export async function pinsMatch(ctx: GraphJobContext, descriptor: JobDescriptor): Promise<boolean> {
  for (const source of descriptor.sources) {
    if (!source.etag) return false;
    if ("url" in source.location) {
      const http = new HttpRangeSource(source.location.url, { rangeOnly: true, etag: source.etag });
      const size = await http.contentLength();
      if (http.validator() !== source.etag || size !== source.size) return false;
    } else {
      const head = await r2Bucket(ctx.env, source.location.bucket).get(source.location.key, {
        range: { suffix: 1 },
        onlyIf: { etagMatches: source.etag },
      });
      if (!head || head.body === null) return false;
      if (head.size != null && head.size !== source.size) return false;
    }
  }
  return true;
}

export function storedPlan(descriptor: JobDescriptor, graph: GraphPlan | null, graphWire: Uint8Array): JobPlan {
  if (descriptor.kind === "concat") {
    return {
      kind: "concat",
      bucket: descriptor.bucket ?? "",
      key: descriptor.key ?? "",
      programs: graphWire.byteLength > 0 ? [graphWire] : [],
    };
  }
  if (descriptor.kind === "parquet") {
    return {
      kind: "parquet",
      bucket: descriptor.bucket ?? "",
      prefix: descriptor.prefix ?? "",
      graph: (graph ?? { kind: "metadata_partial", partial: new Uint8Array(), resultColumns: new Uint8Array() }) as ExecutionPlan,
      graphWire,
    };
  }
  if (descriptor.kind === "model") {
    if (!descriptor.modelName || !descriptor.modelOptions) throw new Error("model job descriptor is incomplete");
    return {
      kind: "model",
      name: descriptor.modelName,
      options: descriptor.modelOptions,
      graph: (graph ?? { kind: "metadata_partial", partial: new Uint8Array(), resultColumns: new Uint8Array() }) as ExecutionPlan,
      graphWire,
    };
  }
  return {
    kind: "json",
    late: descriptor.lateBody != null,
    rowCap: descriptor.rowCap,
    graph: (graph ?? { kind: "metadata_partial", partial: new Uint8Array(), resultColumns: new Uint8Array() }) as ExecutionPlan,
    graphWire,
  };
}

export async function preparedFromStorage(
  ctx: GraphJobContext,
  jobId: string,
  descriptor: JobDescriptor,
  graph: GraphPlan | null,
  digest?: string,
): Promise<Prepared> {
  const bucket = outputBucket(ctx.env);
  const graphWire =
    (digest ? await objectBytes(bucket, graphObjectKey(jobId, digest)) : null) ??
    (await objectBytes(bucket, graphObjectKey(jobId))) ??
    new Uint8Array();
  const plan = storedPlan(descriptor, graph, graphWire);
  return {
    plan,
    graph,
    graphWire,
    descriptor,
    sources: descriptor.sources.map((source) => toTaskSource(source.location)),
  };
}

export function readSources(env: GraphJobEnv, pinned: PinnedSource[]) {
  const readers = pinned.map((source) => {
    if ("url" in source.location) {
      const http = new HttpRangeSource(source.location.url, { rangeOnly: true, etag: source.etag || null });
      return (offset: number, length: number) => http.read(offset, length);
    }
    const range = new R2RangeSource(r2Bucket(env, source.location.bucket), source.location.key, source.etag || undefined);
    return (offset: number, length: number) => range.read(offset, length);
  });
  return (source: number, offset: number, length: number) => {
    const read = readers[source];
    if (!read) throw new Error(`ReadAt source ${source} out of range`);
    return read(offset, length);
  };
}

/** The planner request for `body`: the same for a job's plan and its late read-back. */
export function planRequest(
  env: GraphJobEnv,
  body: string,
  footers: { fileSize: number; footer: Uint8Array }[],
): JobPlanRequest {
  return {
    body,
    footers,
    maxNodes: maxNodesFrom(body, Number(env.WCOL_MAX_NODES ?? 32)),
    minBytesPerNode: minBytesPerNode(body),
    hashReducers: hashReducers(body),
  };
}

/** Footers of a job's pinned sources. A source whose etag changed fails the job. */
export async function pinnedFooters(
  env: GraphJobEnv,
  sources: PinnedSource[],
): Promise<{ fileSize: number; footer: Uint8Array }[]> {
  return Promise.all(
    sources.map(async (source) => {
      const read = await readFooter(env, source.location);
      if (read.etag !== source.etag) throw new Error("a job source changed while the job ran");
      return { fileSize: read.fileSize, footer: read.footer };
    }),
  );
}

function maxNodesFrom(body: string, fallback: number): number {
  try {
    const asked = (JSON.parse(body) as { maxNodes?: unknown }).maxNodes;
    if (typeof asked === "number" && Number.isInteger(asked) && asked >= 1) return asked;
  } catch {
    return fallback;
  }
  return fallback;
}

function minBytesPerNode(body: string): number | undefined {
  try {
    const asked = (JSON.parse(body) as { minBytesPerNode?: unknown }).minBytesPerNode;
    if (typeof asked === "number" && Number.isFinite(asked) && asked >= 1) return Math.floor(asked);
  } catch {
    // planJob still receives the original body; the planner ignores unknown JSON keys.
  }
  return undefined;
}

function hashReducers(body: string): number | undefined {
  try {
    const asked = (JSON.parse(body) as { hashReducers?: unknown }).hashReducers;
    if (typeof asked === "number" && Number.isFinite(asked) && asked >= 1) return Math.floor(asked);
  } catch {
    // Same as minBytesPerNode: unknown JSON keys stay on the body for the planner.
  }
  return undefined;
}
