import { createLimiter } from "../../../src/runtime/concurrency.ts";
import {
  type JobPlan,
  type JobPlanRequest,
  type JobSourceLocation,
  planJob,
  readParquetFooter,
} from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { decodeExecutionPlanWire, type ExecutionPlan } from "../../../src/runtime/coordinator/execution-graph.ts";
import { assertNoStreamFanOut, isFullyColocated, type GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { HttpRangeSource } from "../../../src/runtime/host/ports/http-range.ts";
import { R2RangeSource } from "../../../src/runtime/host/ports/r2-read.ts";
import { sha256Hex } from "../digest.ts";
import { r2Bucket, type GraphJobSource } from "../node/node.ts";
import type { R2OutputBucket } from "../r2.ts";
import type { JobRequest } from "./job-request.ts";
import {
  byteCap,
  descriptorKey,
  type GraphJobContext,
  type GraphJobEnv,
  graphObjectKey,
  type JobDescriptor,
  type PinnedSource,
  toTaskSource,
} from "./record.ts";

/**
 * Pin each source's etag and size, and keep the immutable graph in R2.
 * Planning stays in `planJob`.
 */

/** Footer reads in flight at once: a job may name hundreds of objects. */
export const FOOTER_READS = 16;

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

/** An R2 object's size and etag, or null when it does not exist. */
export async function objectHead(bucket: R2OutputBucket, key: string): Promise<{ size: number; etag: string } | null> {
  // A binding without `head` (a test fake) answers a one-byte ranged read instead.
  const head = bucket.head ? await bucket.head(key) : await bucket.get(key, { range: { suffix: 1 } });
  if (!head || head.body === null) return null;
  return { size: head.size ?? 0, etag: head.etag || String(head.httpEtag ?? "").replaceAll('"', "") };
}

/** Footer thrift, object size, and the etag that later reads must match. */
export async function readFooter(
  env: GraphJobEnv,
  location: JobSourceLocation,
): Promise<{ fileSize: number; footer: Uint8Array; etag: string }> {
  if ("url" in location) {
    const http = new HttpRangeSource(location.url, { rangeOnly: true });
    const fileSize = await http.contentLength();
    const footer = await readParquetFooter((offset, length) => http.read(offset, length), fileSize);
    return { fileSize, footer, etag: http.validator() ?? "" };
  }
  const bucket = r2Bucket(env, location.bucket);
  const head = await objectHead(bucket, location.key);
  if (!head) throw new Error(`object not found: ${location.bucket}/${location.key}`);
  const { size: fileSize, etag } = head;
  if (!fileSize) throw new Error(`object has no size: ${location.bucket}/${location.key}`);
  // An etag names one immutable version, so its footer can be reused across jobs
  // (and fetched by tooling: R2 has no ranged reads from outside a Worker).
  const cacheKey = etag ? footerCacheKey(location.bucket, location.key, etag) : null;
  const cached = cacheKey && env.OUTPUT ? await env.OUTPUT.get(cacheKey) : null;
  if (cached && cached.body !== null) {
    return { fileSize, footer: new Uint8Array(await cached.arrayBuffer()), etag };
  }
  const range = new R2RangeSource(bucket, location.key, etag || undefined);
  const footer = await readParquetFooter((offset, length) => range.read(offset, length), fileSize);
  if (cacheKey && env.OUTPUT) await env.OUTPUT.put(cacheKey, footer);
  return { fileSize, footer, etag };
}

/** `_footers/{bucket}/{key}/{etag}.bin`: the Parquet footer of one object version. */
export function footerCacheKey(bucket: string, key: string, etag: string): string {
  return `_footers/${bucket}/${key}/${etag}.bin`;
}

/** A source with its etag, size and footer, read once per submission. */
export type PinnedFooter = PinnedSource & { footer: Uint8Array };

/** Read each source's footer, pinning the etag and size later reads must match. */
export async function pinSources(env: GraphJobEnv, locations: readonly JobSourceLocation[]): Promise<PinnedFooter[]> {
  const limit = createLimiter(FOOTER_READS);
  return Promise.all(
    locations.map((location) =>
      limit(async () => {
        const read = await readFooter(env, location);
        return { location, etag: read.etag, size: read.fileSize, footer: read.footer };
      }),
    ),
  );
}

/** Plan `request` over its pinned sources. No I/O beyond loading the planner. */
export async function planPinned(env: GraphJobEnv, request: JobRequest, pinned: readonly PinnedFooter[], fingerprint: string): Promise<Prepared> {
  const plan = planJob(await loadWasm(), planRequest(request, pinned.map((item) => ({ fileSize: item.size, footer: item.footer }))));
  const graph = plan.graph.kind === "graph" ? plan.graph : null;
  if (graph) assertNoStreamFanOut(graph);
  const descriptor: JobDescriptor = {
    fingerprint,
    sources: pinned.map(({ location, etag, size }) => ({ location, etag, size })),
    rowCap: plan.kind === "json" ? plan.rowCap : 0,
    byteCap: byteCap(env),
    kind: plan.kind,
    bucket: plan.kind === "parquet" ? plan.bucket : undefined,
    prefix: plan.kind === "parquet" ? plan.prefix : undefined,
    modelName: plan.kind === "model" ? plan.name : undefined,
    modelOptions: plan.kind === "model" ? plan.options : undefined,
    lateBody: plan.kind === "json" && plan.late ? request.body : undefined,
  };
  return { plan, graph, graphWire: plan.graphWire, descriptor, sources: pinned.map((item) => toTaskSource(item.location)) };
}

export function unpinned(descriptor: JobDescriptor): boolean {
  return descriptor.sources.some((source) => !source.etag);
}

export function allowUnpinned(plan: JobPlan, env: GraphJobEnv): boolean {
  if (plan.kind !== "json" || plan.graph.kind !== "graph") return false;
  if (!isFullyColocated(plan.graph)) return false;
  return Number(env.WCOL_JOB_SYNC_MS ?? 10_000) > 0;
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

export async function loadDescriptor(ctx: GraphJobContext, jobId: string, digest: string): Promise<JobDescriptor> {
  const bytes = await objectBytes(outputBucket(ctx.env), descriptorKey(jobId, digest));
  if (!bytes) throw new Error("job descriptor missing");
  return JSON.parse(new TextDecoder().decode(bytes)) as JobDescriptor;
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

function storedPlan(descriptor: JobDescriptor, graph: GraphPlan | null, graphWire: Uint8Array): JobPlan {
  const executable = (graph ?? { kind: "metadata_partial", partial: new Uint8Array(), resultColumns: new Uint8Array() }) as ExecutionPlan;
  if (descriptor.kind === "parquet") {
    return { kind: "parquet", bucket: descriptor.bucket ?? "", prefix: descriptor.prefix ?? "", graph: executable, graphWire };
  }
  if (descriptor.kind === "model") {
    if (!descriptor.modelName || !descriptor.modelOptions) throw new Error("model job descriptor is incomplete");
    return { kind: "model", name: descriptor.modelName, options: descriptor.modelOptions, graph: executable, graphWire };
  }
  return { kind: "json", late: descriptor.lateBody != null, rowCap: descriptor.rowCap, graph: executable, graphWire };
}

/** A job as recovery runs it: its immutable graph, read back from R2 once. */
export async function loadPrepared(ctx: GraphJobContext, jobId: string, descriptor: JobDescriptor, digest: string): Promise<Prepared> {
  const graphWire = await objectBytes(outputBucket(ctx.env), graphObjectKey(jobId, digest));
  if (!graphWire) throw new Error("job graph missing");
  const decoded = decodeExecutionPlanWire(graphWire);
  const graph = decoded.kind === "graph" ? decoded : null;
  return {
    plan: storedPlan(descriptor, graph, graphWire),
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
  return (source: number, offset: bigint | number, length: number) => {
    const read = readers[source];
    if (!read) throw new Error(`ReadAt source ${source} out of range`);
    return read(Number(offset), length);
  };
}

/** The planner request for a job: the same for its plan and its late read-back. */
export function planRequest(
  request: Pick<JobRequest, "body" | "maxNodes" | "minBytesPerNode" | "hashReducers">,
  footers: { fileSize: number; footer: Uint8Array }[],
): JobPlanRequest {
  return {
    body: request.body,
    footers,
    maxNodes: request.maxNodes,
    minBytesPerNode: request.minBytesPerNode,
    hashReducers: request.hashReducers,
  };
}

/** Footers of a job's pinned sources. A source whose etag changed fails the job. */
export async function pinnedFooters(
  env: GraphJobEnv,
  sources: PinnedSource[],
): Promise<{ fileSize: number; footer: Uint8Array }[]> {
  const pinned = await pinSources(env, sources.map((source) => source.location));
  return pinned.map((read, index) => {
    if (read.etag !== sources[index]!.etag) throw new Error("a job source changed while the job ran");
    return { fileSize: read.size, footer: read.footer };
  });
}
