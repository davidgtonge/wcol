import type { ContainerInstance } from "../node/containers.ts";
import type { ReducerRuntime } from "../node/session.ts";
import { parseReducerRequest, type ReducerRequest } from "./reducer-choice.ts";
import type { JobSourceLocation } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import type { GraphJobSource, GraphTaskEnv } from "../node/node.ts";
import type { R2Env, R2OutputBucket } from "../r2.ts";

/** Durable Object values stay small. Payloads move on the WCS stream, not in storage. */
export const MAX_DO_VALUE_BYTES = 64 * 1024;

export function encodedValueBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

export function assertDoValue(key: string, value: unknown): void {
  const bytes = encodedValueBytes(value);
  if (bytes > MAX_DO_VALUE_BYTES) {
    throw new Error(`durable object value ${key} is ${bytes} bytes; limit is ${MAX_DO_VALUE_BYTES}`);
  }
}

type PutStorage = {
  put(key: string, value: unknown): Promise<unknown>;
};

/** Rejects `put` of a JSON value larger than 64 KiB. Other methods pass through. */
export function boundStorage<T extends PutStorage>(storage: T): T {
  return new Proxy(storage, {
    get(target, prop, receiver) {
      if (prop === "put") {
        return (key: string, value: unknown) => {
          assertDoValue(key, value);
          return target.put(key, value);
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

/**
 * Host-level isolate knobs on `POST /jobs`. Not planner inputs: they bound
 * TypeScript fan-in and DO SQLite page cache, not the physical graph.
 */

export type HostExecutionHints = {
  /** Concurrent streams a reducer may hold. Default 8. */
  maxActiveIngress?: number;
  /** Where the job's hash reducers should run (`reducers`, `reducerInstance`). Default auto. */
  reducers: ReducerRequest;
};

export function parseHostExecutionHints(body: unknown): HostExecutionHints {
  const row = body != null && typeof body === "object" ? (body as Record<string, unknown>) : {};
  const hints: HostExecutionHints = { reducers: parseReducerRequest(row) };
  const ingress = Number(row.maxActiveIngress);
  if (Number.isInteger(ingress) && ingress >= 1 && ingress <= 64) {
    hints.maxActiveIngress = ingress;
  }
  return hints;
}

/**
 * Job identity and storage keys. No R2 or SQLite calls.
 */


export type GraphJobEnv = GraphTaskEnv & {
  OUTPUT?: R2OutputBucket;
  /** Sync budget for `json` jobs, in ms (default 10 000). */
  WCOL_JOB_SYNC_MS?: string;
  /** Upper bound on scan nodes per job (default 32). */
  WCOL_MAX_NODES?: string;
  /** `1` on celld, which holds a streamed RPC until its body ends (cloudflare/celld/README.md). */
  WCOL_BUFFER_SCAN_EDGES?: string;
  /** This worker's own coordinator namespace. */
  WCOL_COORDINATOR?: { idFromName(name: string): unknown; get(id: unknown, options?: { locationHint: string }): unknown };
  /** Node Worker deploy id; must match `wcol-node` for Wasm invalidation. */
  WCOL_NODE_WASM_BUILD?: string;
  /** This deploy; node calls carry it so stale node instances reset (scripts/deploy-worker.ts). */
  WCOL_DEPLOY_ID?: string;
  /**
   * Cap on concurrent RPC/stream establishment (default 6). Burst smoother, not a
   * documented Durable Object RPC hard cap.
   */
  WCOL_DISPATCH_CONCURRENCY?: string;
  /** Concurrent ingress streams at a reducer (default 8). */
  WCOL_MAX_ACTIVE_INGRESS?: string;
  /** Concurrent hash-exchange scans (default 16). Does not bypass the scan window. */
  WCOL_MAX_ACTIVE_SCANS?: string;
  /** Shared incoming buffer budget per consumer isolate (default 2 MiB). */
  WCOL_INGRESS_BYTES?: string;
  WCOL_JSON_MAX_BYTES?: string;
  WCOL_DISPATCH_TRACE?: string;
  WCOL_JOB_TEST_HOOKS?: string;
  /** No task-progress object newer than this is a stalled attempt. Default 90s. */
  WCOL_STALL_MS?: string;
};

export type JobStore = {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete?(key: string): Promise<boolean | void>;
  setAlarm?(time: number): Promise<void>;
};

export type GraphJobContext = {
  env: GraphJobEnv;
  store: JobStore;
  transaction<T>(run: (txn: JobStore) => Promise<T>): Promise<T>;
  setAlarm(time: number): Promise<void>;
  getAlarm(): Promise<number | null>;
  deleteAlarm(): Promise<void>;
  waitUntil(promise: Promise<unknown>): void;
  list(prefix: string): Promise<Map<string, unknown>>;
  /** Scan work queues for this job's attempts (in memory on the coordinator). */
  work: import("../../../src/runtime/coordinator/unit-claims.ts").WorkQueues;
  /** Each job's latest attempt, for the console's live view (in memory on the coordinator). */
  live: Map<string, LiveAttempt>;
};

/** What the console's live view reads of a running or finished attempt. */
export type LiveAttempt = {
  attempt: number;
  startedAt: number;
  graph: import("./live.ts").LiveGraph;
  timeline: import("../../../src/runtime/coordinator/stage-timeline.ts").StageTimeline;
  /** What each task has reported: scans when they end, reducers once prepared. */
  facts: Map<number, import("./live.ts").TaskFacts>;
  coordinatorColo: string | null;
  /** Size of the job's source objects, for the console's funnel. */
  sourceBytes: number;
};

export type PublishingRow = {
  executionAttempt: number;
  sourceKey: string;
  destinationKey: string;
};

export type ModelTrainingSummary = { iterations: number; converged: boolean; inertia: number };

const JOB_KINDS = new Set(["json", "parquet", "concat", "model"]);
const JOB_STATES = new Set(["accepted", "running", "completed", "failed"]);

/** What `GET /jobs/:id` reports. Bounded: ids, states, keys and counts only. */
export type GraphJobRecord = {
  jobId: string;
  kind: "json" | "parquet" | "concat" | "model";
  state: "accepted" | "running" | "completed" | "failed";
  executionAttempt: number;
  failedAttempts: number;
  nodeCount: number;
  createdAt: number;
  updatedAt: number;
  leaseUntil?: number;
  /** `json` that outlived the request: its body in R2 (`OUTPUT`). */
  resultKey?: string;
  /** Authoritative parquet manifest, `_jobs/{jobId}/manifest.json`. */
  manifestKey?: string;
  outputObjects?: number;
  outputBytes?: number;
  outputKey?: string;
  outputEtag?: string;
  error?: string;
  /** Concat (or any shared key) whose R2 put is not yet recorded here. */
  publishing?: PublishingRow;
  /** Set when a source has no etag and this job must not become a 202. */
  syncOnly?: boolean;
  /** Host fan-in cap for this job. Omit to use `WCOL_MAX_ACTIVE_INGRESS`. */
  maxActiveIngress?: number;
  /** Where the job's hash reducers run. Omitted: node Durable Objects. */
  reducerRuntime?: ReducerRuntime;
  /** Why they run there (`chooseReducers`), for the console and the job status. */
  reducerReason?: string;
  /** Instance type for container reducers. Omitted: the container class's default. */
  reducerInstance?: ContainerInstance;
  /** SHA-256 of the claimed request body. A second body for this job id is rejected. */
  requestFingerprint?: string;
  /** SHA-256 of the graph bytes. Locates the content-addressed R2 objects. */
  graphDigest?: string;
  modelName?: string;
  modelKey?: string;
  modelSummary?: ModelTrainingSummary;
};

export type PinnedSource = { location: JobSourceLocation; etag: string; size: number };

export type JobDescriptor = {
  fingerprint: string;
  sources: PinnedSource[];
  rowCap: number;
  byteCap: number;
  bucket?: string;
  prefix?: string;
  key?: string;
  kind: GraphJobRecord["kind"];
  modelName?: string;
  modelOptions?: { numClusters: number; maxIterations: number; seed: number; tolerance: number };
  /** A late row job's request body: its read-back is planned from the pick's rows. */
  lateBody?: string;
};

export const LEASE_MS = 120_000;
export const RENEW_MS = 30_000;
export const ALARM_MS = 45_000;
export const MAX_ATTEMPTS = 3;
/** Attempt with no newer task progress object. Longer than the 2s progress throttle. */
export const STALL_MS = 90_000;
export const JSON_MAX_BYTES = 1_048_576;
export const CONCAT_PUT_LIMIT = 5 * 1024 * 1024 * 1024;

/** Storage boundary: required ledger fields. Optional fields stay as stored. */
export function readJobRecord(value: unknown): GraphJobRecord | undefined {
  if (value == null || typeof value !== "object") return undefined;
  const row = value as Record<string, unknown>;
  if (typeof row.jobId !== "string" || row.jobId.length === 0) return undefined;
  if (typeof row.kind !== "string" || !JOB_KINDS.has(row.kind)) return undefined;
  if (typeof row.state !== "string" || !JOB_STATES.has(row.state)) return undefined;
  if (typeof row.executionAttempt !== "number" || !Number.isInteger(row.executionAttempt)) return undefined;
  return value as GraphJobRecord;
}

export function requireJobRecord(value: unknown): GraphJobRecord {
  const record = readJobRecord(value);
  if (!record) throw new Error("job ledger row is missing or invalid");
  return record;
}

export const jobKey = (jobId: string) => `graph-job:${jobId}`;
export const idempotencyKey = (key: string) => `graph-job-idempotency:${key}`;
export const taskKey = (jobId: string, taskId: number) => `graph-job-task:${jobId}:${taskId}`;
/** Legacy key when `digest` is omitted. A digest never overwrites another submission's graph. */
export const graphObjectKey = (jobId: string, digest?: string) =>
  digest ? `_jobs/${jobId}/graph-${digest}.bin` : `_jobs/${jobId}/graph.bin`;
export const descriptorKey = (jobId: string, digest?: string) =>
  digest ? `_jobs/${jobId}/descriptor-${digest}.json` : `_jobs/${jobId}/descriptor.json`;

export function attemptObjectKey(prefix: string, jobId: string, taskId: number, attempt: number): string {
  const base = prefix.endsWith("/") ? prefix : `${prefix}/`;
  return `${base}_runs/${jobId}/${taskId}/${attempt}/part.parquet`;
}

export function concatAttemptKey(jobId: string, taskId: number, attempt: number): string {
  return `_runs/${jobId}/${taskId}/${attempt}/part.parquet`;
}

export function authoritativeManifestKey(jobId: string): string {
  return `_jobs/${jobId}/manifest.json`;
}

export function pointerKey(prefix: string): string {
  return prefix.endsWith("/") ? `${prefix}_manifest.json` : `${prefix}/_manifest.json`;
}

export function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 500);
}

export function byteCap(env: GraphJobEnv): number {
  const n = Number(env.WCOL_JSON_MAX_BYTES ?? JSON_MAX_BYTES);
  return Number.isFinite(n) && n > 0 ? n : JSON_MAX_BYTES;
}

export function toTaskSource(location: JobSourceLocation): GraphJobSource {
  return "url" in location ? { url: location.url } : { bucket: location.bucket, key: location.key };
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  // Every caller hashes bytes it owns (a program, a request body), backed by an ArrayBuffer;
  // TypeScript types a bare Uint8Array as possibly shared, which `digest` does not accept.
  const digest = await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function taskRows(jobId: string, graph: { tasks: { id: number; slot: number; requestCount: number; fetchedBytes: number }[] } | null, attempt: number) {
  if (!graph) return [];
  return graph.tasks.map((task) => ({
    key: taskKey(jobId, task.id),
    value: {
      taskId: task.id,
      slot: task.slot,
      state: "pending",
      executionAttempt: attempt,
      requestCount: task.requestCount,
      fetchedBytes: task.fetchedBytes,
    },
  }));
}
