import type { ContainerInstance } from "./containers.ts";
import type { ReducerRuntime } from "./session.ts";
import type { HostBytes } from "../../../src/runtime/host/host-bytes.ts";
import type { ReducerIngressSession } from "../../../src/runtime/host/scan-edge-session.ts";
import type { DictTraffic, MemGrowthSample, SpillBoundary } from "../../../src/runtime/host/host-types.ts";
import type { StreamRunStats } from "../../../src/runtime/host/wasm-stream.ts";
import type { R2Env } from "../r2.ts";
import type { ExportObject } from "../../../src/runtime/host/hive-upload-sink.ts";

/** One object a task reads: an R2 binding name and key, or an http(s) URL. */
export type GraphJobSource = { url: string } | { bucket: string; key: string };

export type HashScanDestination = { partition: number; slot: number; taskId?: number };

export type RunTaskParams = {
  jobId: string;
  executionAttempt: number;
  /** Where this job's hash reducers run; scans send their edges there. Default `node`. */
  reducerRuntime?: ReducerRuntime;
  /** The coordinator's deploy; a node instance from another deploy resets itself. */
  deployId: string;
  taskId: number;
  slot: number;
  program: Uint8Array;
  sources: GraphJobSource[];
  hashDestinations?: HashScanDestination[];
  /** Producer outputs for a gather task. Empty means the task reads `sources`. */
  inputs?: Uint8Array[];
  hashScanIndex?: number;
  failTaskId?: number;
  trace?: boolean;
  /** Etag parallel to `sources`. Empty string means the source is not pinned. */
  etags?: string[];
};

export type ResidentKMeansLoadParams = RunTaskParams & {
  sessionId: string;
  /** First global barrier the resident is expected to execute after reload. */
  startIteration: number;
};

export type ResidentKMeansRoundParams = {
  jobId: string;
  executionAttempt: number;
  sessionId: string;
  iteration: number;
  globalState: Uint8Array;
};

export type ResidentKMeansNormalizeParams = {
  jobId: string;
  executionAttempt: number;
  sessionId: string;
  normalization: Uint8Array;
};

export type ResidentKMeansReleaseParams = {
  jobId: string;
  executionAttempt: number;
  sessionId: string;
};

export type RunParquetShardParams = RunTaskParams & {
  objectKey: string;
  /** R2 binding name for the output bucket; empty uses `OUTPUT`. */
  outputBucket: string;
  /** The plan's output layout: `hive` splits the task's rows by `partitionBy` values. */
  layout: import("../../../src/runtime/coordinator/execution-graph.ts").OutputLayout;
};

export type RunConcatTaskParams = {
  jobId: string;
  executionAttempt: number;
  /** The coordinator's deploy; a node instance from another deploy resets itself. */
  deployId: string;
  slot: number;
  program: Uint8Array;
  sourceKeys: string[];
  outputKey: string;
  outputBucket: string;
};

export type PrepareReducerParams = {
  jobId: string;
  executionAttempt: number;
  /** The coordinator's deploy; a node instance from another deploy resets itself. */
  deployId: string;
  taskId: number;
  slot: number;
  program: Uint8Array;
  sources: GraphJobSource[];
  expectedScans: number;
  /** Container reducers start on this instance type; node reducers ignore it. */
  containerInstance?: ContainerInstance;
  failTaskId?: number;
  trace?: boolean;
  etags?: string[];
  /** Hash shard: finalize program fed by this reducer. Range sort: same as `program`. */
  exportProgram?: Uint8Array;
  exportObjectKey?: string;
};

export type AcceptScanEdgeParams = {
  jobId: string;
  executionAttempt: number;
  scanTaskId: number;
  scanIndex: number;
  partition: number;
  stream: ReadableStream<Uint8Array>;
};

export type HashAttemptParams = {
  jobId: string;
  executionAttempt: number;
  scanIndex?: number;
};

export type RunTaskHandle = {
  invokedSlots: number[];
  body: ReadableStream<Uint8Array>;
};

export type PrepareReducerHandle = {
  ready: true;
  invokedSlots: number[];
  body: ReadableStream<Uint8Array>;
};

/** A gather task's output, with what the console shows of it. */
export type GatherTaskResult = { bytes: Uint8Array; colo: string; sourceReads: number; sourceReadBytes: number };

/** Where a reducer runs, for the console: its data centre and runtime. */
export type ReducerPlacementFacts =
  | { colo: string; runtime: "node" }
  | { colo: string; runtime: "container"; instance: ContainerInstance; readyMs: number };

export type GraphTaskNode = {
  runTask(params: RunTaskParams): Promise<RunTaskHandle>;
  runGatherTask(params: RunTaskParams): Promise<GatherTaskResult>;
  loadResidentKMeans(params: ResidentKMeansLoadParams): Promise<{ moments: Uint8Array }>;
  normalizeResidentKMeans(params: ResidentKMeansNormalizeParams): Promise<{ ready: true }>;
  runResidentKMeansRound(params: ResidentKMeansRoundParams): Promise<Uint8Array>;
  releaseResidentKMeans(params: ResidentKMeansReleaseParams): Promise<void>;
  runParquetShardTask(params: RunParquetShardParams): Promise<{ objects: ExportObject[] }>;
  runConcatTask(params: RunConcatTaskParams): Promise<{ bytes: number }>;
  prepareReducer(params: PrepareReducerParams): Promise<PrepareReducerHandle>;
  acceptScanEdge(params: AcceptScanEdgeParams): Promise<{ accepted: true }>;
  waitConsumeReady(params: HashAttemptParams): Promise<void>;
  completeScan(params: HashAttemptParams): Promise<void>;
  sealReducer(params: HashAttemptParams): Promise<void>;
  abortHashAttempt(params: HashAttemptParams & { error?: string }): Promise<void>;
  /** Asked after `prepareReducer`, off the job's critical path. */
  placement(): Promise<ReducerPlacementFacts>;
};

export type GraphTaskEnv = R2Env & {
  WCOL_NODE: DurableObjectNamespace;
  /** Container-backed hash reducers (`reducer-container.ts`), for jobs that ask for them. */
  WCOL_REDUCER_CONTAINER?: DurableObjectNamespace;
  WCOL_COORDINATOR?: DurableObjectNamespace;
  WCOL_DISPATCH_TRACE?: string;
  /** Must match on node after deploy; invalidates cached Wasm in a long-lived isolate. */
  WCOL_NODE_WASM_BUILD?: string;
  /** Maximum peak bytes for resident feature storage; default 40 MiB. */
  WCOL_RESIDENT_MEMORY_BYTES?: string;
  /** Set per deploy on both workers (scripts/deploy-worker.ts). */
  WCOL_DEPLOY_ID?: string;
  /** Optional first-create placement for graph slot objects (Wrangler var). */
  WCOL_DO_LOCATION_HINT?: string;
};

import type { PoolInputLatch } from "../../../src/runtime/host/pool-streams.ts";
import type { StreamPool } from "../../../src/runtime/host/stream-pool.ts";

export type RunProgramOptions = {
  env: GraphTaskEnv;
  wasm: import("./session.ts").NodeWasm;
  params: RunTaskParams;
  poolLayout: { inputs: number; outputs: number };
  write: (port: number, chunk: Uint8Array) => void | Promise<void>;
  chunks: () => number;
  closePort?: (port: number) => void | Promise<void>;
  openPort?: (port: number) => void | Promise<void>;
  onActivity?: () => void;
  onYield?: () => void;
  inputLatch?: PoolInputLatch;
  onPoolReady?: (pool: StreamPool) => void;
  onResidentReady?: (graph: import("../../../src/runtime/host/resident-graph.ts").ResidentGraph) => void;
  /** When the node RPC arrived (before claiming the node), for the task profile. */
  rpcAt?: number;
  /** The task's host-byte ledger; a reducer's edges count into it before the run starts. */
  hostBytes?: HostBytes;
};

export type RunTaskOnNodeOptions = {
  /** When the RPC entered the node, the node was claimed, and its lease alarm was set. */
  timing?: { enteredAt: number; claimedAt: number; armedAt: number };
  env: GraphTaskEnv;
  wasm: import("./session.ts").NodeWasm;
  params: RunTaskParams;
  durableObjectId: string;
  waitUntil: (promise: Promise<unknown>) => void;
  hooks?: NodeRunHooks;
};

export type PrepareReducerOnNodeOptions = {
  env: GraphTaskEnv;
  wasm: import("./session.ts").NodeWasm;
  params: PrepareReducerParams;
  holder: HashIngressHolder;
  durableObjectId: string;
  waitUntil: (promise: Promise<unknown>) => void;
  hooks?: NodeRunHooks;
};

export type RunGatherTaskOnNodeOptions = {
  env: GraphTaskEnv;
  wasm: import("./session.ts").NodeWasm;
  params: RunTaskParams;
  onActivity?: () => void;
  rpcAt?: number;
};

export type NodeRunHooks = {
  onActivity?: () => void;
  /** False once a newer execution owns the node. Wasm and ingress stay with that owner. */
  ownsSession?: () => boolean;
  /** Fires when the node releases this execution (abort, lease expiry, failure). */
  signal?: AbortSignal;
};

export type HashIngressHolder = {
  session: ReducerIngressSession | null;
  stream?: PoolInputLatch | null;
  /** The reducer's host-byte ledger: each accepted edge counts the bytes it relays. */
  hostBytes?: HostBytes;
  /**
   * One per accepted scan edge still piping in. An accept RPC stays open until its stream
   * is consumed, so releasing the reducer must end these pipes; otherwise the scan's
   * accept (and the scan) stays open, and the instance is never evicted.
   */
  edges?: Set<AbortController>;
};

/** Drop streams, ports and open edge pipes so the isolate can hibernate. */
export function releaseHashIngress(holder: HashIngressHolder, error?: unknown): void {
  const reason = error ?? new Error("hash ingress released");
  for (const edge of holder.edges ?? []) edge.abort(reason);
  holder.edges?.clear();
  holder.session?.abort(reason);
  holder.session = null;
  void holder.stream?.abort(reason);
  holder.stream = null;
  holder.hostBytes = undefined;
}

export type IsolateProgress = {
  at: number;
  wallMs: number;
  pollIterations?: number;
  inputBytes: number;
  outputBytes: number;
  wasmMemoryBytes: number;
  leftoverBytes?: number;
  outstandingReads?: number;
  residentGroups?: number;
  residentGroupBytes?: number;
  spillRuns?: number;
  spillGroupsFlushed?: number;
  spillPeakLiveBytes?: number;
  spillPeakInternerBytes?: number;
  spillBoundaries?: SpillBoundary[];
  egressPendingBytes?: number;
  egressPeakPendingBytes?: number;
  yieldCount?: number;
  maxPollsBetweenYields?: number;
  /** Final record only: one sample per 8 MB of Wasm growth, with the phase that grew it. */
  memGrowth?: MemGrowthSample[];
  /** Final record only: dictionary bytes sent or installed on this task's hash edges. */
  dictTraffic?: DictTraffic;
};

/** `t{task}-slots.json`: written by every node program when it ends. Epoch ms. */
export type TaskTimingRecord = {
  slots: number[];
  outcome: "ok" | "failed";
  startedAt: number;
  /** When every input had ended (reducers); after this the task only finishes (eg. spill merge). */
  inputsEndedAt?: number;
  endedAt: number;
  wasmLoadMs: number;
  writeWaitMs: number;
  yieldCount: number;
  spillRuns: number;
  /** Source range reads (a scan's R2 GETs of the input) and their bytes. */
  sourceReads?: number;
  sourceReadBytes?: number;
  /** A reducer's spill run uploads and reads (R2 PUTs and GETs). */
  spillIo?: import("../../../src/runtime/host/spill-r2-pool-driver.ts").SpillIo;
};

export type TaskTiming = {
  wallMs: number;
  /** Source range reads and their bytes, from the pool's read profile. */
  sourceReads: number;
  sourceReadBytes: number;
  wasmLoadMs: number;
  writeWaitMs: number;
  inputBytes: number;
  outputBytes: number;
  wasmMemoryBytes: number;
  residentGroups: number;
  residentGroupBytes: number;
  spillRuns: number;
  spillGroupsFlushed: number;
  spillPeakLiveBytes: number;
  spillPeakInternerBytes: number;
  spillDiag?: NonNullable<StreamRunStats["spillDiag"]>;
};

export type GraphDoLocationHint = "wnam" | "enam" | "sam" | "weur" | "eeur" | "apac" | "oc" | "afr" | "me";
