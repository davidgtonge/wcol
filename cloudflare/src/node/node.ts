/**
 * One node Durable Object. Accept a program and port bindings, then call run().
 * Job records, publication, and recovery stay on the coordinator.
 */

import { DurableObject } from "cloudflare:workers";
import { coloOnce } from "../colo.ts";
import type { ResidentGraph, ResidentMemoryStats } from "../../../src/runtime/host/resident-graph.ts";
import {
  abortHashAttemptOnNode,
  acceptScanEdgeOnNode,
  refuseScanEdge,
  completeScanOnNode,
  prepareReducerOnNode,
  sealReducerOnNode,
  waitConsumeReadyOnNode,
} from "./reducer.ts";
import { runGatherTaskOnNode, runResidentTaskOnNode, runConcatTaskOnNode, runParquetShardTaskOnNode } from "./run.ts";
import { runTaskOnNode } from "./scan.ts";
import {
  beginNodeExecution,
  emptyNodeSessionLease,
  nodeLeaseExpired,
  ownsExecution,
  nodeWasm,
  prepareNodeSession,
  releaseIfOwner,
  releaseNodeSession,
  sameNodeAttempt,
  touchNodeLease,
  type NodeExecution,
  type NodeSessionLease,
  type SessionEnd,
  attemptSuperseded,
  noteAborted,
  checkDeploy,
  type AbortedAttempts,
} from "./session.ts";
import {
  releaseHashIngress,
  type AcceptScanEdgeParams,
  type GraphTaskEnv,
  type HashAttemptParams,
  type HashIngressHolder,
  type PrepareReducerParams,
  type ReducerPlacementFacts,
  type RunTaskParams,
  type ResidentKMeansLoadParams,
  type ResidentKMeansNormalizeParams,
  type ResidentKMeansRoundParams,
  type ResidentKMeansReleaseParams,
} from "./types.ts";

export {
  emptyNodeSessionLease,
  graphNodeStub,
  graphSlotObjectName,
  prepareNodeSession,
  releaseNodeSession,
} from "./session.ts";
export { r2Bucket } from "./program.ts";
export type {
  GraphJobSource,
  GraphTaskEnv,
  GraphTaskNode,
  HashIngressHolder,
  RunTaskParams,
} from "./types.ts";

export type NodePingPayload = {
  ok: true;
  doId: string;
  constructedAt: number;
  handledAt: number;
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/**
 * Pooled nodes run one execution at a time; a claim on a busy node waits this long for
 * it to finish (jobs take turns) before failing. Also bounds how long an edge that
 * arrives before its reducer is prepared waits for that reducer's session.
 */
const NODE_CLAIM_WAIT_MS = 120 * 1000;
const RESIDENT_SESSION_TTL_MS = 10 * 60 * 1000;

type ResidentMemorySample = ResidentMemoryStats & { wallMs: number };

async function measureResident(
  graph: ResidentGraph,
  started: number,
): Promise<ResidentMemorySample> {
  const memory = await graph.memory();
  return { ...memory, wallMs: performance.now() - started };
}

type ResidentKMeansSession = {
  jobId: string;
  attempt: number;
  sessionId: string;
  graph: ResidentGraph;
  taskId: number;
  memory: {
    load: ResidentMemorySample;
    normalization?: ResidentMemorySample;
    rounds?: ResidentMemorySample & { count: number };
  };
  nextIteration: number;
  expiresAt: number;
};

/** Graph task execution on a node Durable Object. */
export class WcolNode extends DurableObject {
  protected readonly nodeState: DurableObjectState;
  protected readonly nodeEnv: GraphTaskEnv;
  private readonly constructedAt: number;
  private readonly sessionLease: NodeSessionLease = emptyNodeSessionLease();
  /** This object's own Wasm instance (never shared with other objects in the isolate). */
  private readonly wasm = nodeWasm(this.sessionLease);
  private residentKMeans: ResidentKMeansSession | null = null;
  private readonly hashIngress: HashIngressHolder = { session: null };
  /** Attempts this node was told to abort; their late RPCs fail at once. */
  private readonly aborted: AbortedAttempts = new Map();
  private execution: NodeExecution | null = null;
  /**
   * Aborted when the node releases `execution`. Releasing frees the node for the next
   * claim, so work still running for the old execution must stop: a scan that kept going
   * held its own Wasm instance beside the next scan's, past the isolate's memory limit.
   */
  private executionAbort: AbortController | null = null;
  private lastEnd: SessionEnd | null = null;
  private nextExecutionId = 1;

  constructor(state: DurableObjectState, env: GraphTaskEnv) {
    super(state, env);
    this.nodeState = state;
    this.nodeEnv = env;
    this.constructedAt = performance.now();
  }

  /** Cold-start probe: no Wasm, no storage. */
  async ping(): Promise<NodePingPayload> {
    return {
      ok: true,
      doId: this.nodeState.id.toString(),
      constructedAt: this.constructedAt,
      handledAt: performance.now(),
    };
  }

  /**
   * An instance from an earlier deploy can outlive it (it is not evicted while it has I/O
   * pending), and would run old code against new programs. Reset it: the caller's retry
   * reaches a fresh instance of this object on the current deploy.
   */
  private resetIfStale(deployId: string | undefined): void {
    const check = checkDeploy(this.nodeEnv.WCOL_DEPLOY_ID, deployId);
    if (check.kind === "current") return;
    if (check.kind === "coordinator-stale") throw new Error(check.message);
    const reason = check.message;
    console.warn(JSON.stringify({ event: "stale_node_reset", doId: this.nodeState.id.toString().slice(0, 8), reason }));
    this.dropResidentKMeans();
    releaseHashIngress(this.hashIngress, new Error(reason));
    this.nodeState.abort(reason);
    throw new Error(reason);
  }

  /** Resolved on every claim, release, and reducer session start; waiters re-check. */
  private readonly stateWaiters: Array<() => void> = [];

  private stateChanged(): void {
    for (const wake of this.stateWaiters.splice(0)) wake();
  }

  /** Wait until `ready()` holds, at most `ms`. Re-checked on each state change. */
  private async whenReady(ready: () => boolean, ms: number): Promise<void> {
    const deadline = Date.now() + ms;
    while (!ready()) {
      const left = deadline - Date.now();
      if (left <= 0) return;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, left);
        this.stateWaiters.push(() => {
          clearTimeout(timer);
          resolve();
        });
      });
    }
  }

  /** A node runs one execution at a time: wait for the current one, then claim. */
  private async claim(params: { jobId: string; executionAttempt: number; deployId?: string }): Promise<NodeExecution> {
    this.resetIfStale(params.deployId);
    this.refuseSuperseded(params);
    await this.whenReady(() => this.execution === null || attemptSuperseded(this.aborted, this.execution, params), NODE_CLAIM_WAIT_MS);
    this.refuseSuperseded(params);
    if (this.residentKMeans && (this.residentKMeans.jobId !== params.jobId || this.residentKMeans.attempt !== params.executionAttempt)) {
      await this.dropResidentKMeans();
    }
    // Throws "node is executing" if the bound passed with the node still busy.
    const execution = beginNodeExecution(this.execution, this.nextExecutionId, params);
    this.nextExecutionId += 1;
    this.execution = execution;
    this.executionAbort = new AbortController();
    this.stateChanged();
    return execution;
  }

  /**
   * Scans start alongside reducer setup, so an edge can arrive before this attempt's
   * reducer is prepared (or while the pooled node finishes another job). Wait for the
   * session instead of refusing; an attempt that already ended here will not come back.
   */
  private async awaitSession(params: { jobId: string; executionAttempt: number }): Promise<void> {
    await this.whenReady(
      () =>
        (this.execution !== null && sameNodeAttempt(this.execution, params) && this.hashIngress.session !== null) ||
        (this.lastEnd !== null && sameNodeAttempt(this.lastEnd, params)) ||
        attemptSuperseded(this.aborted, this.execution, params),
      NODE_CLAIM_WAIT_MS,
    );
    this.refuseSuperseded(params);
    this.requireActive(params);
  }

  private refuseSuperseded(params: { jobId: string; executionAttempt: number }): void {
    const resident = this.residentKMeans;
    const residentSuperseded = resident?.jobId === params.jobId && resident.attempt > params.executionAttempt;
    if (!residentSuperseded && !attemptSuperseded(this.aborted, this.execution, params)) return;
    throw new Error(`hash attempt ${params.executionAttempt} of ${params.jobId} was aborted or superseded on this node`);
  }

  private noteProgress(executionId: number): void {
    if (this.execution?.id !== executionId) return;
    touchNodeLease(this.execution);
  }

  private owns(executionId: number): boolean {
    return ownsExecution(this.execution, executionId);
  }

  private releaseOwned(executionId: number, reason: string): void {
    const released = releaseIfOwner(this.execution, executionId, reason);
    if (!released.ended) return;
    this.lastEnd = released.ended;
    this.execution = released.current;
    this.executionAbort?.abort(new Error(`node execution released: ${reason}`));
    this.executionAbort = null;
    releaseHashIngress(this.hashIngress);
    if (!this.residentKMeans) releaseNodeSession(this.sessionLease);
    if (this.residentKMeans) void this.nodeState.storage.setAlarm(this.residentKMeans.expiresAt).catch(() => undefined);
    else void this.nodeState.storage.deleteAlarm().catch(() => undefined);
    this.stateChanged();
  }

  /** Cleanup runs only while this id still owns the node. A later session is left alone. */
  private persist(executionId: number, promise: Promise<unknown>): void {
    this.nodeState.waitUntil(promise.finally(() => this.releaseOwned(executionId, "stream settled")));
  }

  /**
   * The lease alarm only backstops a hung node. `allowUnconfirmed` lets the RPC reply
   * without waiting for that storage write to be confirmed (the output gate).
   */
  private async arm(execution: NodeExecution): Promise<void> {
    await this.nodeState.storage.setAlarm(execution.expiresAt, { allowUnconfirmed: true });
  }

  private requireActive(params: { jobId: string; executionAttempt: number }): void {
    if (!this.execution || !sameNodeAttempt(this.execution, params)) {
      const current = this.execution
        ? `current id ${this.execution.id} job ${this.execution.jobId} attempt ${this.execution.executionAttempt}`
        : "no current execution";
      const last = this.lastEnd
        ? `last end id ${this.lastEnd.id} job ${this.lastEnd.jobId} attempt ${this.lastEnd.executionAttempt} reason ${this.lastEnd.reason}`
        : "no prior end";
      throw new Error(
        `hash attempt is not the active node session; rpc job ${params.jobId} attempt ${params.executionAttempt}; ${current}; ${last}`,
      );
    }
    touchNodeLease(this.execution);
  }

  /** One hash scan. Returns a streaming RPC body when the scan finishes. */
  async runTask(params: RunTaskParams) {
    const enteredAt = Date.now();
    const execution = await this.claim(params);
    const claimedAt = Date.now();
    try {
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      await this.arm(execution);
      return await runTaskOnNode({
        env: this.nodeEnv,
        wasm: this.wasm,
        params,
        timing: { enteredAt, claimedAt, armedAt: Date.now() },
        durableObjectId: this.nodeState.id.toString(),
        waitUntil: (promise) => this.persist(execution.id, promise),
        hooks: {
          onActivity: () => this.noteProgress(execution.id),
          ownsSession: () => this.owns(execution.id),
          signal: this.executionAbort?.signal,
        },
      });
    } catch (error) {
      this.releaseOwned(execution.id, "runTask failed");
      throw error;
    }
  }

  async runGatherTask(params: RunTaskParams) {
    const rpcAt = Date.now();
    const execution = await this.claim(params);
    try {
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      await this.arm(execution);
      return await runGatherTaskOnNode({
        env: this.nodeEnv,
        wasm: this.wasm,
        params,
        rpcAt,
        onActivity: () => this.noteProgress(execution.id),
      });
    } finally {
      this.releaseOwned(execution.id, "gather finished");
    }
  }

  async loadResidentKMeans(params: ResidentKMeansLoadParams) {
    const execution = await this.claim(params);
    try {
      await this.dropResidentKMeans();
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      await this.arm(execution);
      const started = performance.now();
      const graph = await runResidentTaskOnNode({
        env: this.nodeEnv,
        wasm: this.wasm,
        params,
        rpcAt: Date.now(),
        onActivity: () => this.noteProgress(execution.id),
      });
      let loadMemory: ResidentMemorySample;
      try {
        loadMemory = await measureResident(graph, started);
      } catch (error) {
        await graph.release();
        throw error;
      }
      this.residentKMeans = {
        jobId: params.jobId,
        attempt: params.executionAttempt,
        sessionId: params.sessionId,
        graph,
        taskId: params.taskId,
        memory: { load: loadMemory },
        nextIteration: params.startIteration,
        expiresAt: Date.now() + RESIDENT_SESSION_TTL_MS,
      };
      return { moments: graph.metadata };
    } finally {
      this.releaseOwned(execution.id, "resident graph load finished");
    }
  }

  async normalizeResidentKMeans(params: ResidentKMeansNormalizeParams) {
    const execution = await this.claim(params);
    try {
      const session = this.requireResidentKMeans(params);
      const started = performance.now();
      await session.graph.request(1, params.normalization);
      session.memory.normalization = await measureResident(session.graph, started);
      session.expiresAt = Date.now() + RESIDENT_SESSION_TTL_MS;
      return { ready: true as const };
    } finally {
      this.releaseOwned(execution.id, "resident K-means normalized");
    }
  }

  async runResidentKMeansRound(params: ResidentKMeansRoundParams) {
    const execution = await this.claim(params);
    try {
      const session = this.requireResidentKMeans(params);
      if (params.iteration !== session.nextIteration) throw new Error("resident K-means iteration is stale or out of order");
      const started = performance.now();
      const partial = await session.graph.request(2, params.globalState);
      const memory = await measureResident(session.graph, started);
      session.memory.rounds = {
        ...memory,
        wallMs: (session.memory.rounds?.wallMs ?? 0) + memory.wallMs,
        count: (session.memory.rounds?.count ?? 0) + 1,
      };
      session.nextIteration += 1;
      session.expiresAt = Date.now() + RESIDENT_SESSION_TTL_MS;
      return partial;
    } finally {
      this.releaseOwned(execution.id, "resident K-means round finished");
    }
  }

  async releaseResidentKMeans(params: ResidentKMeansReleaseParams) {
    const session = this.residentKMeans;
    if (!session || session.jobId !== params.jobId || session.attempt !== params.executionAttempt || session.sessionId !== params.sessionId) return;
    await this.dropResidentKMeans();
    if (!this.execution && !this.residentKMeans) await this.nodeState.storage.deleteAlarm().catch(() => undefined);
  }

  private async dropResidentKMeans(): Promise<void> {
    const session = this.residentKMeans;
    this.residentKMeans = null;
    if (session) {
      // Detach this Wasm before awaiting cleanup, so a later claim cannot reuse it.
      releaseNodeSession(this.sessionLease);
      await session.graph.release();
      await this.nodeEnv.OUTPUT?.put(
        `_jobs/${session.jobId}/a${session.attempt}/t${session.taskId}-resident-memory.json`,
        JSON.stringify(session.memory),
      ).catch((error: unknown) => {
        console.warn(JSON.stringify({ event: "resident_memory_report_failed", error: String(error) }));
      });
    }
  }

  private requireResidentKMeans(params: { jobId: string; executionAttempt: number; sessionId: string }): ResidentKMeansSession {
    const session = this.residentKMeans;
    if (!session || session.jobId !== params.jobId || session.attempt !== params.executionAttempt || session.sessionId !== params.sessionId) {
      throw new Error("resident K-means session is missing or fenced");
    }
    if (Date.now() >= session.expiresAt) throw new Error("resident K-means session expired");
    return session;
  }

  async runParquetShardTask(params: import("./types.ts").RunParquetShardParams) {
    const execution = await this.claim(params);
    try {
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      await this.arm(execution);
      return await runParquetShardTaskOnNode({
        env: this.nodeEnv,
        wasm: this.wasm,
        params,
        onActivity: () => this.noteProgress(execution.id),
      });
    } finally {
      this.releaseOwned(execution.id, "parquet shard finished");
    }
  }

  async runConcatTask(params: import("./types.ts").RunConcatTaskParams) {
    const execution = await this.claim(params);
    try {
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      await this.arm(execution);
      return await runConcatTaskOnNode({ env: this.nodeEnv, wasm: this.wasm, params });
    } finally {
      this.releaseOwned(execution.id, "concat finished");
    }
  }

  async prepareReducer(params: PrepareReducerParams) {
    const execution = await this.claim(params);
    try {
      prepareNodeSession(this.sessionLease, this.nodeEnv, params);
      if (this.hashIngress.session && !sameNodeAttempt(this.hashIngress.session, params)) {
        releaseHashIngress(this.hashIngress, new Error("superseded hash attempt"));
      }
      await this.arm(execution);
      const handle = await prepareReducerOnNode({
        env: this.nodeEnv,
        wasm: this.wasm,
        params,
        holder: this.hashIngress,
        durableObjectId: this.nodeState.id.toString(),
        waitUntil: (promise) => this.persist(execution.id, promise),
        hooks: { onActivity: () => this.noteProgress(execution.id), ownsSession: () => this.owns(execution.id) },
      });
      // Edges that arrived before this reducer was ready are waiting on its session.
      this.stateChanged();
      return handle;
    } catch (error) {
      this.releaseOwned(execution.id, "prepareReducer failed");
      throw error;
    }
  }

  async acceptScanEdge(params: AcceptScanEdgeParams) {
    try {
      await this.awaitSession(params);
      return await acceptScanEdgeOnNode(this.hashIngress, params);
    } catch (error) {
      refuseScanEdge(params.stream);
      throw error;
    }
  }

  async waitConsumeReady(params: HashAttemptParams) {
    await this.awaitSession(params);
    return waitConsumeReadyOnNode(this.hashIngress, params);
  }

  async completeScan(params: HashAttemptParams) {
    await this.awaitSession(params);
    completeScanOnNode(this.hashIngress, params);
  }

  async sealReducer(params: HashAttemptParams) {
    // Once every port has reached EOF the reducer finishes and releases before
    // the coordinator's seal arrives. Its result or failure is on the body
    // stream, so a late seal for that same attempt has nothing left to do.
    if (!this.execution && this.lastEnd?.reason === "stream settled" && sameNodeAttempt(this.lastEnd, params)) return;
    this.requireActive(params);
    sealReducerOnNode(this.hashIngress, params);
  }

  async placement(): Promise<ReducerPlacementFacts> {
    return { colo: await coloOnce(), runtime: "node" };
  }

  async abortHashAttempt(params: HashAttemptParams & { error?: string }) {
    noteAborted(this.aborted, params.jobId, params.executionAttempt);
    // Edges and claims waiting for this attempt fail now instead of at their timeout.
    this.stateChanged();
    if (this.execution && !sameNodeAttempt(this.execution, params)) return;
    abortHashAttemptOnNode(this.hashIngress, params);
    if (this.execution && sameNodeAttempt(this.execution, params)) {
      this.releaseOwned(this.execution.id, "abortHashAttempt");
    }
    else if (this.residentKMeans) await this.nodeState.storage.setAlarm(this.residentKMeans.expiresAt).catch(() => undefined);
    else await this.nodeState.storage.deleteAlarm().catch(() => undefined);
    if (this.residentKMeans?.jobId === params.jobId && this.residentKMeans.attempt === params.executionAttempt) {
      await this.dropResidentKMeans();
      if (!this.execution && !this.residentKMeans) await this.nodeState.storage.deleteAlarm().catch(() => undefined);
    }
  }

  async alarm(): Promise<void> {
    const execution = this.execution;
    if (!execution) {
      if (this.residentKMeans && Date.now() < this.residentKMeans.expiresAt) {
        await this.nodeState.storage.setAlarm(this.residentKMeans.expiresAt);
        return;
      }
      await this.dropResidentKMeans();
      if (!this.execution && !this.residentKMeans) await this.nodeState.storage.deleteAlarm().catch(() => undefined);
      return;
    }
    if (!nodeLeaseExpired(execution)) {
      await this.nodeState.storage.setAlarm(execution.expiresAt);
      return;
    }
    abortHashAttemptOnNode(this.hashIngress, {
      jobId: execution.jobId,
      executionAttempt: execution.executionAttempt,
      error: "hash attempt lease expired",
    });
    this.releaseOwned(execution.id, "lease expired");
    if (this.residentKMeans?.jobId === execution.jobId && this.residentKMeans.attempt === execution.executionAttempt) {
      await this.dropResidentKMeans();
      if (!this.execution && !this.residentKMeans) await this.nodeState.storage.deleteAlarm().catch(() => undefined);
    }
  }

  /** Drop this object's SQLite database and alarm. One write, not a per-row delete. */
  async purgeStorage(): Promise<{ ok: true; tablesBefore: number }> {
    let tablesBefore = -1;
    try {
      tablesBefore = [...this.nodeState.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table'")].length;
    } catch {
      tablesBefore = -2;
    }
    await this.nodeState.storage.deleteAlarm().catch(() => undefined);
    await this.dropResidentKMeans();
    await this.nodeState.storage.deleteAll();
    this.execution = null;
    return { ok: true, tablesBefore };
  }

  async fetch(_request: Request): Promise<Response> {
    return json({ error: "not found" }, 404);
  }
}
