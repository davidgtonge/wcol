import { createNodeWasm } from "../wasm.ts";
import type { WcolStreamWasm } from "../../../src/runtime/host/wasm-stream.ts";
import type { GraphDoLocationHint, GraphTaskEnv, GraphTaskNode, RunTaskParams } from "./types.ts";

const GRAPH_DO_LOCATION_HINTS = new Set<string>(["wnam", "enam", "sam", "weur", "eeur", "apac", "oc", "afr", "me"]);

/**
 * Where new Durable Objects are created: `WCOL_DO_LOCATION_HINT`, near the data bucket, so
 * scans, their coordinator and R2 share a region. A hint only places an object on first
 * access; an existing object stays where it is.
 */
export function doLocation(env: { WCOL_DO_LOCATION_HINT?: string }): { locationHint: GraphDoLocationHint } | undefined {
  const raw = env.WCOL_DO_LOCATION_HINT?.trim();
  if (!raw) return undefined;
  if (!GRAPH_DO_LOCATION_HINTS.has(raw)) throw new Error(`invalid WCOL_DO_LOCATION_HINT: ${raw}`);
  return { locationHint: raw as GraphDoLocationHint };
}

/** Highest aborted attempt per job this node has been told about, newest jobs last. */
export type AbortedAttempts = Map<string, number>;

/** Jobs remembered; an abort older than this many jobs ago no longer matters. */
const ABORTED_JOBS_KEPT = 64;

export function noteAborted(aborted: AbortedAttempts, jobId: string, attempt: number): void {
  const previous = aborted.get(jobId) ?? 0;
  aborted.delete(jobId);
  aborted.set(jobId, Math.max(previous, attempt));
  for (const oldest of aborted.keys()) {
    if (aborted.size <= ABORTED_JOBS_KEPT) break;
    aborted.delete(oldest);
  }
}

/**
 * Whether work for `params` is already dead on this node: its attempt was aborted here, or
 * a newer attempt of the same job holds the node. An RPC can arrive after the abort that
 * should have stopped it; it must fail at once, not hold the node or wait for a session.
 */
export function attemptSuperseded(
  aborted: AbortedAttempts,
  execution: { jobId: string; executionAttempt: number } | null,
  params: { jobId: string; executionAttempt: number },
): boolean {
  if ((aborted.get(params.jobId) ?? 0) >= params.executionAttempt) return true;
  return execution !== null && execution.jobId === params.jobId && execution.executionAttempt > params.executionAttempt;
}

/** Error prefix for a node instance that resets itself because it is from an old deploy. */
export const STALE_NODE_MARK = "stale node instance:";
/** Error prefix for a call from a coordinator still on an older deploy (a rollout in progress). */
export const STALE_COORDINATOR_MARK = "stale coordinator:";

/**
 * How a node treats a call from a coordinator on deploy `coordinator`. An instance can outlive
 * its deploy and would run old code against new programs: it resets (`node-stale`). During a
 * rollout the coordinator can be the old side instead; the node refuses that call but keeps
 * its state (`coordinator-stale`), since resetting a current node helps no one. Deploy ids
 * start with their deploy time in base 36 (scripts/deploy-worker.ts); ids without one, and
 * unset ids (local runs, tests), keep the older rule: any mismatch resets the node.
 */
export type DeployCheck =
  | { readonly kind: "current" }
  | { readonly kind: "node-stale"; readonly message: string }
  | { readonly kind: "coordinator-stale"; readonly message: string };

export function checkDeploy(mine: string | undefined, coordinator: string | undefined): DeployCheck {
  if (!mine || !coordinator || mine === coordinator) return { kind: "current" };
  const mineAt = deployTime(mine);
  const coordinatorAt = deployTime(coordinator);
  if (mineAt !== null && coordinatorAt !== null && coordinatorAt < mineAt) {
    return { kind: "coordinator-stale", message: `${STALE_COORDINATOR_MARK} deploy ${coordinator}, node is on ${mine}` };
  }
  return { kind: "node-stale", message: `${STALE_NODE_MARK} deploy ${mine}, coordinator is on ${coordinator}` };
}

function deployTime(id: string): number | null {
  const prefix = id.split("-")[0] ?? "";
  if (!/^[0-9a-z]+$/.test(prefix)) return null;
  const at = Number.parseInt(prefix, 36);
  return Number.isFinite(at) ? at : null;
}

/** The Durable Object name of plan slot `slot` in one job attempt: fresh objects per attempt. */
export function graphSlotObjectName(jobId: string, attempt: number, slot: number): string {
  return `graph:${jobId}:a${attempt}:s${slot}`;
}

export function graphNodeStub(env: GraphTaskEnv, jobId: string, attempt: number, slot: number): GraphTaskNode {
  const id = env.WCOL_NODE.idFromName(graphSlotObjectName(jobId, attempt, slot));
  return env.WCOL_NODE.get(id, doLocation(env)) as unknown as GraphTaskNode;
}

/** Where a job's hash reducers run: node Durable Objects, or containers (`reducer-container.ts`). */
export type ReducerRuntime = "node" | "container";

/** The reducer at plan slot `slot`, wherever this job's reducers run. */
export function reducerStub(
  env: GraphTaskEnv,
  jobId: string,
  attempt: number,
  slot: number,
  runtime: ReducerRuntime,
): GraphTaskNode {
  switch (runtime) {
    case "node":
      return graphNodeStub(env, jobId, attempt, slot);
    case "container": {
      const containers = env.WCOL_REDUCER_CONTAINER;
      if (!containers) throw new Error("container reducers need the WCOL_REDUCER_CONTAINER binding");
      const id = containers.idFromName(graphSlotObjectName(jobId, attempt, slot));
      return containers.get(id, doLocation(env)) as unknown as GraphTaskNode;
    }
  }
}

export type NodeSessionLease = {
  wasmBuild: string | null;
  jobId: string | null;
  executionAttempt: number | null;
  /** This object's Wasm instance; dropped between attempts and after each task. */
  wasm: Promise<WcolStreamWasm> | null;
};

/** One node object's Wasm: loaded once per session, shared by that object's tasks only. */
export type NodeWasm = { load(): Promise<WcolStreamWasm>; reset(): void };

export function nodeWasm(lease: NodeSessionLease): NodeWasm {
  return {
    load: () => (lease.wasm ??= createNodeWasm()),
    reset: () => {
      lease.wasm = null;
    },
  };
}

/**
 * Abort a hash attempt only after this long with no progress.
 * Activity renews the deadline. The alarm checks it before aborting.
 */
export const NODE_HASH_LEASE_MS = 10 * 60 * 1000;

/** One in-flight execution on a node. A second claim is rejected until this one releases. */
export type NodeExecution = {
  id: number;
  jobId: string;
  executionAttempt: number;
  expiresAt: number;
};

export function beginNodeExecution(
  current: NodeExecution | null,
  nextId: number,
  params: { jobId: string; executionAttempt: number },
  now = Date.now(),
): NodeExecution {
  if (current) {
    throw new Error(`node is executing ${current.jobId} attempt ${current.executionAttempt}`);
  }
  return {
    id: nextId,
    jobId: params.jobId,
    executionAttempt: params.executionAttempt,
    expiresAt: now + NODE_HASH_LEASE_MS,
  };
}

export function touchNodeLease(execution: NodeExecution, now = Date.now()): void {
  execution.expiresAt = now + NODE_HASH_LEASE_MS;
}

export function nodeLeaseExpired(execution: NodeExecution, now = Date.now()): boolean {
  return now >= execution.expiresAt;
}

export function ownsExecution(current: { id: number } | null, executionId: number): boolean {
  return current?.id === executionId;
}

export type SessionEnd = {
  id: number;
  jobId: string;
  executionAttempt: number;
  reason: string;
  at: number;
};

/**
 * Release only the execution this cleanup still owns.
 * A late `finally` from an older task returns the current execution unchanged.
 */
export function releaseIfOwner(
  current: NodeExecution | null,
  executionId: number,
  reason: string,
  now = Date.now(),
): { current: NodeExecution | null; ended: SessionEnd | null } {
  if (!current || !ownsExecution(current, executionId)) {
    return { current, ended: null };
  }
  return {
    current: null,
    ended: {
      id: current.id,
      jobId: current.jobId,
      executionAttempt: current.executionAttempt,
      reason,
      at: now,
    },
  };
}

export function sameNodeAttempt(
  execution: { jobId: string; executionAttempt: number },
  params: { jobId: string; executionAttempt: number },
): boolean {
  return execution.jobId === params.jobId && execution.executionAttempt === params.executionAttempt;
}

export function emptyNodeSessionLease(): NodeSessionLease {
  return { wasmBuild: null, jobId: null, executionAttempt: null, wasm: null };
}

/**
 * Drop stale Wasm after a Worker deploy and between job attempts on this DO.
 * Call synchronously at the start of a task before streaming work.
 */
export function prepareNodeSession(
  lease: NodeSessionLease,
  env: GraphTaskEnv,
  params: Pick<RunTaskParams, "jobId" | "executionAttempt">,
): void {
  const expectedBuild = env.WCOL_NODE_WASM_BUILD?.trim() || null;
  if (expectedBuild && lease.wasmBuild !== expectedBuild) {
    lease.wasm = null;
    lease.wasmBuild = expectedBuild;
    lease.jobId = null;
    lease.executionAttempt = null;
  }
  const jobChanged = lease.jobId !== params.jobId || lease.executionAttempt !== params.executionAttempt;
  if (jobChanged) {
    lease.wasm = null;
    lease.jobId = params.jobId;
    lease.executionAttempt = params.executionAttempt;
  }
}

/** Drop Wasm so a node isolate can hibernate after a task. */
export function releaseNodeSession(lease: NodeSessionLease): void {
  lease.wasm = null;
  lease.jobId = null;
  lease.executionAttempt = null;
}
