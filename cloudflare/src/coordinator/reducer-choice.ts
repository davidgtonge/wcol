/**
 * Where a job's hash reducers run: node Durable Objects or containers. A pure decision from
 * the request and the planner's estimate of the aggregate's group state.
 *
 * Node reducers wake in under 200 ms but hold 16 MiB of groups each, at most 128 of them;
 * past that they spill to R2. Container reducers take 2–6 s to start but hold the instance's
 * memory and spill to local disk. So containers win only when the groups would not fit.
 */
import { CONTAINER_INSTANCES, containerGroupBytes, DEFAULT_CONTAINER_INSTANCE, type ContainerInstance } from "../node/containers.ts";

const MIB = 1024 * 1024;
/** Groups node reducers keep in memory: 128 reducers × the 16 MiB default target. */
export const NODE_REDUCER_MEMORY_BYTES = 128 * 16 * MIB;
/**
 * Reducers for a container job that does not fix a count. Measured on Q32 (full hits,
 * basic instances): 32 reducers ran in 8.9 s, 64 in 6.8 s.
 */
export const CONTAINER_REDUCERS = 64;

/** What the request asked for: a runtime, or `auto` to let the estimate decide. */
export type ReducerRequest = { runtime: "auto" | "node" | "container"; instance: ContainerInstance };

export type ReducerChoice =
  | { runtime: "node"; reason: string }
  | { runtime: "container"; instance: ContainerInstance; reason: string };

export function parseReducerRequest(row: Record<string, unknown>): ReducerRequest {
  const runtime = row.reducers === "node" || row.reducers === "container" ? row.reducers : "auto";
  const instance = CONTAINER_INSTANCES.find((name) => name === row.reducerInstance) ?? DEFAULT_CONTAINER_INSTANCE;
  return { runtime, instance };
}

/** `groupBytes` is the planner's estimate under node budgets; 0 means no aggregate. */
export function chooseReducers(request: ReducerRequest, groupBytes: number): ReducerChoice {
  const estimate = `estimated group state ${formatBytes(groupBytes)}`;
  switch (request.runtime) {
    case "node":
      return { runtime: "node", reason: "requested node reducers" };
    case "container":
      return { runtime: "container", instance: request.instance, reason: `requested container reducers (${request.instance})` };
    case "auto":
      return groupBytes > NODE_REDUCER_MEMORY_BYTES
        ? {
            runtime: "container",
            instance: request.instance,
            reason: `${estimate} exceeds the ${formatBytes(NODE_REDUCER_MEMORY_BYTES)} node reducers hold in memory`,
          }
        : { runtime: "node", reason: groupBytes > 0 ? `${estimate} fits node reducers` : "no hash aggregate" };
  }
}

/**
 * The request the planner sees for container reducers: the instance's group budget and
 * `CONTAINER_REDUCERS`, unless the request set its own.
 */
export function containerRequestBody(body: string, instance: ContainerInstance): string {
  const request = JSON.parse(body) as Record<string, unknown>;
  return JSON.stringify({
    ...request,
    targetGroupBytes: request.targetGroupBytes ?? containerGroupBytes(instance),
    hashReducers: request.hashReducers ?? CONTAINER_REDUCERS,
  });
}

export function formatBytes(bytes: number): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }
  return `${value >= 10 || unit === 0 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}
