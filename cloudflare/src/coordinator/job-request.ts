/**
 * A `POST /jobs` body, parsed once at the coordinator. The planner reads the body text
 * itself (`jobSources`, `planJob`); this is what the host reads of it: the job's id, its
 * fan-out and where its reducers run, each checked against this deployment's limits.
 * Pure: no I/O.
 */
import { CONTAINER_INSTANCES, type ContainerInstance } from "../node/containers.ts";
import type { JobSourceLocation } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { containerPlanKnobs, parseReducerRequest, type ReducerRequest } from "./reducer-choice.ts";
import type { GraphJobEnv } from "./record.ts";

export type JobRequest = {
  /** The body as the planner reads it. */
  readonly body: string;
  readonly fields: Readonly<Record<string, unknown>>;
  readonly jobId: string;
  /** Fault injection for retry tests (`"failFirstAttemptTaskId": "n<id>"`): that task's first attempt fails mid-stream. */
  readonly failTaskId?: number;
  /** Scans to plan, at most `WCOL_MAX_NODES`. */
  readonly maxNodes: number;
  readonly minBytesPerNode?: number;
  readonly hashReducers?: number;
  readonly targetGroupBytes?: number;
  /** Concurrent streams a reducer may hold; omitted uses `WCOL_MAX_ACTIVE_INGRESS`. */
  readonly maxActiveIngress?: number;
  readonly reducers: ReducerRequest;
};

/** What one job may ask of this deployment. */
export type JobLimits = {
  readonly maxNodes: number;
  readonly maxReducers: number;
  readonly maxContainerInstance: ContainerInstance;
  /** Hostnames URL sources may name. Empty: URL sources are refused. */
  readonly urlSourceHosts: ReadonlySet<string>;
};

export type ParsedJobRequest = { kind: "ok"; request: JobRequest } | { kind: "invalid"; error: string };

function positiveInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 1 ? Math.floor(value) : undefined;
}

const envInteger = (value: string | undefined): number | undefined => (value?.trim() ? positiveInteger(Number(value)) : undefined);

export function jobLimits(
  env: Pick<GraphJobEnv, "WCOL_MAX_NODES" | "WCOL_MAX_REDUCERS" | "WCOL_MAX_CONTAINER_INSTANCE" | "WCOL_URL_SOURCE_HOSTS">,
): JobLimits {
  const instance = CONTAINER_INSTANCES.find((name) => name === env.WCOL_MAX_CONTAINER_INSTANCE?.trim());
  return {
    maxNodes: envInteger(env.WCOL_MAX_NODES) ?? 32,
    maxReducers: envInteger(env.WCOL_MAX_REDUCERS) ?? 128,
    // `standard-4` (4 vCPU, 12 GiB) only where a deployment says so.
    maxContainerInstance: instance ?? "standard-3",
    urlSourceHosts: new Set(
      (env.WCOL_URL_SOURCE_HOSTS ?? "")
        .split(",")
        .map((host) => host.trim().toLowerCase())
        .filter((host) => host.length > 0),
    ),
  };
}

export function parseJobRequest(body: string, limits: JobLimits): ParsedJobRequest {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return { kind: "invalid", error: "body is not valid JSON" };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return { kind: "invalid", error: "body must be a JSON object" };
  const fields = value as Record<string, unknown>;
  if (typeof fields.jobId !== "string" || fields.jobId.length === 0) return { kind: "invalid", error: "jobId required" };
  const hashReducers = positiveInteger(fields.hashReducers);
  if (hashReducers !== undefined && hashReducers > limits.maxReducers) {
    return { kind: "invalid", error: `hashReducers must be at most ${limits.maxReducers}` };
  }
  const reducers = parseReducerRequest(fields);
  if (CONTAINER_INSTANCES.indexOf(reducers.instance) > CONTAINER_INSTANCES.indexOf(limits.maxContainerInstance)) {
    return { kind: "invalid", error: `reducerInstance must be at most ${limits.maxContainerInstance}` };
  }
  const fail = fields.failFirstAttemptTaskId;
  const ingress = positiveInteger(fields.maxActiveIngress);
  return {
    kind: "ok",
    request: {
      body,
      fields,
      jobId: fields.jobId,
      failTaskId: typeof fail === "string" && /^n\d+$/.test(fail) ? Number(fail.slice(1)) : undefined,
      // `WCOL_MAX_NODES` is the bound: a request may ask for fewer scans, not more.
      maxNodes: Math.min(positiveInteger(fields.maxNodes) ?? limits.maxNodes, limits.maxNodes),
      minBytesPerNode: positiveInteger(fields.minBytesPerNode),
      hashReducers,
      targetGroupBytes: positiveInteger(fields.targetGroupBytes),
      maxActiveIngress: ingress !== undefined && ingress <= 64 ? ingress : undefined,
      reducers,
    },
  };
}

/** The request as the planner sees it for container reducers on `instance`. */
export function withContainerReducers(request: JobRequest, instance: ContainerInstance): JobRequest {
  const knobs = containerPlanKnobs(request, instance);
  const fields = { ...request.fields, ...knobs };
  return { ...request, ...knobs, fields, body: JSON.stringify(fields) };
}

/** Why `sources` may not be read here, or null. Checked before any source is fetched. */
export function refuseUrlSources(sources: readonly JobSourceLocation[], limits: JobLimits): string | null {
  for (const source of sources) {
    if (!("url" in source)) continue;
    let host: string;
    try {
      host = new URL(source.url).hostname.toLowerCase();
    } catch {
      return `source URL is not valid: ${source.url}`;
    }
    if (!limits.urlSourceHosts.has(host)) {
      return limits.urlSourceHosts.size === 0
        ? "URL sources are disabled here (WCOL_URL_SOURCE_HOSTS is unset)"
        : `source host ${host} is not in WCOL_URL_SOURCE_HOSTS`;
    }
  }
  return null;
}
