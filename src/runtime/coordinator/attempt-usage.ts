/**
 * What one attempt used, summed from its tasks' timing records, and what that costs.
 * Pure: the coordinator reads the records, these functions only add them up.
 */

export type TaskRole = "scan" | "reducer";

/** One task's timing record, as a node writes it (`TaskTimingRecord`), with its role. */
export type TaskUsageInput = {
  readonly role: TaskRole;
  readonly startedAt: number;
  readonly endedAt: number;
  readonly sourceReads?: number;
  readonly sourceReadBytes?: number;
  readonly spillIo?: {
    readonly puts: number;
    readonly putBytes: number;
    readonly reads: number;
    readonly readBytes: number;
  };
};

export type RoleUsage = {
  readonly tasks: number;
  /** Sum of each task's wall time from start to end. */
  readonly busyMs: number;
  readonly maxMs: number;
};

export type AttemptUsage = {
  readonly scan: RoleUsage;
  readonly reducer: RoleUsage;
  /** Source range reads (R2 GETs of the input). */
  readonly sourceReads: number;
  readonly sourceReadBytes: number;
  /** Spill run uploads (R2 PUTs) and reads (R2 GETs). */
  readonly spillPuts: number;
  readonly spillPutBytes: number;
  readonly spillReads: number;
  readonly spillReadBytes: number;
};

export function summarizeUsage(records: readonly TaskUsageInput[]): AttemptUsage {
  const role = (wanted: TaskRole): RoleUsage => {
    const spans = records.filter((r) => r.role === wanted).map((r) => Math.max(0, r.endedAt - r.startedAt));
    return {
      tasks: spans.length,
      busyMs: spans.reduce((sum, ms) => sum + ms, 0),
      maxMs: spans.reduce((max, ms) => Math.max(max, ms), 0),
    };
  };
  const sum = (pick: (r: TaskUsageInput) => number | undefined) =>
    records.reduce((total, r) => total + (pick(r) ?? 0), 0);
  return {
    scan: role("scan"),
    reducer: role("reducer"),
    sourceReads: sum((r) => r.sourceReads),
    sourceReadBytes: sum((r) => r.sourceReadBytes),
    spillPuts: sum((r) => r.spillIo?.puts),
    spillPutBytes: sum((r) => r.spillIo?.putBytes),
    spillReads: sum((r) => r.spillIo?.reads),
    spillReadBytes: sum((r) => r.spillIo?.readBytes),
  };
}

/** Workers Paid list prices in USD (Cloudflare pricing pages, Nov 2026). */
export type Pricing = {
  readonly doGbSecond: number;
  readonly doMemoryGb: number;
  readonly doRequest: number;
  readonly r2ClassA: number;
  readonly r2ClassB: number;
};

export const WORKERS_PAID_PRICING: Pricing = {
  doGbSecond: 12.5 / 1e6,
  // Every Durable Object is billed as 128 MB, whatever it uses.
  doMemoryGb: 0.128,
  doRequest: 0.15 / 1e6,
  r2ClassA: 4.5 / 1e6,
  r2ClassB: 0.36 / 1e6,
};

export type CostEstimate = {
  readonly doSeconds: number;
  readonly doDuration: number;
  readonly doRequests: number;
  readonly sourceReads: number;
  readonly spill: number;
  readonly total: number;
};

/**
 * List-price cost of one attempt, ignoring free allowances. Durable Object time counts each
 * task's busy span plus the coordinator for the job's wall time; a pooled node that stays in
 * memory after its task is not counted, so this is a lower bound on duration.
 * `rpcSessions` is the number of Durable Object RPC sessions the attempt opened.
 */
export function estimateCost(
  usage: AttemptUsage,
  wallMs: number,
  rpcSessions: number,
  pricing: Pricing = WORKERS_PAID_PRICING,
): CostEstimate {
  const doSeconds = (usage.scan.busyMs + usage.reducer.busyMs + wallMs) / 1000;
  const doDuration = doSeconds * pricing.doMemoryGb * pricing.doGbSecond;
  const doRequests = rpcSessions * pricing.doRequest;
  const sourceReads = usage.sourceReads * pricing.r2ClassB;
  const spill = usage.spillPuts * pricing.r2ClassA + usage.spillReads * pricing.r2ClassB;
  return { doSeconds, doDuration, doRequests, sourceReads, spill, total: doDuration + doRequests + sourceReads + spill };
}
