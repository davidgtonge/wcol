/** Sync FNV-128-style id. Works in Node and workerd (no `node:crypto`). */
function hashParts(parts: readonly string[]): string {
  let a = 2166136261;
  let b = 2166136261 ^ 0x9e3779b9;
  const text = parts.join("\0");
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    a = Math.imul(a ^ c, 16777619);
    b = Math.imul(b ^ (c + i), 16777619);
  }
  const hex = (n: number) => (n >>> 0).toString(16).padStart(8, "0");
  let out = "";
  let x = a >>> 0;
  let y = b >>> 0;
  for (let i = 0; i < 4; i++) {
    out += hex(x);
    const next = Math.imul(x ^ y, 16777619) >>> 0;
    x = y;
    y = next;
  }
  return out.slice(0, 32);
}

/**
 * Stable task id for a contiguous partition slot (HTTP coordinator and Cloudflare DO).
 * Prefer this in [`runDistributedHitsExecGroup`](./hits-exec-group.ts) so shard `i`
 * always maps to the same id when `rowGroupsPerJob` is fixed.
 *
 * Rust [`task_id_for_shard`](../../rust/wcol/src/plan/distributed.rs) uses `shard_index`
 * + `SourceVersion.object_key_hash` (u64); wire that id from the planner in DO runners.
 */
export function taskIdForShardIndex(
  sourceKey: string,
  sql: string,
  shardIndex: number,
): string {
  return hashParts(["wcol-coordinator-task-v1", sourceKey, sql, String(shardIndex)]);
}

/** Display form for a Rust planner [`TaskId`](../../rust/wcol/src/plan/distributed.rs) (u64). */
export function formatRustTaskId(taskIdU64: bigint): string {
  return taskIdU64.toString(16).padStart(16, "0");
}

/** Host envelope aligned with Rust [`JobAttempt`](../../rust/wcol/src/plan/distributed.rs). */
/**
 * Task id for one node of a physical plan: the source, the node slot, and the exact program
 * bytes. A retry of the same node on another worker gets the same id.
 */
export function taskIdForPlanNode(sourceKey: string, node: number, program: Uint8Array): string {
  let digest = 0x811c9dc5;
  let spread = 0x9e3779b9;
  for (let i = 0; i < program.byteLength; i++) {
    digest = Math.imul(digest ^ program[i]!, 16777619);
    spread = Math.imul(spread ^ (program[i]! + i), 16777619);
  }
  return hashParts([
    "wcol-plan-node",
    sourceKey,
    String(node),
    String(program.byteLength),
    (digest >>> 0).toString(16),
    (spread >>> 0).toString(16),
  ]);
}

export type JobAttemptRecord = {
  taskId: string;
  attempt: number;
  rowGroups: readonly number[];
  sourceObjectKeyHash: bigint;
  wireProgram: Uint8Array;
};

/** Stable logical task id (source + SQL + row-group set); attempt is separate. */
export function taskIdForShard(
  sourceKey: string,
  sql: string,
  rowGroups: number[],
): string {
  const sorted = [...rowGroups].sort((a, b) => a - b);
  return hashParts(["wcol-task-set", sourceKey, sql, sorted.join(",")]);
}

export type TaskAttemptResult =
  | { ok: true; taskId: string; attempt: number }
  | { ok: false; reason: "duplicate_completed" | "empty_partial" };

export type CoordinatorFinalizeError = {
  kind: "missing_tasks" | "unexpected_tasks";
  taskIds: string[];
};

/**
 * Coordinator v1: at most one **completed** partial per `taskId`.
 * Failed attempts are recorded but never merged; retries use a higher attempt number.
 */
export class CoordinatorTaskRegistry {
  private readonly completed = new Map<string, { attempt: number; partial: Uint8Array }>();
  private readonly failedAttempts = new Map<string, number[]>();

  registerCompletedAttempt(
    taskId: string,
    attempt: number,
    partial: Uint8Array,
  ): TaskAttemptResult {
    if (partial.byteLength === 0) {
      return { ok: false, reason: "empty_partial" };
    }
    const prior = this.completed.get(taskId);
    if (prior) {
      return { ok: false, reason: "duplicate_completed" };
    }
    this.completed.set(taskId, { attempt, partial });
    return { ok: true, taskId, attempt };
  }

  /** Record a failed worker run; does not contribute to reduce. */
  recordFailedAttempt(taskId: string, attempt: number): void {
    const list = this.failedAttempts.get(taskId) ?? [];
    if (!list.includes(attempt)) {
      list.push(attempt);
      this.failedAttempts.set(taskId, list);
    }
  }

  failedAttemptCount(taskId: string): number {
    return this.failedAttempts.get(taskId)?.length ?? 0;
  }

  completedCount(): number {
    return this.completed.size;
  }

  has(taskId: string): boolean {
    return this.completed.has(taskId);
  }

  /**
   * Require exactly one completed partial per expected task before Reduce.
   */
  finalizeForReduce(expectedTaskIds: readonly string[]):
    | { ok: true; partials: Uint8Array[] }
    | { ok: false; error: CoordinatorFinalizeError } {
    const expected = new Set(expectedTaskIds);
    const missing: string[] = [];
    for (const id of expectedTaskIds) {
      if (!this.completed.has(id)) {
        missing.push(id);
      }
    }
    if (missing.length > 0) {
      return { ok: false, error: { kind: "missing_tasks", taskIds: missing } };
    }
    const extra: string[] = [];
    for (const id of this.completed.keys()) {
      if (!expected.has(id)) {
        extra.push(id);
      }
    }
    if (extra.length > 0) {
      return { ok: false, error: { kind: "unexpected_tasks", taskIds: extra } };
    }
    const partials = expectedTaskIds.map((id) => this.completed.get(id)!.partial);
    return { ok: true, partials };
  }
}

/** Run a worker closure with bounded retries; only successful attempts register. */
export async function runTaskWithRetries<T>(
  registry: CoordinatorTaskRegistry,
  taskId: string,
  maxAttempts: number,
  run: (attempt: number) => Promise<{ partial: Uint8Array } & T>,
): Promise<{ partial: Uint8Array; attempt: number } & T> {
  if (maxAttempts < 1) {
    throw new Error("maxAttempts must be >= 1");
  }
  let lastError: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await run(attempt);
      const reg = registry.registerCompletedAttempt(taskId, attempt, result.partial);
      if (!reg.ok) {
        throw new Error(`coordinator rejected task ${taskId} attempt ${attempt}: ${reg.reason}`);
      }
      return { ...result, attempt };
    } catch (error) {
      lastError = error;
      registry.recordFailedAttempt(taskId, attempt);
      if (attempt >= maxAttempts) {
        break;
      }
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error(`task ${taskId} failed after ${maxAttempts} attempts`);
}

/** Cap parallel worker jobs from observed or estimated peak Wasm bytes per job. */
export function memoryAwareConcurrentJobs(options: {
  requestedConcurrency: number;
  availableMemoryBytes: number;
  peakMemoryPerJobBytes: number;
  headroomBytes?: number;
}): number {
  const headroom = options.headroomBytes ?? 8 * 1024 * 1024;
  const usable = Math.max(0, options.availableMemoryBytes - headroom);
  const perJob = Math.max(1, options.peakMemoryPerJobBytes);
  const cap = Math.max(1, Math.floor(usable / perJob));
  return Math.max(1, Math.min(options.requestedConcurrency, cap));
}
