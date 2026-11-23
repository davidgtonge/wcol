import { COORDINATOR_SLOT, type ExecutionPlan } from "./execution-graph.ts";

export type GraphPlan = Extract<ExecutionPlan, { kind: "graph" }>;

/** A streamed task with two consumers would run twice or be read twice. */
export function assertNoStreamFanOut(plan: GraphPlan): void {
  const outgoing = new Map<number, number>();
  for (const edge of plan.edges) {
    // A hash exchange is one producer execution with many partition edges.
    if (edge.partition !== undefined) continue;
    outgoing.set(edge.from, (outgoing.get(edge.from) ?? 0) + 1);
  }
  for (const [taskId, count] of outgoing) {
    if (count > 1) {
      throw new Error(`task ${taskId} streams to ${count} consumers; fan-out is not supported`);
    }
  }
}

export function isFullyColocated(plan: GraphPlan): boolean {
  return (
    plan.tasks.every((task) => task.slot === COORDINATOR_SLOT) &&
    plan.edges.every((edge) => edge.transport === "colocated" || edge.transport === "in_process")
  );
}

export function sinkTaskIds(plan: GraphPlan): number[] {
  const outgoing = new Set(plan.edges.map((edge) => edge.from));
  return plan.tasks.filter((task) => !outgoing.has(task.id)).map((task) => task.id);
}

/**
 * Object-sink tasks in `durableTaskIds` are skipped. Every other sink runs,
 * and running a sink recomputes its streamed producers.
 */
export function sinksToReplay(plan: GraphPlan, durableTaskIds: ReadonlySet<number>): number[] {
  return sinkTaskIds(plan).filter((taskId) => !durableTaskIds.has(taskId));
}

export type LeaseView = {
  state: string;
  leaseUntil?: number;
};

/**
 * `getAlarm()` is null while the handler is already running. A live lease
 * means that is not a stranded job.
 */
export function shouldArmAlarm(job: LeaseView, alarmAt: number | null, now: number): boolean {
  if (job.state === "completed" || job.state === "failed") return false;
  if ((job.leaseUntil ?? 0) > now) return false;
  return alarmAt == null;
}
