/**
 * Run-time scan balancing. The planner gives every hash scan of a split object its own
 * queue of units (row groups whose fetches stand alone) and a catalog of all of them. A
 * scan asks for its next unit: its own first, then one stolen from the back of the
 * fullest queue in its group, so slow scans shed their tails to fast ones. The planner
 * deals units to the queues by cost, most expensive first, so every scan starts on heavy
 * work and the back of each queue is the cheapest work left. The queues live
 * on the job's coordinator, per attempt: a new attempt starts from the plan again.
 */

/** The Wasm scan's ReadAt source for "next unit": four bytes come back, the unit id. */
export const CLAIM_SOURCE = 0xffff;
/** The answer when no unit is left for this scan. */
export const NO_UNIT = 0xffff_ffff;

/** A scan's planned queue, as the execution graph carries it. */
export type TaskWork = { readonly group: number; readonly ownUnits: readonly number[] };

/** Remaining units per task, per work group, per `jobId#aN` attempt. */
export type WorkQueues = Map<string, Map<number, Map<number, number[]>>>;

export function attemptKey(jobId: string, attempt: number): string {
  return `${jobId}#a${attempt}`;
}

/** Queue each task's own units for `attempt`. Tasks without work are not scans that balance. */
export function registerWork(
  queues: WorkQueues,
  attempt: string,
  tasks: readonly { readonly id: number; readonly work?: TaskWork }[],
): void {
  const groups = new Map<number, Map<number, number[]>>();
  for (const task of tasks) {
    if (!task.work) continue;
    let group = groups.get(task.work.group);
    if (!group) {
      group = new Map();
      groups.set(task.work.group, group);
    }
    group.set(task.id, [...task.work.ownUnits]);
  }
  queues.set(attempt, groups);
}

export function forgetWork(queues: WorkQueues, attempt: string): void {
  queues.delete(attempt);
}

/** The next unit for `taskId`: its own front, else the back of its group's fullest queue. */
export function nextUnit(queues: WorkQueues, attempt: string, taskId: number): number | null {
  const group = [...(queues.get(attempt)?.values() ?? [])].find((tasks) => tasks.has(taskId));
  if (!group) throw new Error(`task ${taskId} has no work queue in ${attempt}`);
  const own = group.get(taskId)!;
  if (own.length > 0) return own.shift()!;
  let fullest: number[] | undefined;
  for (const queue of group.values()) {
    if (queue.length > (fullest?.length ?? 0)) fullest = queue;
  }
  return fullest?.pop() ?? null;
}

type ScanReadAt = (source: number, offset: bigint | number, length: number) => Promise<Uint8Array>;

/** A scan's ReadAt: "next unit" goes to `next`, every other read to `read`. */
export function claimingReadAt(read: ScanReadAt, next: () => Promise<number | null> | number | null): ScanReadAt {
  return async (source, offset, length) => {
    if (source !== CLAIM_SOURCE) return read(source, offset, length);
    const answer = new Uint8Array(4);
    new DataView(answer.buffer).setUint32(0, (await next()) ?? NO_UNIT, true);
    return answer;
  };
}

/** Work queues for one run (local runners and tests). */
export function localWork(tasks: readonly { readonly id: number; readonly work?: TaskWork }[]): (taskId: number) => number | null {
  const queues: WorkQueues = new Map();
  registerWork(queues, "local", tasks);
  return (taskId) => nextUnit(queues, "local", taskId);
}
