/** The R2 records a node task leaves for the coordinator: errors, progress, timing, traces. */
import { type DualR2Env } from "../r2.ts";
import { type IsolateProgress, type RunTaskParams, type TaskTimingRecord } from "./types.ts";

export function traceObjectKey(jobId: string, taskId: number): string {
  return `_jobs/${jobId}/trace/${taskId}`;
}

export function slotsObjectKey(jobId: string, attempt: number, taskId: number): string {
  return `_jobs/${jobId}/a${attempt}/t${taskId}-slots.json`;
}

export function taskErrorObjectKey(jobId: string, attempt: number, taskId: number): string {
  return `_jobs/${jobId}/a${attempt}/t${taskId}-error.json`;
}

export function progressObjectKey(jobId: string, attempt: number, taskId: number): string {
  return `_jobs/${jobId}/a${attempt}/t${taskId}-progress.json`;
}

export function admitObjectKey(jobId: string, attempt: number, taskId: number): string {
  return `_jobs/${jobId}/a${attempt}/t${taskId}-admit.json`;
}

export async function recordTaskError(
  env: DualR2Env,
  jobId: string,
  attempt: number,
  taskId: number,
  slot: number,
  phase: string,
  error: unknown,
  admits?: unknown,
): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  const stack = error instanceof Error ? error.stack : undefined;
  console.error(JSON.stringify({ event: "runTask_failed", jobId, attempt, taskId, slot, phase, message }));
  if (!env.OUTPUT) return;
  await env.OUTPUT.put(
    taskErrorObjectKey(jobId, attempt, taskId),
    JSON.stringify({ taskId, slot, phase, message, stack, at: Date.now(), admits }),
  );
}

export async function writeIsolateProgress(
  env: DualR2Env,
  jobId: string,
  attempt: number,
  taskId: number,
  snapshot: IsolateProgress,
): Promise<void> {
  if (!env.OUTPUT) return;
  await env.OUTPUT.put(progressObjectKey(jobId, attempt, taskId), JSON.stringify(snapshot));
}

export async function writeTaskTiming(env: DualR2Env, params: RunTaskParams, record: TaskTimingRecord): Promise<void> {
  if (!env.OUTPUT) return;
  await env.OUTPUT.put(slotsObjectKey(params.jobId, params.executionAttempt, params.taskId), JSON.stringify(record));
}
