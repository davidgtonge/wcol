/** The attempt report and its `timing.json`: stage spans, reducer records and usage. */
import type { GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { type HashJobTiming } from "../../../src/runtime/coordinator/run-hash-job.ts";
import {
  summarizeStages,
  type StageSpan,
  type StageTimeline,
} from "../../../src/runtime/coordinator/stage-timeline.ts";
import { slotsObjectKey } from "../node/records.ts";
import type { TaskTimingRecord } from "../node/types.ts";
import { createLimiter } from "../../../src/runtime/concurrency.ts";
import { summarizeUsage, type AttemptUsage } from "../../../src/runtime/coordinator/attempt-usage.ts";
import { outputBucket } from "./plan.ts";
import { type GraphJobContext } from "./record.ts";
import type { TaskFacts } from "./live.ts";

/** What one attempt reports besides its outputs; persisted even when the attempt fails. */
export type AttemptReport = {
  readonly timeline: StageTimeline;
  schedule?: HashJobTiming;
  /**
   * What tasks reported: each scan's reads, location and partition bytes from its receipt
   * (scans write no timing record), each reducer's placement. The live view reads it too.
   */
  readonly facts: Map<number, TaskFacts>;
};

/** Reducers split ingest from finish work; only the node can see that boundary. */
type TaskRecordEntry = { taskId: number; reducer: boolean; record: Partial<TaskTimingRecord> };

/** Each reducer's timing record for the attempt; one small read each, after the attempt. */
async function taskRecords(ctx: GraphJobContext, jobId: string, attempt: number, graph: GraphPlan): Promise<TaskRecordEntry[]> {
  const reducerIds = new Set(graph.edges.filter((edge) => edge.partition !== undefined).map((edge) => edge.to));
  const limit = createLimiter(16);
  const records = await Promise.all(
    graph.tasks
      .filter((task) => reducerIds.has(task.id))
      .map((task) =>
        limit(async () => {
          const object = await outputBucket(ctx.env).get(slotsObjectKey(jobId, attempt, task.id));
          if (!object || object.body === null) return null;
          const record = JSON.parse(await object.text()) as Partial<TaskTimingRecord>;
          return { taskId: task.id, reducer: reducerIds.has(task.id), record };
        }),
      ),
  );
  return records.filter((entry): entry is TaskRecordEntry => entry !== null);
}

/** Reducers from their timing records; scans from the coordinator's scan spans and receipts. */
function attemptUsage(entries: readonly TaskRecordEntry[], report: AttemptReport): AttemptUsage {
  const scans = report.timeline
    .spans()
    .filter((span) => span.stage === "scan" && span.taskId !== undefined)
    .map((span) => {
      const facts = report.facts.get(span.taskId!);
      const reads = facts?.kind === "scan" ? facts : undefined;
      return {
        role: "scan" as const,
        startedAt: span.startMs,
        endedAt: span.endMs,
        sourceReads: reads?.sourceReads,
        sourceReadBytes: reads?.sourceReadBytes,
      };
    });
  return summarizeUsage([
    ...scans,
    ...entries.flatMap(({ reducer, record }) =>
      record.startedAt == null || record.endedAt == null
        ? []
        : [
            {
              role: reducer ? ("reducer" as const) : ("scan" as const),
              startedAt: record.startedAt,
              endedAt: record.endedAt,
              sourceReads: record.sourceReads,
              sourceReadBytes: record.sourceReadBytes,
              spillIo: record.spillIo,
            },
          ],
    ),
  ]);
}

function reducerSpans(entries: readonly TaskRecordEntry[]): StageSpan[] {
  const spans: StageSpan[] = [];
  for (const entry of entries) {
    if (!entry.reducer) continue;
    const { taskId, record } = entry;
    if (record.startedAt == null || record.endedAt == null) continue;
    const outcome = record.outcome === "ok" ? "ok" : "failed";
    if (record.inputsEndedAt == null) {
      spans.push({ stage: "reducerIngest", taskId, startMs: record.startedAt, endMs: record.endedAt, outcome });
      continue;
    }
    spans.push({ stage: "reducerIngest", taskId, startMs: record.startedAt, endMs: record.inputsEndedAt, outcome: "ok" });
    spans.push({ stage: "reducerFinish", taskId, startMs: record.inputsEndedAt, endMs: record.endedAt, outcome });
  }
  return spans;
}

export async function writeAttemptTiming(
  ctx: GraphJobContext,
  jobId: string,
  attempt: number,
  graph: GraphPlan | undefined,
  report: AttemptReport,
  failure: unknown,
): Promise<void> {
  const records = graph ? await taskRecords(ctx, jobId, attempt, graph).catch(() => []) : [];
  const spans = [...report.timeline.spans(), ...reducerSpans(records)];
  await outputBucket(ctx.env).put(
    `_jobs/${jobId}/timing.json`,
    JSON.stringify({
      attempt,
      outcome: failure === undefined ? "ok" : "failed",
      ...(failure === undefined ? {} : { error: failure instanceof Error ? failure.message : String(failure) }),
      ...(report.schedule ? { schedule: report.schedule } : {}),
      stages: summarizeStages(spans),
      ...(graph ? { usage: attemptUsage(records, report) } : {}),
      spans,
      tasks: graph?.tasks.map((task) => ({ taskId: task.id, slot: task.slot, requests: task.requestCount })) ?? [],
    }),
  );
}
