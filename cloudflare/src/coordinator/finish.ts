/** Commit one attempt: record outputs, then publish JSON, Parquet or a model. */
import type { GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { type ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { outputBucket, type Prepared } from "./plan.ts";
import { publishModel, publishParquet } from "./publish.ts";
import { runQueryConcatFinalizer } from "./finalizer-concat.ts";
import { type GraphJobContext, type GraphJobRecord, jobKey, requireJobRecord, taskKey } from "./record.ts";
import type { GraphRun } from "./run.ts";

async function rememberOutputs(ctx: GraphJobContext, job: GraphJobRecord, graph: GraphPlan, outputs: ObjectPart[], attempt: number): Promise<void> {
  for (const part of outputs) {
    const task = graph.tasks.find((item) => item.id === part.taskId);
    await ctx.store.put(taskKey(job.jobId, part.taskId ?? -1), {
      taskId: part.taskId,
      slot: task?.slot,
      state: "durable",
      executionAttempt: attempt,
      requestCount: task?.requestCount ?? 0,
      fetchedBytes: task?.fetchedBytes ?? 0,
      objectKey: part.objectKey,
      bytes: part.bytes,
    });
  }
}

export async function finishPrepared(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  ran: GraphRun,
  attempt: number,
  sync: boolean,
): Promise<string | null> {
  const latest = requireJobRecord(await ctx.store.get(jobKey(job.jobId)));
  if (latest.executionAttempt !== attempt) return null;
  if (prepared.graph) await rememberOutputs(ctx, job, prepared.graph, ran.outputs, attempt);
  if (prepared.plan.kind === "model") {
    if (!ran.model) throw new Error("iterative model execution returned no model artifact");
    await publishModel(ctx, job, attempt, prepared.plan.name, ran.model.artifact, ran.model.summary);
    return null;
  }
  if (prepared.plan.kind === "json") {
    const key = `_jobs/${job.jobId}/result.json`;
    if (!sync) await outputBucket(ctx.env).put(key, ran.jsonText ?? "");
    let committed = false;
    await ctx.transaction(async (txn) => {
      const record = requireJobRecord(await txn.get(jobKey(job.jobId)));
      if (record.executionAttempt !== attempt || record.state === "completed" || record.state === "failed") return;
      record.state = "completed";
      delete record.error;
      if (!sync) record.resultKey = key;
      record.leaseUntil = 0;
      record.updatedAt = Date.now();
      await txn.put(jobKey(job.jobId), record);
      committed = true;
    });
    if (committed) await ctx.deleteAlarm().catch(() => {});
    return ran.jsonText ?? null;
  }
  if (prepared.plan.kind === "parquet") {
    let outputs = ran.outputs;
    const graph = prepared.graph;
    if (
      graph &&
      graph.finalizer?.kind === "concat_parquet" &&
      graph.output?.layout === "single_file"
    ) {
      const finalPart = await runQueryConcatFinalizer(
        ctx,
        job.jobId,
        attempt,
        outputs,
        { bucket: prepared.plan.bucket, prefix: prepared.plan.prefix },
        graph.finalizer.partitionOrder,
      );
      outputs = [finalPart];
    }
    await publishParquet(
      ctx,
      job,
      prepared.plan.prefix,
      prepared.plan.bucket,
      outputs,
      attempt,
      graph?.output?.layout,
    );
    return null;
  }
  if (prepared.plan.kind === "concat") {
    // One output object: its create-once pointer claims that key (`<key>/_manifest.json`).
    // An empty prefix gave every concat job in the bucket the same pointer.
    await publishParquet(
      ctx,
      job,
      prepared.plan.key,
      prepared.plan.bucket,
      ran.outputs,
      attempt,
      "single_file",
      prepared.plan.key,
    );
    return null;
  }
  throw new Error("unsupported job kind at finish");
}
