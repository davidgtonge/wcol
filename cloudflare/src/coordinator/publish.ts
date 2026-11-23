import type { OutputLayout } from "../../../src/runtime/coordinator/execution-graph.ts";
import type { ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { r2Bucket } from "../node/node.ts";
import { jobEnded } from "./sweep.ts";
import {
  authoritativeManifestKey,
  type GraphJobContext,
  type GraphJobRecord,
  type ModelTrainingSummary,
  jobKey,
  pointerKey,
  requireJobRecord,
} from "./record.ts";

/** Create-once pointer: the existing body is this attempt's manifest, or it belongs to someone else. */
export function publishedPointerMatches(body: string, jobId: string, attempt: number): boolean {
  try {
    const parsed = JSON.parse(body) as { jobId?: string; parts?: { attempt?: number }[] };
    return (
      parsed.jobId === jobId &&
      Array.isArray(parsed.parts) &&
      parsed.parts.length > 0 &&
      parsed.parts.every((part) => part.attempt === attempt)
    );
  } catch {
    return false;
  }
}

export async function publishParquet(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prefix: string,
  bucketName: string,
  outputs: ObjectPart[],
  attempt: number,
  layout?: OutputLayout,
  resultKey?: string,
): Promise<void> {
  const current = requireJobRecord(await ctx.store.get(jobKey(job.jobId)));
  if (current.executionAttempt !== attempt || current.state === "completed" || current.state === "failed") return;
  const manifest = {
    jobId: job.jobId,
    publishedAt: new Date().toISOString(),
    format: "parquet" as const,
    layout: layout ?? (outputs.length > 1 ? "partitioned" : "single_file"),
    resultKey: resultKey ?? outputs[0]?.objectKey,
    parts: outputs.map((part) => ({
      objectKey: part.objectKey,
      bytes: part.bytes,
      ...(part.rows === undefined ? {} : { rows: part.rows }),
      ...(part.partitionValues.length ? { partitionValues: part.partitionValues } : {}),
      taskId: part.taskId,
      attempt,
    })),
  };
  const key = authoritativeManifestKey(job.jobId);
  const bucket = r2Bucket(ctx.env, bucketName);
  const body = JSON.stringify(manifest);
  await bucket.put(key, body);
  const pointer = pointerKey(prefix);
  const created = await bucket.put(pointer, body, { onlyIf: { etagDoesNotMatch: "*" } });
  if (created == null) {
    const existing = await bucket.get(pointer);
    const text = existing && existing.body != null ? await existing.text() : "";
    if (!publishedPointerMatches(text, job.jobId, attempt)) {
      throw new Error("published pointer already exists for a different result");
    }
  }
  let committed = false;
  await ctx.transaction(async (txn) => {
    const record = requireJobRecord(await txn.get(jobKey(job.jobId)));
    if (record.executionAttempt !== attempt || record.state === "completed" || record.state === "failed") return;
    record.manifestKey = key;
    record.outputObjects = outputs.length;
    record.outputBytes = outputs.reduce((n, part) => n + part.bytes, 0);
    record.state = "completed";
    record.leaseUntil = 0;
    record.updatedAt = Date.now();
    await txn.put(jobKey(job.jobId), record);
    committed = true;
  });
  if (committed) await jobEnded(ctx, job.jobId, outputs.map((part) => part.objectKey));
}

export async function publishModel(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  attempt: number,
  name: string,
  artifact: { centroids: number[]; features: string[]; means: number[]; scales: number[]; options: Record<string, number> },
  summary: ModelTrainingSummary,
): Promise<void> {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error("invalid model name");
  if (!Number.isInteger(summary.iterations) || summary.iterations < 0 || !Number.isFinite(summary.inertia) || summary.inertia < 0) {
    throw new Error("invalid model training summary");
  }
  const current = requireJobRecord(await ctx.store.get(jobKey(job.jobId)));
  if (current.executionAttempt !== attempt || current.state === "completed" || current.state === "failed") return;
  const key = `_models/${name}/model.json`;
  const body = JSON.stringify({ formatVersion: 1, ...artifact, training: summary, jobId: job.jobId });
  const bucket = ctx.env.OUTPUT;
  if (!bucket) throw new Error("R2 output binding is missing");
  const created = await bucket.put(key, body, {
    onlyIf: { etagDoesNotMatch: "*" },
    customMetadata: { wcolJobId: job.jobId, modelFormatVersion: "1" },
    httpMetadata: { contentType: "application/json" },
  });
  if (created == null) {
    const existing = await bucket.get(key);
    if (existing?.customMetadata?.wcolJobId !== job.jobId) {
      throw new Error(`model ${name} already exists`);
    }
  }
  let committed = false;
  await ctx.transaction(async (txn) => {
    const record = requireJobRecord(await txn.get(jobKey(job.jobId)));
    if (record.executionAttempt !== attempt || record.state === "completed" || record.state === "failed") return;
    record.modelName = name;
    record.modelKey = key;
    record.modelSummary = summary;
    record.state = "completed";
    record.leaseUntil = 0;
    record.updatedAt = Date.now();
    delete record.error;
    await txn.put(jobKey(job.jobId), record);
    committed = true;
  });
  if (committed) await jobEnded(ctx, job.jobId, []);
}

export async function failJob(ctx: GraphJobContext, jobId: string, message: string): Promise<void> {
  let failed = false;
  await ctx.transaction(async (txn) => {
    const current = (await txn.get(jobKey(jobId))) as GraphJobRecord | undefined;
    if (!current || current.state === "completed") return;
    failed = current.state !== "failed";
    current.state = "failed";
    current.error = message.slice(0, 500);
    current.updatedAt = Date.now();
    current.leaseUntil = 0;
    current.publishing = undefined;
    await txn.put(jobKey(jobId), current);
  });
  // Sweep only on the transition: a completed job's outputs are its result.
  if (failed) await jobEnded(ctx, jobId, []);
  else await ctx.deleteAlarm().catch(() => {});
}
