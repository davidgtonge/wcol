/**
 * Run opcode-11 page copy on a node isolate (the coordinator has no node Wasm): ReadAt
 * indexes map to `sourceKeys` in order. The query finalizer and standalone concat jobs
 * (compaction and row-group splits) both run here.
 */

import type { ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { planConcatProgram } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { finalizerObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { loadWasm, outputBucket } from "./plan.ts";
import type { GraphJobContext } from "./record.ts";
import { graphNodeStub } from "../node/session.ts";

async function runConcatOnNode(
  ctx: GraphJobContext,
  jobId: string,
  attempt: number,
  task: { program: Uint8Array; sourceKeys: string[]; outputKey: string; outputBucket: string },
): Promise<number> {
  const slot = 0;
  const done = await graphNodeStub(ctx.env, jobId, attempt, slot).runConcatTask({
    jobId,
    executionAttempt: attempt,
    deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
    slot,
    ...task,
  });
  return done.bytes;
}

export async function runQueryConcatFinalizer(
  ctx: GraphJobContext,
  jobId: string,
  attempt: number,
  shardParts: ObjectPart[],
  output: { bucket: string; prefix: string },
  partitionOrder: number[],
): Promise<ObjectPart> {
  const ordered = partitionOrder.map((taskId) => {
    const part = shardParts.find((item) => item.taskId === taskId);
    if (!part) throw new Error(`missing durable shard for task ${taskId}`);
    return part;
  });
  const wasm = await loadWasm();
  const footers = await Promise.all(
    ordered.map(async (part) => {
      const object = await outputBucket(ctx.env).get(part.objectKey);
      if (!object || object.body == null) throw new Error(`shard missing: ${part.objectKey}`);
      const fileSize = object.size ?? 0;
      const tail = await object.arrayBuffer();
      const footerBytes = new Uint8Array(tail);
      const len = new DataView(footerBytes.buffer, footerBytes.byteOffset, footerBytes.byteLength).getUint32(
        fileSize - 8,
        true,
      );
      return { fileSize, footer: footerBytes.subarray(fileSize - 8 - len, fileSize - 8) };
    }),
  );
  const program = planConcatProgram(wasm, footers);
  const outputKey = finalizerObjectKeyJobs(jobId, attempt);
  const bytes = await runConcatOnNode(ctx, jobId, attempt, {
    program,
    sourceKeys: ordered.map((part) => part.objectKey),
    outputKey,
    outputBucket: output.bucket,
  });
  return {
    objectKey: outputKey,
    partitionValues: [],
    bytes,
    parts: 1,
    taskId: 0xffff_fffe,
  };
}

export async function runStandaloneConcatJob(
  ctx: GraphJobContext,
  jobId: string,
  attempt: number,
  bucketName: string,
  outputKey: string,
  program: Uint8Array,
  inputKeys: string[],
): Promise<ObjectPart> {
  const bytes = await runConcatOnNode(ctx, jobId, attempt, {
    program,
    sourceKeys: inputKeys,
    outputKey,
    outputBucket: bucketName,
  });
  return { objectKey: outputKey, partitionValues: [], bytes, parts: 1, taskId: 0xffff_fffd };
}
