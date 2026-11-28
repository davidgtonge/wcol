/**
 * Run opcode-11 page copy on a node isolate (the coordinator has no node Wasm): ReadAt
 * indexes map to `sourceKeys` in order. A single-file export's shards are joined here.
 */

import { createLimiter } from "../../../src/runtime/concurrency.ts";
import type { ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { planConcatProgram, readParquetFooter } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { finalizerObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { R2RangeSource } from "../../../src/runtime/host/ports/r2-read.ts";
import { FOOTER_READS, loadWasm, objectHead, outputBucket } from "./plan.ts";
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
  const bucket = outputBucket(ctx.env);
  // Only each shard's footer is read, a few shards at a time: there can be hundreds.
  const limit = createLimiter(FOOTER_READS);
  const footers = await Promise.all(
    ordered.map((part) =>
      limit(async () => {
        const head = await objectHead(bucket, part.objectKey);
        if (!head) throw new Error(`shard missing: ${part.objectKey}`);
        const range = new R2RangeSource(bucket, part.objectKey, head.etag || undefined);
        return { fileSize: head.size, footer: await readParquetFooter((offset, length) => range.read(offset, length), head.size) };
      }),
    ),
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
