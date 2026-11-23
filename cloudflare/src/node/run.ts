/** Node tasks outside a hash exchange: gather, resident, Parquet shard and concat. */
import { concatBytes } from "../../../src/runtime/host/bytes.ts";
import { attachBytesInputs } from "../../../src/runtime/host/pool-streams.ts";
import { type ResidentGraph } from "../../../src/runtime/host/resident-graph.ts";
import { r2BindingSession } from "../r2.ts";
import { ParquetUploadSink } from "../../../src/runtime/host/parquet-upload-sink.ts";
import { runConcatProgram } from "../../../src/runtime/host/run-parquet-concat.ts";
import { coloOnce } from "../colo.ts";
import { type NodeWasm } from "./session.ts";
import {
  type GatherTaskResult,
  type GraphTaskEnv,
  type RunGatherTaskOnNodeOptions,
  type RunParquetShardParams,
  type RunConcatTaskParams,
} from "./types.ts";
import { outputBucketFor, runProgram } from "./program.ts";

export type { NodeRunHooks } from "./types.ts";

/** Load a resident plan through the same engine and retain its stream-pool handle. */
export async function runResidentTaskOnNode(options: RunGatherTaskOnNodeOptions): Promise<ResidentGraph> {
  let resident: ResidentGraph | undefined;
  try {
    await runProgram({
      ...options,
      write: () => { throw new Error("resident plan emitted a byte stream"); },
      chunks: () => 0,
      poolLayout: { inputs: 0, outputs: 1 },
      onResidentReady: (graph) => { resident = graph; },
    });
    if (!resident) throw new Error("task plan is not resident");
    return resident;
  } catch (error) {
    await resident?.release();
    throw error;
  }
}

/** One gather task. The RPC returns when the program has finished and its bytes are complete. */
export async function runGatherTaskOnNode(options: RunGatherTaskOnNodeOptions): Promise<GatherTaskResult> {
  const { env, params, onActivity, rpcAt } = options;
  const parts: Uint8Array[] = [];
  const inputs = params.inputs ?? [];
  // Asked now so the answer is in hand when the task returns; one fetch per isolate.
  const colo = coloOnce();
  const timing = await runProgram({
    wasm: options.wasm,
    env,
    params,
    rpcAt,
    write: async (_port, chunk) => {
      parts.push(chunk);
    },
    chunks: () => parts.length,
    onActivity,
    poolLayout: { inputs: inputs.length, outputs: 1 },
    onPoolReady: attachBytesInputs(inputs),
  });
  return { bytes: concatBytes(parts), colo: await colo, sourceReads: timing.sourceReads, sourceReadBytes: timing.sourceReadBytes };
}

/** Upload one parquet shard from a gather-style program. */
export async function runParquetShardTaskOnNode(options: {
  env: GraphTaskEnv;
  wasm: NodeWasm;
  params: RunParquetShardParams;
  onActivity?: () => void;
}): Promise<{ bytes: number }> {
  const { env, params, onActivity } = options;
  const bucket = outputBucketFor(env, params.outputBucket);
  const session = r2BindingSession(bucket, params.objectKey);
  const sink = new ParquetUploadSink(session, params.objectKey);
  const inputs = params.inputs ?? [];
  await runProgram({
    wasm: options.wasm,
    env,
    params,
    write: sink.writePort,
    chunks: () => 0,
    onActivity,
    poolLayout: { inputs: inputs.length, outputs: 1 },
    onPoolReady: attachBytesInputs(inputs),
  });
  const bytes = await sink.finish();
  return { bytes };
}

export async function runConcatTaskOnNode(options: {
  env: GraphTaskEnv;
  wasm: NodeWasm;
  params: RunConcatTaskParams;
}): Promise<{ bytes: number }> {
  const bucket = outputBucketFor(options.env, options.params.outputBucket);
  const wasm = await options.wasm.load();
  const bytes = await runConcatProgram({
    wasm,
    program: options.params.program,
    bucket,
    sourceKeys: options.params.sourceKeys,
    outputKey: options.params.outputKey,
    writeSession: r2BindingSession(bucket, options.params.outputKey),
  });
  return { bytes };
}
