/** Hash reducers: hold the ingress session, accept scan edges, and run the reducer program. */
import { createReducerIngressSession } from "../../../src/runtime/host/scan-edge-session.ts";
import { runWcolEngine } from "../../../src/runtime/host/run-wcol-engine.ts";
import { createHostBytes } from "../../../src/runtime/host/host-bytes.ts";
import { drainReadable } from "../../../src/runtime/host/ports/rpc.ts";
import { createPoolInputLatch } from "../../../src/runtime/host/pool-streams.ts";
import { r2BindingSession } from "../r2.ts";
import { finishParquetShard, ParquetUploadSink } from "../../../src/runtime/host/parquet-upload-sink.ts";
import { EDGE_READ_AHEAD, portBytes } from "../../../src/runtime/host/port-budget.ts";
import {
  releaseHashIngress,
  type AcceptScanEdgeParams,
  type HashAttemptParams,
  type HashIngressHolder,
  type PrepareReducerHandle,
  type PrepareReducerOnNodeOptions,
  type RunProgramOptions,
  type RunTaskParams,
} from "./types.ts";
import { admitObjectKey, traceObjectKey } from "./records.ts";
import { eventLoopYield, outputBucketFor, runProgram, streamed } from "./program.ts";

function requireHashSession(holder: HashIngressHolder, params: HashAttemptParams) {
  const session = holder.session;
  if (!session) throw new Error("reducer ingress session is missing");
  session.assertAttempt(params.jobId, params.executionAttempt);
  return session;
}

export async function prepareReducerOnNode(options: PrepareReducerOnNodeOptions): Promise<PrepareReducerHandle> {
  const { env, params, holder, durableObjectId, waitUntil, hooks } = options;
  if (holder.session) holder.session.assertAttempt(params.jobId, params.executionAttempt);
  else {
    holder.session = createReducerIngressSession({
      jobId: params.jobId,
      executionAttempt: params.executionAttempt,
      expectedScans: params.expectedScans,
    });
  }
  const session = holder.session;
  const runParams: RunTaskParams = {
    jobId: params.jobId,
    executionAttempt: params.executionAttempt,
    deployId: params.deployId,
    taskId: params.taskId,
    slot: params.slot,
    program: params.program,
    sources: params.sources,
    failTaskId: params.failTaskId,
    trace: params.trace,
    etags: params.etags,
  };
  const inputLatch = createPoolInputLatch();
  holder.stream = inputLatch;
  const hostBytes = createHostBytes();
  holder.hostBytes = hostBytes;
  session.readyToAccept();
  const meta = { env, wasm: options.wasm, jobId: params.jobId, attempt: params.executionAttempt, taskId: params.taskId, slot: params.slot };
  const owns = hooks?.ownsSession ?? (() => true);
  const releaseOwned = () => {
    if (owns()) releaseHashIngress(holder);
  };
  if (params.program.byteLength === 0) {
    return {
      ready: true,
      invokedSlots: [params.slot],
      body: streamed(waitUntil, meta, async () => undefined, releaseOwned, owns),
    };
  }
  const body = streamed(
    waitUntil,
    meta,
    async (write, chunks) => {
      if (params.trace) {
        if (!env.OUTPUT) throw new Error("R2 output binding is missing for a dispatch trace");
        await env.OUTPUT.put(
          traceObjectKey(params.jobId, params.taskId),
          JSON.stringify({ durableObjectId, slot: params.slot, taskId: params.taskId, hashReducer: true }),
        );
      }
      let yieldCount = 0;
      const runReducer = (writePort: RunProgramOptions["write"]) =>
        runProgram({
          wasm: options.wasm,
          env,
          params: runParams,
          write: writePort,
          chunks,
          onActivity: hooks?.onActivity,
          onYield: () => {
            yieldCount += 1;
          },
          inputLatch,
          hostBytes,
          poolLayout: { inputs: session.expectedScans, outputs: 1 },
        });
      try {
        if (params.exportObjectKey && params.exportProgram) {
          const bucket = outputBucketFor(env, "");
          const uploadSession = r2BindingSession(bucket, params.exportObjectKey);
          const sink = new ParquetUploadSink(uploadSession, params.exportObjectKey);
          const rangeWorker = params.exportProgram === params.program;
          if (rangeWorker) {
            await runReducer(sink.writePort);
          } else {
            const exportLatch = createPoolInputLatch();
            const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
            exportLatch.attachInput(0, readable);
            const wasm = await options.wasm.load();
            const exportWork = runWcolEngine({
              wasm,
              plan: params.exportProgram,
              scratchBytes: 1024 * 1024,
              layout: { inputs: 1, outputs: 1 },
              inputLatch: exportLatch,
              host: {
                writePort: sink.writePort,
                yield: eventLoopYield,
              },
            });
            await runReducer(async (_port, chunk) => {
              const writer = writable.getWriter();
              await writer.write(chunk);
              writer.releaseLock();
            });
            await writable.close();
            exportLatch.sealInput(0);
            await exportWork;
          }
          const parquetPart = await finishParquetShard(sink, params.exportObjectKey, params.taskId);
          await write(new TextEncoder().encode(JSON.stringify({ parquetPart })));
          return;
        }
        await runReducer(async (_port, chunk) => write(chunk));
      } finally {
        if (env.OUTPUT) {
          await env.OUTPUT.put(
            admitObjectKey(params.jobId, params.executionAttempt, params.taskId),
            JSON.stringify({ taskId: params.taskId, slot: params.slot, yieldCount, accepts: session.admissions() }),
          ).catch(() => undefined);
        }
      }
    },
    releaseOwned,
    owns,
  );
  return { ready: true, invokedSlots: [params.slot], body };
}

/**
 * Hold the accept RPC open until the reducer has consumed this edge. If it
 * returns at once, the reducer DO has no request in flight while it ingests;
 * the runtime then routes later accepts to a fresh instance, which has no
 * session and rejects them, while the original keeps running.
 */
export async function acceptScanEdgeOnNode(
  holder: HashIngressHolder,
  params: AcceptScanEdgeParams,
): Promise<{ accepted: true }> {
  const session = requireHashSession(holder, params);
  const hostBytes = holder.hostBytes;
  const relay = new TransformStream<Uint8Array, Uint8Array>(
    {
      // Counted out by the stream pool once copied into Wasm.
      transform(chunk, controller) {
        hostBytes?.add("edges", chunk.byteLength);
        controller.enqueue(chunk);
      },
    },
    undefined,
    new ByteLengthQueuingStrategy({ highWaterMark: portBytes(EDGE_READ_AHEAD, session.expectedScans) }),
  );
  // Released with the reducer: a stalled or orphaned edge must not hold this RPC open.
  const edge = new AbortController();
  holder.edges ??= new Set();
  holder.edges.add(edge);
  try {
    // The caller cancels nothing on failure: `refuseScanEdge` reads the stream to its end.
    const consumed = params.stream.pipeTo(relay.writable, { signal: edge.signal, preventCancel: true });
    try {
      session.acceptScanEdge(params.scanIndex, relay.readable);
      holder.stream?.attachInput(params.scanIndex, relay.readable);
    } catch (error) {
      edge.abort(error);
      await consumed.catch(() => undefined);
      throw error;
    }
    await consumed;
    return { accepted: true };
  } finally {
    holder.edges.delete(edge);
  }
}

/**
 * Release a scan edge this reducer will not consume. Cancelling the transferred stream does
 * not end the accept call: it stayed open, with no CPU, until the isolate was reset, and
 * enough of them ran later jobs on that object out of memory. Read to the end instead; the
 * refused scan fails on the accept's error and aborts its pipes, which ends the stream.
 */
export function refuseScanEdge(stream: ReadableStream<Uint8Array>): void {
  if (stream.locked) return;
  void drainReadable(stream).catch(() => undefined);
}


export function waitConsumeReadyOnNode(holder: HashIngressHolder, params: HashAttemptParams): Promise<void> {
  if (params.scanIndex == null) throw new Error("waitConsumeReady needs scanIndex");
  return requireHashSession(holder, params).waitConsumeReady(params.scanIndex);
}

export function completeScanOnNode(holder: HashIngressHolder, params: HashAttemptParams): void {
  if (params.scanIndex == null) throw new Error("completeScan needs scanIndex");
  const session = requireHashSession(holder, params);
  session.completeScan(params.scanIndex);
  holder.stream?.sealInput(params.scanIndex);
}

export function sealReducerOnNode(holder: HashIngressHolder, params: HashAttemptParams): void {
  const session = requireHashSession(holder, params);
  session.seal();
  for (let index = 0; index < session.expectedScans; index++) holder.stream?.sealInput(index);
}

export function abortHashAttemptOnNode(holder: HashIngressHolder, params: HashAttemptParams & { error?: string }): void {
  const session = holder.session;
  if (session && (session.jobId !== params.jobId || session.executionAttempt !== params.executionAttempt)) return;
  releaseHashIngress(holder, new Error(params.error ?? "hash attempt aborted"));
}
