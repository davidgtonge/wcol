/** The coordinator's side of a hash job: reducer and scan RPCs to node objects. */
import { isHashReducer } from "../../../src/runtime/coordinator/execution-graph.ts";
import type { HashRemote, ObjectPart, PreparedHashReducer, RunnableTask } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { concatBytes } from "../../../src/runtime/host/bytes.ts";
import { attemptObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { graphNodeStub } from "../node/node.ts";
import { reducerStub, type ReducerRuntime } from "../node/session.ts";
import type { AttemptReport } from "./attempt-timing.ts";
import type { Prepared } from "./plan.ts";
import type { GraphJobContext, GraphJobRecord } from "./record.ts";

export type HashLimits = { openingConcurrency: number; maxActiveIngress: number };

type HashRemoteContext = {
  ctx: GraphJobContext;
  job: GraphJobRecord;
  prepared: Prepared;
  attempt: number;
  failTaskId: number | undefined;
  stalled: Promise<string> | undefined;
  report: AttemptReport;
  limits: HashLimits;
};

type RpcTarget = { taskId: number; slot: number };
type RpcCaller = { taskId: number | null; scanIndex: number | null };
type CallRpc = <T>(method: string, caller: RpcCaller, target: RpcTarget, started: number, work: () => Promise<T>) => Promise<T>;
type FirstRpcFailure = {
  method: string;
  callerTaskId: number | null;
  scanIndex: number | null;
  targetTaskId: number;
  targetSlot: number;
  started: number;
  message: string;
};

/** Read a node's response body to its end; a stall cancels the read and fails it with the reason. */
export async function readBytes(stream: ReadableStream<Uint8Array>, stalled?: Promise<string>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  const reader = stream.getReader();
  const cancelOnStall = stalled?.then((reason) => {
    void reader.cancel(reason).catch(() => undefined);
    return reason;
  });
  try {
    for (;;) {
      const pending = reader.read();
      const read = cancelOnStall
        ? await Promise.race([
            pending.then((result) => ({ kind: "read" as const, result })),
            cancelOnStall.then((reason) => ({ kind: "stall" as const, reason })),
          ])
        : { kind: "read" as const, result: await pending };
      if (read.kind === "stall") throw new Error(read.reason);
      if (read.result.done) break;
      if (read.result.value) parts.push(read.result.value);
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {
      // cancel already released the reader
    }
  }
  return concatBytes(parts);
}

const message = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** Records the attempt's first failed RPC, and separately its first overload, to R2. */
function firstRpcRecorder(ctx: GraphJobContext, jobId: string, attempt: number): (info: FirstRpcFailure) => Promise<void> {
  let firstRpcRecorded = false;
  let firstOverloadRecorded = false;
  return async (info) => {
    if (!ctx.env.OUTPUT) return;
    const overload = info.message.includes("Durable Object is overloaded");
    if (firstRpcRecorded && (firstOverloadRecorded || !overload)) return;
    const body = JSON.stringify({ ...info, failed: Date.now() });
    if (!firstRpcRecorded) {
      firstRpcRecorded = true;
      await ctx.env.OUTPUT.put(`_jobs/${jobId}/a${attempt}/first-rpc.json`, body).catch(() => undefined);
    }
    if (overload && !firstOverloadRecorded) {
      firstOverloadRecorded = true;
      await ctx.env.OUTPUT.put(`_jobs/${jobId}/a${attempt}/first-overload.json`, body).catch(() => undefined);
    }
  };
}

/** The reducer calls a hash job makes after `prepareReducer`, each through `callRpc`. */
function reducerCalls(
  stub: ReturnType<typeof graphNodeStub>,
  callRpc: CallRpc,
  target: RpcTarget,
  jobId: string,
  attempt: number,
): Omit<PreparedHashReducer, "invokedSlots" | "parquetPart"> {
  return {
    waitConsumeReady: (scanIndex) =>
      callRpc("waitConsumeReady", { taskId: null, scanIndex }, target, Date.now(), () =>
        stub.waitConsumeReady({ jobId, executionAttempt: attempt, scanIndex }),
      ),
    accept: async (scanIndex, stream) => {
      await callRpc("acceptScanEdge", { taskId: null, scanIndex }, target, Date.now(), () =>
        stub.acceptScanEdge({
          jobId,
          executionAttempt: attempt,
          scanTaskId: target.taskId,
          scanIndex,
          partition: 0,
          stream,
        }),
      );
    },
    completeScan: async (scanIndex) => {
      await callRpc("completeScan", { taskId: null, scanIndex }, target, Date.now(), () =>
        stub.completeScan({ jobId, executionAttempt: attempt, scanIndex }),
      );
    },
    seal: async () => {
      await callRpc("sealReducer", { taskId: null, scanIndex: null }, target, Date.now(), () =>
        stub.sealReducer({ jobId, executionAttempt: attempt }),
      );
    },
    abort: async (error) => {
      await callRpc("abortHashAttempt", { taskId: null, scanIndex: null }, target, Date.now(), () =>
        stub.abortHashAttempt({ jobId, executionAttempt: attempt, error: message(error) }),
      );
    },
  };
}

export function createHashRemote(context: HashRemoteContext): HashRemote {
  const { ctx, job, prepared, attempt, failTaskId, stalled, report, limits } = context;
  const trace = ctx.env.WCOL_DISPATCH_TRACE === "1";
  const etags = () => prepared.descriptor.sources.map((source) => source.etag);
  const recordFirstRpc = firstRpcRecorder(ctx, job.jobId, attempt);
  // Reducers run where the job asked; scans and every other task run on nodes.
  const reducerRuntime: ReducerRuntime = job.reducerRuntime ?? "node";
  const taskStub = (task: RunnableTask) =>
    isHashReducer(task.program)
      ? reducerStub(ctx.env, job.jobId, attempt, task.slot, reducerRuntime)
      : graphNodeStub(ctx.env, job.jobId, attempt, task.slot);
  const callRpc: CallRpc = async (method, caller, target, started, work) => {
    try {
      return await work();
    } catch (error) {
      await recordFirstRpc({
        method,
        callerTaskId: caller.taskId,
        scanIndex: caller.scanIndex,
        targetTaskId: target.taskId,
        targetSlot: target.slot,
        started,
        message: message(error),
      });
      throw error;
    }
  };
  return {
    prepareReducer: async (task, expectedScans, finalizer, exportProgram) => {
      const stub = reducerStub(ctx.env, job.jobId, attempt, task.slot, reducerRuntime);
      const target = { taskId: task.taskId, slot: task.slot };
      const exportObjectKey = exportProgram ? attemptObjectKeyJobs(job.jobId, task.taskId, attempt) : undefined;
      const handle = await callRpc("prepareReducer", { taskId: null, scanIndex: null }, target, Date.now(), () =>
        stub.prepareReducer({
          jobId: job.jobId,
          executionAttempt: attempt,
          deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
          taskId: task.taskId,
          slot: task.slot,
          program: task.program,
          sources: prepared.sources,
          expectedScans,
          ...(job.reducerInstance ? { containerInstance: job.reducerInstance } : {}),
          failTaskId: attempt === 1 ? failTaskId : undefined,
          trace,
          openingConcurrency: limits.openingConcurrency,
          maxActiveIngress: limits.maxActiveIngress,
          etags: etags(),
          exportProgram,
          exportObjectKey,
        }),
      );
      // For the console only: where it runs. Off the critical path, and a miss is harmless.
      void stub.placement().then(
        (placement) => report.facts.set(task.taskId, { kind: "reducer", reportedAt: Date.now(), ...placement }),
        () => undefined,
      );
      const calls = reducerCalls(stub, callRpc, target, job.jobId, attempt);
      if (!exportProgram || !exportObjectKey) {
        finalizer.attachFinalizerInput(handle.body);
        return { ...calls, invokedSlots: handle.invokedSlots };
      }
      // An exporting reducer uploads its own Parquet part; its body carries only the receipt.
      const receipt = readBytes(handle.body, stalled).then((bytes) => {
        const parsed = JSON.parse(new TextDecoder().decode(bytes)) as { parquetPart?: ObjectPart; error?: string };
        if (parsed.error) throw new Error(parsed.error);
        if (!parsed.parquetPart) throw new Error("partition parquet upload produced no part");
        return parsed.parquetPart;
      });
      void receipt.catch(() => undefined);
      const ready: PreparedHashReducer = {
        ...calls,
        invokedSlots: handle.invokedSlots,
        seal: async () => {
          await calls.seal();
          ready.parquetPart = await receipt;
        },
      };
      return ready;
    },
    abortTask: async (task, error) => {
      await callRpc("abortHashAttempt", { taskId: null, scanIndex: null }, { taskId: task.taskId, slot: task.slot }, Date.now(), () =>
        taskStub(task).abortHashAttempt({
          jobId: job.jobId,
          executionAttempt: attempt,
          error: message(error),
        }),
      );
    },
    runScan: async (task, scanIndex, destinations) => {
      const started = Date.now();
      const handle = await callRpc("runTask", { taskId: null, scanIndex }, { taskId: task.taskId, slot: task.slot }, started, () =>
        graphNodeStub(ctx.env, job.jobId, attempt, task.slot).runTask({
          jobId: job.jobId,
          executionAttempt: attempt,
          reducerRuntime,
          deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
          taskId: task.taskId,
          slot: task.slot,
          program: task.program,
          sources: prepared.sources,
          hashDestinations: destinations,
          hashScanIndex: scanIndex,
          failTaskId: attempt === 1 ? failTaskId : undefined,
          trace,
          openingConcurrency: limits.openingConcurrency,
          maxActiveIngress: limits.maxActiveIngress,
          etags: etags(),
        }),
      );
      try {
        const receipt = JSON.parse(new TextDecoder().decode(await readBytes(handle.body, stalled))) as {
          openedPartitions?: number[];
          bytes?: number;
          invokedSlots?: number[];
          error?: string;
          sourceReads?: number;
          sourceReadBytes?: number;
          partitionBytes?: Record<string, number>;
          colo?: string;
          /** Each edge's send, and how long its reducer took to accept all of it. */
          admits?: { partition: number; waitMs: number; sentAt: number }[];
        };
        if (receipt.error) throw new Error(receipt.error);
        report.facts.set(task.taskId, {
          kind: "scan",
          reportedAt: Date.now(),
          colo: receipt.colo ?? "unknown",
          sourceReads: receipt.sourceReads ?? 0,
          sourceReadBytes: receipt.sourceReadBytes ?? 0,
          partitionBytes: receipt.partitionBytes ?? {},
          edgeTimes: Object.fromEntries(
            (receipt.admits ?? []).map((admit) => [admit.partition, [admit.sentAt, admit.sentAt + admit.waitMs] as const]),
          ),
          outputBytes: receipt.bytes ?? 0,
        });
        return {
          openedPartitions: receipt.openedPartitions ?? [],
          bytes: receipt.bytes ?? 0,
          invokedSlots: [...handle.invokedSlots, ...(receipt.invokedSlots ?? [])],
        };
      } catch (error) {
        await recordFirstRpc({
          method: "runTask.body",
          callerTaskId: null,
          scanIndex,
          targetTaskId: task.taskId,
          targetSlot: task.slot,
          started,
          message: message(error),
        });
        throw error;
      }
    },
  };
}
