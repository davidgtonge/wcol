/** Gather (non-hash) graphs: run ready tasks wave by wave, on nodes or here. */
import { planLateRead, rowsByIdJson } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import { COORDINATOR_SLOT, edgesInto, type ExecutionPlan } from "../../../src/runtime/coordinator/execution-graph.ts";
import type { GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { type ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { type StageTimeline } from "../../../src/runtime/coordinator/stage-timeline.ts";
import { createLimiter } from "../../../src/runtime/concurrency.ts";
import { attemptKey, claimingReadAt, nextUnit } from "../../../src/runtime/coordinator/unit-claims.ts";
import { concatBytes } from "../../../src/runtime/host/bytes.ts";
import { runWcolEngine } from "../../../src/runtime/host/run-wcol-engine.ts";
import { attachBytesInputs } from "../../../src/runtime/host/pool-streams.ts";
import { graphNodeStub, r2Bucket } from "../node/node.ts";
import { loadWasm, outputBucket, pinnedFooters, planRequest, type Prepared, readSources } from "./plan.ts";
import { attemptObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { exportSink, type ExportObject } from "../../../src/runtime/host/hive-upload-sink.ts";
import { r2BindingSession } from "../r2.ts";
import { type GraphJobContext, type GraphJobRecord } from "./record.ts";
import type { TaskFacts } from "./live.ts";

type GraphTask = GraphPlan["tasks"][number];

/** Pending tasks whose producers have all finished. A graph with none left is broken. */
function readyTasks(plan: GraphPlan, pending: ReadonlySet<number>, outputs: ReadonlyMap<number, Uint8Array>): GraphTask[] {
  const ready = plan.tasks.filter(
    (task) => pending.has(task.id) && edgesInto(task.id, plan.edges).every((edge) => outputs.has(edge.from)),
  );
  if (ready.length === 0) throw new Error("execution graph has a cycle or a missing producer");
  return ready;
}

/**
 * One gather task on its planned node; resolves with the task's output bytes. Its location
 * and reads go to the attempt's facts, for the console.
 */
async function runRemoteGather(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
  failTaskId: number | undefined,
  task: GraphTask,
  inputs: Uint8Array[],
  facts: Map<number, TaskFacts>,
): Promise<Uint8Array> {
  const result = await graphNodeStub(ctx.env, job.jobId, attempt, task.slot).runGatherTask({
    jobId: job.jobId,
    executionAttempt: attempt,
    deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
    taskId: task.id,
    slot: task.slot,
    program: task.program,
    sources: prepared.sources,
    inputs,
    failTaskId: attempt === 1 ? failTaskId : undefined,
    trace: ctx.env.WCOL_DISPATCH_TRACE === "1",
    etags: prepared.descriptor.sources.map((source) => source.etag),
  });
  facts.set(task.id, {
    kind: "scan",
    reportedAt: Date.now(),
    colo: result.colo,
    sourceReads: result.sourceReads,
    sourceReadBytes: result.sourceReadBytes,
    partitionBytes: {},
    edgeTimes: {},
    outputBytes: result.bytes.byteLength,
  });
  return result.bytes;
}

export async function runParquetGatherPrepared(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  wasm: Awaited<ReturnType<typeof loadWasm>>,
  attempt: number,
  failTaskId: number | undefined,
  facts: Map<number, TaskFacts>,
): Promise<ObjectPart[]> {
  const plan = prepared.graph as Extract<ExecutionPlan, { kind: "graph" }>;
  const bucketName = prepared.descriptor.bucket;
  const bucket = bucketName ? r2Bucket(ctx.env, bucketName) : outputBucket(ctx.env);
  const layout = plan.output?.layout ?? "single_file";
  const claimNext = (taskId: number) => () => nextUnit(ctx.work, attemptKey(job.jobId, attempt), taskId);
  const asParts = (taskId: number, objects: ExportObject[]): ObjectPart[] =>
    objects.map((object) => ({ ...object, parts: 1, taskId }));
  const uploadTask = async (taskId: number, slot: number, program: Uint8Array, inputs: Uint8Array[]): Promise<ObjectPart[]> => {
    const key = attemptObjectKeyJobs(job.jobId, taskId, attempt);
    if (slot !== COORDINATOR_SLOT) {
      const uploaded = await graphNodeStub(ctx.env, job.jobId, attempt, slot).runParquetShardTask({
        jobId: job.jobId,
        executionAttempt: attempt,
        deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
        taskId,
        slot,
        program,
        sources: prepared.sources,
        inputs,
        objectKey: key,
        outputBucket: bucketName ?? "",
        layout,
        failTaskId: attempt === 1 ? failTaskId : undefined,
        trace: ctx.env.WCOL_DISPATCH_TRACE === "1",
        etags: prepared.descriptor.sources.map((source) => source.etag),
      });
      return asParts(taskId, uploaded.objects);
    }
    const sink = exportSink(layout, (objectKey) => r2BindingSession(bucket, objectKey), key);
    try {
      await runWcolEngine({
        wasm,
        plan: program,
        scratchBytes: 1024 * 1024,
        layout: { inputs: inputs.length, outputs: 1 },
        onPoolReady: attachBytesInputs(inputs),
        host: {
          writePort: (port, chunk) => sink.writePort(port, chunk),
          yield: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
          readAt: inputs.length ? undefined : claimingReadAt(readSources(ctx.env, prepared.descriptor.sources), claimNext(taskId)),
        },
      });
      return asParts(taskId, await sink.finish());
    } catch (error) {
      await sink.abort(error);
      throw error;
    }
  };

  if (plan.edges.length === 0) {
    // Settle every task before failing: a task's RPC returns once its uploads end, so no
    // file of this attempt lands after the attempt (and its sweep), and a retry does not
    // run beside this attempt's remaining tasks.
    const settled = await Promise.allSettled(
      plan.tasks.map((task) => uploadTask(task.id, task.slot, task.program, [])),
    );
    const failed = settled.find((result) => result.status === "rejected");
    if (failed) throw failed.reason;
    return settled.flatMap((result) => (result.status === "fulfilled" ? result.value : []));
  }

  const outputs = new Map<number, Uint8Array>();
  const pending = new Set(plan.tasks.map((task) => task.id));
  const parts: ObjectPart[] = [];
  while (pending.size > 0) {
    const ready = readyTasks(plan, pending, outputs);
    for (const task of ready) {
      const incoming = edgesInto(task.id, plan.edges).map((edge) => outputs.get(edge.from)!);
      if (task.id === plan.rootTaskId) {
        parts.push(...(await uploadTask(task.id, task.slot, task.program, incoming)));
        outputs.set(task.id, new Uint8Array());
      } else {
        const bytes =
          task.slot === COORDINATOR_SLOT
            ? await runGatherLocal(
                wasm,
                task.program,
                incoming,
                claimingReadAt(readSources(ctx.env, prepared.descriptor.sources), claimNext(task.id)),
              )
            : await runRemoteGather(ctx, job, prepared, attempt, failTaskId, task, incoming, facts);
        outputs.set(task.id, bytes);
      }
      pending.delete(task.id);
    }
  }
  if (parts.length === 0) {
    throw new Error("parquet gather produced no uploaded shard");
  }
  return parts;
}

/**
 * Rows of a late row job: plan the read-back of the rows `picked` names, run it with
 * `gather`, and print its rows in the pick's order.
 */
export async function readLateRows(
  ctx: GraphJobContext,
  prepared: Prepared,
  wasm: Awaited<ReturnType<typeof loadWasm>>,
  picked: Uint8Array,
  gather: (graph: GraphPlan) => Promise<Uint8Array>,
): Promise<string> {
  const { lateBody } = prepared.descriptor;
  if (prepared.plan.kind !== "json" || lateBody == null) throw new Error("not a late row job");
  const footers = await pinnedFooters(ctx.env, prepared.descriptor.sources);
  const read = planLateRead(wasm, planRequest(ctx.env, lateBody, footers), picked);
  const rows = read.graph?.kind === "graph" ? await gather(read.graph) : new Uint8Array();
  return rowsByIdJson(
    wasm,
    { resultColumns: prepared.plan.graph.resultColumns, rows: picked },
    { resultColumns: read.resultColumns, rows },
    prepared.plan.rowCap,
  );
}

export async function runGatherPrepared(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  plan: GraphPlan,
  wasm: Awaited<ReturnType<typeof loadWasm>>,
  attempt: number,
  failTaskId: number | undefined,
  timeline: StageTimeline,
  maxActiveTasks: number,
  facts: Map<number, TaskFacts>,
): Promise<Uint8Array> {
  if (plan.edges.some((edge) => edge.partition !== undefined)) {
    throw new Error("runGatherPrepared requires a graph without hash-partition edges");
  }
  const read = readSources(ctx.env, prepared.descriptor.sources);
  const claimNext = (taskId: number) => () => nextUnit(ctx.work, attemptKey(job.jobId, attempt), taskId);
  const outputs = new Map<number, Uint8Array>();
  const pending = new Set(plan.tasks.map((task) => task.id));
  // Tasks whose producers are done are independent; run them together, bounded
  // like hash scans. Each output stays in memory until its consumer runs.
  const limit = createLimiter(maxActiveTasks);
  while (pending.size > 0) {
    const ready = readyTasks(plan, pending, outputs);
    const wave = await Promise.all(ready.map((task) => limit(async () => {
      const incoming = edgesInto(task.id, plan.edges).map((edge) => outputs.get(edge.from)!);
      const bytes = await timeline.span("gatherTask", { taskId: task.id }, () =>
        task.slot === COORDINATOR_SLOT
          ? runGatherLocal(wasm, task.program, incoming, claimingReadAt(read, claimNext(task.id)))
          : runRemoteGather(ctx, job, prepared, attempt, failTaskId, task, incoming, facts),
      );
      return [task.id, bytes] as const;
    })));
    for (const [taskId, bytes] of wave) {
      outputs.set(taskId, bytes);
      pending.delete(taskId);
    }
  }
  const root = outputs.get(plan.rootTaskId);
  if (!root) throw new Error("graph root produced no bytes");
  return root;
}

async function runGatherLocal(
  wasm: Awaited<ReturnType<typeof loadWasm>>,
  program: Uint8Array,
  inputs: Uint8Array[],
  readAt: (source: number, offset: number, length: number) => Promise<Uint8Array>,
): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  await runWcolEngine({
    wasm,
    plan: program,
    scratchBytes: 1024 * 1024,
    layout: { inputs: inputs.length, outputs: 1 },
    onPoolReady: attachBytesInputs(inputs),
    readAt: inputs.length ? undefined : (source, offset, length) => readAt(source, Number(offset), length),
    host: {
      readAt: inputs.length ? undefined : readAt,
      writePort: async (_port, chunk) => {
        parts.push(chunk);
      },
      yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
    },
  });
  return concatBytes(parts);
}
