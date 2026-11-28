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
import type { SourceReadAt } from "../../../src/runtime/host/stream-pool.ts";
import { graphNodeStub, r2Bucket } from "../node/node.ts";
import { eventLoopYield } from "../node/program.ts";
import { jobLimits, parseJobRequest } from "./job-request.ts";
import { loadWasm, outputBucket, pinnedFooters, planRequest, type Prepared, readSources } from "./plan.ts";
import { attemptObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { exportSink, type ExportObject } from "../../../src/runtime/host/hive-upload-sink.ts";
import { r2BindingSession } from "../r2.ts";
import { type GraphJobContext, type GraphJobRecord } from "./record.ts";
import type { TaskFacts } from "./live.ts";

type GraphTask = GraphPlan["tasks"][number];
type Wasm = Awaited<ReturnType<typeof loadWasm>>;

/** Pending tasks whose producers have all finished. A graph with none left is broken. */
function readyTasks(plan: GraphPlan, pending: ReadonlySet<number>, outputs: ReadonlyMap<number, Uint8Array>): GraphTask[] {
  const ready = plan.tasks.filter(
    (task) => pending.has(task.id) && edgesInto(task.id, plan.edges).every((edge) => outputs.has(edge.from)),
  );
  if (ready.length === 0) throw new Error("execution graph has a cycle or a missing producer");
  return ready;
}

/**
 * The gather scheduler: run `plan` wave by wave. Each wave is the tasks whose producers have
 * finished, at most `maxActive` at once; each output stays in memory until its consumer runs.
 * What a task does with its inputs (and where its output goes) is `run`'s.
 */
async function runWaves(
  plan: GraphPlan,
  maxActive: number,
  run: (task: GraphTask, incoming: Uint8Array[]) => Promise<Uint8Array>,
): Promise<Map<number, Uint8Array>> {
  const outputs = new Map<number, Uint8Array>();
  const pending = new Set(plan.tasks.map((task) => task.id));
  const limit = createLimiter(maxActive);
  while (pending.size > 0) {
    const ready = readyTasks(plan, pending, outputs);
    const wave = await Promise.all(
      ready.map((task) =>
        limit(async () => {
          const incoming = edgesInto(task.id, plan.edges).map((edge) => outputs.get(edge.from)!);
          return [task.id, await run(task, incoming)] as const;
        }),
      ),
    );
    for (const [taskId, bytes] of wave) {
      outputs.set(taskId, bytes);
      pending.delete(taskId);
    }
  }
  return outputs;
}

/** One gather task where the plan put it: here (`COORDINATOR_SLOT`) or on its node. */
function gatherTask(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  wasm: Wasm,
  attempt: number,
  failTaskId: number | undefined,
  facts: Map<number, TaskFacts>,
): (task: GraphTask, incoming: Uint8Array[]) => Promise<Uint8Array> {
  const read = readSources(ctx.env, prepared.descriptor.sources);
  return (task, incoming) =>
    task.slot === COORDINATOR_SLOT
      ? runGatherLocal(wasm, task.program, incoming, claimingReadAt(read, () => nextUnit(ctx.work, attemptKey(job.jobId, attempt), task.id)))
      : runRemoteGather(ctx, job, prepared, attempt, failTaskId, task, incoming, facts);
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
  wasm: Wasm,
  attempt: number,
  failTaskId: number | undefined,
  maxActiveTasks: number,
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
          yield: eventLoopYield,
          readAt: inputs.length ? undefined : claimingReadAt(readSources(ctx.env, prepared.descriptor.sources), claimNext(taskId)),
        },
      });
      return asParts(taskId, await sink.finish());
    } catch (error) {
      await sink.abort(error);
      throw error;
    }
  };

  const limit = createLimiter(maxActiveTasks);
  if (plan.edges.length === 0) {
    // Settle every task before failing: a task's RPC returns once its uploads end, so no
    // file of this attempt lands after the attempt (and its sweep), and a retry does not
    // run beside this attempt's remaining tasks.
    const settled = await Promise.allSettled(
      plan.tasks.map((task) => limit(() => uploadTask(task.id, task.slot, task.program, []))),
    );
    const failed = settled.find((result) => result.status === "rejected");
    if (failed) throw failed.reason;
    return settled.flatMap((result) => (result.status === "fulfilled" ? result.value : []));
  }

  // The root writes Parquet; every other task gathers bytes for it.
  const parts: ObjectPart[] = [];
  const gather = gatherTask(ctx, job, prepared, wasm, attempt, failTaskId, facts);
  await runWaves(plan, maxActiveTasks, async (task, incoming) => {
    if (task.id !== plan.rootTaskId) return gather(task, incoming);
    parts.push(...(await uploadTask(task.id, task.slot, task.program, incoming)));
    return new Uint8Array();
  });
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
  wasm: Wasm,
  picked: Uint8Array,
  gather: (graph: GraphPlan) => Promise<Uint8Array>,
): Promise<string> {
  const { lateBody } = prepared.descriptor;
  if (prepared.plan.kind !== "json" || lateBody == null) throw new Error("not a late row job");
  const late = parseJobRequest(lateBody, jobLimits(ctx.env));
  if (late.kind === "invalid") throw new Error(`stored late row request is invalid: ${late.error}`);
  const footers = await pinnedFooters(ctx.env, prepared.descriptor.sources);
  const read = planLateRead(wasm, planRequest(late.request, footers), picked);
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
  wasm: Wasm,
  attempt: number,
  failTaskId: number | undefined,
  timeline: StageTimeline,
  maxActiveTasks: number,
  facts: Map<number, TaskFacts>,
): Promise<Uint8Array> {
  if (plan.edges.some((edge) => edge.partition !== undefined)) {
    throw new Error("runGatherPrepared requires a graph without hash-partition edges");
  }
  const gather = gatherTask(ctx, job, prepared, wasm, attempt, failTaskId, facts);
  const outputs = await runWaves(plan, maxActiveTasks, (task, incoming) =>
    timeline.span("gatherTask", { taskId: task.id }, () => gather(task, incoming)),
  );
  const root = outputs.get(plan.rootTaskId);
  if (!root) throw new Error("graph root produced no bytes");
  return root;
}

async function runGatherLocal(
  wasm: Wasm,
  program: Uint8Array,
  inputs: Uint8Array[],
  readAt: SourceReadAt,
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
      yield: eventLoopYield,
    },
  });
  return concatBytes(parts);
}
