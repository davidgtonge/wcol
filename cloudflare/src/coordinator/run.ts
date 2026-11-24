import { resultJson } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import type { GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { type ObjectPart, runHashJob } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { createStageTimeline, type StageSpan } from "../../../src/runtime/coordinator/stage-timeline.ts";
import { r2Bucket } from "../node/node.ts";
import { attemptKey, forgetWork, registerWork } from "../../../src/runtime/coordinator/unit-claims.ts";
import { coloOnce } from "../colo.ts";
import { loadWasm, outputBucket, type Prepared, readSources } from "./plan.ts";
import { attemptObjectKeyJobs } from "../../../src/runtime/host/parquet-attempt-key.ts";
import { uploadParquetBuffer } from "../../../src/runtime/host/parquet-upload-sink.ts";
import { r2BindingSession } from "../r2.ts";
import { runStandaloneConcatJob } from "./finalizer-concat.ts";
import { type GraphJobContext, type GraphJobRecord, type LiveAttempt } from "./record.ts";
import { liveGraph } from "./live.ts";
import { isHashReducer } from "../../../src/runtime/coordinator/execution-graph.ts";
import { type AttemptReport, writeAttemptTiming } from "./attempt-timing.ts";
import { readLateRows, runGatherPrepared, runParquetGatherPrepared } from "./gather.ts";
import { runKMeansModel } from "./kmeans.ts";
import { createHashRemote } from "./hash-remote.ts";
import { recordStall } from "./renewal.ts";
import { rememberGraphSlots, rememberSlots } from "./slots.ts";

/**
 * One execution of a prepared graph, then the coordinator commits that attempt's outputs.
 * Recovery calls this same pair; it does not grow a second runner.
 */
export type GraphRun = {
  jsonText?: string;
  outputs: ObjectPart[];
  model?: {
    artifact: { centroids: number[]; features: string[]; means: number[]; scales: number[]; options: Record<string, number> };
    summary: { iterations: number; converged: boolean; inertia: number };
  };
};

/** Jobs whose latest attempt a coordinator keeps for the console. */
const LIVE_JOBS = 8;

/** Show this attempt in the console, replacing the job's previous one. */
function registerLive(ctx: GraphJobContext, jobId: string, attempt: number, prepared: Prepared, report: AttemptReport): void {
  const graph = prepared.graph?.kind === "graph" ? liveGraph(prepared.graph, isHashReducer) : { tasks: [], edges: [], rootTaskId: 0 };
  const entry: LiveAttempt = {
    attempt,
    startedAt: Date.now(),
    graph,
    timeline: report.timeline,
    facts: report.facts,
    coordinatorColo: null,
    sourceBytes: prepared.descriptor.sources.reduce((sum, source) => sum + source.size, 0),
  };
  ctx.live.delete(jobId);
  ctx.live.set(jobId, entry);
  for (const oldest of [...ctx.live.keys()].slice(0, Math.max(0, ctx.live.size - LIVE_JOBS))) ctx.live.delete(oldest);
  void coloOnce().then((colo) => {
    entry.coordinatorColo = colo;
  });
}

/**
 * Run one attempt and always persist its stage timeline (`_jobs/{job}/timing.json`).
 * The timeline is written in the background: timing is diagnostics, never on the
 * query's critical path, so `timing.json` may land shortly after the job completes.
 */
export async function runPrepared(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
  failTaskId: number | undefined,
  stalled?: Promise<string>,
  planSpan?: StageSpan,
): Promise<GraphRun> {
  const timeline = createStageTimeline();
  if (planSpan) timeline.record(planSpan);
  // Queue each balancing scan's planned units for this attempt before any scan asks.
  const work = attemptKey(job.jobId, attempt);
  registerWork(ctx.work, work, prepared.graph?.tasks ?? []);
  const report: AttemptReport = { timeline, facts: new Map() };
  registerLive(ctx, job.jobId, attempt, prepared, report);
  let failure: unknown;
  try {
    return await runAttempt(ctx, job, prepared, attempt, failTaskId, stalled, report);
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    forgetWork(ctx.work, work);
    // PROFILE: the coordinator's spans with absolute times, to line up with node profiles.
    const fields = {
      event: "attempt_spans",
      jobId: job.jobId,
      attempt,
      spans: report.timeline.spans(),
    };
    // Off the critical path: the colo lookup is one fetch per isolate.
    void coloOnce().then((colo) => console.log(JSON.stringify({ ...fields, colo })));
    ctx.waitUntil(writeAttemptTiming(ctx, job.jobId, attempt, prepared.graph, report, failure).catch(() => undefined));
  }
}

async function runAttempt(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
  failTaskId: number | undefined,
  stalled: Promise<string> | undefined,
  report: AttemptReport,
): Promise<GraphRun> {
  const wasm = await loadWasm();
  if (prepared.plan.kind === "concat") {
    const program = prepared.plan.programs[0];
    if (!program) throw new Error("concat job has no program");
    const inputKeys = prepared.descriptor.sources.map((source) => {
      if (!("key" in source.location)) throw new Error("concat inputs must be bucket objects");
      return source.location.key;
    });
    const part = await runStandaloneConcatJob(
      ctx,
      job.jobId,
      attempt,
      prepared.plan.bucket,
      prepared.plan.key,
      program,
      inputKeys,
    );
    return { outputs: [part] };
  }
  if (prepared.plan.kind === "model") {
    if (!prepared.graph || prepared.plan.graph.kind !== "graph") throw new Error("CREATE MODEL requires a row execution graph");
    const run = () => runKMeansModel(ctx, job, prepared, attempt, report, wasm);
    return await report.timeline.span("iterativeModel", {}, run);
  }
  if (!prepared.graph) {
    if (prepared.plan.kind !== "json" || prepared.plan.graph.kind !== "metadata_partial") {
      throw new Error("job has no graph");
    }
    const text = resultJson(wasm, prepared.plan.graph, { partial: prepared.plan.graph.partial }, prepared.plan.rowCap);
    assertJsonSize(text, prepared.descriptor.byteCap);
    return { jsonText: text, outputs: [] };
  }
  const bucketName = prepared.descriptor.bucket;
  const numberFrom = (value: string | undefined, fallback: number) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 1 ? parsed : fallback;
  };
  const limits = {
    openingConcurrency: numberFrom(ctx.env.WCOL_DISPATCH_CONCURRENCY, 6),
    maxActiveIngress:
      job.maxActiveIngress != null && job.maxActiveIngress >= 1
        ? Math.floor(job.maxActiveIngress)
        : numberFrom(ctx.env.WCOL_MAX_ACTIVE_INGRESS, 8),
    maxActiveScans: numberFrom(ctx.env.WCOL_MAX_ACTIVE_SCANS, 16),
  };
  if (!prepared.graph.edges.some((edge) => edge.partition !== undefined)) {
    // Non-hash plans only: each task output is held in memory until the root runs.
    // Hash-partitioned jobs must use runHashJob (streaming scan → reducer → finalizer).
    if (prepared.plan.kind === "parquet") {
      const outputs = await runParquetGatherPrepared(ctx, job, prepared, wasm, attempt, failTaskId, report.facts);
      await rememberGraphSlots(ctx, job.jobId, prepared.graph);
      return { outputs };
    }
    const gather = (graph: GraphPlan, failing: number | undefined) =>
      runGatherPrepared(ctx, job, prepared, graph, wasm, attempt, failing, report.timeline, limits.maxActiveScans, report.facts);
    const bytes = await gather(prepared.graph, failTaskId);
    await rememberGraphSlots(ctx, job.jobId, prepared.graph);
    if (prepared.plan.kind !== "json") throw new Error("non-hash parquet output is not published by this host");
    if (prepared.plan.late) {
      const text = await readLateRows(ctx, prepared, wasm, bytes, (graph) => {
        // The read-back's scans balance their units too. The pick graph has finished, so its
        // queues give way to these under the same attempt.
        registerWork(ctx.work, attemptKey(job.jobId, attempt), graph.tasks);
        return gather(graph, undefined);
      });
      assertJsonSize(text, prepared.descriptor.byteCap);
      return { jsonText: text, outputs: [] };
    }
    const graph = prepared.plan.graph;
    const data =
      graph.kind === "graph" && graph.rootOutput === "partial" ? { partial: bytes } : { rows: bytes };
    if (bytes.byteLength > prepared.descriptor.byteCap) {
      throw new Error(`json result exceeds ${prepared.descriptor.byteCap} bytes`);
    }
    const text = resultJson(wasm, graph, data, prepared.plan.rowCap);
    assertJsonSize(text, prepared.descriptor.byteCap);
    return { jsonText: text, outputs: [] };
  }
  const result = await runHashJob({
    timeline: report.timeline,
    wasm: { node: wasm, coordinator: wasm },
    plan: prepared.graph,
    readSources: readSources(ctx.env, prepared.descriptor.sources),
    openingConcurrency: limits.openingConcurrency,
    maxActiveIngress: limits.maxActiveIngress,
    maxActiveScans: limits.maxActiveScans,
    stalled,
    onStall: (reason) => recordStall(ctx, job.jobId, attempt, reason),
    yieldToEventLoop: undefined,
    simulateFinalizerError: undefined,
    uploadParquetRoot:
      prepared.plan.kind === "parquet"
        ? async (bytes, rootTaskId) => {
            const key = attemptObjectKeyJobs(job.jobId, rootTaskId, attempt);
            const bucket = bucketName ? r2Bucket(ctx.env, bucketName) : outputBucket(ctx.env);
            const written = await uploadParquetBuffer(r2BindingSession(bucket, key), key, bytes);
            return {
              objectKey: key,
              partitionValues: [],
              bytes: written,
              parts: 1,
              taskId: rootTaskId,
            };
          }
        : undefined,
    hashRemote: createHashRemote({ ctx, job, prepared, attempt, failTaskId, stalled, report, limits }),
  });
  await rememberSlots(ctx, job.jobId, result.invokedSlots);
  report.schedule = result.timing;
  // Logs only: how close each job comes to the finalizer's input budget.
  console.log(JSON.stringify({
    event: "finalizer_input",
    jobId: job.jobId,
    bytes: result.timing.finalizerInputBytes,
  }));
  const bucket = bucketName ? r2Bucket(ctx.env, bucketName) : outputBucket(ctx.env);
  for (const part of result.outputs) {
    if (part.localBytes) await bucket.put(part.objectKey, part.localBytes);
  }
  if (prepared.plan.kind === "json") {
    const payload = result.rows ?? result.partial;
    if (!payload) throw new Error("graph root produced no rows");
    if (payload.byteLength > prepared.descriptor.byteCap) {
      throw new Error(`json result exceeds ${prepared.descriptor.byteCap} bytes`);
    }
    const text = resultJson(
      wasm,
      prepared.plan.graph,
      result.rows ? { rows: result.rows } : { partial: result.partial! },
      prepared.plan.rowCap,
    );
    assertJsonSize(text, prepared.descriptor.byteCap);
    return { jsonText: text, outputs: [] };
  }
  return { outputs: result.outputs };
}

function assertJsonSize(text: string, max: number): void {
  if (new TextEncoder().encode(text).byteLength > max) {
    throw new Error(`json result exceeds ${max} bytes`);
  }
}
