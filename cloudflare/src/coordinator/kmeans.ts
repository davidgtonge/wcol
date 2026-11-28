/** CREATE MODEL: K-means over resident scan partitions, checkpointed per round. */
import { COORDINATOR_SLOT } from "../../../src/runtime/coordinator/execution-graph.ts";
import { createLimiter } from "../../../src/runtime/concurrency.ts";
import { graphNodeStub } from "../node/node.ts";
import { loadWasm, type Prepared } from "./plan.ts";
import { runIterativeExec } from "../../../src/runtime/coordinator/iterative-exec.ts";
import {
  decodeResidentKMeansNormalization,
  residentKMeansCentroids,
  mergeResidentKMeansMoments,
  mergeResidentKMeansPartials,
  prepareResidentKMeans,
  updateResidentKMeans,
} from "../../../src/runtime/host/iterative-wasm.ts";
import { type GraphJobContext, type GraphJobRecord, jobKey, requireJobRecord } from "./record.ts";
import type { GraphRun } from "./run.ts";
import type { AttemptReport } from "./attempt-timing.ts";
import { rememberSlots } from "./slots.ts";

/** The last committed round. `state` is stored as bytes: a JSON number array is 4x larger. */
type KMeansCheckpoint = {
  version: 3;
  iteration: number;
  state: Uint8Array;
  inertia: number;
  converged: boolean;
};

export async function runKMeansModel(
  ctx: GraphJobContext,
  job: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
  report: AttemptReport,
  coordinatorWasm: Awaited<ReturnType<typeof loadWasm>>,
): Promise<GraphRun> {
  if (prepared.plan.kind !== "model" || prepared.plan.graph.kind !== "graph" || !prepared.graph) throw new Error("invalid model plan");
  const plan = prepared.plan.graph;
  const modelOptions = prepared.plan.options;
  const residentTasks = plan.tasks;
  if (residentTasks.length === 0) throw new Error("model plan has no resident scan tasks");
  const slots = residentTasks.map((task) => task.slot === COORDINATOR_SLOT ? 0 : task.slot);
  if (new Set(slots).size !== slots.length) throw new Error("model scan tasks share a node slot and cannot own separate resident partitions");
  const featureColumns = decodeFeatureNames(plan.resultColumns);
  const dimensions = featureColumns.length;
  if (dimensions === 0) throw new Error("model query must select at least one feature");
  const clusters = modelOptions.numClusters;
  const checkpointKey = `graph-job-iterative:${job.jobId}`;
  const saved = await ctx.store.get(checkpointKey) as KMeansCheckpoint | undefined;
  const checkpoint = saved == null ? undefined : saved;
  if (checkpoint && (
    checkpoint.version !== 3 ||
    !Number.isInteger(checkpoint.iteration) || checkpoint.iteration < 0 || checkpoint.iteration > modelOptions.maxIterations ||
    !(checkpoint.state instanceof Uint8Array) || checkpoint.state.byteLength === 0 ||
    !Number.isFinite(checkpoint.inertia) || checkpoint.inertia < 0 || typeof checkpoint.converged !== "boolean"
  )) throw new Error("stored K-means barrier checkpoint is invalid");
  const sessionId = `a${attempt}-${crypto.randomUUID()}`;
  const configuredConcurrency = Number(ctx.env.WCOL_DISPATCH_CONCURRENCY ?? 6);
  const rpcConcurrency = Number.isFinite(configuredConcurrency) ? Math.max(1, Math.floor(configuredConcurrency)) : 6;
  const opening = createLimiter(rpcConcurrency);
  const common = {
    jobId: job.jobId,
    executionAttempt: attempt,
    sessionId,
  };
  const residentNodes = residentTasks.map((task, index) => ({
    task,
    slot: slots[index]!,
    stub: graphNodeStub(ctx.env, job.jobId, attempt, slots[index]!),
  }));
  const releaseResidents = () => Promise.allSettled(residentNodes.map(({ stub }) => opening(() => stub.releaseResidentKMeans(common))));
  await rememberSlots(ctx, job.jobId, slots);
  const loadedResults = await report.timeline.span("iterativeLoad", {}, () => Promise.allSettled(residentNodes.map(({ task, slot, stub }) => opening(async () => ({
    slot,
    taskId: task.id,
    moments: (await stub.loadResidentKMeans({
      ...common,
      deployId: ctx.env.WCOL_DEPLOY_ID ?? "",
      taskId: task.id,
      slot,
      program: task.program,
      sources: prepared.sources,
      etags: prepared.descriptor.sources.map((source) => source.etag),
      startIteration: (checkpoint?.iteration ?? 0) + 1,
      trace: ctx.env.WCOL_DISPATCH_TRACE === "1",
    })).moments,
  })))));
  const loadFailure = loadedResults.find((result): result is PromiseRejectedResult => result.status === "rejected");
  if (loadFailure) {
    await releaseResidents();
    throw loadFailure.reason;
  }
  const loaded = loadedResults.flatMap((result) => result.status === "fulfilled" ? [result.value] : []);
  const preparation = await (async () => {
    try {
      const mergedMoments = loaded.map((item) => item.moments).reduce(
        (left, right) => mergeResidentKMeansMoments(coordinatorWasm, left, right, dimensions),
      );
      return prepareResidentKMeans(coordinatorWasm, mergedMoments, {
        dimensions,
        clusters,
        maxIterations: modelOptions.maxIterations,
        tolerance: modelOptions.tolerance,
        seed: modelOptions.seed,
      });
    } catch (error) {
      await releaseResidents();
      throw error;
    }
  })();
  const normalized = await report.timeline.span("iterativeNormalize", {}, () => Promise.allSettled(residentNodes.map(({ stub }) => opening(() => stub.normalizeResidentKMeans({
    ...common,
    normalization: preparation.normalization,
  })))));
  const normalizeFailure = normalized.find((result): result is PromiseRejectedResult => result.status === "rejected");
  if (normalizeFailure) {
    await releaseResidents();
    throw normalizeFailure.reason;
  }
  const kernelConfig = {
    dimensions,
    clusters,
    maxIterations: modelOptions.maxIterations,
    tolerance: modelOptions.tolerance,
  };
  const initialState = checkpoint ? checkpoint.state : preparation.state;
  let inertia = checkpoint?.inertia ?? 0;
  let roundInertia = inertia;
  const kernel = {
    encodeState: (state: Uint8Array) => state,
    decodePartial: (bytes: Uint8Array) => bytes,
    merge: (left: Uint8Array, right: Uint8Array) => mergeResidentKMeansPartials(coordinatorWasm, left, right, kernelConfig),
    update: (state: Uint8Array, partial: Uint8Array) => {
      const updated = updateResidentKMeans(coordinatorWasm, state, partial, kernelConfig);
      roundInertia = updated.inertia;
      return { state: updated.state, converged: updated.converged };
    },
  };
  const result = await runIterativeExec({
    jobId: job.jobId,
    attempt,
    sessionId,
    residents: residentNodes.map(({ slot, stub }) => ({
      slot,
      runRound: ({ jobId, attempt: executionAttempt, sessionId: residentSession, iteration: round, globalState }: {
        jobId: string; attempt: number; sessionId: string; iteration: number; globalState: Uint8Array;
      }) => report.timeline.span("iterativeRound", {}, () => stub.runResidentKMeansRound({
        jobId,
        executionAttempt,
        sessionId: residentSession,
        iteration: round,
        globalState,
      })),
      release: ({ jobId, attempt: executionAttempt, sessionId: residentSession }: { jobId: string; attempt: number; sessionId: string }) =>
        stub.releaseResidentKMeans({ jobId, executionAttempt, sessionId: residentSession }),
    })),
    kernel,
    initialState,
    maxIterations: modelOptions.maxIterations,
    fanIn: 16,
    rpcConcurrency,
    checkpoint: {
      async load() {
        return checkpoint ? {
          iteration: checkpoint.iteration,
          state: checkpoint.state,
          converged: checkpoint.converged,
        } : null;
      },
      async commit({ attempt: committingAttempt, iteration: committedIteration, state, converged: committedConverged }) {
        inertia = roundInertia;
        const committed: KMeansCheckpoint = {
          version: 3,
          iteration: committedIteration,
          // A copy: storage clones a view's whole buffer, not just the view.
          state: state.slice(),
          inertia,
          converged: committedConverged,
        };
        await ctx.transaction(async (txn) => {
          const current = requireJobRecord(await txn.get(jobKey(job.jobId)));
          if (current.executionAttempt !== committingAttempt || current.state !== "running") throw new Error("model barrier attempt was superseded");
          await txn.put(checkpointKey, committed);
        });
      },
    },
  });
  return {
    outputs: [],
    model: {
      artifact: {
        centroids: residentKMeansCentroids(coordinatorWasm, result.state, kernelConfig),
        features: featureColumns,
        ...decodeResidentKMeansNormalization(preparation.normalization, dimensions),
        options: {
          numClusters: clusters,
          maxIterations: modelOptions.maxIterations,
          seed: modelOptions.seed,
          tolerance: modelOptions.tolerance,
        },
      },
      summary: { iterations: result.iterations, converged: result.converged, inertia },
    },
  };
}

function decodeFeatureNames(bytes: Uint8Array): string[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.byteLength < 4) throw new Error("model plan has no result columns");
  let at = 0;
  const count = view.getUint32(at, true); at += 4;
  const names: string[] = [];
  for (let i = 0; i < count; i++) {
    if (at + 4 > bytes.byteLength) throw new Error("truncated model result columns");
    const length = view.getUint32(at, true); at += 4;
    if (at + length + 4 > bytes.byteLength) throw new Error("truncated model result column name");
    names.push(new TextDecoder().decode(bytes.subarray(at, at + length))); at += length;
    at += 4;
  }
  if (at !== bytes.byteLength) throw new Error("trailing model result columns bytes");
  return names;
}
