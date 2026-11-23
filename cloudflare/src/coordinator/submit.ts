import { chooseReducers, containerRequestBody, type ReducerChoice, type ReducerRequest } from "./reducer-choice.ts";
import { jobSources } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import type { StageSpan, StageSummary } from "../../../src/runtime/coordinator/stage-timeline.ts";
import { FATAL_TASK_MARK, isFatalTaskError } from "../../../src/runtime/coordinator/task-errors.ts";
import { shouldArmAlarm, sinkTaskIds, sinksToReplay, type GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { preferHashFailure, type ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { r2Bucket } from "../node/node.ts";
import type { R2OutputBucket } from "../r2.ts";
import { allowUnpinned, loadDescriptor, loadGraph, loadWasm, pinsMatch, prepare, type Prepared, preparedFromStorage, rejectUnexecutable, unpinned, writeImmutable } from "./plan.ts";
import { failJob, publishParquet } from "./publish.ts";
import { finishPrepared } from "./finish.ts";
import { startRenewal } from "./renewal.ts";
import { runPrepared, type GraphRun } from "./run.ts";
import {
  ALARM_MS,
  type CommittedObject,
  readDurableRow,
  errorMessage,
  type GraphJobContext,
  type GraphJobRecord,
  idempotencyKey,
  jobKey,
  LEASE_MS,
  MAX_ATTEMPTS,
  parseHostExecutionHints,
  readJobRecord,
  requireJobRecord,
  sha256Hex,
  taskKey,
  taskRows,
} from "./record.ts";

/**
 * Alarm recovery. `rerun` is the same execution path as the first attempt.
 * This module does not import the HTTP entry, which calls back into here.
 */


/** Run one claimed attempt of an already prepared graph. */
export type RerunJob = (
  ctx: GraphJobContext,
  record: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
) => Promise<void>;

export async function arm(ctx: GraphJobContext, when = Date.now() + ALARM_MS): Promise<void> {
  await ctx.setAlarm(when);
}

/** Each durable task's committed objects, read from the attempt lists its rows name. */
async function durableOutputs(ctx: GraphJobContext, jobId: string, graph: GraphPlan, bucket: R2OutputBucket): Promise<Map<number, CommittedObject[]>> {
  const lists = new Map<string, CommittedObject[]>();
  const byTask = new Map<number, CommittedObject[]>();
  for (const task of graph.tasks) {
    const row = readDurableRow(await ctx.store.get(taskKey(jobId, task.id)));
    if (!row) continue;
    if (!lists.has(row.outputsKey)) {
      const object = await bucket.get(row.outputsKey);
      lists.set(row.outputsKey, object?.body ? (JSON.parse(await object.text()) as CommittedObject[]) : []);
    }
    const objects = lists.get(row.outputsKey)!.filter((object) => object.taskId === task.id);
    if (objects.length > 0) byTask.set(task.id, objects);
  }
  return byTask;
}

/** Tasks whose committed objects are all still in R2 at their recorded size. */
async function durableIds(bucket: R2OutputBucket, outputs: ReadonlyMap<number, CommittedObject[]>): Promise<Set<number>> {
  const ids = new Set<number>();
  for (const [taskId, objects] of outputs) {
    const verified = await Promise.all(
      objects.map(async ({ objectKey, bytes }) => {
        const object = bucket.head ? await bucket.head(objectKey) : await bucket.get(objectKey);
        return object != null && (object.size == null || object.size === bytes);
      }),
    );
    if (verified.every(Boolean)) ids.add(taskId);
  }
  return ids;
}

function ledgerOutputs(graph: GraphPlan, outputs: ReadonlyMap<number, CommittedObject[]>): ObjectPart[] {
  return sinkTaskIds(graph).flatMap((taskId) => (outputs.get(taskId) ?? []).map((object) => ({ ...object, parts: 1 })));
}

/** Alarm entry. Reschedules itself after a recoverable failure. */
export async function recoverJob(ctx: GraphJobContext, rerun: RerunJob): Promise<void> {
  const listed = await ctx.list("graph-job:");
  let record: GraphJobRecord | undefined;
  for (const [key, value] of listed) {
    if (!key.startsWith("graph-job:") || key.startsWith("graph-job-idempotency:") || key.startsWith("graph-job-task:") || key.startsWith("graph-job-test:")) continue;
    const candidate = readJobRecord(value);
    if (!candidate) continue;
    record = candidate;
    break;
  }
  if (!record) {
    await ctx.deleteAlarm().catch(() => {});
    return;
  }
  try {
    if (record.state === "completed" || record.state === "failed") {
      await ctx.deleteAlarm().catch(() => {});
      return;
    }
    if (ctx.env.WCOL_JOB_TEST_HOOKS === "1" && (await ctx.store.get("graph-job-test:throw"))) {
      await ctx.store.delete?.("graph-job-test:throw");
      throw new Error("injected handler failure");
    }
    if (record.publishing) {
      await failJob(ctx, record.jobId, "incomplete publish from removed concat path");
      return;
    }
    if ((record.leaseUntil ?? 0) > Date.now()) {
      await arm(ctx, record.leaseUntil);
      return;
    }
    if (record.kind === "concat") {
      // Records written before the concat path changed can still say "publishing".
      const stored: string = record.state;
      if (stored === "publishing") {
        await failJob(ctx, record.jobId, "incomplete publish from removed concat path");
      }
      return;
    }
    const descriptor = await loadDescriptor(ctx, record.jobId, record.graphDigest);
    if (!(await pinsMatch(ctx, descriptor))) {
      await failJob(ctx, record.jobId, "source object changed since the job was planned");
      return;
    }
    const graph = await loadGraph(ctx, record.jobId, descriptor, record.graphDigest);
    const prepared = await preparedFromStorage(ctx, record.jobId, descriptor, graph, record.graphDigest);
    const committed =
      graph && descriptor.bucket
        ? await durableOutputs(ctx, record.jobId, graph, r2Bucket(ctx.env, descriptor.bucket))
        : new Map<number, CommittedObject[]>();
    const durable = descriptor.bucket ? await durableIds(r2Bucket(ctx.env, descriptor.bucket), committed) : new Set<number>();
    const only = graph ? new Set(sinksToReplay(graph, durable)) : undefined;
    if (only && only.size === 0 && descriptor.kind !== "json") {
      if (descriptor.kind === "parquet") {
        const outputs = ledgerOutputs(graph!, committed);
        await publishParquet(ctx, record, descriptor.prefix ?? "", descriptor.bucket ?? "", outputs, record.executionAttempt, graph!.output?.layout);
      } else {
        await failJob(ctx, record.jobId, "concat output cannot execute; plan a hash-partitioned json or parquet job");
        return;
      }
      const done = requireJobRecord(await ctx.store.get(jobKey(record.jobId)));
      if (
        done.executionAttempt === record.executionAttempt &&
        done.state !== "completed" &&
        done.state !== "failed"
      ) {
        await ctx.transaction(async (txn) => {
          const current = requireJobRecord(await txn.get(jobKey(record.jobId)));
          if (current.executionAttempt !== record.executionAttempt) return;
          if (current.state === "completed" || current.state === "failed") return;
          current.state = "completed";
          current.leaseUntil = 0;
          current.updatedAt = Date.now();
          await txn.put(jobKey(record.jobId), current);
        });
      }
      const terminal = requireJobRecord(await ctx.store.get(jobKey(record.jobId)));
      if (terminal.executionAttempt === record.executionAttempt && (terminal.state === "completed" || terminal.state === "failed")) {
        await ctx.deleteAlarm().catch(() => {});
      }
      return;
    }
    if (record.error && isFatalTaskError(record.error)) {
      await failJob(ctx, record.jobId, record.error);
      return;
    }
    const attempt = record.executionAttempt + 1;
    if (attempt > MAX_ATTEMPTS) {
      await failJob(ctx, record.jobId, `failed after ${MAX_ATTEMPTS} attempts`);
      return;
    }
    let start = false;
    await ctx.transaction(async (txn) => {
      const current = requireJobRecord(await txn.get(jobKey(record!.jobId)));
      if (current.publishing) return;
      if (current.state === "completed" || current.state === "failed") return;
      current.executionAttempt = attempt;
      current.state = "running";
      current.leaseUntil = Date.now() + LEASE_MS;
      current.updatedAt = Date.now();
      await txn.put(jobKey(record!.jobId), current);
      await txn.setAlarm?.(Date.now() + ALARM_MS);
      start = true;
    });
    if (!start) return;
    // The alarm invocation itself is limited to 15 minutes. The attempt keeps running
    // on waitUntil, and the durable alarm above fires again if that invocation is cut.
    ctx.waitUntil(
      rerun(ctx, { ...record, executionAttempt: attempt }, prepared, attempt).catch(async (error) => {
        const current = readJobRecord(await ctx.store.get(jobKey(record.jobId)));
        if (!current || current.state === "completed" || current.state === "failed" || current.publishing) return;
        if (isFatalTaskError(error)) {
          await failJob(ctx, record.jobId, errorMessage(error));
          return;
        }
        current.error = errorMessage(error);
        current.failedAttempts += 1;
        current.updatedAt = Date.now();
        await ctx.store.put(jobKey(record.jobId), current);
        await arm(ctx);
      }),
    );
  } catch (error) {
    const current = readJobRecord(await ctx.store.get(jobKey(record.jobId)));
    if (current && current.state !== "completed" && current.state !== "failed") {
      if (isFatalTaskError(error)) {
        await failJob(ctx, record.jobId, errorMessage(error));
        return;
      }
      current.error = errorMessage(error);
      current.failedAttempts += 1;
      current.updatedAt = Date.now();
      await ctx.store.put(jobKey(record.jobId), current);
      await arm(ctx);
    }
  }
}

/**
 * `POST /jobs` on the coordinator: validate, plan, persist a recoverable job, and answer HTTP.
 * Pinning, execution, publication, and alarm recovery live in the modules next to this file.
 * The physical graph is executed by `runExecutionGraph`. This file does not schedule tasks.
 *
 * Storage holds job and task status only. The immutable graph and its programs live in R2.
 */



export type HashScheduleSummary = {
  coordinatorShuffleBytes?: number;
  openedEdges?: number;
};

/** The latest attempt's `timing.json`: schedule counters and per-stage summary. */
type AttemptTiming = {
  schedule?: HashScheduleSummary;
  stages?: StageSummary[];
  usage?: import("../../../src/runtime/coordinator/attempt-usage.ts").AttemptUsage;
};

async function readAttemptTiming(ctx: GraphJobContext, jobId: string): Promise<AttemptTiming> {
  if (!ctx.env.OUTPUT) return {};
  const object = await ctx.env.OUTPUT.get(`_jobs/${jobId}/timing.json`);
  if (!object || object.body === null) return {};
  try {
    const timing = JSON.parse(await object.text()) as AttemptTiming;
    return {
      ...(timing.schedule
        ? {
            schedule: {
              coordinatorShuffleBytes: timing.schedule.coordinatorShuffleBytes,
              openedEdges: timing.schedule.openedEdges,
            },
          }
        : {}),
      ...(timing.stages ? { stages: timing.stages } : {}),
      ...(timing.usage ? { usage: timing.usage } : {}),
    };
  } catch {
    return {};
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

function jsonText(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "content-type": "application/json" } });
}

async function rerun(
  ctx: GraphJobContext,
  record: GraphJobRecord,
  prepared: Prepared,
  attempt: number,
): Promise<void> {
  const renewal = startRenewal(ctx, record.jobId, attempt);
  try {
    const ran = await runPrepared(ctx, record, prepared, attempt, undefined, renewal.stalled);
    const latest = requireJobRecord(await ctx.store.get(jobKey(record.jobId)));
    if (latest.executionAttempt !== attempt) return;
    await finishPrepared(ctx, latest, prepared, ran, attempt, false);
  } finally {
    renewal.stop();
  }
}

/** Alarm entry. Reschedules itself after a recoverable failure. */
export function recoverGraphJob(ctx: GraphJobContext): Promise<void> {
  return recoverJob(ctx, rerun);
}

/**
 * Plan the job and decide where its reducers run. `auto` plans with node budgets first; when
 * the estimated group state needs containers it plans again with the container's budget.
 */
async function planReducers(
  ctx: GraphJobContext,
  body: string,
  fingerprint: string,
  request: ReducerRequest,
): Promise<{ prepared: Prepared; reducers: ReducerChoice }> {
  const planFor = (choice: ReducerChoice) =>
    prepare(ctx, choice.runtime === "container" ? containerRequestBody(body, choice.instance) : body, fingerprint);
  if (request.runtime !== "auto") {
    const reducers = chooseReducers(request, 0);
    return { prepared: await planFor(reducers), reducers };
  }
  const nodePlan = await prepare(ctx, body, fingerprint);
  const plan = nodePlan.plan;
  const groupBytes = plan.kind === "json" || plan.kind === "parquet" ? (plan.groupBytes ?? 0) : 0;
  const reducers = chooseReducers(request, groupBytes);
  return { prepared: reducers.runtime === "container" ? await planFor(reducers) : nodePlan, reducers };
}

/** `POST /jobs`. `jobId` is the coordinator's name for this job. */
type SubmitOptions = { failTaskId?: number; hold?: boolean; crashBeforeComplete?: boolean };

export async function submitGraphJob(
  ctx: GraphJobContext,
  body: string,
  jobId: string,
  options: SubmitOptions = {},
): Promise<Response> {
  let request: ReturnType<typeof jobSources>;
  try {
    request = jobSources(await loadWasm(), body);
  } catch (error) {
    return json({ error: errorMessage(error) }, 400);
  }
  const fingerprint = await sha256Hex(new TextEncoder().encode(body));
  if (request.idempotencyKey) {
    const existing = (await ctx.store.get(idempotencyKey(request.idempotencyKey))) as { jobId: string; fingerprint: string } | undefined;
    if (existing) {
      if (existing.fingerprint !== fingerprint) return json({ error: "idempotency key was used with a different request" }, 409);
      const status = await graphJobStatus(ctx, existing.jobId);
      return json({ ...status, idempotent: true });
    }
  }
  let hints = parseHostExecutionHints(undefined);
  try {
    hints = parseHostExecutionHints(JSON.parse(body) as unknown);
  } catch {
    hints = parseHostExecutionHints(undefined);
  }
  let prepared: Prepared;
  let reducers: ReducerChoice;
  const planStartMs = Date.now();
  try {
    ({ prepared, reducers } = await planReducers(ctx, body, fingerprint, hints.reducers));
  } catch (error) {
    return json({ error: errorMessage(error) }, 400);
  }
  if (prepared.plan.kind === "model") {
    const existing = await ctx.env.OUTPUT?.get(`_models/${prepared.plan.name}/model.json`);
    if (existing) return json({ error: `model ${prepared.plan.name} already exists` }, 409);
  }
  // Footer reads and planning; recorded on the first attempt's timeline.
  const planSpan: StageSpan = { stage: "plan", startMs: planStartMs, endMs: Date.now(), outcome: "ok" };
  if (unpinned(prepared.descriptor) && !allowUnpinned(prepared.plan, ctx.env)) {
    return json({ error: "source has no etag; a recoverable job needs a pinned object" }, 400);
  }
  const rejected = rejectUnexecutable(prepared);
  if (rejected) return json({ error: rejected }, 400);
  const digest = await sha256Hex(prepared.graphWire);
  const now = Date.now();
  const record: GraphJobRecord = {
    jobId,
    kind: prepared.plan.kind,
    state: "accepted",
    executionAttempt: 1,
    failedAttempts: 0,
    nodeCount: 0,
    createdAt: now,
    updatedAt: now,
    leaseUntil: now + LEASE_MS,
    syncOnly: unpinned(prepared.descriptor),
    requestFingerprint: fingerprint,
    graphDigest: digest,
    ...(hints.maxActiveIngress != null ? { maxActiveIngress: hints.maxActiveIngress } : {}),
    ...(reducers.runtime === "container" ? { reducerRuntime: "container", reducerInstance: reducers.instance } : {}),
    reducerReason: reducers.reason,
  };
  const inserted = await ctx.transaction(async (txn) => {
    if (request.idempotencyKey) {
      const race = (await txn.get(idempotencyKey(request.idempotencyKey))) as { jobId: string } | undefined;
      if (race) return false;
    }
    if (await txn.get(jobKey(jobId))) return false;
    await txn.put(jobKey(jobId), record);
    if (request.idempotencyKey) {
      await txn.put(idempotencyKey(request.idempotencyKey), { jobId, fingerprint });
    }
    for (const row of taskRows(jobId, prepared.graph, 1)) await txn.put(row.key, row.value);
    await txn.setAlarm?.(now + ALARM_MS);
    return true;
  });
  if (!inserted) {
    const existingId = request.idempotencyKey
      ? ((await ctx.store.get(idempotencyKey(request.idempotencyKey))) as { jobId: string }).jobId
      : jobId;
    const existing = readJobRecord(await ctx.store.get(jobKey(existingId)));
    if (existing?.requestFingerprint && existing.requestFingerprint !== fingerprint) {
      return json({ error: "job id was used with a different request" }, 409);
    }
    const status = await graphJobStatus(ctx, existingId);
    return json({ ...status, idempotent: true });
  }
  // Descriptor + graph in R2 exist only for recovery. Write them beside the first
  // attempt rather than before it; the attempt waits for them only before it commits.
  const immutable = writeImmutable(ctx.env, jobId, prepared, digest).then(
    () => undefined,
    (error: unknown) => error,
  );
  const recoverable = async (): Promise<void> => {
    const failed = await immutable;
    // Not retryable: another attempt would commit a job that cannot be recovered either.
    if (failed !== undefined) throw new Error(`${FATAL_TASK_MARK} recovery artefacts not written: ${errorMessage(failed)}`);
  };
  if (options.crashBeforeComplete && ctx.env.WCOL_JOB_TEST_HOOKS === "1") {
    await recoverable();
    await ctx.store.put(`graph-job-test:crash:${jobId}`, { crash: true });
  }
  if (options.hold) {
    try {
      await recoverable();
    } catch (error) {
      await failJob(ctx, jobId, errorMessage(error));
      return json({ error: errorMessage(error) }, 500);
    }
    return json({ jobId, kind: prepared.plan.kind, state: "accepted" }, 202);
  }

  const deadline = prepared.plan.kind === "json" ? Number(ctx.env.WCOL_JOB_SYNC_MS ?? 10_000) : 0;
  // A json job that will not answer on this request must leave its body in R2.
  let deliverInline = prepared.plan.kind === "json" && deadline > 0;

  const run = async (attempt: number, stalled: Promise<string>) => {
    const latest = requireJobRecord(await ctx.store.get(jobKey(jobId)));
    if (latest.executionAttempt !== attempt) return null;
    latest.state = "running";
    latest.leaseUntil = Date.now() + LEASE_MS;
    latest.updatedAt = Date.now();
    await ctx.store.put(jobKey(jobId), latest);
    const ran = await runPrepared(ctx, latest, prepared, attempt, options.failTaskId, stalled,
      attempt === 1 ? planSpan : undefined);
    await recoverable();
    const after = requireJobRecord(await ctx.store.get(jobKey(jobId)));
    if (after.executionAttempt !== attempt) return null;
    return finishPrepared(ctx, after, prepared, ran, attempt, deliverInline);
  };

  const finished = (async () => {
    let last: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      const renewal = startRenewal(ctx, jobId, attempt);
      try {
        if (attempt > 1) {
          await ctx.transaction(async (txn) => {
            const current = requireJobRecord(await txn.get(jobKey(jobId)));
            current.executionAttempt = attempt;
            current.failedAttempts += 1;
            current.leaseUntil = Date.now() + LEASE_MS;
            current.updatedAt = Date.now();
            await txn.put(jobKey(jobId), current);
            await txn.setAlarm?.(Date.now() + ALARM_MS);
          });
        }
        return await run(attempt, renewal.stalled);
      } catch (error) {
        const current = readJobRecord(await ctx.store.get(jobKey(jobId)));
        if (current?.publishing) return null;
        last = preferHashFailure(last, error);
        const message = errorMessage(last);
        if (isFatalTaskError(message) || message.includes("stalled:") || attempt >= MAX_ATTEMPTS) {
          const terminal = isFatalTaskError(message) || message.includes("stalled:")
            ? message
            : `failed after ${MAX_ATTEMPTS} attempts: ${message}`;
          await failJob(ctx, jobId, terminal);
          throw last;
        }
      } finally {
        renewal.stop();
      }
    }
    throw last;
  })();

  if (prepared.plan.kind !== "json") {
    ctx.waitUntil(finished.catch((error) => settleBackground(ctx, jobId, error)));
    return json({ jobId, kind: prepared.plan.kind, state: "accepted" }, 202);
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    if (deadline <= 0) resolve("timeout");
    else {
      timer = setTimeout(() => {
        deliverInline = false;
        resolve("timeout");
      }, deadline);
    }
  });
  try {
    const outcome = await Promise.race([timeout, finished]);
    if (outcome !== "timeout") {
      const recordNow = requireJobRecord(await ctx.store.get(jobKey(jobId)));
      // No R2 read here: the timeline is written in the background; GET /jobs/:id has it.
      const meta = JSON.stringify({
        jobId,
        nodeCount: recordNow.nodeCount,
        failedTaskAttempts: recordNow.failedAttempts,
      });
      return jsonText(`${meta.slice(0, -1)},${(outcome ?? "{}").slice(1)}`);
    }
  } catch (error) {
    return json({ jobId, error: errorMessage(error) }, 500);
  } finally {
    clearTimeout(timer);
  }
  if (record.syncOnly) {
    ctx.waitUntil(finished.catch((error) => settleBackground(ctx, jobId, error)));
    return json({ jobId, error: "unpinned source cannot be recovered" }, 500);
  }
  ctx.waitUntil(finished.catch((error) => settleBackground(ctx, jobId, error)));
  return json({ jobId, kind: "json", state: "running" }, 202);
}

async function settleBackground(ctx: GraphJobContext, jobId: string, error: unknown): Promise<void> {
  const current = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!current || current.publishing || current.state === "completed" || current.state === "failed") return;
  await failJob(ctx, jobId, errorMessage(error));
}

/** `GET /jobs/:id`. A live lease with no alarm is not treated as stranded. */
export async function graphJobStatus(
  ctx: GraphJobContext,
  jobId: string,
): Promise<GraphJobRecord & AttemptTiming & { result?: unknown; idempotent?: boolean }> {
  const record = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!record) throw new Error("unknown job");
  const alarmAt = await ctx.getAlarm();
  if (shouldArmAlarm(record, alarmAt, Date.now())) await arm(ctx);
  const timing = await readAttemptTiming(ctx, jobId);
  if (record.state === "completed" && record.resultKey && ctx.env.OUTPUT) {
    const body = await ctx.env.OUTPUT.get(record.resultKey);
    if (body && body.body !== null) {
      return { ...record, result: JSON.parse(await body.text()), ...timing };
    }
  }
  return { ...record, ...timing };
}

export async function isGraphJob(ctx: GraphJobContext, jobId: string): Promise<boolean> {
  return (await ctx.store.get(jobKey(jobId))) != null;
}
