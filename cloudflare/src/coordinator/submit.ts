/**
 * `POST /jobs` on the coordinator, and alarm recovery. Both claim an attempt and run it
 * through `runClaimedAttempt`; the physical graph is executed by `runPrepared`, so this file
 * does not schedule tasks. Storage holds job and task status only; the immutable graph and
 * its programs live in R2.
 */
import { chooseReducers, type ReducerChoice } from "./reducer-choice.ts";
import { createLimiter } from "../../../src/runtime/concurrency.ts";
import { jobSources, type JobSources } from "../../../src/runtime/coordinator/plan-dispatch.ts";
import type { StageSpan, StageSummary } from "../../../src/runtime/coordinator/stage-timeline.ts";
import type { AttemptUsage } from "../../../src/runtime/coordinator/attempt-usage.ts";
import { FATAL_TASK_MARK, isFatalTaskError } from "../../../src/runtime/coordinator/task-errors.ts";
import { shouldArmAlarm, sinkTaskIds, sinksToReplay, type GraphPlan } from "../../../src/runtime/coordinator/job-replay.ts";
import { preferHashFailure, type ObjectPart } from "../../../src/runtime/coordinator/run-hash-job.ts";
import { sha256Hex } from "../digest.ts";
import { r2Bucket } from "../node/node.ts";
import type { R2OutputBucket } from "../r2.ts";
import { jobLimits, refuseUrlSources, withContainerReducers, type JobRequest } from "./job-request.ts";
import {
  allowUnpinned,
  loadDescriptor,
  loadPrepared,
  loadWasm,
  pinSources,
  pinsMatch,
  planPinned,
  type PinnedFooter,
  type Prepared,
  unpinned,
  writeImmutable,
} from "./plan.ts";
import { failJob } from "./publish.ts";
import { finishPrepared } from "./finish.ts";
import { startRenewal } from "./renewal.ts";
import { runPrepared } from "./run.ts";
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
  readJobRecord,
  requireJobRecord,
  taskKey,
  taskRows,
} from "./record.ts";

export async function arm(ctx: GraphJobContext, when = Date.now() + ALARM_MS): Promise<void> {
  await ctx.setAlarm(when);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

function jsonText(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "content-type": "application/json" } });
}

// --- one attempt -------------------------------------------------------------------------------

/**
 * Claim `attempt`, fenced on the previous one still being current: an alarm and an in-process
 * retry cannot both claim the same attempt. `failed`: the previous attempt failed here, and
 * counts against the job; `lost`: its coordinator went away mid-run, which does not.
 */
async function claimAttempt(ctx: GraphJobContext, jobId: string, attempt: number, previous: "failed" | "lost"): Promise<boolean> {
  return ctx.transaction(async (txn) => {
    const current = requireJobRecord(await txn.get(jobKey(jobId)));
    if (current.executionAttempt !== attempt - 1 || current.state === "completed" || current.state === "failed") return false;
    current.executionAttempt = attempt;
    current.state = "running";
    if (previous === "failed") current.failedAttempts += 1;
    current.leaseUntil = Date.now() + LEASE_MS;
    current.updatedAt = Date.now();
    await txn.put(jobKey(jobId), current);
    await txn.setAlarm?.(Date.now() + ALARM_MS);
    return true;
  });
}

type AttemptOptions = {
  failTaskId?: number;
  /** Footer reads and planning, recorded on the first attempt's timeline. */
  planSpan?: StageSpan;
  /** Awaited after the run and before its commit. */
  beforeCommit?: () => Promise<void>;
  /** Whether a JSON result is answered on the request rather than stored in R2. */
  deliverInline: () => boolean;
};

/** Run a claimed attempt and commit it. Null when a newer attempt took the job meanwhile. */
async function runClaimedAttempt(
  ctx: GraphJobContext,
  jobId: string,
  prepared: Prepared,
  attempt: number,
  options: AttemptOptions,
): Promise<string | null> {
  const renewal = startRenewal(ctx, jobId, attempt);
  try {
    const latest = requireJobRecord(await ctx.store.get(jobKey(jobId)));
    if (latest.executionAttempt !== attempt) return null;
    latest.state = "running";
    latest.leaseUntil = Date.now() + LEASE_MS;
    latest.updatedAt = Date.now();
    await ctx.store.put(jobKey(jobId), latest);
    const ran = await runPrepared(ctx, latest, prepared, attempt, options.failTaskId, renewal.stalled, options.planSpan);
    await options.beforeCommit?.();
    const after = requireJobRecord(await ctx.store.get(jobKey(jobId)));
    if (after.executionAttempt !== attempt) return null;
    return await finishPrepared(ctx, after, prepared, ran, attempt, options.deliverInline());
  } finally {
    renewal.stop();
  }
}

// --- alarm recovery ----------------------------------------------------------------------------

/** The job this coordinator holds: one coordinator per job. */
async function storedJob(ctx: GraphJobContext): Promise<GraphJobRecord | undefined> {
  for (const value of (await ctx.list("graph-job:")).values()) {
    const record = readJobRecord(value);
    if (record) return record;
  }
  return undefined;
}

/** Each durable task's committed objects, read from the attempt lists its rows name. */
async function durableOutputs(ctx: GraphJobContext, jobId: string, graph: GraphPlan, bucket: R2OutputBucket): Promise<Map<number, CommittedObject[]>> {
  const lists = new Map<string, CommittedObject[]>();
  const byTask = new Map<number, CommittedObject[]>();
  for (const task of graph.tasks) {
    const row = readDurableRow(await ctx.store.get(taskKey(jobId, task.id)));
    if (!row) continue;
    let list = lists.get(row.outputsKey);
    if (!list) {
      const object = await bucket.get(row.outputsKey);
      list = object?.body ? (JSON.parse(await object.text()) as CommittedObject[]) : [];
      lists.set(row.outputsKey, list);
    }
    const objects = list.filter((object) => object.taskId === task.id);
    if (objects.length > 0) byTask.set(task.id, objects);
  }
  return byTask;
}

/** Of `outputs`, the tasks whose committed objects are all still in R2 at their recorded size. */
async function verifiedOutputs(bucket: R2OutputBucket, outputs: ReadonlyMap<number, CommittedObject[]>): Promise<Map<number, CommittedObject[]>> {
  // A `partitionBy` export can commit thousands of objects: check a few at a time.
  const limit = createLimiter(16);
  const present = (object: CommittedObject) =>
    limit(async () => {
      const found = bucket.head ? await bucket.head(object.objectKey) : await bucket.get(object.objectKey, { range: { suffix: 1 } });
      return found != null && (found.size == null || found.size === object.bytes);
    });
  const verified = new Map<number, CommittedObject[]>();
  for (const [taskId, objects] of outputs) {
    if ((await Promise.all(objects.map(present))).every(Boolean)) verified.set(taskId, objects);
  }
  return verified;
}

/**
 * A Parquet job whose every sink output survived the lost attempt is committed through the
 * normal finish, so a single-file export is concatenated and published exactly as the first
 * attempt would. True when that happened.
 */
async function publishSurvivors(ctx: GraphJobContext, record: GraphJobRecord, prepared: Prepared): Promise<boolean> {
  const { graph, descriptor } = prepared;
  if (!graph || !descriptor.bucket || prepared.plan.kind !== "parquet") return false;
  const bucket = r2Bucket(ctx.env, descriptor.bucket);
  const survivors = await verifiedOutputs(bucket, await durableOutputs(ctx, record.jobId, graph, bucket));
  if (sinksToReplay(graph, new Set(survivors.keys())).length > 0) return false;
  const outputs: ObjectPart[] = sinkTaskIds(graph).flatMap((taskId) => (survivors.get(taskId) ?? []).map((object) => ({ ...object, parts: 1 })));
  await finishPrepared(ctx, record, prepared, { outputs }, record.executionAttempt, false);
  const terminal = requireJobRecord(await ctx.store.get(jobKey(record.jobId)));
  if (terminal.state === "completed" || terminal.state === "failed") await ctx.deleteAlarm().catch(() => {});
  return true;
}

/** Alarm entry: continue a job whose attempt was lost. Reschedules itself after a recoverable failure. */
export async function recoverGraphJob(ctx: GraphJobContext): Promise<void> {
  const record = await storedJob(ctx);
  if (!record || record.state === "completed" || record.state === "failed") {
    await ctx.deleteAlarm().catch(() => {});
    return;
  }
  if ((record.leaseUntil ?? 0) > Date.now()) {
    await arm(ctx, record.leaseUntil);
    return;
  }
  try {
    if (!record.graphDigest) {
      await failJob(ctx, record.jobId, "job record has no graph digest");
      return;
    }
    const descriptor = await loadDescriptor(ctx, record.jobId, record.graphDigest);
    if (!(await pinsMatch(ctx, descriptor))) {
      await failJob(ctx, record.jobId, "source object changed since the job was planned");
      return;
    }
    const prepared = await loadPrepared(ctx, record.jobId, descriptor, record.graphDigest);
    if (await publishSurvivors(ctx, record, prepared)) return;
    if (record.error && isFatalTaskError(record.error)) {
      await failJob(ctx, record.jobId, record.error);
      return;
    }
    const attempt = record.executionAttempt + 1;
    if (attempt > MAX_ATTEMPTS) {
      await failJob(ctx, record.jobId, `failed after ${MAX_ATTEMPTS} attempts`);
      return;
    }
    if (!(await claimAttempt(ctx, record.jobId, attempt, "lost"))) return;
    // The alarm invocation itself is limited to 15 minutes. The attempt keeps running
    // on waitUntil, and the durable alarm the claim set fires again if that invocation is cut.
    ctx.waitUntil(
      runClaimedAttempt(ctx, record.jobId, prepared, attempt, { deliverInline: () => false }).catch((error) =>
        recordRecoveryFailure(ctx, record.jobId, error),
      ),
    );
  } catch (error) {
    await recordRecoveryFailure(ctx, record.jobId, error);
  }
}

/**
 * Note a failed recovery and re-arm the alarm, or fail the job. Counting here matters: a
 * failure before an attempt is claimed (a missing descriptor, an unreachable source) never
 * advances `executionAttempt`, so the attempt limit alone would re-arm forever.
 */
async function recordRecoveryFailure(ctx: GraphJobContext, jobId: string, error: unknown): Promise<void> {
  const current = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!current || current.state === "completed" || current.state === "failed") return;
  const message = errorMessage(error);
  if (isFatalTaskError(message) || current.failedAttempts + 1 >= MAX_ATTEMPTS) {
    await failJob(ctx, jobId, isFatalTaskError(message) ? message : `failed after ${MAX_ATTEMPTS} attempts: ${message}`);
    return;
  }
  current.error = message;
  current.failedAttempts += 1;
  current.updatedAt = Date.now();
  await ctx.store.put(jobKey(jobId), current);
  await arm(ctx);
}

// --- submission --------------------------------------------------------------------------------

export type HashScheduleSummary = {
  coordinatorShuffleBytes?: number;
  openedEdges?: number;
};

/** The latest attempt's `timing.json`: schedule counters and per-stage summary. */
type AttemptTiming = {
  schedule?: HashScheduleSummary;
  stages?: StageSummary[];
  usage?: AttemptUsage;
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

/**
 * Plan the job and decide where its reducers run, reading each footer once. `auto` plans
 * with node budgets first; when the estimated group state needs containers it plans again
 * with the container's budget.
 */
async function planReducers(
  ctx: GraphJobContext,
  request: JobRequest,
  pinned: readonly PinnedFooter[],
  fingerprint: string,
): Promise<{ prepared: Prepared; reducers: ReducerChoice }> {
  const planFor = (choice: ReducerChoice) =>
    planPinned(ctx.env, choice.runtime === "container" ? withContainerReducers(request, choice.instance) : request, pinned, fingerprint);
  if (request.reducers.runtime !== "auto") {
    const reducers = chooseReducers(request.reducers, 0);
    return { prepared: await planFor(reducers), reducers };
  }
  const nodePlan = await planPinned(ctx.env, request, pinned, fingerprint);
  const plan = nodePlan.plan;
  const groupBytes = plan.kind === "json" || plan.kind === "parquet" ? (plan.groupBytes ?? 0) : 0;
  const reducers = chooseReducers(request.reducers, groupBytes);
  return { prepared: reducers.runtime === "container" ? await planFor(reducers) : nodePlan, reducers };
}

/** `POST /jobs`: validate, plan, persist a recoverable job, run it, and answer HTTP. */
export async function submitGraphJob(ctx: GraphJobContext, request: JobRequest): Promise<Response> {
  const { jobId } = request;
  let sources: JobSources;
  try {
    sources = jobSources(await loadWasm(), request.body);
  } catch (error) {
    return json({ error: errorMessage(error) }, 400);
  }
  const refused = refuseUrlSources(sources.sources, jobLimits(ctx.env));
  if (refused) return json({ error: refused }, 400);
  const fingerprint = await sha256Hex(new TextEncoder().encode(request.body));
  if (sources.idempotencyKey) {
    const existing = (await ctx.store.get(idempotencyKey(sources.idempotencyKey))) as { jobId: string; fingerprint: string } | undefined;
    if (existing) {
      if (existing.fingerprint !== fingerprint) return json({ error: "idempotency key was used with a different request" }, 409);
      const status = await graphJobStatus(ctx, existing.jobId);
      return json({ ...status, idempotent: true });
    }
  }
  let prepared: Prepared;
  let reducers: ReducerChoice;
  const planStartMs = Date.now();
  try {
    const pinned = await pinSources(ctx.env, sources.sources);
    ({ prepared, reducers } = await planReducers(ctx, request, pinned, fingerprint));
  } catch (error) {
    return json({ error: errorMessage(error) }, 400);
  }
  if (prepared.plan.kind === "model") {
    const existing = await ctx.env.OUTPUT?.get(`_models/${prepared.plan.name}/model.json`);
    if (existing) return json({ error: `model ${prepared.plan.name} already exists` }, 409);
  }
  const planSpan: StageSpan = { stage: "plan", startMs: planStartMs, endMs: Date.now(), outcome: "ok" };
  if (unpinned(prepared.descriptor) && !allowUnpinned(prepared.plan, ctx.env)) {
    return json({ error: "source has no etag; a recoverable job needs a pinned object" }, 400);
  }
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
    ...(request.maxActiveIngress != null ? { maxActiveIngress: request.maxActiveIngress } : {}),
    ...(reducers.runtime === "container" ? { reducerRuntime: "container", reducerInstance: reducers.instance } : {}),
    reducerReason: reducers.reason,
  };
  const key = sources.idempotencyKey;
  const inserted = await ctx.transaction(async (txn) => {
    if (key && (await txn.get(idempotencyKey(key)))) return false;
    if (await txn.get(jobKey(jobId))) return false;
    await txn.put(jobKey(jobId), record);
    if (key) await txn.put(idempotencyKey(key), { jobId, fingerprint });
    for (const row of taskRows(jobId, prepared.graph, 1)) await txn.put(row.key, row.value);
    await txn.setAlarm?.(now + ALARM_MS);
    return true;
  });
  if (!inserted) {
    const existingId = key ? ((await ctx.store.get(idempotencyKey(key))) as { jobId: string }).jobId : jobId;
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
  const deadline = prepared.plan.kind === "json" ? Number(ctx.env.WCOL_JOB_SYNC_MS ?? 10_000) : 0;
  // A json job that will not answer on this request must leave its body in R2.
  let deliverInline = prepared.plan.kind === "json" && deadline > 0;

  const finished = (async () => {
    let last: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        // The alarm may have claimed a later attempt, or ended the job, meanwhile.
        if (attempt > 1 && !(await claimAttempt(ctx, jobId, attempt, "failed"))) return null;
        return await runClaimedAttempt(ctx, jobId, prepared, attempt, {
          failTaskId: request.failTaskId,
          planSpan: attempt === 1 ? planSpan : undefined,
          beforeCommit: recoverable,
          deliverInline: () => deliverInline,
        });
      } catch (error) {
        last = preferHashFailure(last, error);
        const message = errorMessage(last);
        if (isFatalTaskError(message) || message.includes("stalled:") || attempt >= MAX_ATTEMPTS) {
          const terminal = isFatalTaskError(message) || message.includes("stalled:")
            ? message
            : `failed after ${MAX_ATTEMPTS} attempts: ${message}`;
          await failJob(ctx, jobId, terminal);
          throw last;
        }
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
  // The deadline can pass after the attempt chose to answer inline, so it may finish with the
  // rows only in memory: keep them for `GET /jobs/:id`.
  const settled = finished.then(
    (text) => (text == null ? undefined : keepInlineResult(ctx, jobId, text)),
    (error) => settleBackground(ctx, jobId, error),
  );
  ctx.waitUntil(settled);
  if (record.syncOnly) return json({ jobId, error: "unpinned source cannot be recovered" }, 500);
  return json({ jobId, kind: "json", state: "running" }, 202);
}

/** Store a completed JSON result that was meant for the request, unless it is stored already. */
async function keepInlineResult(ctx: GraphJobContext, jobId: string, text: string): Promise<void> {
  const current = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!current || current.state !== "completed" || current.resultKey || !ctx.env.OUTPUT) return;
  const key = `_jobs/${jobId}/result.json`;
  await ctx.env.OUTPUT.put(key, text);
  await ctx.transaction(async (txn) => {
    const record = requireJobRecord(await txn.get(jobKey(jobId)));
    if (record.state !== "completed" || record.resultKey) return;
    record.resultKey = key;
    record.updatedAt = Date.now();
    await txn.put(jobKey(jobId), record);
  });
}

async function settleBackground(ctx: GraphJobContext, jobId: string, error: unknown): Promise<void> {
  const current = readJobRecord(await ctx.store.get(jobKey(jobId)));
  if (!current || current.state === "completed" || current.state === "failed") return;
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
