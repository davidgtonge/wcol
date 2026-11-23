import { DurableObject } from "cloudflare:workers";
import type { R2Env } from "../r2.ts";
import { createRouter } from "../router.ts";
import { liveSnapshot } from "./live-status.ts";
import { graphJobStatus, isGraphJob, recoverGraphJob, submitGraphJob } from "./submit.ts";
import { boundStorage, type GraphJobEnv, type JobStore, type LiveAttempt } from "./record.ts";
import { attemptKey, nextUnit, type WorkQueues } from "../../../src/runtime/coordinator/unit-claims.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export { parseHostExecutionHints, readJobRecord } from "./record.ts";

export type ShardCoordinatorEnv = R2Env &
  GraphJobEnv & {
    WCOL_NODE: DurableObjectNamespace;
  };

/**
 * One coordinator per job: plans the `POST /jobs` body (docs/job-api.md) and runs
 * it as a graph on node Durable Objects. Its storage is bounded status records (I11).
 */
export class WcolCoordinator extends DurableObject<ShardCoordinatorEnv> {
  private readonly router = createRouter();
  /** Scan work queues for this job's attempts. In memory: an attempt ends if this object restarts. */
  private readonly work: WorkQueues = new Map();
  private readonly live = new Map<string, LiveAttempt>();

  constructor(
    private readonly state: DurableObjectState,
    env: ShardCoordinatorEnv,
  ) {
    super(state, env);

    const ctx = this.router;
    ctx.post("/jobs", async (request) => {
      const text = await request.text();
      const body = JSON.parse(text) as {
        jobId?: string;
        failFirstAttemptTaskId?: string;
        testHold?: boolean;
        testCrashBeforeComplete?: boolean;
      };
      if (!body.jobId) return json({ error: "jobId required" }, 400);
      // Fault injection for retry tests: the first attempt of task `n<id>` fails mid-stream.
      // It affects only the caller's own job, so it is not behind WCOL_JOB_TEST_HOOKS.
      const fail = body.failFirstAttemptTaskId;
      const failTaskId = fail?.startsWith("n") ? Number(fail.slice(1)) : undefined;
      const hooks = this.env.WCOL_JOB_TEST_HOOKS === "1";
      return submitGraphJob(this.context(), text, body.jobId, {
        failTaskId,
        hold: hooks && body.testHold === true,
        crashBeforeComplete: hooks && body.testCrashBeforeComplete === true,
      });
    });
    ctx.get("/jobs", async (_request, url) => {
      const jobId = url.searchParams.get("jobId");
      if (!jobId) return json({ error: "jobId required" }, 400);
      const jobCtx = this.context();
      if (!(await isGraphJob(jobCtx, jobId))) return json({ error: "unknown job" }, 404);
      return json(await graphJobStatus(jobCtx, jobId));
    });
    ctx.get("/jobs/live", async (_request, url) => {
      const jobId = url.searchParams.get("jobId");
      if (!jobId) return json({ error: "jobId required" }, 400);
      const status = await liveSnapshot(this.context(), jobId, Date.now());
      return status ? json(status) : json({ error: "unknown job" }, 404);
    });
    if (env.WCOL_JOB_TEST_HOOKS === "1") {
      ctx.get("/jobs/test/state", (request, url) => this.testHook(url, request));
      for (const path of ["abort-accept", "expire", "throw", "alarm"]) {
        ctx.post(`/jobs/test/${path}`, (request, url) => this.testHook(url, request));
      }
    }
  }

  private context() {
    const storage = this.state.storage;
    const store = boundStorage(storage);
    return {
      env: this.env,
      store,
      transaction: <T>(run: (txn: JobStore) => Promise<T>) =>
        storage.transaction(async (txn) => run(boundStorage(txn) as JobStore)),
      setAlarm: (time: number) => storage.setAlarm(time),
      getAlarm: async () => {
        return storage.getAlarm();
      },
      deleteAlarm: () => storage.deleteAlarm(),
      waitUntil: (promise: Promise<unknown>) => this.state.waitUntil(promise),
      list: (prefix: string) => storage.list({ prefix }),
      work: this.work,
      live: this.live,
    };
  }

  /** A hash scan asks for its next unit of work; `null` when none is left (see unit-claims.ts). */
  nextUnit(params: { jobId: string; attempt: number; taskId: number }): number | null {
    return nextUnit(this.work, attemptKey(params.jobId, params.attempt), params.taskId);
  }

  /** Drop this object's SQLite database and alarm so a recovery alarm cannot restart a job. */
  async purgeStorage(): Promise<{ ok: true }> {
    await this.state.storage.deleteAlarm().catch(() => undefined);
    await this.state.storage.deleteAll();
    return { ok: true };
  }

  async alarm(): Promise<void> {
    // Recovery claims the attempt and continues it with waitUntil. A job longer than
    // the 15-minute alarm limit is retried from the durable alarm, not finished inside it.
    await recoverGraphJob(this.context());
  }

  fetch(request: Request): Promise<Response> {
    return this.router.fetch(request);
  }

  /** Hooks for the SQLite recovery proofs. Absent unless `WCOL_JOB_TEST_HOOKS=1`. */
  private async testHook(url: URL, request: Request): Promise<Response> {
    const ctx = this.context();
    const jobId = url.searchParams.get("jobId");
    if (url.pathname === "/jobs/test/abort-accept" && request.method === "POST") {
      try {
        await this.state.storage.transaction(async (txn) => {
          await txn.put("graph-job:abort-probe", { jobId: "abort-probe", state: "accepted" });
          await txn.setAlarm(Date.now() + 60_000);
          throw new Error("abort");
        });
      } catch {
        // The transaction rolls back the row and the alarm together.
      }
      return json({
        row: (await this.state.storage.get("graph-job:abort-probe")) ?? null,
        alarm: await ctx.getAlarm(),
      });
    }
    if (!jobId) return json({ error: "jobId required" }, 400);
    if (url.pathname === "/jobs/test/state" && request.method === "GET") {
      if (!(await isGraphJob(ctx, jobId))) return json({ error: "unknown job" }, 404);
      return json({ record: await graphJobStatus(ctx, jobId), alarm: await ctx.getAlarm() });
    }
    if (url.pathname === "/jobs/test/expire" && request.method === "POST") {
      const record = (await ctx.store.get(`graph-job:${jobId}`)) as { leaseUntil?: number } | undefined;
      if (!record) return json({ error: "unknown job" }, 404);
      record.leaseUntil = 0;
      await ctx.store.put(`graph-job:${jobId}`, record);
      await ctx.deleteAlarm();
      return json({ alarm: await ctx.getAlarm() });
    }
    if (url.pathname === "/jobs/test/throw" && request.method === "POST") {
      await ctx.store.put("graph-job-test:throw", { throw: true });
      return json({ ok: true });
    }
    if (url.pathname === "/jobs/test/alarm" && request.method === "POST") {
      await this.alarm();
      return json({ alarm: await ctx.getAlarm(), record: await graphJobStatus(ctx, jobId) });
    }
    return json({ error: "not found" }, 404);
  }
}
