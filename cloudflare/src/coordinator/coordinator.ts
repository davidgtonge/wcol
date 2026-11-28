import { DurableObject } from "cloudflare:workers";
import type { R2Env } from "../r2.ts";
import { createRouter } from "../router.ts";
import { liveSnapshot } from "./live-status.ts";
import { graphJobStatus, isGraphJob, recoverGraphJob, submitGraphJob } from "./submit.ts";
import { boundStorage, type GraphJobEnv, type JobStore, type LiveAttempt } from "./record.ts";
import { jobLimits, parseJobRequest } from "./job-request.ts";
import { attemptKey, nextUnit, type WorkQueues } from "../../../src/runtime/coordinator/unit-claims.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export { readJobRecord } from "./record.ts";

export type ShardCoordinatorEnv = R2Env &
  GraphJobEnv & {
    WCOL_NODE: DurableObjectNamespace;
  };

/**
 * One coordinator per job: plans the `POST /jobs` body (docs/job-api.md) and runs
 * it as a graph on node Durable Objects. Its storage is bounded status records.
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
      const parsed = parseJobRequest(await request.text(), jobLimits(this.env));
      if (parsed.kind === "invalid") return json({ error: parsed.error }, 400);
      return submitGraphJob(this.context(), parsed.request);
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
      getAlarm: () => storage.getAlarm(),
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
}
