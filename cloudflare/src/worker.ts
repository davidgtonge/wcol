import { shutdownAuthorized, shutdownJobs, type ShutdownJobRequest } from "./admin-shutdown.ts";
import { WcolCoordinator, type ShardCoordinatorEnv } from "./coordinator/coordinator.ts";
import { doLocation } from "./node/session.ts";
import consoleHtml from "../dist/console.html";

export { WcolNode } from "./node/node.ts";
export { WcolCoordinator };

export type CoordinatorEnv = ShardCoordinatorEnv & {
  WCOL_COORDINATOR: DurableObjectNamespace;
  WCOL_SHUTDOWN_SECRET?: string;
  /** When set, `/jobs` requests need `Authorization: Bearer <token>`. The console asks for it. */
  WCOL_API_TOKEN?: string;
};

/** A job's coordinator Durable Object, which serves that job's `/jobs` routes. */
function coordinatorFor(
  env: CoordinatorEnv,
  jobId: string,
  options?: { locationHint?: string },
): { fetch(url: string, init?: RequestInit): Promise<Response> } {
  return env.WCOL_COORDINATOR.get(env.WCOL_COORDINATOR.idFromName(jobId), options) as {
    fetch(url: string, init?: RequestInit): Promise<Response>;
  };
}

/** Whether a `/jobs` request carries the API token, compared in constant time. */
function apiAuthorized(env: CoordinatorEnv, request: Request): boolean {
  if (!env.WCOL_API_TOKEN) return true;
  const sent = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? "";
  const encoder = new TextEncoder();
  const [a, b] = [encoder.encode(sent), encoder.encode(env.WCOL_API_TOKEN)];
  return a.byteLength === b.byteLength && crypto.subtle.timingSafeEqual(a, b);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** A client `idempotencyKey` names the job, so a repeat reaches the same coordinator. */
async function jobIdFor(idempotencyKey: string | undefined): Promise<string> {
  if (!idempotencyKey) return crypto.randomUUID();
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(idempotencyKey)));
  return `k-${[...digest.subarray(0, 16)].map((b) => b.toString(16).padStart(2, "0")).join("")}`;
}

/** The front Worker: the console, the job API, and an authorised shutdown. */
export default {
  async fetch(request: Request, env: CoordinatorEnv): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (url.pathname === "/" && request.method === "GET") {
        return new Response(consoleHtml, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" } });
      }
      if (url.pathname.startsWith("/jobs") && !apiAuthorized(env, request)) {
        return json({ error: "unauthorized: send Authorization: Bearer <WCOL_API_TOKEN>" }, 401);
      }
      if (url.pathname === "/jobs" && request.method === "POST") {
        const payload = (await request.json()) as { idempotencyKey?: string; jobId?: string };
        const jobId = payload.jobId ?? (await jobIdFor(payload.idempotencyKey));
        // A new job's coordinator is created here: near the bucket, with its scans.
        return coordinatorFor(env, jobId, doLocation(env)).fetch("http://do/jobs", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ ...payload, jobId }),
        });
      }
      const live = /^\/jobs\/([^/]+)\/live$/.exec(url.pathname);
      if (live && request.method === "GET") {
        const jobId = decodeURIComponent(live[1]!);
        return coordinatorFor(env, jobId).fetch(`http://do/jobs/live?jobId=${encodeURIComponent(jobId)}`);
      }
      if (url.pathname.startsWith("/jobs/") && request.method === "GET") {
        const jobId = url.pathname.slice("/jobs/".length);
        if (!jobId || jobId.includes("/")) return json({ error: "not found" }, 404);
        return coordinatorFor(env, jobId).fetch(`http://do/jobs?jobId=${encodeURIComponent(jobId)}`);
      }
      if (url.pathname === "/version" && request.method === "GET") {
        // The deploy script waits for this to report the new deploy before it returns.
        return json({ deployId: env.WCOL_DEPLOY_ID ?? null });
      }
      if (url.pathname === "/admin/shutdown" && request.method === "POST") {
        if (!shutdownAuthorized(env, request)) return json({ error: "forbidden" }, 403);
        const body = (await request.json().catch(() => ({}))) as ShutdownJobRequest;
        return json(await shutdownJobs(env, body));
      }
      return json({ error: "not found" }, 404);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      return json({ error }, 500);
    }
  },
};
