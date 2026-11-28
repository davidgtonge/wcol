import { concatBytes } from "../../src/runtime/host/bytes.ts";
import { parseShutdownRequest, shutdownAuthorized, shutdownJobs } from "./admin-shutdown.ts";
import { WcolCoordinator, type ShardCoordinatorEnv } from "./coordinator/coordinator.ts";
import { sameSecret } from "./digest.ts";
import { JOB_ID, jobIdFor } from "./job-id.ts";
import { doLocation } from "./node/session.ts";
import consoleHtml from "../dist/console.html";

/** Recorded console runs by query name, bundled by `scripts/build-workers.ts`. */
declare const __RECORDINGS__: Readonly<Record<string, string>>;
/** The console page's Content-Security-Policy, with its inline script's hash (`scripts/build-workers.ts`). */
declare const __CONSOLE_CSP__: string;

export { WcolNode } from "./node/node.ts";
export { WcolCoordinator };

export type CoordinatorEnv = ShardCoordinatorEnv & {
  WCOL_COORDINATOR: DurableObjectNamespace;
  WCOL_SHUTDOWN_SECRET?: string;
  /** `/jobs` requests need `Authorization: Bearer <token>`. The console asks for it. */
  WCOL_API_TOKEN?: string;
  /** `"1"` opens `/jobs` to anyone when no token is set; for local Miniflare runs only. */
  WCOL_ALLOW_ANONYMOUS?: string;
};

/** A job request is SQL and a few options; anything larger is refused before it is parsed. */
const MAX_BODY_BYTES = 256 * 1024;

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

/**
 * Whether a `/jobs` request carries the API token. A Worker without a token refuses every
 * job unless anonymous use is switched on explicitly: one request can start hundreds of
 * Durable Objects and containers.
 */
async function apiAuthorized(env: CoordinatorEnv, request: Request): Promise<boolean> {
  if (!env.WCOL_API_TOKEN) return env.WCOL_ALLOW_ANONYMOUS === "1";
  const sent = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? "";
  return sameSecret(sent, env.WCOL_API_TOKEN);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** A JSON object body, or the response that refuses it (too large, not JSON, not an object). */
async function objectBody(request: Request): Promise<{ kind: "ok"; value: Record<string, unknown> } | { kind: "refused"; response: Response }> {
  const tooLarge = { kind: "refused" as const, response: json({ error: `body exceeds ${MAX_BODY_BYTES} bytes` }, 413) };
  if (Number(request.headers.get("content-length") ?? 0) > MAX_BODY_BYTES) return tooLarge;
  const parts: Uint8Array[] = [];
  let size = 0;
  const reader = request.body?.getReader();
  for (;;) {
    const read = await reader?.read();
    if (!read || read.done) break;
    size += read.value.byteLength;
    if (size > MAX_BODY_BYTES) {
      await reader?.cancel().catch(() => undefined);
      return tooLarge;
    }
    parts.push(read.value);
  }
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(concatBytes(parts)));
  } catch {
    return { kind: "refused", response: json({ error: "body is not valid JSON" }, 400) };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { kind: "refused", response: json({ error: "body must be a JSON object" }, 400) };
  }
  return { kind: "ok", value: value as Record<string, unknown> };
}

function consolePage(): Response {
  return new Response(consoleHtml, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-cache",
      "content-security-policy": __CONSOLE_CSP__,
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
    },
  });
}

/** `POST /jobs`: name the job, then hand it to its own coordinator. */
async function submitJob(env: CoordinatorEnv, request: Request): Promise<Response> {
  const body = await objectBody(request);
  if (body.kind === "refused") return body.response;
  const payload = body.value;
  const requested = payload.jobId;
  if (requested !== undefined && !(typeof requested === "string" && JOB_ID.test(requested))) {
    return json({ error: "jobId must match [A-Za-z0-9_-]{1,64}" }, 400);
  }
  const idempotencyKey = typeof payload.idempotencyKey === "string" ? payload.idempotencyKey : undefined;
  const jobId = typeof requested === "string" ? requested : await jobIdFor(idempotencyKey);
  // A new job's coordinator is created here: near the bucket, with its scans.
  return coordinatorFor(env, jobId, doLocation(env)).fetch("http://do/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ...payload, jobId }),
  });
}

async function route(request: Request, env: CoordinatorEnv, url: URL): Promise<Response> {
  if (url.pathname === "/" && request.method === "GET") return consolePage();
  // Public: visitors without a token replay these instead of running jobs.
  const recording = /^\/recordings\/([^/]+)\.json$/.exec(url.pathname)?.[1];
  if (recording !== undefined && request.method === "GET") {
    const text = Object.hasOwn(__RECORDINGS__, recording) ? __RECORDINGS__[recording] : undefined;
    if (text === undefined) return json({ error: "not found" }, 404);
    return new Response(text, { headers: { "content-type": "application/json", "cache-control": "public, max-age=300" } });
  }
  if (url.pathname.startsWith("/jobs") && !(await apiAuthorized(env, request))) {
    return json({ error: "unauthorized: send Authorization: Bearer <WCOL_API_TOKEN>" }, 401);
  }
  if (url.pathname === "/jobs" && request.method === "POST") return submitJob(env, request);
  const live = /^\/jobs\/([^/]+)\/live$/.exec(url.pathname)?.[1];
  if (live !== undefined && request.method === "GET") {
    const jobId = decodeURIComponent(live);
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
    if (!(await shutdownAuthorized(env, request))) return json({ error: "forbidden" }, 403);
    const body = await objectBody(request);
    if (body.kind === "refused") return body.response;
    const parsed = parseShutdownRequest(body.value);
    if ("error" in parsed) return json(parsed, 400);
    return json(await shutdownJobs(env, parsed));
  }
  return json({ error: "not found" }, 404);
}

/** The front Worker: the console, the job API, and an authorised shutdown. */
export default {
  async fetch(request: Request, env: CoordinatorEnv): Promise<Response> {
    const url = new URL(request.url);
    try {
      return await route(request, env, url);
    } catch (err) {
      // The cause goes to the logs; the caller gets an id to find it by, not the message.
      const incident = crypto.randomUUID();
      const jobId = /^\/jobs\/([^/]+)/.exec(url.pathname)?.[1];
      const message = err instanceof Error ? (err.stack ?? err.message) : String(err);
      console.error(JSON.stringify({ event: "worker_error", incident, jobId, method: request.method, path: url.pathname, message }));
      return json({ error: "internal error", incident, ...(jobId ? { jobId } : {}) }, 500);
    }
  },
};
