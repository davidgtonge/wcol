/**
 * Minimal Durable Object cold-start probe. Timing runs inside the Worker (same PoP as DO RPC).
 *
 *   POST /bench/intra  — primary benchmark (rpc|fetch, optional warmWorker + samples)
 *   POST /bench        — legacy external shape (parallel fetch only)
 */

import { DurableObject } from "cloudflare:workers";
import {
  compactIntra,
  compactHintTune,
  compactIdentityReuse,
  parseIntraBenchBody,
  parseIdentityReuseBody,
  runHintTune,
  runIdentityReuseBench,
  runIntraBench,
  type DoLocationHint,
  type PingPayload,
  type ProbeEnv,
} from "./bench-lib.ts";
import { compactParallelScale, runParallelChunk, type ParallelChunkRequest } from "./parallel-scale.ts";

interface Env extends ProbeEnv {}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** Empty DO: first RPC/fetch after `get()` is the measured start. */
export class StartProbe extends DurableObject {
  private readonly state: DurableObjectState;
  private readonly constructedAt: number;
  private readonly constructInstanceId: string;

  constructor(state: DurableObjectState, _env: Env) {
    super(state, _env);
    this.state = state;
    this.constructedAt = performance.now();
    this.constructInstanceId = crypto.randomUUID();
  }

  async ping(): Promise<PingPayload> {
    return {
      ok: true,
      doId: this.state.id.toString(),
      constructedAt: this.constructedAt,
      handledAt: performance.now(),
      constructInstanceId: this.constructInstanceId,
    };
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/ping") {
      return json(await this.ping());
    }
    return json({ error: "not found" }, 404);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && url.pathname === "/") {
        return json({
          service: "do-start-probe",
          intra: "POST /bench/intra",
          identityReuse: "POST /bench/identity-reuse — fresh vs same-name after idle",
          chunk: "POST /bench/chunk — up to 6 parallel cold starts in this invocation",
          parallelScale: "POST /bench/parallel-scale",
        });
      }
      if (request.method === "POST" && url.pathname === "/bench/chunk") {
        const body = (await request.json()) as ParallelChunkRequest;
        return json(await runParallelChunk(env.START_PROBE, body));
      }
      if (request.method === "POST" && url.pathname === "/bench/tune") {
        const body = (await request.json().catch(() => ({}))) as { samplesPerHint?: number };
        const result = await runHintTune(env.START_PROBE, "StartProbe", {
          samplesPerHint: body.samplesPerHint,
        });
        return json(result);
      }
      if (request.method === "POST" && url.pathname === "/bench/intra") {
        const body = await request.json().catch(() => ({}));
        const result = await runIntraBench(env, parseIntraBenchBody(body));
        return json(result);
      }
      if (request.method === "POST" && url.pathname === "/bench/identity-reuse") {
        const body = await request.json().catch(() => ({}));
        const result = await runIdentityReuseBench(env, parseIdentityReuseBody(body));
        return json(result);
      }
      if (request.method === "POST" && url.pathname === "/bench") {
        const body = await request.json().catch(() => ({}));
        const parsed = parseIntraBenchBody(body);
        const result = await runIntraBench(env, {
          ...parsed,
          transport: "fetch",
          modes: parsed.modes ?? ["parallel"],
        });
        return json({ runId: result.runId, results: result.results });
      }
      return json({ error: "not found" }, 404);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return json({ error: message }, 500);
    }
  },
};

export { compactIntra, compactIdentityReuse, runIntraBench, runIdentityReuseBench, parseIntraBenchBody, parseIdentityReuseBody };
