/**
 * Driver: cold-start tuning + parallel scale (fan-out chunks of 6 DO RPCs per invocation).
 *
 *   POST /run/parallel-scale
 *   POST /chunk  — returns immediately; cold DO RPCs run in waitUntil
 */

import {
  compactHintTune,
  compactIntra,
  compactIdentityReuse,
  parseIntraBenchBody,
  parseIdentityReuseBody,
  runHintTune,
  touchColdDo,
  type DoBinding,
  type DoLocationHint,
  type IntraBenchResult,
} from "./bench-lib.ts";
import {
  chunkResultKey,
  compactParallelScale,
  pollChunkResults,
  runParallelChunk,
  runParallelScaleOnBinding,
  type ChunkAccept,
  type ParallelChunkRequest,
} from "./parallel-scale.ts";

interface Env {
  PROBE: Fetcher;
  WCOL_NODE: DoBinding;
  SELF: Fetcher;
  BENCH_KV: KVNamespace;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function parseHint(raw: unknown): DoLocationHint | undefined {
  if (raw === "enam" || raw === "weur" || raw === "eeur" || raw === "wnam" || raw === "sam") {
    return raw;
  }
  return undefined;
}

async function forwardToProbe(env: Env, body: unknown): Promise<Response> {
  parseIntraBenchBody(body);
  const t0 = performance.now();
  const probeResponse = await env.PROBE.fetch("https://do-start-probe.internal/bench/intra", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const driverRoundTripMs = performance.now() - t0;
  const text = await probeResponse.text();
  if (!probeResponse.ok) {
    return json({ error: text.slice(0, 500), driverRoundTripMs }, probeResponse.status);
  }
  const probePayload = JSON.parse(text) as IntraBenchResult;
  return json({
    measuredFrom: "driver-service-binding",
    driverRoundTripMs,
    ...compactIntra(probePayload),
  });
}

async function dispatchNodeChunk(env: Env, req: ParallelChunkRequest): Promise<ChunkAccept> {
  const response = await env.SELF.fetch("https://do-start-driver.internal/chunk", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(req),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`chunk dispatch failed (${response.status}): ${text.slice(0, 300)}`);
  return JSON.parse(text) as ChunkAccept;
}

async function forwardIdentityReuse(env: Env, body: unknown): Promise<Response> {
  parseIdentityReuseBody(body);
  const t0 = performance.now();
  const probeResponse = await env.PROBE.fetch("https://do-start-probe.internal/bench/identity-reuse", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const driverRoundTripMs = performance.now() - t0;
  const text = await probeResponse.text();
  if (!probeResponse.ok) {
    return json({ error: text.slice(0, 500), driverRoundTripMs }, probeResponse.status);
  }
  const probePayload = JSON.parse(text);
  return json({
    measuredFrom: "driver-service-binding",
    driverRoundTripMs,
    ...compactIdentityReuse(probePayload),
  });
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/chunk") {
        const body = (await request.json()) as ParallelChunkRequest;
        ctx.waitUntil(
          runParallelChunk(env.WCOL_NODE, body)
            .then((result) =>
              env.BENCH_KV.put(chunkResultKey(body.runId, body.chunkIndex), JSON.stringify(result), {
                expirationTtl: 3600,
              }),
            )
            .catch(async (err) => {
              const message = err instanceof Error ? err.message : String(err);
              await env.BENCH_KV.put(
                chunkResultKey(body.runId, body.chunkIndex),
                JSON.stringify({ count: body.count, totalMs: -1, perDoMs: { min: -1, max: -1, p50: -1 }, error: message }),
                { expirationTtl: 3600 },
              );
            }),
        );
        return json({ accepted: true, runId: body.runId, chunkIndex: body.chunkIndex });
      }
      if (request.method === "GET" && url.pathname === "/") {
        return json({
          service: "do-start-driver",
          chunk: "POST /chunk — instant accept; DO starts in waitUntil",
          parallelScale: "POST /run/parallel-scale",
          identityReuse: "POST /run/identity-reuse",
        });
      }
      if (request.method === "POST" && url.pathname === "/run/identity-reuse") {
        const body = await request.json().catch(() => ({}));
        return forwardIdentityReuse(env, body);
      }
      if (request.method === "POST" && url.pathname === "/run/parallel-scale") {
        const body = (await request.json().catch(() => ({}))) as {
          totals?: number[];
          budgetMs?: number;
          locationHint?: string;
          includeSingleInvocation?: boolean;
          includeFanOut?: boolean;
          chunkSize?: number;
          pollTimeoutMs?: number;
        };
        const locationHint = parseHint(body.locationHint) ?? "enam";
        const pollTimeoutMs = body.pollTimeoutMs ?? 120_000;
        await touchColdDo(env.WCOL_NODE, `${crypto.randomUUID()}:warmup`, "rpc", locationHint);
        const result = await runParallelScaleOnBinding(
          "WcolNode",
          (req) => dispatchNodeChunk(env, { ...req, locationHint: req.locationHint ?? locationHint }),
          (armRunId, chunkCount) => pollChunkResults(env.BENCH_KV, armRunId, chunkCount, pollTimeoutMs),
          {
            totals: body.totals,
            budgetMs: body.budgetMs ?? 400,
            locationHint,
            chunkSize: body.chunkSize,
            includeSingleInvocation: body.includeSingleInvocation,
            includeFanOut: body.includeFanOut,
            pollTimeoutMs,
          },
        );
        return json(compactParallelScale(result));
      }
      if (request.method === "POST" && url.pathname === "/run/tune") {
        const body = (await request.json().catch(() => ({}))) as { samplesPerHint?: number };
        const samplesPerHint = body.samplesPerHint;
        const t0 = performance.now();
        const [probe, node] = await Promise.all([
          env.PROBE.fetch("https://do-start-probe.internal/bench/tune", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ samplesPerHint }),
          }).then(async (res) => {
            const text = await res.text();
            if (!res.ok) throw new Error(`probe tune: ${text.slice(0, 300)}`);
            return JSON.parse(text);
          }),
          runHintTune(env.WCOL_NODE, "WcolNode", { samplesPerHint }),
        ]);
        return json({
          measuredFrom: "driver",
          driverRoundTripMs: performance.now() - t0,
          goalMs: 200,
          StartProbe: compactHintTune(probe),
          WcolNode: compactHintTune(node),
        });
      }
      if (request.method === "POST" && url.pathname === "/run/node-one") {
        const overrides = (await request.json().catch(() => ({}))) as {
          locationHint?: string;
          samples?: number;
        };
        const hint = parseHint(overrides.locationHint) ?? "enam";
        const samples = Math.min(Math.max(overrides.samples ?? 15, 3), 30);
        const runId = crypto.randomUUID();
        await touchColdDo(env.WCOL_NODE, `${runId}:warmup`, "rpc", hint);
        const touches = [];
        for (let i = 0; i < samples; i++) {
          touches.push(await touchColdDo(env.WCOL_NODE, `${runId}:n:${i}`, "rpc", hint));
        }
        const totals = touches.map((t) => t.totalMs).sort((a, b) => a - b);
        const mid = Math.floor(totals.length / 2);
        const p50 = totals.length % 2 === 0 ? (totals[mid - 1]! + totals[mid]!) / 2 : totals[mid]!;
        return json({
          measuredFrom: "driver",
          target: "WcolNode",
          locationHint: hint,
          samples,
          p50Ms: Math.round(p50 * 100) / 100,
          minMs: totals[0],
          under200ms: totals.filter((n) => n < 200).length,
          totalsMs: totals.map((n) => Math.round(n * 100) / 100),
        });
      }
      if (request.method === "POST" && url.pathname === "/run/one") {
        const overrides = (await request.json().catch(() => ({}))) as Record<string, unknown>;
        return forwardToProbe(env, {
          ...overrides,
          counts: [],
          samples: 12,
          warmWorker: true,
          transport: "rpc",
        });
      }
      if (request.method === "POST" && url.pathname === "/run") {
        const body = await request.json().catch(() => ({}));
        return forwardToProbe(env, body);
      }
      return json({ error: "not found" }, 404);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return json({ error: message }, 500);
    }
  },
};
