import {
  WCS_BENCH_RPC_PIPE,
  WCS_INGRESS_SHARD,
  WCS_STREAM_SHARD,
  WCS_STREAM_SHARD_RPC,
} from "./inter-do-wcs-paths.ts";
import prod, { type CoordinatorEnv } from "../../cloudflare/src/worker.ts";

export { WcolCoordinator } from "../../cloudflare/src/worker.ts";
export { WcolNode } from "./wcol-node-bench.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** Jobs plus WCS transport bench proxies (dev / bench Worker only). */
export default {
  async fetch(request: Request, env: CoordinatorEnv): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (url.pathname === "/wcs/bench" && request.method === "POST") {
        const body = (await request.json()) as {
          bytes?: number;
          bufferCap?: number;
          chunk?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch("http://do/wcs/bench/drain", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            bytes: body.bytes ?? 4 * 1024 * 1024,
            bufferCap: body.bufferCap ?? 256 * 1024,
            chunk: body.chunk ?? 64 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      if (url.pathname === "/wcs/bench/wcs" && request.method === "POST") {
        const body = (await request.json()) as {
          bytes?: number;
          bufferCap?: number;
          chunk?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch("http://do/wcs/bench/consume-wcs", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            bytes: body.bytes ?? 4 * 1024 * 1024,
            bufferCap: body.bufferCap ?? 256 * 1024,
            chunk: body.chunk ?? 64 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      if (url.pathname === "/wcs/bench/partial-wcs" && request.method === "POST") {
        const body = (await request.json()) as { bytes?: number; bufferCap?: number; chunk?: number; delayMs?: number };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch("http://do/wcs/bench/consume-partial-wcs", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            bytes: body.bytes ?? 256 * 1024,
            bufferCap: body.bufferCap ?? 256 * 1024,
            chunk: body.chunk ?? 8 * 1024,
            delayMs: body.delayMs ?? 1,
          }),
        });
      }
      if (url.pathname === "/wcs/bench/shard-wcs" && request.method === "POST") {
        const body = (await request.json()) as {
          url?: string;
          wireProgram?: string;
          bufferCap?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch(`http://do${WCS_INGRESS_SHARD}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            url: body.url,
            wireProgram: body.wireProgram,
            bufferCap: body.bufferCap ?? 256 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      if (url.pathname === WCS_STREAM_SHARD && request.method === "POST") {
        const body = (await request.json()) as {
          url?: string;
          wireProgram?: string;
          bufferCap?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch(`http://do${WCS_INGRESS_SHARD}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            url: body.url,
            wireProgram: body.wireProgram,
            bufferCap: body.bufferCap ?? 256 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      if (url.pathname === WCS_STREAM_SHARD_RPC && request.method === "POST") {
        const body = (await request.json()) as {
          url?: string;
          wireProgram?: string;
          bufferCap?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch(`http://do${WCS_INGRESS_SHARD_RPC}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            url: body.url,
            wireProgram: body.wireProgram,
            bufferCap: body.bufferCap ?? 256 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      if (url.pathname === WCS_BENCH_RPC_PIPE && request.method === "POST") {
        const body = (await request.json()) as {
          bytes?: number;
          bufferCap?: number;
          chunk?: number;
          delayMs?: number;
        };
        const consumer = env.WCOL_NODE.get(env.WCOL_NODE.idFromName("wcs-consumer"));
        return consumer.fetch(`http://do${WCS_BENCH_RPC_PIPE}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            producerNode: "wcs-producer",
            bytes: body.bytes ?? 512 * 1024,
            bufferCap: body.bufferCap ?? 256 * 1024,
            chunk: body.chunk ?? 64 * 1024,
            delayMs: body.delayMs ?? 0,
          }),
        });
      }
      return prod.fetch(request, env);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      return json({ error }, 500);
    }
  },
};
