/**
 * celld local entry: one application that exports both Durable Object classes
 * and installs both Wasm modules. Cloudflare deploy stays two Workers
 * (`wrangler.toml` / `wrangler.node.toml`); celld runs one fleet app.
 *
 * Hash scans on this host buffer edges as bytes, cache parquet, and finish on
 * the original runTask RPC. waitUntil continuations parked after a nested DO
 * call do not resume on celld.
 */
import coordinatorModule from "./wcol_coordinator.wasm";
import nodeModule from "./wcol_node.wasm";
import { installCoordinatorWasm, installNodeWasm, instantiateWorkerWasm } from "../src/wasm.ts";
import worker, { WcolCoordinator, WcolNode, type CoordinatorEnv } from "../src/worker.ts";
import { parseR2BindingName, resolveR2Bucket, validateObjectKey } from "../src/r2.ts";

let coordinatorWasm: Promise<Awaited<ReturnType<typeof instantiateWorkerWasm>>> | null = null;

installCoordinatorWasm(() => {
  coordinatorWasm ??= instantiateWorkerWasm(coordinatorModule as WebAssembly.Module, "coordinator");
  return coordinatorWasm;
});
// A fresh instance per call: each node object caches its own (`NodeSessionLease`).
installNodeWasm(() => instantiateWorkerWasm(nodeModule as WebAssembly.Module, "node"));

export { WcolCoordinator, WcolNode };

type CelldEnv = CoordinatorEnv & {
  WCOL_JOB_TEST_HOOKS?: string;
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(request: Request, env: CelldEnv): Promise<Response> {
    const url = new URL(request.url);
    if (env.WCOL_JOB_TEST_HOOKS === "1" && url.pathname.startsWith("/dev/r2/")) {
      const key = decodeURIComponent(url.pathname.slice("/dev/r2/".length));
      if (!validateObjectKey(key)) return json({ error: "invalid object key" }, 400);
      const binding = parseR2BindingName(url.searchParams.get("bucket") ?? "output") ?? "output";
      const bucket = resolveR2Bucket(env, binding);
      if (!bucket) return json({ error: `missing ${binding} bucket` }, 500);
      if (request.method === "PUT") {
        await bucket.put(key, request.body ?? new Uint8Array());
        return new Response(null, { status: 204 });
      }
      if (request.method === "GET") {
        const object = await bucket.get(key);
        if (!object) return json({ error: "not found" }, 404);
        const body = object.body ?? (await object.arrayBuffer());
        return new Response(body, { headers: { "content-type": "application/octet-stream" } });
      }
    }
    return worker.fetch(request, env);
  },
};
