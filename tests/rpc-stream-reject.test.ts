import { strict as assert } from "node:assert";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/**
 * The Workers RPC behaviour behind the silent scan hang: on deployed Workers, an RPC that is
 * given a streaming ReadableStream and throws without reading it never settled for the
 * caller, so `acceptScanEdge` returns a typed refusal instead. Miniflare settles the thrown
 * case too, so only the refusal the code relies on is pinned here.
 */
describe("RPC with a stream argument", () => {
  it("settles a returned refusal and a read stream", { timeout: 60_000 }, async () => {
    const outfile = path.join(root, "cloudflare", "dist", "rpc-stream-reject-worker.js");
    await build({
      entryPoints: [path.join(root, "tests", "rpc-stream-reject-worker.ts")],
      outfile,
      bundle: true,
      format: "esm",
      platform: "neutral",
      external: ["cloudflare:workers"],
    });
    const mf = new Miniflare({
      modulesRoot: path.join(root, "cloudflare", "dist"),
      modules: [{ type: "ESModule", path: outfile }],
      compatibilityDate: "2026-01-01",
      durableObjects: {
        CALLEE: { className: "Callee", useSQLite: true },
        CALLER: { className: "Caller", useSQLite: true },
      },
    });
    try {
      const outcome = async (mode: string) => (await (await mf.dispatchFetch(`http://rpc.test/${mode}`)).json()) as {
        settled: boolean;
        value?: unknown;
        error?: string;
      };
      const read = await outcome("read");
      assert.deepEqual(read, { settled: true, value: { accepted: true, bytes: 64 * 64 * 1024 } });
      const returned = await outcome("return");
      assert.deepEqual(returned, { settled: true, value: { accepted: false, error: "not the active node session" } });
    } finally {
      await mf.dispose();
    }
  });
});
