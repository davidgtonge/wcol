import { strict as assert } from "node:assert";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

describe("session ownership fence", () => {
  it("keeps the newer session when an older cleanup settles later", async () => {
    const outfile = path.join(root, "cloudflare", "dist", "session-fence-worker.js");
    await build({
      entryPoints: [path.join(root, "tests", "session-fence-worker.ts")],
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
        FENCE: { className: "SessionFence", useSQLite: true },
      },
    });
    try {
      const response = await mf.dispatchFetch("http://fence.test/exercise");
      const text = await response.text();
      if (!response.ok || text.startsWith("TypeError") || text.startsWith("Error")) {
        throw new Error(text.slice(0, 2000));
      }
      const body = JSON.parse(text) as { lateReleased: boolean; currentId: number | null; lastReason: string | null };
      assert.equal(body.lateReleased, false);
      assert.equal(body.currentId, 2);
      assert.equal(body.lastReason, "abortHashAttempt");
    } finally {
      await mf.dispose();
    }
  });
});
