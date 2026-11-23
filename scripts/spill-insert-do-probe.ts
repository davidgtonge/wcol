/**
 * Probe workerd Durable Object SQLite: does 50-row vs 100-row INSERT work, and which is faster?
 *
 *   npx tsx scripts/spill-insert-do-probe.ts
 */

import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distDir = path.join(ROOT, "cloudflare/dist-spill-insert-probe");
const outFile = path.join(distDir, "worker.js");

fs.mkdirSync(distDir, { recursive: true });
await esbuild.build({
  entryPoints: [path.join(ROOT, "scripts/spill-insert-do-probe-worker.ts")],
  outfile: outFile,
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  logLevel: "silent",
  plugins: [
    {
      name: "cf-external",
      setup(build) {
        build.onResolve({ filter: /^cloudflare:workers$/ }, (args) => ({
          path: args.path,
          external: true,
        }));
      },
    },
  ],
});

const mf = new Miniflare({
  modulesRoot: distDir,
  modules: [{ type: "ESModule", path: outFile }],
  compatibilityDate: "2026-01-01",
  durableObjects: { PROBE: { className: "SpillInsertProbe", useSQLite: true } },
});

try {
  const probe = async (rows: number) => {
    const res = await mf.dispatchFetch(`http://probe.local/probe?rows=${rows}`);
    return res.json();
  };
  const timed = async (chunk: number) => {
    const res = await mf.dispatchFetch(`http://probe.local/timed?chunk=${chunk}&total=5000`);
    return res.json();
  };

  const limits = [];
  for (const rows of [49, 50, 51, 100, 101]) {
    limits.push(await probe(rows));
  }

  const times = [];
  for (const chunk of [50, 100]) {
    await timed(chunk);
    times.push(await timed(chunk));
  }

  console.log(JSON.stringify({ ok: true, limits, times }, null, 2));
} finally {
  await mf.dispose();
}
