/**
 * DO cold-start benchmark.
 *
 *   npm run bench:cf-do-start              — Miniflare, worker-side /bench/intra
 *   DO_START_DRIVER_URL=... npm run bench:cf-do-start — remote driver (service binding → probe)
 */

import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { Miniflare } from "miniflare";
import { compactIntra, type IntraBenchResult } from "../cloudflare/do-start/bench-lib.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const probeDir = path.join(ROOT, "cloudflare/do-start");
const distDir = path.join(probeDir, "dist");
const probeOut = path.join(distDir, "worker.js");
const driverOut = path.join(distDir, "driver-worker.js");

const INTRA_BODY = {
  counts: [1, 10, 100],
  modes: ["parallel"],
  transport: "rpc",
  samples: 5,
  warmWorker: true,
};

async function bundle(entry: string, outfile: string): Promise<void> {
  await esbuild.build({
    entryPoints: [entry],
    outfile,
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    logLevel: "warning",
    plugins: [
      {
        name: "cloudflare-workers-external",
        setup(build) {
          build.onResolve({ filter: /^cloudflare:workers$/ }, (args) => ({
            path: args.path,
            external: true,
          }));
        },
      },
    ],
  });
}

async function build(): Promise<void> {
  fs.mkdirSync(distDir, { recursive: true });
  await bundle(path.join(probeDir, "worker.ts"), probeOut);
  await bundle(path.join(probeDir, "driver-worker.ts"), driverOut);
}

function print(label: string, payload: unknown, clientWallMs?: number): void {
  console.log(JSON.stringify({ target: label, clientWallMs, ...(payload as object) }, null, 2));
}

async function runLocal(): Promise<void> {
  await build();
  const mf = new Miniflare({
    modulesRoot: distDir,
    modules: [{ type: "ESModule", path: probeOut }],
    compatibilityDate: "2026-01-01",
    durableObjects: { START_PROBE: { className: "StartProbe", useSQLite: true } },
  });

  for (const transport of ["rpc", "fetch"] as const) {
    const t0 = performance.now();
    const response = await mf.dispatchFetch("http://do-start.local/bench/intra", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...INTRA_BODY, transport, warmWorker: transport === "rpc" }),
    });
    const wallMs = performance.now() - t0;
    const text = await response.text();
    if (!response.ok) throw new Error(`intra bench failed (${response.status}): ${text}`);
    const result = JSON.parse(text) as IntraBenchResult;
    print(`miniflare/${transport}`, compactIntra(result), Math.round(wallMs * 100) / 100);
  }

  await mf.dispose();
}

async function runRemoteDriver(baseUrl: string): Promise<void> {
  for (const transport of ["rpc", "fetch"] as const) {
    const t0 = performance.now();
    const response = await fetch(new URL("/run", baseUrl), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...INTRA_BODY, transport, warmWorker: true }),
    });
    const wallMs = performance.now() - t0;
    const text = await response.text();
    if (!response.ok) throw new Error(`driver run failed (${response.status}): ${text}`);
    print(`${baseUrl}/${transport}`, JSON.parse(text), Math.round(wallMs * 100) / 100);
  }
}

const driver = process.env.DO_START_DRIVER_URL?.replace(/\/$/, "");
if (driver) {
  await runRemoteDriver(driver);
} else {
  await runLocal();
}
