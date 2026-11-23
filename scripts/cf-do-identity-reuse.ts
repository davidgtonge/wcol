/**
 * DO identity reuse vs fresh names (hibernate / wake experiment).
 *
 *   npm run bench:cf-do-identity-reuse
 *   DO_START_DRIVER_URL=... npm run bench:cf-do-identity-reuse
 *
 * Env:
 *   IDENTITY_REUSE_QUICK=1     — idle [1], 1 trial (smoke)
 *   IDENTITY_SIMULATE_POOL_MS  — extra delay before each ping (models pool lease RPC)
 *   IDENTITY_LOCATION_HINT     — e.g. enam
 */

import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { Miniflare } from "miniflare";
import { compactIdentityReuse, type IdentityReuseBenchResult } from "../cloudflare/do-start/bench-lib.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const probeDir = path.join(ROOT, "cloudflare/do-start");
const distDir = path.join(probeDir, "dist");
const probeOut = path.join(distDir, "worker.js");
const driverOut = path.join(distDir, "driver-worker.js");

function benchBody(): Record<string, unknown> {
  const quick = process.env.IDENTITY_REUSE_QUICK === "1";
  const simulatePoolRpcMs = Number(process.env.IDENTITY_SIMULATE_POOL_MS ?? 0);
  const locationHint = process.env.IDENTITY_LOCATION_HINT;
  return {
    idleSeconds: quick ? [1] : [1, 5, 15, 60],
    freshSamples: quick ? 2 : 8,
    trialsPerIdle: quick ? 1 : 3,
    transport: "rpc",
    warmWorker: true,
    simulatePoolRpcMs: Number.isFinite(simulatePoolRpcMs) ? simulatePoolRpcMs : 0,
    ...(locationHint ? { locationHint } : {}),
  };
}

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

  const body = benchBody();
  const t0 = performance.now();
  const response = await mf.dispatchFetch("http://do-start.local/bench/identity-reuse", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const wallMs = performance.now() - t0;
  const text = await response.text();
  if (!response.ok) throw new Error(`identity-reuse bench failed (${response.status}): ${text}`);
  const result = JSON.parse(text) as IdentityReuseBenchResult;
  print("miniflare/StartProbe", compactIdentityReuse(result), Math.round(wallMs * 100) / 100);
  await mf.dispose();
}

async function runRemoteDriver(baseUrl: string): Promise<void> {
  const body = benchBody();
  const t0 = performance.now();
  const response = await fetch(new URL("/run/identity-reuse", baseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const wallMs = performance.now() - t0;
  const text = await response.text();
  if (!response.ok) throw new Error(`driver identity-reuse failed (${response.status}): ${text}`);
  print(`${baseUrl}/StartProbe`, JSON.parse(text), Math.round(wallMs * 100) / 100);
}

const driver = process.env.DO_START_DRIVER_URL?.replace(/\/$/, "");
if (driver) {
  await runRemoteDriver(driver);
} else {
  await runLocal();
}
