/**
 * Deploy both Workers: the node Worker first (the coordinator binds its Durable Object class),
 * then the coordinator. Build the Wasm first (`npm run build:node-wasm`, `build:coordinator-wasm`).
 */

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { buildWorkers, distDir, ROOT } from "./build-workers.ts";

function wasmBuildId(): string {
  const wasmPath = path.join(distDir, "wcol_node.wasm");
  return createHash("sha256").update(fs.readFileSync(wasmPath)).digest("hex").slice(0, 16);
}

function deploy(config: string, vars: Record<string, string> = {}): string {
  const args = ["wrangler", "deploy", "--config", config];
  for (const [key, value] of Object.entries(vars)) {
    args.push("--var", `${key}:${value}`);
  }
  const result = spawnSync("npx", args, {
    cwd: path.join(ROOT, "cloudflare"),
    encoding: "utf8",
  });
  process.stdout.write(`${result.stdout ?? ""}\n${result.stderr ?? ""}`);
  if (result.status !== 0) throw new Error(`wrangler deploy --config ${config} failed (${result.status})`);
  return `${result.stdout}\n${result.stderr}`;
}

await buildWorkers("prod");
const wasmBuild = wasmBuildId();
console.log(`WCOL_NODE_WASM_BUILD=${wasmBuild}`);
// Both workers name node DOs: the coordinator prepares reducers, scans on the node
// worker address them. A mismatch sends accepts to the wrong (fresh) object.
// Nodes from an earlier deploy reset themselves when the coordinator's id differs.
const deployId = `${Date.now().toString(36)}-${wasmBuild.slice(0, 8)}`;
deploy("wrangler.node.toml", { WCOL_NODE_WASM_BUILD: wasmBuild, WCOL_DEPLOY_ID: deployId });
const output = deploy("wrangler.toml", {
  WCOL_NODE_WASM_BUILD: wasmBuild,
  WCOL_DEPLOY_ID: deployId,
  WCOL_DISPATCH_CONCURRENCY: process.env.WCOL_DISPATCH_CONCURRENCY ?? "32",
  // High-cardinality spill can go minutes between progress without a yield heartbeat.
  WCOL_STALL_MS: process.env.WCOL_STALL_MS ?? "300000",
});
const url = output.match(/https:\/\/[a-z0-9.-]+\.workers\.dev/i);
console.log(url ? url[0] : "deployed (no workers.dev URL in output)");
const secrets = spawnSync("npx", ["wrangler", "secret", "list", "--config", "wrangler.toml"], {
  cwd: path.join(ROOT, "cloudflare"),
  encoding: "utf8",
});
if (!secrets.stdout?.includes("WCOL_API_TOKEN")) {
  console.warn("WCOL_API_TOKEN is not set: /jobs refuses every job until you run `npx wrangler secret put WCOL_API_TOKEN --config cloudflare/wrangler.toml`.");
}
if (url) await waitForRollout(url[0], deployId);

/**
 * A deploy rolls out gradually: for a while requests can still reach the old coordinator,
 * whose calls the new nodes refuse. Return once the coordinator reports the new deploy on
 * several requests in a row.
 */
async function waitForRollout(base: string, expected: string): Promise<void> {
  const started = Date.now();
  let inRow = 0;
  while (Date.now() - started < 180_000) {
    const seen = await fetch(`${base}/version`)
      .then((response) => response.json() as Promise<{ deployId?: string | null }>)
      .then((body) => body.deployId ?? null)
      .catch(() => null);
    inRow = seen === expected ? inRow + 1 : 0;
    if (inRow >= 5) {
      console.log(`rollout: coordinator on ${expected} after ${Date.now() - started} ms`);
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  console.warn(`rollout: coordinator not seen on ${expected} within 180 s`);
}
