/**
 * Production Cloudflare artifacts: optimized wasm + Workers without WCS bench on node DOs.
 *
 *   npm run build:cloudflare:prod
 */

import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildWorkers, reportNodeWorkerBundle, ROOT } from "./build-workers.ts";

function npmRun(script: string): void {
  const result = spawnSync("npm", ["run", script], {
    cwd: ROOT,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    throw new Error(`npm run ${script} failed (${result.status})`);
  }
}

npmRun("build:node-wasm");
npmRun("build:coordinator-wasm");
await buildWorkers("prod");

const jsReport = await reportNodeWorkerBundle("prod");
console.log(JSON.stringify({ cloudflareProd: { nodeWorker: jsReport } }, null, 2));
