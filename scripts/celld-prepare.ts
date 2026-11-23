/**
 * Copy Worker artifacts into `cloudflare/celld` for optional `celld dev`.
 * Production validation uses Miniflare (`scripts/cf-workerd-jobs.ts`, `npm run build:workers`).
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { distDir, ROOT } from "./build-workers.ts";

const celldDir = path.join(ROOT, "cloudflare", "celld");
const copies = [
  "wcol_coordinator.wasm",
  "wcol_node.wasm",
  "coordinator-worker.js",
  "node-worker.js",
] as const;

function npmRun(script: string): void {
  const result = spawnSync("npm", ["run", script], { cwd: ROOT, stdio: "inherit" });
  if (result.status !== 0) throw new Error(`npm run ${script} failed (${result.status})`);
}

function ensureDist(): void {
  const nodeWasm = path.join(distDir, "wcol_node.wasm");
  const coordinatorWasm = path.join(distDir, "wcol_coordinator.wasm");
  const nodeJs = path.join(distDir, "node-worker.js");
  const coordinatorJs = path.join(distDir, "coordinator-worker.js");
  if (fs.existsSync(nodeWasm) && fs.existsSync(coordinatorWasm) && fs.existsSync(nodeJs) && fs.existsSync(coordinatorJs)) {
    return;
  }
  if (!fs.existsSync(nodeWasm)) npmRun("build:node-wasm");
  if (!fs.existsSync(coordinatorWasm)) npmRun("build:coordinator-wasm");
  npmRun("build:workers");
}

ensureDist();
fs.mkdirSync(celldDir, { recursive: true });
for (const name of copies) {
  fs.copyFileSync(path.join(distDir, name), path.join(celldDir, name));
}
const varsExample = path.join(celldDir, ".dev.vars.example");
const varsPath = path.join(celldDir, ".dev.vars");
if (!fs.existsSync(varsPath) && fs.existsSync(varsExample)) {
  fs.copyFileSync(varsExample, varsPath);
  console.log(`wrote ${path.relative(ROOT, varsPath)} from .dev.vars.example`);
}
console.log(`celld artifacts copied into ${path.relative(ROOT, celldDir)}`);
