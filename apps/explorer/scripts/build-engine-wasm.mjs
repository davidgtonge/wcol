#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const crate = join(appRoot, "engine");
const outDir = join(appRoot, "pkg");

const result = spawnSync(
  "wasm-pack",
  ["build", crate, "--target", "web", "--out-dir", outDir, "--release"],
  { stdio: "inherit", cwd: appRoot },
);

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

console.log(`wcol-engine wasm built to ${outDir}/`);
