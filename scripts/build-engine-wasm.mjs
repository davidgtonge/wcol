#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const crate = join(root, "rust/wcol-engine");
const outDir = join(root, "pkg");

const result = spawnSync(
  "wasm-pack",
  ["build", crate, "--target", "web", "--out-dir", outDir, "--release"],
  { stdio: "inherit", cwd: root }
);

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

console.log(`wcol-engine wasm built to ${outDir}/`);
