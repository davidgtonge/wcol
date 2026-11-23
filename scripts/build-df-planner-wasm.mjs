#!/usr/bin/env node
/**
 * Build df-planner-wasm for Node: cargo wasm-size + wasm-bindgen (wasm-pack cannot use custom profiles).
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const rustDir = join(root, "rust");
const crate = join(rustDir, "df-planner-wasm");
const outDir = join(crate, "pkg");
const profile = process.env.DF_PLANNER_WASM_PROFILE ?? "wasm-size";
const wasm = join(
  rustDir,
  "target/wasm32-unknown-unknown",
  profile,
  "df_planner_wasm.wasm",
);

function run(cmd, args, cwd = root) {
  const r = spawnSync(cmd, args, { stdio: "inherit", cwd });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

run("cargo", [
  "build",
  "-p",
  "df-planner-wasm",
  "--target",
  "wasm32-unknown-unknown",
  "--profile",
  profile,
], rustDir);

mkdirSync(outDir, { recursive: true });
const outBundler = join(crate, "pkg-bundler");
const outWeb = join(crate, "pkg-web");
mkdirSync(outBundler, { recursive: true });
mkdirSync(outWeb, { recursive: true });

run("wasm-bindgen", [
  wasm,
  "--target",
  "nodejs",
  "--out-dir",
  outDir,
  "--out-name",
  "df_planner_wasm",
]);

run("wasm-bindgen", [
  wasm,
  "--target",
  "bundler",
  "--out-dir",
  outBundler,
  "--out-name",
  "df_planner_wasm",
]);

run("wasm-bindgen", [
  wasm,
  "--target",
  "web",
  "--out-dir",
  outWeb,
  "--out-name",
  "df_planner_wasm",
]);

writeFileSync(
  join(outDir, "package.json"),
  JSON.stringify({ type: "commonjs" }, null, 2),
);

console.log(`df-planner wasm-bindgen output: ${outDir}/`);
