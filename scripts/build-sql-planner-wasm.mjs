#!/usr/bin/env node
/**
 * Build release wcol-sql-wasm + wasm-bindgen (nodejs) for planner benchmarks.
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, renameSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const rustDir = resolve(root, "rust");
const wasmPath = resolve(
  rustDir,
  "target/wasm32-unknown-unknown/release/wcol_sql_wasm.wasm",
);
const pkgDir = resolve(rustDir, "wcol-sql-wasm/pkg");

const env = {
  ...process.env,
  CC: process.env.WCOL_WASM_CC ?? "/opt/homebrew/opt/llvm/bin/clang",
  CXX: process.env.WCOL_WASM_CXX ?? "/opt/homebrew/opt/llvm/bin/clang++",
  AR: process.env.WCOL_WASM_AR ?? "/opt/homebrew/opt/llvm/bin/llvm-ar",
  CARGO_PROFILE_RELEASE_OPT_LEVEL: process.env.CARGO_PROFILE_RELEASE_OPT_LEVEL ?? "3",
  CARGO_PROFILE_RELEASE_LTO: process.env.CARGO_PROFILE_RELEASE_LTO ?? "fat",
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "1",
  CARGO_PROFILE_RELEASE_STRIP: process.env.CARGO_PROFILE_RELEASE_STRIP ?? "symbols",
};

function run(cmd, args, cwd = rustDir) {
  const r = spawnSync(cmd, args, { cwd, env, stdio: "inherit" });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

run("cargo", [
  "build",
  "--release",
  "--target",
  "wasm32-unknown-unknown",
  "-p",
  "wcol-sql-wasm",
  "--features",
  "datafusion",
]);

mkdirSync(pkgDir, { recursive: true });
run("wasm-bindgen", [
  wasmPath,
  "--target",
  "nodejs",
  "--out-dir",
  pkgDir,
  "--out-name",
  "wcol_sql_wasm",
]);
renameSync(resolve(pkgDir, "wcol_sql_wasm.js"), resolve(pkgDir, "wcol_sql_wasm.cjs"));

console.log(`planner wasm pkg: ${pkgDir}`);
