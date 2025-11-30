#!/usr/bin/env node
/**
 * Stage demo/data/crates_versions.wcol for the browser bundle (full crates.io versions table).
 * Prefers data/crates_versions.wcol when present; otherwise converts from parquet.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PARQUET = path.join(ROOT, "data", "crates_versions.parquet");
const SOURCE_WCOL = path.join(ROOT, "data", "crates_versions.wcol");
const OUT = path.join(ROOT, "demo", "data", "crates_versions.wcol");
const CLI = path.join(ROOT, "rust", "target", "release", "wcol-cli");

function newestMtime(paths) {
  return Math.max(...paths.map((p) => fs.statSync(p).mtimeMs));
}

function needsRebuild() {
  if (!fs.existsSync(OUT)) return true;
  const outM = fs.statSync(OUT).mtimeMs;
  const inputs = [PARQUET, SOURCE_WCOL].filter((p) => fs.existsSync(p));
  if (!inputs.length) return false;
  return newestMtime(inputs) > outM;
}

function copyFrom(pathFrom) {
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  try {
    fs.unlinkSync(OUT);
  } catch {
    // ignore
  }
  try {
    fs.linkSync(pathFrom, OUT);
  } catch {
    fs.copyFileSync(pathFrom, OUT);
  }
  const mb = (fs.statSync(OUT).size / (1024 * 1024)).toFixed(1);
  console.log(`prepare-demo-crates: linked/copied ${path.relative(ROOT, pathFrom)} → ${path.relative(ROOT, OUT)} (${mb} MB)`);
}

function convertFromParquet() {
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  const build = spawnSync(
    "cargo",
    ["build", "-p", "wcol-cli", "--release", "--manifest-path", path.join(ROOT, "rust", "Cargo.toml")],
    { stdio: "inherit", cwd: ROOT }
  );
  if (build.status !== 0) process.exit(build.status ?? 1);
  const convert = spawnSync(CLI, ["convert", PARQUET, "-o", OUT], { stdio: "inherit", cwd: ROOT });
  if (convert.status !== 0) process.exit(convert.status ?? 1);
  const mb = (fs.statSync(OUT).size / (1024 * 1024)).toFixed(1);
  console.log(`prepare-demo-crates: wrote ${path.relative(ROOT, OUT)} (${mb} MB)`);
}

function main() {
  if (!needsRebuild()) {
    const mb = (fs.statSync(OUT).size / (1024 * 1024)).toFixed(1);
    console.log(`prepare-demo-crates: up to date ${path.relative(ROOT, OUT)} (${mb} MB)`);
    return;
  }

  if (fs.existsSync(SOURCE_WCOL)) {
    copyFrom(SOURCE_WCOL);
    return;
  }

  if (fs.existsSync(PARQUET)) {
    convertFromParquet();
    return;
  }

  if (fs.existsSync(OUT)) {
    console.log(`prepare-demo-crates: using existing ${path.relative(ROOT, OUT)}`);
    return;
  }

  console.warn(
    `prepare-demo-crates: skip — need ${path.relative(ROOT, SOURCE_WCOL)} or ${path.relative(ROOT, PARQUET)} (run ./scripts/prepare-crates-parquet.sh && ./scripts/convert-crates-wcol.sh)`
  );
}

main();
