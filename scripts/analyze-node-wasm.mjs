/**
 * Break down `wcol_node.wasm`: Cargo dependency tree, wasm sections, exports, and
 * symbol-weight by engine module (via twiggy on an unstripped build).
 *
 *   npm run analyze:node-wasm
 *   npm run analyze:node-wasm -- --skip-build   # reuse target/wcol_node.analyze.wasm
 */

import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rustDir = path.join(ROOT, "rust");
const distWasm = path.join(ROOT, "cloudflare/dist/wcol_node.wasm");
const analyzeWasm = path.join(rustDir, "target/wasm32-unknown-unknown/release/wcol_node.analyze.wasm");
const skipBuild = process.argv.includes("--skip-build");

const PLANNER_EXPORTS = new Set(["wcol_plan_query", "wcol_plan_output_ptr", "wcol_columns_for_sql"]);

function run(cmd, args, opts = {}) {
  return execFileSync(cmd, args, { encoding: "utf8", ...opts }).trimEnd();
}

function buildAnalyzeWasm() {
  const env = {
    ...process.env,
    RUSTFLAGS: '-C panic=abort -C target-feature=+simd128 --cfg getrandom_backend="custom"',
  };
  spawnSync(
    "cargo",
    [
      "build",
      "-p",
      "wcol-node-wasm",
      "--target",
      "wasm32-unknown-unknown",
      "--release",
      "--offline",
      "--config",
      'profile.release.strip="none"',
    ],
    { cwd: rustDir, stdio: "inherit", env },
  );
  const built = path.join(rustDir, "target/wasm32-unknown-unknown/release/wcol_node.wasm");
  fs.copyFileSync(built, analyzeWasm);
}

function wasmExportNames(bin) {
  if (bin[0] !== 0 || bin[1] !== 0x61 || bin[2] !== 0x73 || bin[3] !== 0x6d) {
    throw new Error("not a wasm module");
  }
  let at = 8;
  const readUleb = () => {
    let value = 0;
    let shift = 0;
    for (;;) {
      const byte = bin[at++];
      value |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
      shift += 7;
    }
  };
  while (at < bin.length) {
    const id = bin[at++];
    const size = readUleb();
    const end = at + size;
    if (id === 7) {
      const count = readUleb();
      const names = [];
      for (let i = 0; i < count; i++) {
        const len = readUleb();
        names.push(Buffer.from(bin.subarray(at, at + len)).toString("utf8"));
        at += len;
        at += 1;
        readUleb();
      }
      return names;
    }
    at = end;
  }
  return [];
}

function parseSectionSizes(wasmPath) {
  const header = run("wasm-objdump", ["-h", wasmPath]);
  const sections = {};
  for (const line of header.split("\n")) {
    const m = line.match(/^\s*(\w[\w ]*?)\s+start=.*size=0x([0-9a-f]+)/i);
    if (m) sections[m[1].trim()] = parseInt(m[2], 16);
  }
  return sections;
}

function classifySymbol(name) {
  if (name.startsWith("custom section")) return "debug/metadata";
  if (name.startsWith("data segment") || name === "data[0]") return "data/rodata";
  if (name.startsWith("code[")) return "code (stripped index)";
  if (name.includes("compact_parquet_thrift")) return "dep: compact-parquet-thrift";
  if (name.includes("dlmalloc")) return "dep: dlmalloc";
  if (name.startsWith("wcol_engine::exec::")) return "engine: exec";
  if (name.startsWith("wcol_engine::formats::")) return "engine: formats (parquet)";
  if (name.startsWith("wcol_engine::ops::")) return "engine: ops";
  if (name.startsWith("wcol_engine::stream::")) return "engine: stream";
  if (name.startsWith("wcol_engine::spill::")) return "engine: spill";
  if (name.startsWith("wcol_engine::program::")) return "engine: program (wire)";
  if (name.startsWith("wcol_engine::")) return "engine: other";
  if (name.startsWith("wcs_") || name.startsWith("wcol_")) return "ffi export body";
  if (name.startsWith("<core::") || name.startsWith("core::")) return "rust core";
  return "other";
}

function twiggyBreakdown(wasmPath) {
  let text;
  try {
    text = run("twiggy", ["top", "-n", "400", wasmPath]);
  } catch {
    console.warn("twiggy not installed; skip symbol breakdown (cargo install twiggy)");
    return null;
  }
  const groups = {};
  let accounted = 0;
  for (const line of text.split("\n")) {
    const m = line.match(/^\s*(\d+)\s*[┊│]\s*[\d.]+%\s*[┊│]\s*(.+)$/);
    if (!m) continue;
    const bytes = Number(m[1]);
    const item = m[2].trim();
    if (item.startsWith("Σ")) continue;
    if (item.includes("... and")) continue;
    const bucket = classifySymbol(item);
    groups[bucket] = (groups[bucket] ?? 0) + bytes;
    accounted += bytes;
  }
  const sorted = Object.entries(groups).sort((a, b) => b[1] - a[1]);
  return { groups: sorted, accountedBytes: accounted };
}

function cargoTree() {
  return run("cargo", [
    "tree",
    "-p",
    "wcol-node-wasm",
    "--target",
    "wasm32-unknown-unknown",
    "-e",
    "normal",
  ], { cwd: rustDir });
}

function unexpectedDeps(tree) {
  const banned = ["datafusion", "sqlparser", "wcol-plan", "rusqlite"];
  return banned.filter((name) => tree.includes(name));
}

if (!skipBuild) {
  buildAnalyzeWasm();
}
if (!fs.existsSync(analyzeWasm)) {
  console.error(`missing ${analyzeWasm}; run without --skip-build`);
  process.exit(1);
}

const prodBin = fs.existsSync(distWasm) ? fs.readFileSync(distWasm) : null;
const analyzeBin = fs.readFileSync(analyzeWasm);

const tree = cargoTree();
const unexpected = unexpectedDeps(tree);

const prodSections = prodBin ? parseSectionSizes(distWasm) : null;
const analyzeSections = parseSectionSizes(analyzeWasm);
const exports = wasmExportNames(prodBin ?? analyzeBin);
const plannerOnNode = exports.filter((name) => PLANNER_EXPORTS.has(name));
const reduceOnNode = exports.filter((name) => name.startsWith("wcol_reduce_"));

const twiggy = twiggyBreakdown(analyzeWasm);

const report = {
  deployArtifact: prodBin
    ? {
        path: "cloudflare/dist/wcol_node.wasm",
        bytes: prodBin.length,
        gzipBytes: gzipSync(prodBin).length,
        sections: prodSections,
        exportCount: exports.length,
      }
    : null,
  analyzeBuild: {
    path: "rust/target/wasm32-unknown-unknown/release/wcol_node.analyze.wasm",
    bytes: analyzeBin.length,
    sections: analyzeSections,
    note: "release build with strip=none for twiggy names; deploy artifact is wasm-opt stripped",
  },
  dependencyTree: tree.split("\n"),
  unexpectedDependencies: unexpected,
  exports: {
    all: exports,
    plannerOnNode,
    reduceOnNode,
    spillFfi: exports.filter((n) => n.startsWith("wcs_spill_")),
    sessionFfi: exports.filter((n) => n.startsWith("wcs_") && !n.startsWith("wcs_spill_")),
  },
  symbolWeightByBucket: twiggy
    ? twiggy.groups.map(([bucket, bytes]) => ({
        bucket,
        bytes,
        pctOfAccounted: twiggy.accountedBytes ? ((100 * bytes) / twiggy.accountedBytes).toFixed(1) : "0",
      }))
    : null,
};

console.log(JSON.stringify(report, null, 2));

if (unexpected.length) {
  console.error(`unexpected dependencies in node wasm tree: ${unexpected.join(", ")}`);
  process.exit(1);
}
if (plannerOnNode.length) {
  console.error(`node wasm exports planner symbols: ${plannerOnNode.join(", ")}`);
  process.exit(1);
}
