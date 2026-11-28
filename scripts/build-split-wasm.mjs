/**
 * Build the node or coordinator Wasm from `rust/` with the pinned toolchain.
 * `rust/.cargo/config.toml` sets panic=abort for wasm32.
 *
 *   node scripts/build-split-wasm.mjs node
 *   node scripts/build-split-wasm.mjs coordinator
 */
import { execFileSync } from "node:child_process";
import { gzipSync } from "node:zlib";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { NODE_ENTRY_EXPORT, PLANNER_EXPORTS, wasmExportNames } from "./node-wasm-exports.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rustDir = path.join(root, "rust");
const which = process.argv[2];

if (which !== "node" && which !== "coordinator") {
  console.error("usage: build-split-wasm.mjs node|coordinator");
  process.exit(2);
}

const pkg = which === "node" ? "wcol-node-wasm" : "wcol-coordinator-wasm";
const artifact = which === "node" ? "wcol_node.wasm" : "wcol_coordinator.wasm";

const cargo = [
  "build",
  "-p",
  pkg,
  "--target",
  "wasm32-unknown-unknown",
  "--release",
  "--offline",
];
// wasm-opt rewrites the release artifact in place.
const built = path.join(rustDir, "target/wasm32-unknown-unknown/release", artifact);
const optOut = built;
execFileSync("cargo", cargo, {
  cwd: rustDir,
  stdio: "inherit",
  env: {
    ...process.env,
    RUSTFLAGS: '-C panic=abort -C target-feature=+simd128',
  },
});
const optTemp = `${optOut}.opt`;
execFileSync(
  "wasm-opt",
  [
    "-O3",
    "--strip-debug",
    "--strip-producers",
    "--enable-simd",
    "--enable-bulk-memory",
    "--enable-multivalue",
    "--enable-nontrapping-float-to-int",
    "--enable-sign-ext",
    built,
    "-o",
    optTemp,
  ],
  { stdio: "inherit" },
);
fs.renameSync(optTemp, optOut);

const bytes = fs.readFileSync(optOut);
const gzipped = gzipSync(bytes).byteLength;
const exports = wasmExportNames(bytes);
console.log(
  JSON.stringify({
    artifact,
    bytes: bytes.byteLength,
    gzipped,
  }),
);

if (which === "node") {
  // Raised from 1,000,000 with run-time scan claims and the compact fragment wire, then
  // from 1,100,000 for sparse partition dictionaries, the compact spill codec and
  // reducer-side distinct ranking, then from 1,120,000 for the scan-side combiner (compact
  // partial columns, reducers merging partial states), then from 1,160,000 for the
  // partitioned Parquet sink (`partitionBy`), then from 1,190,000 for dlmalloc's in-place
  // realloc (about 96 KB), which keeps a growing buffer from holding two copies at once.
  const budget = 1_250_000;
  // Resident K-means load/round exports add a small amount of node runtime code; raised
  // from 416,000 with the combiner and from 432,000 with the partitioned sink.
  const gzipBudget = 448_000;
  const banned = exports.filter((name) => PLANNER_EXPORTS.includes(name));
  if (banned.length) {
    console.error(`node wasm exports planner symbols: ${banned.join(", ")}`);
    process.exit(1);
  }
  if (!exports.includes(NODE_ENTRY_EXPORT)) {
    console.error(`node wasm is missing ${NODE_ENTRY_EXPORT}`);
    process.exit(1);
  }
  if (bytes.byteLength > budget || gzipped > gzipBudget) {
    console.error(
      `node wasm over budget: ${bytes.byteLength} bytes, ${gzipped} gzipped (limits ${budget} / ${gzipBudget})`,
    );
    process.exit(1);
  }
}
