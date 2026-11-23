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
  // partial columns, reducers merging partial states).
  const budget = 1_160_000;
  // Resident K-means load/round exports add a small amount of node runtime code; raised
  // from 416,000 with the combiner.
  const gzipBudget = 432_000;
  const banned = exports.filter(
    (name) =>
      name === "wcol_plan_query" ||
      name === "wcol_plan_output_ptr" ||
      name === "wcol_columns_for_sql",
  );
  if (banned.length) {
    console.error(`node wasm exports planner symbols: ${banned.join(", ")}`);
    process.exit(1);
  }
  if (!exports.includes("wcs_sp_open_graph")) {
    console.error("node wasm is missing wcs_sp_open_graph");
    process.exit(1);
  }
  if (bytes.byteLength > budget || gzipped > gzipBudget) {
    console.error(
      `node wasm over budget: ${bytes.byteLength} bytes, ${gzipped} gzipped (limits ${budget} / ${gzipBudget})`,
    );
    process.exit(1);
  }
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
