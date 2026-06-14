#!/usr/bin/env node
/**
 * Verify cbor-x (TS worker) ↔ ciborium (Rust wcol-engine) interop.
 */
import { encode, decode } from "cbor-x";
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(appRoot, "../..");
const fixtureDir = join(appRoot, "scripts/.cbor-fixtures");
const crate = join(appRoot, "engine/Cargo.toml");

mkdirSync(fixtureDir, { recursive: true });

const samples = {
  init: { kind: "init" },
  url_changed: {
    kind: "event",
    event: { type: "URL_CHANGED", url: "https://example.com/x" },
  },
  data_drawer_set: {
    kind: "event",
    event: { type: "DATA_DRAWER_SET", open: true },
  },
};

const hex = (bytes) => Buffer.from(bytes).toString("hex");

console.log("=== 1. cbor-x fixtures ===\n");
for (const [name, obj] of Object.entries(samples)) {
  const bytes = encode(obj);
  writeFileSync(join(fixtureDir, `${name}.cbor`), Buffer.from(bytes));
  console.log(`${name}: ${hex(bytes)}`);
}

console.log("\n=== 2. Rust decodes cbor-x (integration tests) ===\n");
const rustTest = spawnSync(
  "cargo",
  ["test", "--manifest-path", crate, "--features", "typegen", "cbor_interop", "--", "--nocapture"],
  { stdio: "inherit", cwd: repoRoot },
);
if (rustTest.status !== 0) process.exit(rustTest.status ?? 1);

console.log("\n=== 3. Round-trip patch apply (TS) ===\n");
const { applyPatches } = await import("../../../engine-shell/ts/src/patch.ts");
const { emptyViewModel } = await import("../demo/arch/empty-view-model.ts");
const outBytes = readFileSync(join(fixtureDir, "url_changed_out.cbor"));
const output = decode(outBytes);
if (!output.patches?.length) {
  throw new Error("expected patches in url_changed_out fixture");
}
const vm = applyPatches(emptyViewModel(), output.patches);
console.log("viewModel.explore.shareableUrl:", vm.explore?.shareableUrl ?? "(n/a)");
console.log("\nOK — CBOR interop");
