#!/usr/bin/env node
/**
 * Verify cbor-x (TS worker) ↔ ciborium (Rust wcol-engine) interop.
 * Run: node scripts/test-cbor-interop.mjs
 */
import { encode, decode } from "cbor-x";
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const fixtureDir = join(root, "scripts/.cbor-fixtures");
const crate = join(root, "rust/wcol-engine/Cargo.toml");

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
  ["test", "--manifest-path", crate, "--test", "cbor_interop"],
  { cwd: root, encoding: "utf8" }
);
process.stdout.write(rustTest.stdout ?? "");
process.stderr.write(rustTest.stderr ?? "");
if (rustTest.status !== 0) process.exit(rustTest.status ?? 1);

console.log("\n=== 3. Rust encodes → cbor-x decodes ===\n");
spawnSync(
  "cargo",
  ["run", "--manifest-path", crate, "--example", "export_cbor", "--quiet"],
  { cwd: root, stdio: "inherit" }
);

for (const name of ["rust_init_out", "url_changed_out"]) {
  const path = join(fixtureDir, `${name}.cbor`);
  const obj = decode(readFileSync(path));
  console.log(`${name}: kind=${obj.kind} patches=${obj.patches?.length ?? "n/a"}`);
}

console.log("\n=== 4. TS applyPatches on Rust patches ===\n");
const { applyPatches } = await import("../../engine-shell/ts/src/patch.ts");
const { emptyViewModel } = await import("../demo/arch/empty-view-model.ts");
const out = JSON.parse(readFileSync(join(fixtureDir, "url_changed_out.json"), "utf8"));
const next = applyPatches(emptyViewModel(), out.patches);
if (next.urlInput !== "https://example.com/x") {
  console.error("applyPatches failed:", next.urlInput);
  process.exit(1);
}
console.log("urlInput patch applies: ok");

console.log("\n✓ cbor-x ↔ ciborium interop OK\n");
