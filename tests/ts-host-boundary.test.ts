import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function tsFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...tsFiles(full));
    else if (entry.name.endsWith(".ts") || entry.name.endsWith(".mts")) out.push(full);
  }
  return out;
}

function mjsFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...mjsFiles(full));
    else if (entry.name.endsWith(".mjs")) out.push(full);
  }
  return out;
}

describe("I-13 TS stream host boundary", () => {
  it("runtime modules do not import legacy chunk exec or distributed graph", () => {
    const runtimeDir = path.join(root, "src/runtime");
    const forbidden = [
      /legacy-ts-chunk-exec/,
      /legacy-ts-stream\/node/,
      /runtime\/exec\//,
      /runStreamNode/,
      /executeGraph/,
    ];
    const violations: string[] = [];
    for (const file of tsFiles(runtimeDir)) {
      const text = fs.readFileSync(file, "utf8");
      for (const pattern of forbidden) {
        if (pattern.test(text)) violations.push(`${path.relative(runtimeDir, file)}: ${pattern}`);
      }
    }
    assert.equal(violations.length, 0, violations.join("\n"));
  });

  it("live code outside archive/ does not import archive/", () => {
    const dirs = [
      path.join(root, "src"),
      path.join(root, "cloudflare"),
      path.join(root, "tests"),
      path.join(root, "scripts"),
    ];
    const violations: string[] = [];
    for (const dir of dirs) {
      for (const file of [...tsFiles(dir), ...mjsFiles(dir)]) {
        const lines = fs.readFileSync(file, "utf8").split("\n");
        for (const line of lines) {
          const trimmed = line.trim();
          if (trimmed.startsWith("//") || trimmed.startsWith("*")) continue;
          if (/from\s+['"][^'"]*\/archive\//.test(line) || /import\s*\(['"][^'"]*\/archive\//.test(line)) {
            violations.push(`${path.relative(root, file)}: ${trimmed}`);
          }
        }
      }
    }
    assert.equal(violations.length, 0, violations.join("\n"));
  });

  it("the host exports runWcolEngine", async () => {
    const mod = await import("../src/runtime/host/run-wcol-engine.ts");
    assert.equal(typeof mod.runWcolEngine, "function");
  });

  it("wasmMemoryOffset treats signed FFI pointers above 2GiB as unsigned", async () => {
    const { wasmMemoryOffset } = await import("../src/runtime/host/wasm.ts");
    assert.equal(wasmMemoryOffset(-2045419224), 2_249_548_072);
  });
});
