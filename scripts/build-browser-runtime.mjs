#!/usr/bin/env node
/** Bundle columnar runtime (main.js + worker.js) into dist/browser — no demo UI. */
import { build } from "esbuild";
import { cpSync, existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(scriptDir, "..");
const outDir = resolve(rootDir, "dist", "browser");
const wasmDir = resolve(outDir, "wasm");
const wasmSrc = resolve(
  rootDir,
  "rust",
  "wcol-wasm",
  "pkg",
  "wasm",
  "wcol_wasm.simd.wasm",
);

mkdirSync(wasmDir, { recursive: true });

const browserDefine = {
  process: "undefined",
  __WCOL_BROWSER_BUILD__: "true",
  __WCOL_BROWSER_WASM_URL__: '"./wasm/wcol_wasm.simd.wasm"',
  __WCOL_BROWSER_WORKER_URL__: '"./worker.js"',
};

const shared = {
  absWorkingDir: rootDir,
  outdir: outDir,
  format: "esm",
  platform: "browser",
  target: "es2022",
  bundle: true,
  splitting: false,
  minify: true,
  sourcemap: false,
  define: browserDefine,
};

await build({
  ...shared,
  entryPoints: {
    main: "src/browser.ts",
    worker: "src/runtime/workers/browser.ts",
  },
});

if (!existsSync(wasmSrc)) {
  console.error(`Missing ${wasmSrc} — run npm run build:wasm first`);
  process.exit(1);
}

cpSync(wasmSrc, resolve(wasmDir, "wcol_wasm.simd.wasm"));
console.log(`Runtime bundle written to ${outDir} (main.js + worker.js + wasm)`);
