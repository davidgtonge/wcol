import { build } from "esbuild";
import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(scriptDir, "..");
const outDir = resolve(rootDir, "dist", "browser");
const wasmDir = resolve(outDir, "wasm");
const wasmSrc = resolve(rootDir, "rust", "wcol-wasm", "pkg", "wasm", "wcol_wasm.simd.wasm");

const browserDefine = {
  process: "undefined",
  __WCOL_BROWSER_BUILD__: "true",
  __WCOL_BROWSER_WASM_URL__: "\"./wasm/wcol_wasm.simd.wasm\"",
  __WCOL_BROWSER_WORKER_URL__: "\"./worker.js\""
};

spawnSync("node", ["scripts/prepare-demo-crates.mjs"], {
  cwd: rootDir,
  stdio: "inherit"
});

rmSync(outDir, { recursive: true, force: true });
mkdirSync(wasmDir, { recursive: true });
const dataOut = resolve(outDir, "data");
mkdirSync(dataOut, { recursive: true });

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

await build({
  ...shared,
  entryPoints: { app: "demo/app.tsx" },
  jsx: "automatic",
  jsxImportSource: "preact",
  external: ["./main.js"],
});

cpSync(wasmSrc, resolve(wasmDir, "wcol_wasm.simd.wasm"));

const demoDir = resolve(rootDir, "demo");
for (const name of ["index.html", "styles.css"]) {
  cpSync(resolve(demoDir, name), resolve(outDir, name));
}

const demoDataDir = resolve(demoDir, "data");
if (existsSync(demoDataDir)) {
  for (const name of readdirSync(demoDataDir)) {
    if (name.endsWith(".wcol")) {
      cpSync(resolve(demoDataDir, name), resolve(dataOut, name));
    }
  }
}

console.log(`Browser bundle + demo written to ${outDir}`);
