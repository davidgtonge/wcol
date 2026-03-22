#!/usr/bin/env node
import * as esbuild from "esbuild";
import { cpSync, mkdirSync, existsSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { spawnSync } from "node:child_process";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const out = join(root, "dist/browser");
const demo = join(root, "demo");

spawnSync(process.execPath, [join(root, "scripts/build-browser-runtime.mjs")], {
  stdio: "inherit",
});

spawnSync(process.execPath, [join(root, "scripts/prepare-demo-datasets.mjs")], {
  stdio: "inherit",
});
const preactRoot = join(root, "node_modules/preact");

mkdirSync(out, { recursive: true });

// Shim so demo code can import Wasm runtime without bundling it.
writeFileSync(join(out, "wcol-query.js"), "export * from './main.js';\n");

const wcolQueryPlugin = {
  name: "external-wcol-query",
  setup(build) {
    build.onResolve({ filter: /wcol-query\.ts$/ }, () => ({
      path: "./wcol-query.js",
      external: true,
    }));
    build.onResolve({ filter: /pkg\/wcol_engine\.js$/ }, () => ({
      path: "./wcol_engine.js",
      external: true,
    }));
  },
};

// One Preact instance for app + @dtonge/engine-shell (duplicate copies break hooks __H).
const preactAlias = {
  preact: join(preactRoot, "dist/preact.module.js"),
  "preact/hooks": join(preactRoot, "hooks/dist/hooks.module.js"),
  "preact/jsx-runtime": join(preactRoot, "jsx-runtime/dist/jsxRuntime.module.js"),
};

const shared = {
  bundle: true,
  format: "esm",
  platform: "browser",
  target: ["es2020"],
  logLevel: "info",
  jsx: "automatic",
  jsxImportSource: "preact",
  alias: preactAlias,
};

await esbuild.build({
  ...shared,
  entryPoints: [join(demo, "app.tsx")],
  outfile: join(out, "app.js"),
  plugins: [wcolQueryPlugin],
});

await esbuild.build({
  ...shared,
  entryPoints: [join(demo, "worker/app-worker.ts")],
  outfile: join(out, "demo-worker.js"),
  plugins: [wcolQueryPlugin],
});

const pkg = join(root, "pkg");
for (const file of ["wcol_engine.js", "wcol_engine_bg.wasm"]) {
  const src = join(pkg, file);
  if (existsSync(src)) {
    cpSync(src, join(out, file));
  }
}

for (const file of ["index.html", "styles.css"]) {
  cpSync(join(demo, file), join(out, file));
}

const dataSrc = join(demo, "data");
const dataDst = join(out, "data");
if (existsSync(dataSrc)) {
  cpSync(dataSrc, dataDst, { recursive: true });
}

console.log("Demo built to dist/browser/ (app.js + demo-worker.js)");
