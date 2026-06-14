#!/usr/bin/env node
import * as esbuild from "esbuild";
import { cpSync, mkdirSync, existsSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(appRoot, "../..");
const out = join(appRoot, "dist");
const demo = join(appRoot, "demo");

spawnSync(process.execPath, [join(repoRoot, "scripts/build-browser-runtime.mjs")], {
  stdio: "inherit",
  cwd: repoRoot,
  env: { ...process.env, WCOL_BROWSER_OUT: join(out, "browser") },
});

spawnSync(process.execPath, [join(appRoot, "scripts/prepare-demo-datasets.mjs")], {
  stdio: "inherit",
});

const preactRoot = join(repoRoot, "node_modules/preact");
const browserOut = join(out, "browser");
mkdirSync(browserOut, { recursive: true });

writeFileSync(join(browserOut, "wcol-query.js"), "export * from './main.js';\n");

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
  outfile: join(browserOut, "app.js"),
  plugins: [wcolQueryPlugin],
});

await esbuild.build({
  ...shared,
  entryPoints: [join(demo, "worker/app-worker.ts")],
  outfile: join(browserOut, "demo-worker.js"),
  plugins: [wcolQueryPlugin],
});

const pkg = join(appRoot, "pkg");
for (const file of ["wcol_engine.js", "wcol_engine_bg.wasm"]) {
  const src = join(pkg, file);
  if (existsSync(src)) {
    cpSync(src, join(browserOut, file));
  }
}

for (const file of ["index.html", "styles.css"]) {
  cpSync(join(demo, file), join(browserOut, file));
}

const dataSrc = join(demo, "data");
const dataDst = join(browserOut, "data");
if (existsSync(dataSrc)) {
  cpSync(dataSrc, dataDst, { recursive: true });
}

console.log(`Explorer built to ${browserOut}/`);
