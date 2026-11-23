/**
 * Bundle the coordinator and node Workers into `cloudflare/dist` (two isolates, two Wasm
 * binaries) and check that each imports only its own Wasm. Used by the workerd harnesses
 * and by `deploy-worker.ts`.
 *
 * Node profile:
 *   prod (default) — graph tasks only (`node.ts`, no WCS bench routes)
 *   bench — includes `wcol-node-bench.ts` for transport benchmarks
 * Coordinator production fetch is jobs only (`worker.ts`). Bench proxies live on `worker.bench.ts`.
 */

import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import { clickbenchQueries } from "./clickbench-catalog.ts";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const distDir = path.join(ROOT, "cloudflare", "dist");
export const nodeOut = path.join(distDir, "node-worker.js");
export const coordinatorOut = path.join(distDir, "coordinator-worker.js");

export type NodeWorkerProfile = "prod" | "bench";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export function bundlePlugin(): esbuild.Plugin {
  return {
    name: "durable-object-stubs",
    setup(build) {
      build.onResolve({ filter: /^cloudflare:workers$/ }, (args) => ({
        path: args.path,
        external: true,
      }));
      build.onResolve({ filter: /\.wasm$/ }, (args) => ({
        path: `./${path.basename(args.path)}`,
        external: true,
      }));
      build.onResolve({ filter: /parquet-job\.ts$/ }, () => ({
        path: "parquet-job-stub",
        namespace: "wcol-stub",
      }));
      build.onResolve({ filter: /parquet-ingest-plan\.ts$/ }, () => ({
        path: "parquet-ingest-plan-stub",
        namespace: "wcol-stub",
      }));
      build.onResolve({ filter: /legacy-ts-stream\/node\.ts$/ }, () => ({
        path: "legacy-node-stub",
        namespace: "wcol-stub",
      }));
      build.onLoad({ filter: /.*/, namespace: "wcol-stub" }, (args) => ({
        contents:
          args.path === "parquet-ingest-plan-stub"
            ? `export function encodeParquetIngestPlan() { throw new Error("stub"); }\n`
            : args.path === "legacy-node-stub"
              ? `export async function runStreamNode() { throw new Error("legacy stream node is not in this bundle"); }\n`
              : `export function runParquetScan() { throw new Error("stub"); }\nexport function planParquetJob() { throw new Error("stub"); }\nexport function planParquetStream() { throw new Error("stub"); }\n`,
        loader: "js",
      }));
    },
  };
}

function nodeEntry(_profile: NodeWorkerProfile): string {
  return path.join(ROOT, "cloudflare/src/node/entry.ts");
}

async function bundle(entry: string, outfile: string): Promise<void> {
  await esbuild.build({
    entryPoints: [entry],
    outfile,
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    logLevel: "warning",
    loader: { ".wcol": "binary", ".html": "text" },
    plugins: [bundlePlugin()],
  });
  const bundled = fs.readFileSync(outfile, "utf8").replaceAll("import(", "unavailableImport(");
  fs.writeFileSync(
    outfile,
    `${bundled}\nfunction unavailableImport(){ throw new Error("no dynamic import in DO bundle"); }\n`,
  );
}

export type NodeWorkerBundleReport = {
  profile: NodeWorkerProfile;
  jsBytes: number;
  jsGzipBytes: number;
  wasmBytes: number;
  wasmGzipBytes: number;
  totalBytes: number;
  topInputs: { file: string; bytesInOutput: number }[];
};

/** Esbuild metafile: which source files landed in the node Worker JS bundle. */
export async function reportNodeWorkerBundle(profile: NodeWorkerProfile): Promise<NodeWorkerBundleReport> {
  const result = await esbuild.build({
    entryPoints: [nodeEntry(profile)],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    metafile: true,
    logLevel: "silent",
    loader: { ".wcol": "binary" },
    plugins: [bundlePlugin()],
  });
  const output = Object.values(result.metafile!.outputs)[0]!;
  const topInputs = Object.entries(output.inputs)
    .map(([file, meta]) => ({
      file: file.replace(`${ROOT}/`, ""),
      bytesInOutput: meta.bytesInOutput,
    }))
    .filter((row) => row.bytesInOutput > 0)
    .sort((a, b) => b.bytesInOutput - a.bytesInOutput);
  const jsBytes = output.bytes;
  const wasmPath = path.join(distDir, "wcol_node.wasm");
  const wasmBytes = fs.existsSync(wasmPath) ? fs.statSync(wasmPath).size : 0;
  return {
    profile,
    jsBytes,
    jsGzipBytes: gzipSync(Buffer.from(result.outputFiles[0]!.contents)).byteLength,
    wasmBytes,
    wasmGzipBytes: wasmBytes ? gzipSync(fs.readFileSync(wasmPath)).byteLength : 0,
    totalBytes: jsBytes + wasmBytes,
    topInputs: topInputs.slice(0, 30),
  };
}

export const consoleOut = path.join(distDir, "console.html");

/** The console page (`cloudflare/console`): its script bundled into the HTML, one file. */
async function buildConsole(): Promise<void> {
  const built = await esbuild.build({
    entryPoints: [path.join(ROOT, "cloudflare/console/app.ts")],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    minify: true,
    logLevel: "warning",
    define: { __CLICKBENCH__: JSON.stringify(clickbenchQueries(ROOT)) },
  });
  const script = built.outputFiles[0]!.text.replaceAll("</script", "<\\/script");
  const page = fs.readFileSync(path.join(ROOT, "cloudflare/console/index.html"), "utf8");
  assert(page.includes("<!-- APP -->"), "console page has no <!-- APP --> marker");
  fs.writeFileSync(consoleOut, page.replace("<!-- APP -->", () => `<script type="module">${script}</script>`));
}

export async function buildWorkers(nodeProfile: NodeWorkerProfile = "prod"): Promise<void> {
  fs.mkdirSync(distDir, { recursive: true });
  await buildConsole();
  await bundle(nodeEntry(nodeProfile), nodeOut);
  await bundle(path.join(ROOT, "cloudflare/src/coordinator/entry.ts"), coordinatorOut);
  fs.copyFileSync(
    path.join(ROOT, "rust/target/wasm32-unknown-unknown/release/wcol_node.wasm"),
    path.join(distDir, "wcol_node.wasm"),
  );
  fs.copyFileSync(
    path.join(ROOT, "rust/target/wasm32-unknown-unknown/release/wcol_coordinator.wasm"),
    path.join(distDir, "wcol_coordinator.wasm"),
  );
  const nodeJs = fs.readFileSync(nodeOut, "utf8");
  const coordinatorJs = fs.readFileSync(coordinatorOut, "utf8");
  assert(nodeJs.includes("wcol_node.wasm"), "node worker does not import node wasm");
  assert(!nodeJs.includes("wcol_coordinator.wasm"), "node worker imports coordinator wasm");
  assert(!nodeJs.includes("wcol_wasm.simd.wasm") && !nodeJs.includes("wcol_parquet.wasm"), "node worker imports a legacy wasm");
  assert(coordinatorJs.includes("wcol_coordinator.wasm"), "coordinator worker does not import coordinator wasm");
  assert(
    !/from\s+["']\.\/wcol_node\.wasm["']/.test(coordinatorJs),
    "coordinator worker imports node wasm",
  );
  if (nodeProfile === "prod") {
    assert(!nodeJs.includes("/wcs/bench/source"), "prod node worker still contains WCS bench routes");
    assert(!coordinatorJs.includes("/wcs/bench"), "prod coordinator worker still contains WCS bench routes");
  } else {
    assert(nodeJs.includes("/wcs/bench/source"), "bench node worker is missing WCS bench routes");
  }
  const nodeWasm = fs.readFileSync(path.join(distDir, "wcol_node.wasm"));
  const nodeModule = new WebAssembly.Module(nodeWasm);
  const nodeExports = new Set(WebAssembly.Module.exports(nodeModule).map((entry) => entry.name));
  for (const sym of ["wcs_sp_open_graph", "wcs_sp_step", "wcs_sp_spill_op", "wcs_sp_spill_complete"]) {
    assert(nodeExports.has(sym), `node wasm missing export ${sym}`);
  }
  const nodeScriptBytes = fs.statSync(nodeOut).size;
  const coordinatorScriptBytes = fs.statSync(coordinatorOut).size;
  const nodeWorkerBytes = nodeScriptBytes + nodeWasm.byteLength;
  const coordinatorWorkerBytes =
    coordinatorScriptBytes + fs.statSync(path.join(distDir, "wcol_coordinator.wasm")).size;
  const bundleReport = await reportNodeWorkerBundle(nodeProfile);
  console.log(
    JSON.stringify(
      {
        nodeProfile,
        nodeWorkerBytes,
        nodeScriptBytes,
        nodeWasmBytes: nodeWasm.byteLength,
        coordinatorWorkerBytes,
        coordinatorScriptBytes,
        nodeBundleTopInputs: bundleReport.topInputs.slice(0, 12),
      },
      null,
      2,
    ),
  );
}

const isMain = Boolean(process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]));
if (isMain) {
  const profile: NodeWorkerProfile = process.argv.includes("--bench") ? "bench" : "prod";
  await buildWorkers(profile);
}
