/**
 * Bundle the coordinator and node Workers into `cloudflare/dist` (two isolates, two Wasm
 * binaries) and check that each imports only its own Wasm. Used by `deploy-worker.ts` and
 * `cf-clickbench-miniflare.ts`.
 */

import esbuild from "esbuild";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { clickbenchQueries } from "./clickbench-catalog.ts";
import { assert, ROOT } from "./harness.ts";

export { ROOT };
export const distDir = path.join(ROOT, "cloudflare", "dist");
export const nodeOut = path.join(distDir, "node-worker.js");
export const coordinatorOut = path.join(distDir, "coordinator-worker.js");

/** `cloudflare:workers` is the runtime's; each Wasm is a module rule beside the bundle. */
function bundlePlugin(): esbuild.Plugin {
  return {
    name: "workers-externals",
    setup(build) {
      build.onResolve({ filter: /^cloudflare:workers$/ }, (args) => ({
        path: args.path,
        external: true,
      }));
      build.onResolve({ filter: /\.wasm$/ }, (args) => ({
        path: `./${path.basename(args.path)}`,
        external: true,
      }));
    },
  };
}

/** Bundle one Worker; returns which source files landed in it, largest first. */
async function bundle(entry: string, outfile: string, define: Record<string, string> = {}): Promise<{ file: string; bytes: number }[]> {
  const result = await esbuild.build({
    entryPoints: [entry],
    outfile,
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    logLevel: "warning",
    loader: { ".wcol": "binary", ".html": "text" },
    define,
    metafile: true,
    plugins: [bundlePlugin()],
  });
  const bundled = fs.readFileSync(outfile, "utf8").replaceAll("import(", "unavailableImport(");
  fs.writeFileSync(
    outfile,
    `${bundled}\nfunction unavailableImport(){ throw new Error("no dynamic import in DO bundle"); }\n`,
  );
  const output = Object.values(result.metafile.outputs)[0];
  return Object.entries(output?.inputs ?? {})
    .map(([file, meta]) => ({ file: file.replace(`${ROOT}/`, ""), bytes: meta.bytesInOutput }))
    .filter((row) => row.bytes > 0)
    .sort((a, b) => b.bytes - a.bytes);
}

/** What the console's query list shows for each recording: how long it ran, how many tasks. */
function recordedSummaries(): { name: string; ms: number; tasks: number }[] {
  return Object.entries(recordings()).map(([name, text]) => {
    const { snapshot } = JSON.parse(text) as { snapshot: { startedAt: number; now: number; graph: { tasks: unknown[] } } };
    return { name, ms: snapshot.now - snapshot.startedAt, tasks: snapshot.graph.tasks.length };
  });
}

export const consoleOut = path.join(distDir, "console.html");

/** Recorded console runs (`scripts/record-console-runs.ts`), by query name, as JSON text. */
function recordings(): Record<string, string> {
  const dir = path.join(ROOT, "cloudflare/console/recordings");
  if (!fs.existsSync(dir)) return {};
  return Object.fromEntries(
    fs
      .readdirSync(dir)
      .filter((file) => file.endsWith(".json"))
      .map((file) => [path.basename(file, ".json"), fs.readFileSync(path.join(dir, file), "utf8")]),
  );
}

/** The console page (`cloudflare/console`): its script bundled into the HTML, one file. */
async function buildConsole(): Promise<string> {
  const built = await esbuild.build({
    entryPoints: [path.join(ROOT, "cloudflare/console/app.ts")],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    minify: true,
    logLevel: "warning",
    define: {
      __CLICKBENCH__: JSON.stringify(clickbenchQueries(ROOT)),
      __RECORDED_QUERIES__: JSON.stringify(recordedSummaries()),
    },
  });
  const script = built.outputFiles[0]!.text.replaceAll("</script", "<\\/script");
  const page = fs.readFileSync(path.join(ROOT, "cloudflare/console/index.html"), "utf8");
  assert(page.includes("<!-- APP -->"), "console page has no <!-- APP --> marker");
  fs.writeFileSync(consoleOut, page.replace("<!-- APP -->", () => `<script type="module">${script}</script>`));
  return consolePolicy(script);
}

/**
 * The console's Content-Security-Policy: its one inline script by hash, inline styles (the
 * panels set widths and colours inline), and requests to this origin only. No framing.
 */
function consolePolicy(script: string): string {
  const hash = createHash("sha256").update(script).digest("base64");
  return [
    "default-src 'none'",
    `script-src 'sha256-${hash}'`,
    "style-src 'unsafe-inline'",
    "img-src 'self' data:",
    "connect-src 'self'",
    "base-uri 'none'",
    "form-action 'none'",
    "frame-ancestors 'none'",
  ].join("; ");
}

export async function buildWorkers(): Promise<void> {
  fs.mkdirSync(distDir, { recursive: true });
  const csp = await buildConsole();
  const nodeInputs = await bundle(path.join(ROOT, "cloudflare/src/node/entry.ts"), nodeOut);
  await bundle(path.join(ROOT, "cloudflare/src/coordinator/entry.ts"), coordinatorOut, {
    __RECORDINGS__: JSON.stringify(recordings()),
    __CONSOLE_CSP__: JSON.stringify(csp),
  });
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
  assert(coordinatorJs.includes("wcol_coordinator.wasm"), "coordinator worker does not import coordinator wasm");
  assert(
    !/from\s+["']\.\/wcol_node\.wasm["']/.test(coordinatorJs),
    "coordinator worker imports node wasm",
  );
  const nodeWasm = fs.readFileSync(path.join(distDir, "wcol_node.wasm"));
  const nodeExports = new Set(WebAssembly.Module.exports(new WebAssembly.Module(nodeWasm)).map((entry) => entry.name));
  for (const sym of ["wcs_sp_open_graph", "wcs_sp_step", "wcs_sp_spill_op", "wcs_sp_spill_complete"]) {
    assert(nodeExports.has(sym), `node wasm missing export ${sym}`);
  }
  const nodeScriptBytes = fs.statSync(nodeOut).size;
  const coordinatorScriptBytes = fs.statSync(coordinatorOut).size;
  console.log(
    JSON.stringify(
      {
        nodeWorkerBytes: nodeScriptBytes + nodeWasm.byteLength,
        nodeScriptBytes,
        nodeWasmBytes: nodeWasm.byteLength,
        coordinatorWorkerBytes: coordinatorScriptBytes + fs.statSync(path.join(distDir, "wcol_coordinator.wasm")).size,
        coordinatorScriptBytes,
        nodeBundleTopInputs: nodeInputs.slice(0, 12),
      },
      null,
      2,
    ),
  );
}

const isMain = Boolean(process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]));
if (isMain) await buildWorkers();
