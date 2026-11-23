/**
 * Warm workerd POST /jobs vs DuckDB on the same hits subset.
 * One coordinator isolate is reused so DataFusion wasm stays loaded.
 */
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { Miniflare } from "miniflare";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const distDir = path.join(ROOT, "cloudflare", "dist");
const outfile = path.join(distDir, "worker.js");
const parquetPath = path.join(ROOT, "data/hits_subset_500k.parquet");
const runs = 3;

const QUERIES: Array<[string, string]> = [
  ["Q0", "SELECT COUNT(*) FROM hits"],
  ["Q1", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"],
  ["Q2", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"],
  ["Q3", "SELECT AVG(UserID) FROM hits"],
  ["Q6", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"],
  ["Q7", "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"],
  ["Q9", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"],
  ["Q15", "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"],
];

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!;
}

async function buildWorker(): Promise<void> {
  fs.mkdirSync(distDir, { recursive: true });
  await esbuild.build({
    entryPoints: [path.join(ROOT, "cloudflare", "src", "worker.ts")],
    outfile,
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    logLevel: "warning",
    loader: { ".wcol": "binary" },
    plugins: [
      {
        name: "durable-object-stubs",
        setup(build) {
          build.onResolve({ filter: /^cloudflare:workers$/ }, (args) => ({ path: args.path, external: true }));
          build.onResolve({ filter: /\.wasm$/ }, (args) => ({ path: `./${path.basename(args.path)}`, external: true }));
          build.onResolve({ filter: /parquet-job\.ts$/ }, () => ({ path: "parquet-job-stub", namespace: "wcol-stub" }));
          build.onResolve({ filter: /parquet-ingest-plan\.ts$/ }, () => ({ path: "parquet-ingest-plan-stub", namespace: "wcol-stub" }));
          build.onResolve({ filter: /legacy-ts-stream\/node\.ts$/ }, () => ({ path: "legacy-node-stub", namespace: "wcol-stub" }));
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
      },
    ],
  });
  for (const wasm of ["wcol_wasm.simd.wasm", "wcol.wasm", "wcol_parquet.wasm"]) {
    const src = path.join(ROOT, `rust/target/wasm32-unknown-unknown/release/${wasm === "wcol_wasm.simd.wasm" ? "wcol.wasm" : wasm}`);
    fs.copyFileSync(src, path.join(distDir, wasm));
  }
  fs.copyFileSync(path.join(ROOT, "rust/wcol-sql-wasm/pkg-web/wcol_sql_wasm_bg.wasm"), path.join(distDir, "wcol_sql_wasm_bg.wasm"));
  const bundled = fs.readFileSync(outfile, "utf8").replaceAll("import(", "unavailableImport(");
  fs.writeFileSync(outfile, `${bundled}\nfunction unavailableImport(){ throw new Error("no dynamic import in DO bundle"); }\n`);
}

function serveParquet(file: Buffer): Promise<{ url: string; close: () => Promise<void> }> {
    const server = createServer((req, res) => {
    const range = req.headers.range;
    console.log(`range ${range ?? "full"}`);
    if (!range) {
      res.writeHead(200, { "content-type": "application/octet-stream", "content-length": file.length, "accept-ranges": "bytes", etag: '"parquet-fixture"' });
      res.end(file);
      return;
    }
    const suffix = /bytes=-(\d+)/.exec(range);
    const match = suffix ? null : /bytes=(\d+)-(\d*)/.exec(range);
    if (!suffix && !match) {
      res.writeHead(416);
      res.end();
      return;
    }
    const start = suffix ? Math.max(0, file.length - Number(suffix[1])) : Number(match![1]);
    const end = suffix ? file.length - 1 : match![2] ? Number(match![2]) : file.length - 1;
    const slice = file.subarray(start, end + 1);
    res.writeHead(206, {
      "content-type": "application/octet-stream",
      "content-range": `bytes ${start}-${end}/${file.length}`,
      "content-length": slice.length,
      "accept-ranges": "bytes",
      etag: '"parquet-fixture"',
    });
    res.end(slice);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}/hits.parquet`,
        close: () => new Promise((done) => server.close(() => done())),
      });
    });
  });
}

type Duck = {
  query: (sql: string) => Promise<{ ms: number; error?: string }>;
  close: () => void;
};

function openDuck(file: string): Duck {
  const child = spawn("python3", ["/tmp/duck_pty.py", file], { stdio: ["pipe", "pipe", "pipe"] });
  let out = "";
  const waiters: Array<(row: { ms: number; error?: string }) => void> = [];
  child.stdout.on("data", (chunk: Buffer) => {
    out += chunk.toString();
    let nl = out.indexOf("\n");
    while (nl >= 0) {
      const line = out.slice(0, nl);
      out = out.slice(nl + 1);
      const row = JSON.parse(line) as { ms: number; error?: string | null };
      waiters.shift()?.({ ms: row.ms, error: row.error ?? undefined });
      nl = out.indexOf("\n");
    }
  });
  let stderr = "";
  child.stderr.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  return {
    query: (sql: string) =>
      new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`duckdb timeout ${sql.slice(0, 80)} :: ${stderr.slice(-400)}`)),
          120000,
        );
        waiters.push((row) => {
          clearTimeout(timer);
          resolve(row);
        });
        child.stdin.write(`${sql}\n`);
      }),
    close: () => child.kill(),
  };
}

async function wcolQuery(mf: Miniflare, url: string, sql: string): Promise<{ ms: number; status: number; nodeCount?: number; shardCount?: number; error?: string; sum?: number }> {
  const started = performance.now();
  const response = await mf.dispatchFetch("http://wcol.local/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      coordinatorKey: "speed-bench",
      sources: { hits: { key: url } },
      sql,
      output: { format: "json" },
    }),
  });
  const text = await response.text();
  const ms = performance.now() - started;
  let body: { nodeCount?: number; shardCount?: number; error?: string; sum?: number } = {};
  try {
    body = JSON.parse(text);
  } catch {
    body = { error: text.slice(0, 300) };
  }
  return {
    ms,
    status: response.status,
    nodeCount: body.nodeCount,
    shardCount: body.shardCount,
    sum: body.sum,
    error: response.ok ? undefined : (body.error ?? text.slice(0, 300)),
  };
}

async function main(): Promise<void> {
await buildWorker();
const file = fs.readFileSync(parquetPath);
const served = await serveParquet(file);
const mf = new Miniflare({
  modulesRoot: distDir,
  modules: [
    { type: "ESModule", path: outfile },
    ...["wcol_wasm.simd.wasm", "wcol_parquet.wasm", "wcol.wasm", "wcol_sql_wasm_bg.wasm"].map((name) => ({
      type: "CompiledWasm" as const,
      path: path.join(distDir, name),
    })),
  ],
  compatibilityDate: "2026-01-01",
  durableObjects: {
    WCOL_NODE: { className: "WcolNode", useSQLite: true },
    WCOL_COORDINATOR: { className: "WcolCoordinator", useSQLite: true },
  },
  r2Buckets: ["OUTPUT", "ANALYTICS"],
});

const rows: Array<Record<string, unknown>> = [];
try {
  console.log("start cold");
  const cold = await Promise.race([
    wcolQuery(mf, served.url, QUERIES[2]![1]),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("wcol timeout cold")), 45000)),
  ]);
  console.log("cold done", cold.ms, cold.error ?? "");
  rows.push({ id: "cold Q2", wcolMs: Math.round(cold.ms), duckMs: null, nodes: cold.nodeCount ?? null, error: cold.error ?? null });
  for (const [id, sql] of QUERIES) {
    const wcol: number[] = [];
    let error: string | undefined;
    let nodes: number | undefined;
    for (let i = 0; i < runs; i++) {
      console.log(`start ${id} run ${i}`);
      const result = await Promise.race([
        wcolQuery(mf, served.url, sql),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`wcol timeout ${id}`)), 45000)),
      ]);
      nodes = result.nodeCount ?? result.shardCount;
      if (result.error) {
        error = result.error;
        break;
      }
      wcol.push(result.ms);
      console.log(`done ${id} run ${i} ${Math.round(result.ms)}ms nodes=${result.nodeCount}`);
    }
    rows.push({
      id,
      wcolMs: wcol.length ? Math.round(median(wcol)) : null,
      duckMs: null,
      nodes: nodes ?? null,
      error: error ?? null,
    });
    console.log(JSON.stringify(rows[rows.length - 1]));
  }
} finally {
  console.log(JSON.stringify({ rows }, null, 2));
  await mf.dispose();
  await served.close();
}
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
