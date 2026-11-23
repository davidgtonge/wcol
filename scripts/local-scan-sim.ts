/**
 * A hash query on the local runner with R2 simulated per scan node, to tune the scan stage
 * (fetch depth, units, balancing) without deploying. CPU is one Node thread for every scan,
 * so this models I/O-bound scans, not CPU contention between scan nodes.
 *
 *   QUERY=Q31 SCANS=7 REDUCERS=8 SIM_LATENCY_MS=200 SIM_SCAN_MBPS=25 npx tsx scripts/local-scan-sim.ts
 *
 * SCANS≈7 gives each scan of the 49-row-group 6M subset about Cloudflare's full-hits load
 * (226 row groups / 32 scans).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "../tests/helpers/memory-r2-spill-bucket.ts";
import { simulatedR2, type SimulatedRead } from "../tests/helpers/simulated-r2.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const QUERIES: Record<string, string> = {
  RegionID: "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 5",
  Q31: "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
};
const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const query = process.env.QUERY ?? "Q31";
const sql = process.env.SQL ?? QUERIES[query];
if (!sql) throw new Error(`unknown QUERY ${query}`);
const parquet = process.env.PARQUET ?? path.join(ROOT, "data/hits_subset.parquet");

const size = fs.statSync(parquet).size;
const fh = await fs.promises.open(parquet, "r");
const readAt = async (offset: number, length: number) => {
  const out = new Uint8Array(length);
  await fh.read(out, 0, length, offset);
  return out;
};
const footer = await readParquetFooter(readAt, size);
const reducers = env("REDUCERS", 8);
const planned = planJob(await loadCoordinatorWasm(), {
  body: JSON.stringify({
    sources: { hits: { key: "http://x/hits.parquet" } },
    sql,
    output: { format: "json" },
    hashReducers: reducers,
  }),
  footers: [{ fileSize: size, footer }],
  maxNodes: env("SCANS", 7),
  minBytesPerNode: 1,
  hashReducers: reducers,
});
if (planned.kind !== "json" || planned.graph.kind !== "graph") throw new Error("expected a hash graph");
const reads: SimulatedRead[] = [];
const r2 = simulatedR2(
  {
    latencyMs: env("SIM_LATENCY_MS", 200),
    jitter: env("SIM_JITTER", 0.5),
    scanMBps: env("SIM_SCAN_MBPS", 25),
    totalMBps: env("SIM_TOTAL_MBPS", 0),
    seed: env("SIM_SEED", 1),
  },
  reads,
);
const started = performance.now();
await runLocalHashGraph({ node: await loadNodeWasm(), coordinator: await loadCoordinatorWasm() }, planned.graph, readAt, {
  spillR2Bucket: createMemoryR2SpillBucket(),
  scanReadAt: (taskId, read) => r2.wrap(taskId, read),
});
const wallMs = performance.now() - started;
await fh.close();

const byScan = new Map<number, SimulatedRead[]>();
for (const read of reads) byScan.set(read.taskId, [...(byScan.get(read.taskId) ?? []), read]);
const lastDone = [...byScan.values()].map((rows) => Math.max(...rows.map((r) => r.doneAt)) - started).sort((a, b) => a - b);
const mb = [...byScan.values()].map((rows) => rows.reduce((sum, r) => sum + r.bytes, 0) / 1e6).sort((a, b) => a - b);
/** Mean reads in flight while a scan had any read in flight (Cloudflare: readMs / readBusyMs). */
const inFlight = [...byScan.values()]
  .map((rows) => {
    const sorted = [...rows].sort((a, b) => a.issuedAt - b.issuedAt);
    let busy = 0;
    let end = -Infinity;
    for (const read of sorted) {
      busy += Math.max(0, read.doneAt - Math.max(read.issuedAt, end));
      end = Math.max(end, read.doneAt);
    }
    return rows.reduce((sum, r) => sum + r.doneAt - r.issuedAt, 0) / Math.max(1, busy);
  })
  .sort((a, b) => a - b);
const pick = (values: number[], q: number) => Math.round(values[Math.min(values.length - 1, Math.floor(q * values.length))]!);
console.log(
  JSON.stringify({
    query,
    scans: byScan.size,
    reducers,
    wallMs: Math.round(wallMs),
    lastReadMs: { min: pick(lastDone, 0), median: pick(lastDone, 0.5), max: pick(lastDone, 1) },
    readsInFlight: { min: inFlight[0]?.toFixed(1), max: inFlight.at(-1)?.toFixed(1) },
    readsPerScan: Math.round(reads.length / Math.max(1, byScan.size)),
    mbPerScan: { min: Math.round(mb[0] ?? 0), max: Math.round(mb.at(-1) ?? 0) },
    totalMB: Math.round(reads.reduce((sum, r) => sum + r.bytes, 0) / 1e6),
  }),
);
