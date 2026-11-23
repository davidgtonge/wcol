/**
 * Local Q33 with in-memory R2 spill (same host path as CF reducers).
 *   npx tsx scripts/local-q33-r2-spill.ts
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { planJob, readParquetFooter } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";
import { createMemoryR2SpillBucket } from "../tests/helpers/memory-r2-spill-bucket.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const parquet = path.join(ROOT, "data/hits_subset_2m.parquet");
const sql = "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10";

async function main(): Promise<void> {
  const size = fs.statSync(parquet).size;
  const readAt = async (offset: number, length: number): Promise<Uint8Array> => {
    const fh = await fs.promises.open(parquet, "r");
    try {
      const out = new Uint8Array(length);
      await fh.read(out, 0, length, offset);
      return out;
    } finally {
      await fh.close();
    }
  };
  const footer = await readParquetFooter(readAt, size);
  const coordinator = await loadCoordinatorWasm();
  const node = await loadNodeWasm();
  const planned = planJob(coordinator, {
    body: JSON.stringify({
      sources: { hits: { key: "http://x/hits.parquet" } },
      sql,
      output: { format: "json" },
      hashReducers: 2,
      groupSpill: true,
      targetGroupBytes: 16 * 1024 * 1024,
      hardGroupBytes: 32 * 1024 * 1024,
    }),
    footers: [{ fileSize: size, footer }],
    maxNodes: 1,
    batchRows: 2048,
    minBytesPerNode: 1,
    hashReducers: 2,
  });
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error("expected json graph plan");
  }
  const spillR2 = createMemoryR2SpillBucket();
  const t0 = performance.now();
  let last = 0;
  const result = await runLocalHashGraph(
    { node, coordinator },
    planned.graph,
    readAt,
    {
      spillR2Bucket: spillR2,
      onReducerProgress: (info) => {
        const now = performance.now();
        if (now - last < 2000) return;
        last = now;
        console.log(
          JSON.stringify({
            wallMs: Math.round(now - t0),
            taskId: info.taskId,
            spillRuns: info.spillRuns,
            bounds: info.spillBoundaries?.length,
          }),
        );
      },
    },
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        wallMs: Math.round(performance.now() - t0),
        spillObjects: spillR2.objects.size,
        top: result.rows?.slice(0, 3),
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
