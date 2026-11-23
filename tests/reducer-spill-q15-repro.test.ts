/**
 * Local Q15-scale spill retention repro via Miniflare (DO SQLite + node wasm).
 *
 * Expectation: post-flush Rust live bytes should not climb ~one SQLite append volume
 * per spill run. When the bug is present, this test fails on the plateau assertion.
 *
 *   npm run test:spill-q15-repro
 *
 * Optional: HITS_PARQUET=data/hits_subset_2m.parquet (default: hits_subset_2m.parquet)
 */
import fs from "node:fs";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { buildWorkers } from "../scripts/build-workers.ts";
import {
  createSpillMiniflare,
  loadReducerProgressSnapshots,
  runSpillJobUntilSettled,
  verifyNodeWasmExports,
  spillOutputBucket,
} from "../scripts/cf-spill-shared.ts";

const ROOT = path.resolve(import.meta.dirname, "..");
const Q15 = "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10";
const TARGET_GROUP_BYTES = 16 * 1024 * 1024;
const MIN_SPILL_RUNS = 15;
const Q15_LIKE_BYTES_PER_RUN = 2 * 1024 * 1024;
const PLATEAU_SLACK = 512 * 1024;

function analyzeReducer(
  taskId: number,
  boundaries: Array<{ run: number; liveBefore: number; liveAfter: number; liveSqlAcked: number }>,
) {
  const first = boundaries[0]!;
  const last = boundaries[boundaries.length - 1]!;
  const warm = boundaries[Math.min(4, boundaries.length - 1)]!;
  const tail = boundaries.slice(Math.min(4, boundaries.length - 1));
  const gaps: number[] = [];
  for (let i = 0; i + 1 < boundaries.length; i += 1) {
    gaps.push(boundaries[i + 1]!.liveBefore - boundaries[i]!.liveAfter);
  }
  return {
    taskId,
    runs: boundaries.length,
    liveAfterRun1: first.liveAfter,
    liveAfterLast: last.liveAfter,
    liveBeforeRun1: first.liveBefore,
    liveBeforeLast: last.liveBefore,
    liveAfterDelta: last.liveAfter - first.liveAfter,
    perRunLiveGrowth: (last.liveAfter - first.liveAfter) / Math.max(1, last.run - first.run),
    perRunLiveBeforeGrowth: (last.liveBefore - first.liveBefore) / Math.max(1, last.run - first.run),
    maxCrossRunGap: gaps.length ? Math.max(...gaps) : 0,
    tailLiveAfterWarm: Math.max(...tail.map((row) => row.liveAfter)) - warm.liveAfter,
    sqliteAppendBytesLast: (last as { sqliteAppendBytes?: number }).sqliteAppendBytes,
  };
}

describe("Q15 spill retention local repro (miniflare)", { concurrency: 1 }, () => {
  it(
    "does not retain ~one SQLite append volume per spill run in Rust live bytes",
    { timeout: 900_000 },
    async () => {
      const hitsPath =
        process.env.HITS_PARQUET ?? path.join(ROOT, "data/hits_subset_2m.parquet");
      assert.ok(fs.existsSync(hitsPath), `missing hits parquet: ${hitsPath}`);

      await buildWorkers("prod");
      verifyNodeWasmExports();
      const mf = createSpillMiniflare();
      try {
        const output = await spillOutputBucket(mf);
        await output.put("hits.parquet", fs.readFileSync(hitsPath));

        const { jobId, state, error } = await runSpillJobUntilSettled(
          mf,
          {
            sources: { hits: { bucket: "output", key: "hits.parquet" } },
            sql: Q15,
            output: { format: "json" },
            hashReducers: 16,
            groupSpill: true,
            targetGroupBytes: TARGET_GROUP_BYTES,
            idempotencyKey: `q15-repro-${Date.now()}`,
          },
          720_000,
        );

        const snapshots = await loadReducerProgressSnapshots(output, jobId, 1);
        assert.ok(snapshots.length > 0, `no reducer progress for job ${jobId} (state=${state})`);
        const top = snapshots[0]!;
        assert.ok(
          top.spillBoundaries.length >= MIN_SPILL_RUNS,
          `task ${top.taskId}: need ${MIN_SPILL_RUNS}+ spill runs, saw ${top.spillBoundaries.length} (job ${state}: ${error ?? ""})`,
        );

        const analysis = analyzeReducer(top.taskId, top.spillBoundaries);
        console.log("\n=== Q15 local repro (heaviest reducer) ===");
        console.log(JSON.stringify({ jobId, state, error, analysis }, null, 2));

        assert.ok(
          top.spillBoundaries.every((row) => row.mergeScanBegins === 0 && row.mergeSteps === 0),
          "merge should not run during ingest flushes",
        );

        assert.ok(
          analysis.perRunLiveGrowth < Q15_LIKE_BYTES_PER_RUN,
          `Q15-like Rust live retention: ${Math.round(analysis.perRunLiveGrowth)} bytes/run ` +
            `(liveAfter ${analysis.liveAfterRun1} → ${analysis.liveAfterLast}, ` +
            `sqliteAppendBytesLast=${analysis.sqliteAppendBytesLast})`,
        );
        assert.ok(
          analysis.tailLiveAfterWarm < PLATEAU_SLACK,
          `post-reset live wobble ${analysis.tailLiveAfterWarm} bytes after warm-up`,
        );
        assert.ok(
          analysis.maxCrossRunGap < 2 * 1024 * 1024,
          `cross-run gap ${analysis.maxCrossRunGap} bytes (GROUP_RESET→NEXT_FLUSH_ENTER)`,
        );
      } finally {
        await mf.dispose();
      }
    },
  );
});
