/**
 * Short local attempt to reproduce the Cloudflare Q15 spill climb:
 * ~3+ MiB SQLite per flush, UserID u64, several scan streams, one Wasm per reducer.
 *
 *   npm run test:spill-cf-signature
 *
 * Fails when post-warm liveAfter grows by about one spill payload per run
 * (the production signature: ~3.5 MiB/run while inflight stays page-sized).
 */
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { runFastUserIdSpillPlateau } from "./helpers/spill-focused-harness.ts";

const TARGET = Number(process.env.SPILL_CF_TARGET_BYTES ?? 4 * 1024 * 1024);
const GROUPS = Number(process.env.SPILL_CF_GROUPS ?? 220_000);
const Q15_LIKE = 1024 * 1024;

describe("CF spill signature (local, short)", { concurrency: 1 }, () => {
  it(
    "UserID multi-MiB flushes do not retain one spill payload per run",
    { timeout: 120_000 },
    async () => {
      process.env.SPILL_CF_LOG = "1";
      const summary = await runFastUserIdSpillPlateau({
        groups: GROUPS,
        targetGroupBytes: TARGET,
        maxNodes: 1,
        hashReducers: 2,
        rowGroupSize: 1_000_000,
        warmup: 2,
      });
      const deltas = summary.boundaries.slice(1).map((row, index) => ({
        run: row.run,
        liveDelta: row.liveAfter - summary.boundaries[index]!.liveAfter,
        sqliteWriteBytes: row.sqliteWriteBytes,
        rustInflight: row.rustInflightSpillBytes,
      }));
      console.log(
        "\n=== CF signature local ===\n" +
          JSON.stringify(
            {
              groups: GROUPS,
              targetGroupBytes: TARGET,
              spillRuns: summary.spillRuns,
              avgSqliteWriteBytesPerRun: Math.round(summary.avgSqliteWriteBytesPerRun),
              totalSqliteWriteBytes: summary.totalSqliteWriteBytes,
              sqliteBytesAcked: summary.sqliteBytesAcked,
              liveAfterRun1: summary.liveAfterRun1,
              liveAfterLast: summary.liveAfterLast,
              perRunLiveGrowthPostWarm: Math.round(summary.perRunLiveGrowthPostWarm),
              tailLiveAfterWarm: summary.tailLiveAfterWarm,
              wasmInstanceId: summary.wasmInstanceId,
              deltas,
            },
            null,
            2,
          ),
      );

      const ackedPerRun = summary.sqliteBytesAcked / summary.spillRuns;
      console.log(
        JSON.stringify({
          event: "cf_signature_verdict",
          sqliteBytesAcked: summary.sqliteBytesAcked,
          ackedPerRun: Math.round(ackedPerRun),
          perRunLiveGrowthPostWarm: Math.round(summary.perRunLiveGrowthPostWarm),
          reproduced: summary.perRunLiveGrowthPostWarm >= Q15_LIKE,
        }),
      );

      assert.ok(summary.spillRuns >= 8, `need >=8 spills to see a slope, saw ${summary.spillRuns}`);
      assert.ok(
        summary.perRunLiveGrowthPostWarm < Q15_LIKE,
        `REPRODUCED CF spill retention: ${Math.round(summary.perRunLiveGrowthPostWarm)} B/run post-warm ` +
          `(sqlite acked ~${Math.round(ackedPerRun)} B/run, ` +
          `liveAfter ${summary.liveAfterRun1} → ${summary.liveAfterLast})`,
      );
    },
  );
});
