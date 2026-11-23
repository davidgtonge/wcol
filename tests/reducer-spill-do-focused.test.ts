/**
 * Fast spill-memory repro (Q15 UserID u64, one reducer, isolated Wasm).
 *
 *   npm run test:spill-do-focused          # ~1–2 min local red gate
 *   SPILL_DO_MINIFLARE=1 npm run test:spill-do-focused   # + ~30s DO smoke (optional)
 */
import fs from "node:fs";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { buildWorkers } from "../scripts/build-workers.ts";
import {
  createSpillMiniflare,
  runGroupBySpillJob,
  verifyNodeWasmExports,
  spillOutputBucket,
} from "../scripts/cf-spill-shared.ts";
import { assertSpillCoverageOrThrow } from "./helpers/spill-coverage.ts";
import { runFastUserIdSpillPlateau } from "./helpers/spill-focused-harness.ts";
import { distDir, ROOT } from "../scripts/build-workers.ts";

const MIN_SPILL_RUNS = 40;
const Q15_LIKE_BYTES_PER_RUN = 2 * 1024 * 1024;
const PLATEAU_SLACK = 2 * 1024 * 1024;

describe("focused spill memory repro", { concurrency: 1 }, () => {
  it(
    "local: 40+ UserID spills, per-isolate metrics, post-flush live plateau",
    { timeout: 240_000 },
    async () => {
      const summary = await runFastUserIdSpillPlateau();
      console.log("\n=== fast UserID spill plateau ===\n" + JSON.stringify(summary, null, 2));

      assertSpillCoverageOrThrow("local reducer", summary.spillRuns, true);
      assert.ok(
        summary.boundaries.every((row) => row.mergeScanBegins === 0 && row.mergeSteps === 0),
        "merge during ingest flushes",
      );
      assert.ok(summary.wasmInstanceId > 0, "expected isolated reducer Wasm instance id");
      assert.ok(
        summary.perRunLiveGrowthPostWarm < Q15_LIKE_BYTES_PER_RUN,
        `Q15-like live growth ${Math.round(summary.perRunLiveGrowthPostWarm)} B/run post-warm ` +
          `(liveAfter ${summary.liveAfterRun1} → ${summary.liveAfterLast})`,
      );
      assert.ok(
        summary.tailLiveAfterWarm < PLATEAU_SLACK,
        `post-flush live wobble ${summary.tailLiveAfterWarm} bytes`,
      );
      const tail = summary.boundaries.slice(5);
      const maxInflight = Math.max(...tail.map((row) => row.rustInflightSpillBytes));
      const maxPage = Math.max(...summary.boundaries.map((row) => row.serializeBytes));
      assert.ok(
        maxInflight <= Math.max(maxPage, 1) * 4,
        `inflight should stay page-sized (maxInflight=${maxInflight}, maxPage=${maxPage})`,
      );
    },
  );

  it(
    "miniflare: DO SQLite spill smoke (optional)",
    { timeout: 120_000, skip: process.env.SPILL_DO_MINIFLARE !== "1" },
    async () => {
      const wasmPath = path.join(distDir, "wcol_node.wasm");
      if (!fs.existsSync(wasmPath)) {
        await buildWorkers("prod");
      }
      verifyNodeWasmExports();

      const { DuckDBInstance } = await import("@duckdb/node-api");
      const rows = Number(process.env.SPILL_DO_MF_ROWS ?? 8_000);
      const targetGroupBytes = Number(process.env.SPILL_DO_MF_TARGET_BYTES ?? 64 * 1024);
      const tmpParquet = path.join(ROOT, "data", `.spill-do-mf-${rows}.parquet`);
      const duck = await (await DuckDBInstance.create(":memory:")).connect();
      const quoted = `'${tmpParquet.replaceAll("'", "''")}'`;
      await duck.run(
        `COPY (SELECT i AS UserID FROM range(${rows}) t(i)) TO ${quoted} (FORMAT PARQUET)`,
      );
      const duckExpected = (
        await duck.runAndReadAll(
          `SELECT UserID, COUNT(*) AS c FROM ${quoted} GROUP BY UserID ORDER BY UserID DESC LIMIT 10`,
        )
      ).getRows() as unknown[][];
      const duckText = duckExpected.map((row) =>
        row.map((v) => (v === null ? "null" : String(v))),
      );

      const mf = createSpillMiniflare();
      try {
        const output = await spillOutputBucket(mf);
        await output.put("hits.parquet", fs.readFileSync(tmpParquet));
        const { timing } = await runGroupBySpillJob(mf, output, {
          sql: "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY UserID DESC LIMIT 10",
          duckExpected: duckText,
          hashReducers: 1,
          groupSpill: true,
          targetGroupBytes,
          jobTimeoutMs: 60_000,
        });
        const m = timing.spill;
        assert.ok(m.begin >= 1 && m.append >= 1 && m.end >= 1, `DO spill telemetry ${JSON.stringify(m)}`);
      } finally {
        await mf.dispose();
      }
    },
  );
});
