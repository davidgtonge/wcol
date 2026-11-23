/**
 * Local host metric scope: shared singleton Wasm vs one instance per reducer/scan.
 */
import fs from "node:fs";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  FANIN_TOPOLOGIES,
  resolveHitsLocation,
  runFanInSpillCase,
} from "./helpers/spill-fanin-harness.ts";

const ROOT = path.resolve(import.meta.dirname, "..");

describe("local spill metric scope", { concurrency: 1 }, () => {
  it(
    "flags shared Wasm when reducer summaries alias across partitions",
    { timeout: 600_000 },
    async () => {
      const topology = FANIN_TOPOLOGIES.find((row) => row.id === "A");
      assert.ok(topology);
      const location = resolveHitsLocation({ forFullGraph: false, repoRoot: ROOT });
      if (!fs.existsSync(location)) {
        console.log(`skip: missing ${location}`);
        return;
      }

      const prev = process.env.SPILL_FANIN_SHARED_WASM;
      process.env.SPILL_FANIN_SHARED_WASM = "1";
      try {
        const shared = await runFanInSpillCase({ topology, hitsLocation: location });
        assert.equal(shared.metricValidation.nodeWasmScope, "shared_singleton");
        assert.equal(shared.metricValidation.distinctReducerWasmInstances, 1);
        assert.ok(shared.reducers.length >= 2);
      } finally {
        if (prev === undefined) delete process.env.SPILL_FANIN_SHARED_WASM;
        else process.env.SPILL_FANIN_SHARED_WASM = prev;
      }

      const isolated = await runFanInSpillCase({ topology, hitsLocation: location });
      assert.equal(isolated.metricValidation.nodeWasmScope, "per_task_isolate");
      assert.ok(isolated.metricValidation.distinctReducerWasmInstances >= 2);
      assert.ok(
        !isolated.metricValidation.suspiciousIdenticalReducerSummaries ||
          new Set(isolated.reducers.map((row) => row.cumulativeWcsInputBytes)).size <= 1,
        "per-isolate Wasm should not alias reducer spill summaries when WCS inputs differ",
      );
      const ids = isolated.reducers.map((row) => row.wasmInstanceId);
      assert.ok(new Set(ids).size >= 2, `expected distinct reducer wasm ids, got ${ids.join(",")}`);
    },
  );
});
