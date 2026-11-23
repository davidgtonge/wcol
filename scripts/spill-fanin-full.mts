#!/usr/bin/env npx tsx
/**
 * FULL fan-in spill experiment without node:test (long runs exhaust test-runner async hooks).
 *
 *   npm run spill-fanin-full-run
 *   SPILL_FANIN_STRICT=1 npm run spill-fanin-full-run
 */
import path from "node:path";
import { strict as assert } from "node:assert";
import { fileURLToPath } from "node:url";
import {
  assertQ15TopRow,
  assertSpillCoverageOrThrow,
  CF_ACTIVE_SCANS,
  CF_DISPATCH_CONCURRENCY,
  FANIN_TOPOLOGIES,
  minSpillRunsForDataset,
  resolveHitsLocation,
  runFanInSpillCase,
} from "../tests/helpers/spill-fanin-harness.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const strict = process.env.SPILL_FANIN_STRICT === "1";
const minRuns = minSpillRunsForDataset(strict);
const topology = FANIN_TOPOLOGIES.find((t) => t.id === "FULL");
if (!topology) throw new Error("missing FULL topology");

const location = resolveHitsLocation({ forFullGraph: true, repoRoot: ROOT });
const Q15_LIKE = 2 * 1024 * 1024;

console.log(JSON.stringify({ event: "spill_fanin_full_start", location, strict, minRuns }, null, 2));

const result = await runFanInSpillCase({ topology, hitsLocation: location });

console.log("\n=== fan-in FULL (script) ===\n" + JSON.stringify(result, null, 2));

assert.ok(result.plannedReducers >= 16 / 2);
assert.ok(result.scheduling.peakSimultaneousScans <= Number(process.env.SPILL_FANIN_ACTIVE_SCANS ?? CF_ACTIVE_SCANS));
assert.ok(result.refReducer, `no ref reducer; task ids: ${result.reducers.map((r) => r.taskId).join(",")}`);

assertQ15TopRow(result.topRows);

if (result.metricValidation.suspiciousIdenticalReducerSummaries) {
  console.warn(
    JSON.stringify({
      event: "spill_fanin_metric_warning",
      message:
        "All reducer spill summaries match but WCS inputs differ — likely shared Wasm spill boundary reads (use per_task_isolate)",
      metricValidation: result.metricValidation,
    }),
  );
}

for (const { label, summary } of [
  { label: "ref", summary: result.refReducer! },
  { label: "worst", summary: result.worstReducer },
]) {
  assert.ok(summary.spillRuns >= 1, `${label}: no spill runs recorded`);
  assertSpillCoverageOrThrow(label, summary.spillRuns, strict);
  assert.ok(
    summary.perRunLiveGrowthPostWarm < Q15_LIKE,
    `${label}: Q15-like growth ${summary.perRunLiveGrowthPostWarm} bytes/run post-warm`,
  );
}

console.log(JSON.stringify({ event: "spill_fanin_full_ok", minRuns, strict }));
