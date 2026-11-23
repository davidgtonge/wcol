/**
 * Controlled fan-in spill retention experiment.
 *
 * Local subset (case A):
 *   npm run test:spill-fanin
 *
 * Full ClickBench over HTTP range reads (case FULL):
 *   npm run test:spill-fanin-full
 *   HITS_URL=https://…/hits.parquet npm run test:spill-fanin-full
 *
 * R2: point HITS_URL at a signed/public HTTPS URL for the object, or use Miniflare
 * (`test:spill-q15-repro`) for native bucket reads inside DOs.
 */
import fs from "node:fs";
import path from "node:path";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  assertSpillCoverageOrThrow,
  assertQ15TopRow,
  CF_ACTIVE_SCANS,
  CF_DISPATCH_CONCURRENCY,
  FANIN_TOPOLOGIES,
  fanInNeedsFullHits,
  formatSpillCoverageNote,
  isHttpHitsLocation,
  minSpillRunsForDataset,
  resolveHitsLocation,
  runFanInSpillCase,
  type FanInTopology,
  type ReducerSpillSummary,
} from "./helpers/spill-fanin-harness.ts";

const ROOT = path.resolve(import.meta.dirname, "..");

const Q15_LIKE_BYTES_PER_RUN = 2 * 1024 * 1024;
const PLATEAU_SLACK = 2 * 1024 * 1024;
const GAP_SLACK = 512 * 1024;

const only = process.env.SPILL_FANIN_ONLY?.toUpperCase();
const strict = process.env.SPILL_FANIN_STRICT === "1";
const minRuns = minSpillRunsForDataset(strict);

function hitsLocationFor(topology: FanInTopology): string {
  return resolveHitsLocation({ forFullGraph: topology.id === "FULL", repoRoot: ROOT });
}

function casesToRun(): FanInTopology[] {
  const list = only ? FANIN_TOPOLOGIES.filter((row) => row.id === only) : FANIN_TOPOLOGIES;
  if (only && list.length === 0) {
    throw new Error(`SPILL_FANIN_ONLY=${only} is not A or FULL`);
  }
  return list;
}

function logResult(result: Awaited<ReturnType<typeof runFanInSpillCase>>): void {
  console.log(
    `\n=== fan-in ${result.topology.id} ===\n` +
      JSON.stringify(
        {
          hitsLocation: result.hitsLocation,
          fileSize: result.fileSize,
          plannedScans: result.plannedScans,
          plannedReducers: result.plannedReducers,
          scheduling: result.scheduling,
          scanIngress: result.scanIngress,
          metricValidation: result.metricValidation,
          topRows: result.topRows.slice(0, 3),
          refReducer: result.refReducer,
          worstReducer: result.worstReducer,
          reducerCount: result.reducers.length,
          ledgerPeakAdapterBytes: result.ledgerPeakAdapterBytes,
        },
        null,
        2,
      ),
  );
}

function assertHealthyPlateau(
  label: string,
  boundaries: number,
  summary: ReducerSpillSummary,
): void {
  assert.ok(
    summary.perRunLiveGrowthPostWarm < Q15_LIKE_BYTES_PER_RUN,
    `${label}: Q15-like live growth ${Math.round(summary.perRunLiveGrowthPostWarm)} bytes/run (post-warm) ` +
      `(CF ~3.5 MiB/run when bug is present); ` +
      `wcsInput=${summary.cumulativeWcsInputBytes} sqlAppend=${summary.cumulativeSqliteAppendBytes}`,
  );
  assert.ok(
    summary.liveAfterTailPeakMinusWarm < PLATEAU_SLACK,
    `${label}: post-warm liveAfter wobble ${summary.liveAfterTailPeakMinusWarm} bytes`,
  );
  assert.ok(
    summary.maxCrossRunGap < GAP_SLACK,
    `${label}: cross-run gap ${summary.maxCrossRunGap} bytes`,
  );
  assert.ok(boundaries >= minRuns, `${label}: need ${minRuns}+ spill runs, saw ${boundaries}`);
}

function assertScheduling(result: Awaited<ReturnType<typeof runFanInSpillCase>>): void {
  const wantActive = Number(process.env.SPILL_FANIN_ACTIVE_SCANS ?? CF_ACTIVE_SCANS);
  assert.ok(
    result.scheduling.peakSimultaneousScans <= wantActive,
    `peak simultaneous scans ${result.scheduling.peakSimultaneousScans} exceeds active cap ${wantActive}`,
  );
  assert.equal(
    result.scheduling.openingConcurrency,
    Number(process.env.SPILL_FANIN_DISPATCH ?? CF_DISPATCH_CONCURRENCY),
  );
  assert.equal(result.scheduling.maxActiveScans, wantActive);
}

describe("reducer spill fan-in experiment", { concurrency: 1 }, () => {
  for (const topology of casesToRun()) {
    const location = hitsLocationFor(topology);
    const skipForDataset = fanInNeedsFullHits(topology, location);
    const run = skipForDataset ? it.skip : it;
    const remoteNote = isHttpHitsLocation(location) ? " [HTTP range]" : "";
    run(
      `${topology.id}: ${topology.label}${skipForDataset ? " (needs full hits)" : remoteNote}`,
      { timeout: strict ? 3_600_000 : isHttpHitsLocation(location) ? 3_600_000 : 900_000 },
      async () => {
        if (!isHttpHitsLocation(location)) {
          assert.ok(fs.existsSync(location), `missing hits: ${location}`);
        }
        const result = await runFanInSpillCase({ topology, hitsLocation: location });
        logResult(result);

        assert.ok(result.plannedReducers >= topology.hashReducers / 2, "expected hash reducers in plan");
        if (topology.id === "FULL") {
          assertScheduling(result);
          assert.ok(result.refReducer, `missing reference reducer task ${result.reducers.map((r) => r.taskId).join(",")}`);
        }

        for (const reducer of [result.refReducer, result.worstReducer].filter(Boolean) as ReducerSpillSummary[]) {
          assert.ok(
            reducer.mergeScanBeginsMax === 0 && reducer.mergeStepsMax === 0,
            `task ${reducer.taskId}: merge during ingest flushes`,
          );
        }

        const byteSourceIsFull =
          isHttpHitsLocation(location) ||
          path.basename(location.split("?")[0]!).toLowerCase() === "hits.parquet";
        if (byteSourceIsFull) {
          assertQ15TopRow(result.topRows);
        } else {
          console.log(
            `note: skipping DuckDB top-row gate (not full hits.parquet): ${location.slice(0, 80)}`,
          );
        }

        const checks: Array<{ label: string; summary: ReducerSpillSummary }> = [];
        if (result.refReducer) {
          checks.push({ label: `${topology.id}/ref slot ${result.refReducer.slot}`, summary: result.refReducer });
        }
        checks.push({ label: `${topology.id}/worst`, summary: result.worstReducer });

        for (const { label, summary } of checks) {
          assert.ok(summary.spillRuns >= 1, `${label}: no spill runs`);
          if (strict) {
            assertSpillCoverageOrThrow(label, summary.spillRuns, true);
            assertHealthyPlateau(label, summary.spillRuns, summary);
          } else if (summary.spillRuns >= minRuns) {
            assertHealthyPlateau(label, summary.spillRuns, summary);
          } else {
            const note = formatSpillCoverageNote(label, summary.spillRuns, false);
            if (note) console.log(note);
          }
        }
        if (result.metricValidation.nodeWasmScope === "per_task_isolate") {
          assert.ok(
            !result.metricValidation.suspiciousIdenticalReducerSummaries ||
              result.topology.id !== "FULL",
            "FULL fan-in: identical reducer spill summaries with divergent WCS — metrics not per-isolate",
          );
        }
      },
    );
  }
});
