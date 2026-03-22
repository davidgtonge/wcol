#!/usr/bin/env node
/**
 * Run every runnable explorer query across all datasets and print timings.
 * Usage: npm run benchmark:queries
 */
import { ALL_ASPIRATIONAL_QUERIES, ALL_RUNNABLE_QUERIES, QUERY_SUITES } from "../tests/query-catalog.ts";
import {
  fixtureExists,
  openCratesFile,
  resolveTrendsQueryFixture,
  rowCount,
  runPlan,
} from "../tests/helpers/wcol-node.ts";

function tier(ms) {
  if (ms < 500) return "fast";
  if (ms < 5_000) return "medium";
  if (ms < 30_000) return "slow";
  return "very slow";
}

function pad(value, width) {
  const s = String(value);
  return s.length >= width ? s : s + " ".repeat(width - s.length);
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return sorted[idx];
}

const results = [];
const skippedSuites = [];

for (const suite of QUERY_SUITES) {
  const fixturePath = await suite.fixture();
  if (!(await fixtureExists(fixturePath))) {
    skippedSuites.push({ suite: suite.id, reason: `missing ${fixturePath}` });
    continue;
  }

  const file = await openCratesFile(fixturePath);
  const rows = Number(file.header.totalRows);
  console.log(`\n== ${suite.label} (${rows.toLocaleString()} rows) ==`);

  for (const query of suite.queries) {
    const label = `${suite.id}/${query.id}`;
    try {
      const queryFile =
        suite.id === "trends" && query.rollup
          ? await openCratesFile(await resolveTrendsQueryFixture(query))
          : file;
      const { result, ms } = await runPlan(queryFile, query.plan);
      const count = rowCount(result);
      const ok = count >= (query.expect.minResults ?? 1);
      const row = {
        suite: suite.id,
        id: query.id,
        label,
        category: query.category,
        question: query.question,
        ms,
        tier: tier(ms),
        count,
        ok,
        budgetMs: query.expect.maxMs ?? null,
        overBudget: query.expect.maxMs != null ? ms > query.expect.maxMs : false,
      };
      results.push(row);
      const mark = ok ? (row.overBudget ? "!" : "✓") : "✗";
      console.log(
        `${mark} ${pad(ms.toFixed(0) + " ms", 10)} ${pad(row.tier, 10)} ${pad(String(count), 6)}  ${query.id}`
      );
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      results.push({
        suite: suite.id,
        id: query.id,
        label,
        category: query.category,
        question: query.question,
        ms: null,
        tier: "error",
        count: 0,
        ok: false,
        budgetMs: query.expect.maxMs ?? null,
        overBudget: false,
        error: message,
      });
      console.log(`✗ ${pad("ERR", 10)} ${pad("error", 10)} ${pad("0", 6)}  ${query.id} — ${message}`);
    }
  }
}

const runnable = results.filter((r) => r.ms != null);
const timings = runnable.map((r) => r.ms).sort((a, b) => a - b);
const bySuite = Object.fromEntries(
  QUERY_SUITES.map((s) => [s.id, runnable.filter((r) => r.suite === s.id)])
);

console.log("\n" + "=".repeat(72));
console.log("SUMMARY");
console.log("=".repeat(72));
console.log(`Runnable queries: ${ALL_RUNNABLE_QUERIES.length} defined, ${results.length} attempted`);
console.log(`Passed: ${results.filter((r) => r.ok && !r.error).length}`);
console.log(`Over budget: ${results.filter((r) => r.overBudget).length}`);
console.log(`Errors: ${results.filter((r) => r.error).length}`);
if (skippedSuites.length) {
  console.log(`Skipped suites: ${skippedSuites.map((s) => s.suite).join(", ")}`);
}
if (timings.length) {
  console.log(
    `Wall time: ${timings.reduce((a, b) => a + b, 0).toFixed(0)} ms total · p50 ${percentile(timings, 0.5).toFixed(0)} ms · p95 ${percentile(timings, 0.95).toFixed(0)} ms · max ${timings[timings.length - 1].toFixed(0)} ms`
  );
}

console.log("\nBy dataset:");
for (const suite of QUERY_SUITES) {
  const rows = bySuite[suite.id] ?? [];
  if (!rows.length) {
    console.log(`  ${pad(suite.id, 14)} — skipped`);
    continue;
  }
  const ms = rows.map((r) => r.ms).sort((a, b) => a - b);
  const sum = ms.reduce((a, b) => a + b, 0);
  const fast = rows.filter((r) => r.tier === "fast").length;
  const slow = rows.filter((r) => r.tier === "slow" || r.tier === "very slow").length;
  console.log(
    `  ${pad(suite.id, 14)} ${pad(String(rows.length), 3)} queries · ${pad(sum.toFixed(0) + " ms", 10)} · p50 ${pad(percentile(ms, 0.5).toFixed(0), 6)} · slow ${slow} · fast ${fast}`
  );
}

console.log("\nBy tier:");
for (const t of ["fast", "medium", "slow", "very slow", "error"]) {
  const n = results.filter((r) => r.tier === t).length;
  if (n) console.log(`  ${pad(t, 12)} ${n}`);
}

console.log("\nSlowest queries:");
for (const row of [...runnable].sort((a, b) => b.ms - a.ms).slice(0, 12)) {
  console.log(`  ${pad(row.ms.toFixed(0) + " ms", 10)} ${row.label} — ${row.question}`);
}

console.log("\nFastest queries:");
for (const row of [...runnable].sort((a, b) => a.ms - b.ms).slice(0, 8)) {
  console.log(`  ${pad(row.ms.toFixed(0) + " ms", 10)} ${row.label} — ${row.question}`);
}

if (ALL_ASPIRATIONAL_QUERIES.length) {
  console.log(`\nAspirational (not benchmarked): ${ALL_ASPIRATIONAL_QUERIES.length}`);
  for (const q of ALL_ASPIRATIONAL_QUERIES) {
    console.log(`  ○ ${q.id}: ${q.expect.skip}`);
  }
}

process.exit(results.some((r) => !r.ok || r.error) ? 1 : 0);
