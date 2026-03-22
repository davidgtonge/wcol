#!/usr/bin/env node
/**
 * Run the crates.io explorer query catalog in Node (Wasm, no browser).
 * Usage: npm run explorer:queries
 */
import { ALL_ASPIRATIONAL_QUERIES, QUERY_SUITES } from "../tests/query-catalog.ts";
import { fixtureExists, groupLabels, openCratesFile, rowCount, runPlan } from "../tests/helpers/wcol-node.ts";

let pass = 0;
let fail = 0;
let skipped = 0;

for (const suite of QUERY_SUITES) {
  const fixturePath = await suite.fixture();
  if (!(await fixtureExists(fixturePath))) {
    skipped += suite.queries.length;
    console.log(`\n== ${suite.label} — skipped (missing fixture) ==\n`);
    continue;
  }

  const file = await openCratesFile(fixturePath);
  console.log(`\n== ${suite.label} (${Number(file.header.totalRows).toLocaleString()} rows) ==\n`);

  for (const q of suite.queries) {
    try {
      const { result, ms } = await runPlan(file, q.plan);
      const count = rowCount(result);
      const labels = result.groups ? (await groupLabels(file, result, 3)).join(", ") : "";
      const status = count >= (q.expect.minResults ?? 1) ? "ok" : "warn";
      if (status === "ok") pass += 1;
      else fail += 1;
      console.log(
        `${status === "ok" ? "✓" : "!"} [${q.category}] ${q.id}\n  Q: ${q.question}\n  → ${count} results in ${ms.toFixed(0)} ms${labels ? ` · top: ${labels}` : ""}\n`
      );
    } catch (e) {
      fail += 1;
      console.log(`✗ [${q.category}] ${q.id}\n  Q: ${q.question}\n  → ${e instanceof Error ? e.message : e}\n`);
    }
  }
}

console.log("--- Aspirational (not run) ---");
for (const q of ALL_ASPIRATIONAL_QUERIES) {
  console.log(`○ ${q.id}: ${q.question}\n  → ${q.expect.skip}\n`);
}

console.log(
  `Done: ${pass} passed, ${fail} failed/warned, ${skipped} skipped (missing fixtures), ${ALL_ASPIRATIONAL_QUERIES.length} aspirational`
);
process.exit(fail > 0 ? 1 : 0);
