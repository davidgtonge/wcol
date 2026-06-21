import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { TRENDS_EXPLORER_QUERIES } from "./trends-queries.ts";
import {
  fixtureExists,
  groupLabels,
  openCratesFile,
  projectionRows,
  resolveTrendsQueryFixture,
  rowCount,
  runPlan,
} from "./helpers/wcol-node.ts";

const queryCases = await Promise.all(
  TRENDS_EXPLORER_QUERIES.map(async (query) => {
    const fixturePath = await resolveTrendsQueryFixture(query);
    return { query, fixturePath, ok: await fixtureExists(fixturePath) };
  })
);

describe("trends explorer queries", () => {
  for (const { query, fixturePath, ok } of queryCases) {
    const run = ok ? it : it.skip;

    run(`${query.id}: ${query.question}`, async () => {
      const file = await openCratesFile(fixturePath);
      const { result, ms } = await runPlan(file, query.plan);
      assert.ok(rowCount(result) >= (query.expect.minResults ?? 1));
      if (query.expect.maxMs) assert.ok(ms <= query.expect.maxMs, `slow: ${ms.toFixed(0)} ms`);

      if (query.expect.topLabelIncludes && result.groups?.keys?.length) {
        const labels = await groupLabels(file, result, 12);
        const needle = query.expect.topLabelIncludes.toLowerCase();
        assert.ok(labels.some((l) => l.toLowerCase().includes(needle)));
      }

      if (query.expect.projectionIncludes?.length) {
        const rows = await projectionRows(file, result, Math.min(10, result.rows?.length ?? 0));
        assert.ok(rows.length > 0);
        for (const col of query.expect.projectionIncludes) {
          assert.ok(col in rows[0], `missing column ${col}`);
        }
      }
    });
  }
});
