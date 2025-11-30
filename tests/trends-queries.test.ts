import assert from "node:assert/strict";
import { before, describe, it } from "node:test";
import { TRENDS_EXPLORER_QUERIES } from "./trends-queries.ts";
import {
  defaultTrendsFixture,
  fixtureExists,
  groupLabels,
  openCratesFile,
  projectionRows,
  resolveTrendsQueryFixture,
  rowCount,
  runPlan,
} from "./helpers/wcol-node.ts";

const dailyOk = await fixtureExists(await defaultTrendsFixture());

describe("trends explorer queries", { skip: !dailyOk && "version_downloads_daily.wcol not found" }, () => {
  for (const query of TRENDS_EXPLORER_QUERIES) {
    it(`${query.id}: ${query.question}`, async () => {
      const fixturePath = await resolveTrendsQueryFixture(query);
      if (!(await fixtureExists(fixturePath))) {
        assert.fail(`missing fixture: ${fixturePath}`);
      }
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
