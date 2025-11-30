import assert from "node:assert/strict";
import { before, describe, it } from "node:test";
import { MAINTAINERS_EXPLORER_QUERIES } from "./maintainers-queries.ts";
import {
  defaultMaintainersFixture,
  fixtureExists,
  groupLabels,
  openCratesFile,
  projectionRows,
  rowCount,
  runPlan,
} from "./helpers/wcol-node.ts";

const fixtureOk = await fixtureExists(await defaultMaintainersFixture());

describe("maintainers explorer queries", { skip: !fixtureOk && "crate_maintainers.wcol not found" }, () => {
  let file: Awaited<ReturnType<typeof openCratesFile>>;

  before(async () => {
    file = await openCratesFile(await defaultMaintainersFixture());
    assert.ok(Number(file.header.totalRows) > 100_000);
  });

  for (const query of MAINTAINERS_EXPLORER_QUERIES) {
    it(`${query.id}: ${query.question}`, async () => {
      const { result, ms } = await runPlan(file, query.plan);
      assert.ok(rowCount(result) >= (query.expect.minResults ?? 1));
      if (query.expect.maxMs) assert.ok(ms <= query.expect.maxMs, `slow: ${ms.toFixed(0)} ms`);

      if (query.expect.topLabelIncludes && result.groups?.keys?.length) {
        const labels = await groupLabels(file, result, 8);
        const needle = query.expect.topLabelIncludes.toLowerCase();
        assert.ok(labels.some((l) => l.toLowerCase().includes(needle)));
      }

      if (query.plan.select?.length) {
        const rows = await projectionRows(file, result, Math.min(50, result.rows?.length ?? 0));
        assert.ok(rows[0]?.crate_name && rows[0]?.owner_login);
      }
    });
  }
});
