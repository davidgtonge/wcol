import assert from "node:assert/strict";
import { before, describe, it } from "node:test";
import { DEPENDENCY_EXPLORER_QUERIES } from "./dependency-queries.ts";
import {
  defaultDepsFixture,
  fixtureExists,
  groupLabels,
  openCratesFile,
  projectionRows,
  rowCount,
  runPlan,
} from "./helpers/wcol-node.ts";

const fixtureOk = await fixtureExists(await defaultDepsFixture());

describe("dependency explorer queries", { skip: !fixtureOk && "crates_dependencies.wcol not found" }, () => {
  let file: Awaited<ReturnType<typeof openCratesFile>>;

  before(async () => {
    file = await openCratesFile(await defaultDepsFixture());
    assert.ok(Number(file.header.totalRows) > 1_000_000);
  });

  for (const query of DEPENDENCY_EXPLORER_QUERIES) {
    it(`${query.id}: ${query.question}`, async () => {
      const { result, ms } = await runPlan(file, query.plan);
      assert.ok(rowCount(result) >= (query.expect.minResults ?? 1), "expected rows/groups");
      if (query.expect.maxMs) assert.ok(ms < query.expect.maxMs, `slow: ${ms.toFixed(0)} ms`);

      if (query.expect.topLabelIncludes && result.groups?.keys?.length) {
        const labels = await groupLabels(file, result, 8);
        const needle = query.expect.topLabelIncludes.toLowerCase();
        assert.ok(
          labels.some((l) => l.toLowerCase().includes(needle)),
          `expected top label matching “${query.expect.topLabelIncludes}”, got: ${labels.join(", ")}`
        );
      }

      if (query.plan.select?.length) {
        const rows = await projectionRows(file, result, 3);
        assert.ok(rows[0]?.parent_crate_name || rows[0]?.dep_crate_name);
      }
    });
  }
});
