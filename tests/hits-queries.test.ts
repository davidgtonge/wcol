import assert from "node:assert/strict";
import { before, describe, it } from "node:test";
import { HITS_EXPLORER_QUERIES } from "./hits-queries.ts";
import {
  defaultHitsFixture,
  fixtureExists,
  openCratesFile,
  projectionRows,
  rowCount,
  runPlan,
} from "./helpers/wcol-node.ts";

const fixtureOk = await fixtureExists(await defaultHitsFixture());

describe("hits explorer queries", { skip: !fixtureOk && "hits_subset_500k.wcol not found" }, () => {
  let file: Awaited<ReturnType<typeof openCratesFile>>;

  before(async () => {
    file = await openCratesFile(await defaultHitsFixture());
    assert.ok(Number(file.header.totalRows) >= 100_000);
  });

  for (const query of HITS_EXPLORER_QUERIES) {
    it(`${query.id}: ${query.question}`, async () => {
      const { result, ms } = await runPlan(file, query.plan);
      assert.ok(rowCount(result) >= (query.expect.minResults ?? 1));
      if (query.expect.maxMs) assert.ok(ms <= query.expect.maxMs, `slow: ${ms.toFixed(0)} ms`);

      if (query.expect.projectionIncludes?.length) {
        const rows = await projectionRows(file, result, Math.min(5, result.rows?.length ?? 0));
        for (const col of query.expect.projectionIncludes) {
          assert.ok(col in rows[0], `missing column ${col}`);
        }
      }
    });
  }
});
