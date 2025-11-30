import assert from "node:assert/strict";
import { describe, it, before } from "node:test";
import {
  ASPIRATIONAL_EXPLORER_QUERIES,
  EXPLORER_QUERIES,
  RUNNABLE_EXPLORER_QUERIES,
} from "./explorer-queries.ts";
import {
  defaultCratesFixture,
  fixtureExists,
  groupLabels,
  openCratesFile,
  projectionRows,
  rowCount,
  runPlan,
  type WcolFileHandle,
} from "./helpers/wcol-node.ts";

const fixturePath = await defaultCratesFixture();
const fixtureOk = await fixtureExists(fixturePath);

describe("explorer query catalog", () => {
  it("defines runnable and aspirational queries", () => {
    assert.ok(EXPLORER_QUERIES.length >= 20);
    assert.ok(RUNNABLE_EXPLORER_QUERIES.length >= 15);
    assert.ok(ASPIRATIONAL_EXPLORER_QUERIES.length >= 10);
    const ids = new Set(EXPLORER_QUERIES.map((q) => q.id));
    assert.equal(ids.size, EXPLORER_QUERIES.length, "duplicate query ids");
  });
});

describe("crates.io explorer queries (Node + Wasm, no browser)", { skip: !fixtureOk && "crates_versions.wcol not found" }, () => {
  let file: WcolFileHandle;

  before(async () => {
    file = await openCratesFile(fixturePath);
    assert.ok(Number(file.header.totalRows) > 1_000_000, "expected full crates dump");
  });

  for (const query of RUNNABLE_EXPLORER_QUERIES) {
    it(`${query.id}: ${query.question}`, async () => {
      const { result, ms } = await runPlan(file, query.plan);
      const count = rowCount(result);

      if (query.expect.minResults != null) {
        assert.ok(
          count >= query.expect.minResults,
          `expected ≥${query.expect.minResults} results, got ${count} (${ms.toFixed(0)} ms)`
        );
      }

      if (query.expect.maxMs != null) {
        assert.ok(ms <= query.expect.maxMs, `expected ≤${query.expect.maxMs} ms, got ${ms.toFixed(0)} ms`);
      }

      if (query.expect.projectionIncludes?.length) {
        const cols = result.projection?.columns.map((c) => c.name) ?? [];
        for (const col of query.expect.projectionIncludes) {
          assert.ok(cols.includes(col), `missing projection column ${col}; have ${cols.join(", ")}`);
        }
      }

      if (query.expect.topLabelIncludes) {
        const labels = await groupLabels(file, result, 10);
        const needle = query.expect.topLabelIncludes.toLowerCase();
        const hit = labels.some((l) => l.toLowerCase().includes(needle));
        assert.ok(hit, `expected a top group matching “${query.expect.topLabelIncludes}”, got: ${labels.join(", ")}`);
        for (const label of labels) {
          assert.ok(!/^#\d+$/.test(label), `group label should be human-readable, got ${label}`);
        }
      }

      if (query.id === "profile_serde_versions") {
        const rows = await projectionRows(file, result, 3);
        assert.equal(typeof rows[0]?.downloads, "number");
        assert.ok(rows[0]?.license && typeof rows[0]?.license === "string");
        assert.match(String(rows[0]?.version), /^\d+\.\d+/, `version should be semver, got ${rows[0]?.version}`);
        assert.ok(!String(rows[0]?.version).startsWith("#string:"), "version should not be a pool id placeholder");
      }

      if (query.id === "top_by_edition") {
        const labels = await groupLabels(file, result, 4);
        assert.ok(labels.some((l) => /^\d{4}$/.test(l)), `edition labels should be years, got: ${labels.join(", ")}`);
      }
    });
  }
});

describe("aspirational explorer queries (documented gaps)", () => {
  for (const query of ASPIRATIONAL_EXPLORER_QUERIES) {
    it(`${query.id} is tracked as future work`, () => {
      assert.ok(query.expect.skip, "aspirational query should have skip reason");
    });
  }
});
