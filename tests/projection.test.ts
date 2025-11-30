import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WcolFile, NodeFileSource, buildPlan } from "../src/index.ts";
import { projectionCellToString } from "../src/runtime/exec/projection.ts";
import { PROJ_KIND_DICT_ID, PROJ_KIND_F64, type RowProjection } from "../src/runtime/core/types.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HITS_WCOL_PATH = path.join(ROOT, "data", "hits_subset.wcol");

async function hasFile(targetPath: string): Promise<boolean> {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

test("select with groupBy is rejected at apply time", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }
  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    await assert.rejects(
      () =>
        file.query(
          buildPlan({
            limit: 1,
            groupBy: { keys: ["CounterID"] },
            select: ["EventDate"]
          }),
          { workers: 1 }
        ),
      /select cannot be used with groupBy/
    );
  } finally {
    if ("close" in file.source && typeof file.source.close === "function") {
      await file.source.close();
    }
  }
});

test("filter + limit + select returns projection columns", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }
  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    const limit = 5;
    const result = await file.query(
      buildPlan({
        limit,
        filters: [{ column: "CounterID", op: "=", value: 62 }],
        select: ["CounterID", "EventDate"]
      }),
      { workers: 1 }
    );
    assert.equal(result.rows.length, limit);
    assert.ok(result.projection);
    assert.equal(result.projection.columns.length, 2);
    for (const col of result.projection.data) {
      assert.equal(col.nulls.length, limit);
      if (col.kind === PROJ_KIND_DICT_ID) {
        assert.equal(col.values.length, limit);
      }
    }
    const counterCol = result.projection.data[0]!;
    const first = projectionCellToString(counterCol, 0, () => undefined);
    assert.ok(first.length > 0 || counterCol.nulls[0] === 0);
  } finally {
    if ("close" in file.source && typeof file.source.close === "function") {
      await file.source.close();
    }
  }
});

function assertProjectionShape(projection: RowProjection, rowCount: number, columnCount: number): void {
  assert.equal(projection.columns.length, columnCount);
  assert.equal(projection.data.length, columnCount);
  for (const col of projection.data) {
    assert.equal(col.nulls.length, rowCount, "nulls length must match row count");
    if (col.kind === PROJ_KIND_DICT_ID || col.kind === PROJ_KIND_F64) {
      assert.equal(col.values.length, rowCount, "values length must match row count");
    } else {
      assert.equal(col.values.length, rowCount, "bool values length must match row count");
    }
  }
}

test("filter + limit + select with larger limit keeps column shapes", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }
  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    const limit = 750;
    const select = ["CounterID", "EventDate", "WatchID"] as const;
    const result = await file.query(
      buildPlan({
        limit,
        filters: [{ column: "CounterID", op: "=", value: 62 }],
        select: [...select]
      }),
      { workers: 1 }
    );
    assert.equal(result.rows.length, limit);
    assert.ok(result.projection);
    assertProjectionShape(result.projection, limit, select.length);
    const kinds = new Set(result.projection.data.map((c) => c.kind));
    assert.ok(kinds.has(PROJ_KIND_DICT_ID));
  } finally {
    if ("close" in file.source && typeof file.source.close === "function") {
      await file.source.close();
    }
  }
});

test("parallel workers match serial projection row count", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }
  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    const plan = buildPlan({
      limit: 8,
      filters: [{ column: "CounterID", op: "=", value: 62 }],
      select: ["CounterID"]
    });
    const serial = await file.query(plan, { workers: 1 });
    const parallel = await file.query(plan, { workers: 2 });
    assert.equal(serial.rows.length, parallel.rows.length);
    assert.ok(serial.projection && parallel.projection);
    assert.equal(serial.projection.data[0]!.values.length, parallel.projection.data[0]!.values.length);
  } finally {
    if ("close" in file.source && typeof file.source.close === "function") {
      await file.source.close();
    }
  }
});
