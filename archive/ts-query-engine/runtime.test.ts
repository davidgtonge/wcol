import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { packPages } from "../src/runtime/io/pages.ts";
import { WcolFile, NodeFileSource, buildPlan, executePlanFromPlan } from "../src/index.ts";

test("packPages builds exec descriptors with payload offsets", () => {
  const requests = [
    { kind: 0, colId: 3, offset: 100, compLen: 4, rawLen: 12 },
    { kind: 1, colId: 3, offset: 200, compLen: 2, rawLen: 8 }
  ];
  const payloads = [new Uint8Array([1, 2, 3, 4]), new Uint8Array([9, 8])];
  const { descs, data } = packPages(requests, payloads);

  assert.equal(descs.length, 10);
  assert.deepEqual(
    Array.from(descs),
    [
      0, 3, 0, 4, 12,
      1, 3, 4, 2, 8
    ]
  );
  assert.deepEqual(Array.from(data), [1, 2, 3, 4, 9, 8]);
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HITS_WCOL_PATH = path.join(ROOT, "data", "hits_subset.wcol");
const PARALLEL_WORKERS = 2;

async function hasFile(targetPath: string): Promise<boolean> {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function normalizeU64List(values: Array<number | bigint>): string[] {
  return values.map((value) => (typeof value === "bigint" ? value.toString() : String(value)));
}

function nearlyEqual(a: number, b: number, eps = 1e-6): boolean {
  return Math.abs(a - b) <= eps;
}

function compareAggStats(
  a: { count: number; sum: number; min: number; max: number; mean: number },
  b: { count: number; sum: number; min: number; max: number; mean: number }
): void {
  assert.equal(a.count, b.count);
  assert.ok(nearlyEqual(a.sum, b.sum));
  assert.ok(nearlyEqual(a.min, b.min));
  assert.ok(nearlyEqual(a.max, b.max));
  assert.ok(nearlyEqual(a.mean, b.mean));
}

function compareAggregates(
  serial: Record<string, { count: number; sum: number; min: number; max: number; mean: number }>,
  parallel: Record<string, { count: number; sum: number; min: number; max: number; mean: number }>
): void {
  const keys = new Set([...Object.keys(serial), ...Object.keys(parallel)]);
  for (const key of keys) {
    assert.ok(serial[key], `Missing serial aggregate for ${key}`);
    assert.ok(parallel[key], `Missing parallel aggregate for ${key}`);
    compareAggStats(serial[key], parallel[key]);
  }
}

function compareGroups(
  serial: Awaited<ReturnType<WcolFile["query"]>>["groups"],
  parallel: Awaited<ReturnType<WcolFile["query"]>>["groups"]
): void {
  if (!serial && !parallel) {
    return;
  }
  assert.ok(serial && parallel, "Group mismatch: one side is null");
  const serialKeys = normalizeU64List(serial.keys);
  const parallelKeys = normalizeU64List(parallel.keys);
  assert.deepEqual(parallelKeys, serialKeys);
  const serialKeys2 = serial.keys2 ? normalizeU64List(serial.keys2) : [];
  const parallelKeys2 = parallel.keys2 ? normalizeU64List(parallel.keys2) : [];
  assert.deepEqual(parallelKeys2, serialKeys2);
  assert.equal(parallel.values.length, serial.values.length);
  for (let row = 0; row < serial.values.length; row += 1) {
    const serialRow = serial.values[row] ?? [];
    const parallelRow = parallel.values[row] ?? [];
    assert.equal(parallelRow.length, serialRow.length);
    for (let col = 0; col < serialRow.length; col += 1) {
      compareAggStats(serialRow[col], parallelRow[col]);
    }
  }
}

function applySql(file: WcolFile, plan: number, sql: string): void {
  const encoder = new TextEncoder();
  const sqlBytes = encoder.encode(sql);
  const ptr = file.wasm.alloc(sqlBytes.byteLength);
  file.wasm.memoryU8().set(sqlBytes, ptr);
  try {
    const code = file.wasm.exports.plan_apply_sql(plan, ptr, sqlBytes.byteLength);
    if (code < 0) {
      throw new Error(`Failed to apply SQL (${code})`);
    }
  } finally {
    file.wasm.free(ptr, sqlBytes.byteLength);
  }
}

function hasSqlApi(file: WcolFile): boolean {
  const sql = "SELECT 1 FROM hits LIMIT 1;";
  const encoder = new TextEncoder();
  const sqlBytes = encoder.encode(sql);
  const ptr = file.wasm.alloc(sqlBytes.byteLength);
  const plan = file.wasm.exports.create_plan(file.runtime);
  file.wasm.exports.plan_reset_results(plan);
  file.wasm.memoryU8().set(sqlBytes, ptr);
  try {
    const code = file.wasm.exports.plan_apply_sql(plan, ptr, sqlBytes.byteLength);
    return code !== -1000;
  } finally {
    file.wasm.free(ptr, sqlBytes.byteLength);
    file.wasm.exports.destroy_plan(plan);
  }
}

async function closeFile(file: WcolFile): Promise<void> {
  const pool = (file.ctx as { workerPool?: { pool?: { close?: () => Promise<void> } } }).workerPool?.pool;
  if (pool?.close) {
    await pool.close();
  }
  if ("close" in file.source && typeof file.source.close === "function") {
    await file.source.close();
  }
}

test("parallel query parity (workers: 1 vs N) for plan and SQL execution", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }

  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    const plans = [
      buildPlan({
        limit: 5,
        filters: [{ column: "CounterID", op: "=", value: 62 }]
      }),
      buildPlan({
        aggregates: [{ column: "ResolutionWidth" }]
      }),
      buildPlan({
        groupBy: { keys: ["CounterID"], value: "ResolutionWidth" }
      })
    ];

    for (const plan of plans) {
      const serial = await file.query(plan, { workers: 1 });
      const parallel = await file.query(plan, { workers: PARALLEL_WORKERS });
      assert.deepEqual(normalizeU64List(parallel.rows), normalizeU64List(serial.rows));
      compareAggregates(serial.aggregates, parallel.aggregates);
      compareGroups(serial.groups, parallel.groups);
    }

    const selectPlan = buildPlan({
      limit: 4,
      filters: [{ column: "CounterID", op: "=", value: 62 }],
      select: ["CounterID", "EventDate"]
    });
    const serialSelect = await file.query(selectPlan, { workers: 1 });
    const parallelSelect = await file.query(selectPlan, { workers: PARALLEL_WORKERS });
    assert.deepEqual(normalizeU64List(parallelSelect.rows), normalizeU64List(serialSelect.rows));
    assert.ok(serialSelect.projection && parallelSelect.projection);
    assert.equal(
      serialSelect.projection.data.length,
      parallelSelect.projection.data.length
    );

    if (!hasSqlApi(file)) {
      t.diagnostic("Skipping SQL parity section because wasm SQL API is disabled.");
      return;
    }

    const sql = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;";
    const serialPlan = file.wasm.exports.create_plan(file.runtime);
    file.wasm.exports.plan_reset_results(serialPlan);
    applySql(file, serialPlan, sql);
    const serial = await executePlanFromPlan(file.ctx, serialPlan, { workers: 1, sql });
    file.wasm.exports.destroy_plan(serialPlan);

    const parallelPlan = file.wasm.exports.create_plan(file.runtime);
    file.wasm.exports.plan_reset_results(parallelPlan);
    applySql(file, parallelPlan, sql);
    const parallel = await executePlanFromPlan(file.ctx, parallelPlan, { workers: PARALLEL_WORKERS, sql });
    file.wasm.exports.destroy_plan(parallelPlan);

    assert.deepEqual(normalizeU64List(parallel.rows), normalizeU64List(serial.rows));
    compareAggregates(serial.aggregates, parallel.aggregates);
    compareGroups(serial.groups, parallel.groups);
  } finally {
    await closeFile(file);
  }
});

test("parallel query parity for complex plan-only clickbench-style plans", async (t) => {
  if (!(await hasFile(HITS_WCOL_PATH))) {
    t.skip(`Missing fixture: ${HITS_WCOL_PATH}`);
    return;
  }

  const file = await WcolFile.open(new NodeFileSource(HITS_WCOL_PATH));
  try {
    const plans = [
      buildPlan({
        limit: 200,
        filters: [
          { column: "CounterID", op: "=", value: 62 },
          { column: "EventDate", op: ">=", value: "2013-07-01" },
          { column: "EventDate", op: "<=", value: "2013-07-31" },
          { column: "TraficSourceID", op: "in", value: [-1, 6] }
        ],
        combine: [0, 1, "AND", 2, "AND", 3, "AND"],
        groupBy: { keys: ["URLHash", "EventDate"], value: "ResolutionWidth" },
        aggregates: [{ column: "ResolutionWidth" }, { column: "UserID" }]
      }),
      buildPlan({
        limit: 500,
        filters: [
          { column: "CounterID", op: "=", value: 62 },
          { column: "CounterID", op: "=", value: 43 },
          { column: "EventDate", op: ">=", value: "2013-07-01" },
          { column: "EventDate", op: "<=", value: "2013-07-31" }
        ],
        combine: [0, 1, "OR", 2, "AND", 3, "AND"],
        groupBy: { keys: ["CounterID"], value: "ResolutionWidth" },
        aggregates: [{ column: "ResolutionWidth" }]
      })
    ];

    for (const plan of plans) {
      const serial = await file.query(plan, { workers: 1 });
      const parallel = await file.query(plan, { workers: PARALLEL_WORKERS });
      assert.deepEqual(normalizeU64List(parallel.rows), normalizeU64List(serial.rows));
      compareAggregates(serial.aggregates, parallel.aggregates);
      compareGroups(serial.groups, parallel.groups);
    }
  } finally {
    await closeFile(file);
  }
});
