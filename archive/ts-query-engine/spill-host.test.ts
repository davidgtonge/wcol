import assert from "node:assert/strict";
import { test } from "node:test";
import {
  applySqliteCache,
  insertSpillRows,
  SPILL_INSERT_CHUNK_ROWS,
  SPILL_INSERT_MAX_BOUND_ROWS,
  trackedSpillSql,
  emptySpillTelemetry,
} from "../src/runtime/stream/spill-host.ts";

test("DO INSERT chunk stays within 100 bound parameters", () => {
  assert.equal(SPILL_INSERT_MAX_BOUND_ROWS, 50);
  assert.equal(SPILL_INSERT_CHUNK_ROWS, 50);
});

test("insertSpillRows uses 50-row multi-value INSERT chunks by default", () => {
  const statements: Array<{ sql: string; n: number }> = [];
  const sql = {
    exec: (query: string, ...bindings: unknown[]) => {
      statements.push({ sql: query, n: bindings.length / 2 });
    },
  };
  const rows = Array.from({ length: SPILL_INSERT_CHUNK_ROWS + 7 }, (_, i) => ({
    key: new Uint8Array([i]),
    partial: new Uint8Array([i + 1]),
  }));
  insertSpillRows(sql, "spill_1", rows);
  assert.equal(statements.length, 2);
  assert.equal(statements[0]?.n, SPILL_INSERT_CHUNK_ROWS);
  assert.equal(statements[1]?.n, 7);
  assert.equal((statements[0]?.n ?? 0) * 2, 100);
  assert.match(statements[0]?.sql ?? "", /VALUES \(\?, \?\), \(\?, \?\)/);
});

test("insertSpillRows honors a larger chunk size for native sqlite", () => {
  const statements: Array<{ n: number }> = [];
  const sql = {
    exec: (_query: string, ...bindings: unknown[]) => {
      statements.push({ n: bindings.length / 2 });
    },
  };
  const rows = Array.from({ length: 250 }, (_, i) => ({
    key: new Uint8Array([i]),
    partial: new Uint8Array([1]),
  }));
  insertSpillRows(sql, "spill_1", rows, 200);
  assert.deepEqual(statements.map((s) => s.n), [200, 50]);
});

test("trackedSpillSql counts multi-row INSERT bindings as rows", () => {
  const telemetry = emptySpillTelemetry();
  const sql = trackedSpillSql({ exec: () => undefined }, telemetry);
  const keys = Array.from({ length: 3 }, (_, i) => new Uint8Array([i]));
  const parts = Array.from({ length: 3 }, (_, i) => new Uint8Array([i, i]));
  sql.exec(
    "INSERT INTO spill_x (key_blob, partial) VALUES (?, ?), (?, ?), (?, ?)",
    keys[0],
    parts[0],
    keys[1],
    parts[1],
    keys[2],
    parts[2],
  );
  assert.equal(telemetry.append, 1);
  assert.equal(telemetry.appendRows, 3);
  assert.equal(telemetry.appendBytes, 9);
});

test("applySqliteCache is accepted only when PRAGMA cache_size reads back", () => {
  let size = 2000;
  const sql = {
    exec: (query: string) => {
      const set = /^PRAGMA cache_size = (-?\d+)$/.exec(query);
      if (set) {
        size = Number(set[1]);
        return;
      }
      if (query === "PRAGMA cache_size") return [{ cache_size: size }];
    },
  };
  const probe = applySqliteCache(sql, 32);
  assert.equal(probe.requested, 32);
  assert.equal(probe.reported, 32);
  assert.equal(probe.accepted, true);
});

test("applySqliteCache is rejected when the pragma is ignored", () => {
  const sql = {
    exec: (query: string) => {
      if (query === "PRAGMA cache_size") return [{ cache_size: 2000 }];
    },
  };
  const probe = applySqliteCache(sql, 32);
  assert.equal(probe.requested, 32);
  assert.equal(probe.reported, 2000);
  assert.equal(probe.accepted, false);
});
