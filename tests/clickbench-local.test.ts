/**
 * Planner-supported ClickBench queries on hits_subset_500k, through the ABI v1 host.
 * DuckDB is the oracle. Approximate distinct uses a different sketch, so those
 * cells are checked only for shape. Top-k ties may name different groups when
 * the order key matches. EventDate in this file is a day number; the oracle
 * view turns it into a date and comparison maps dates back to that day number.
 */
import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runGatherGraph } from "./helpers/run-local-graph.ts";
import { runLocalHashGraph } from "./helpers/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const parquet = path.join(process.cwd(), "data/hits_subset_500k.parquet");
const RESULT_ROW_CAP = 1000;

type Cell = string | null;
type Query = { label: string; sql: string };

function loadQueries(): Query[] {
  const tsv = fs.readFileSync(path.join(process.cwd(), "scripts/clickbench-queries.tsv"), "utf8");
  return tsv
    .split("\n")
    .filter((line) => line.includes("\t"))
    .map((line) => {
      const [label, sql] = line.split("\t");
      return { label: label!, sql: sql!.trim().replace(/;$/, "") };
    });
}

function splitCommas(text: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === "(") depth++;
    else if (char === ")") depth--;
    else if (char === "," && depth === 0) {
      parts.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }
  parts.push(text.slice(start).trim());
  return parts.filter((part) => part.length > 0);
}

function selectItems(sql: string): string[] {
  const match = sql.match(/^\s*select\s+([\s\S]+?)\s+from\s+/i);
  if (!match) return [];
  return splitCommas(match[1]!);
}

function approxIndexes(selects: string[]): number[] {
  return selects.flatMap((item, index) => (/approx_count_distinct\s*\(/i.test(item) ? [index] : []));
}

function orderSpec(sql: string, selects: string[]): { index: number; desc: boolean } | null {
  const match = sql.match(/\border\s+by\s+([\s\S]+?)(?:\blimit\b|\boffset\b|$)/i);
  if (!match) return null;
  const raw = match[1]!.trim();
  const desc = /\bdesc\b/i.test(raw);
  const name = raw.replace(/\s+(?:asc|desc)\s*$/i, "").trim();
  const index = selects.findIndex((item) => {
    const alias = item.match(/\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$/i);
    if (alias && alias[1]!.toLowerCase() === name.toLowerCase()) return true;
    if (item.toLowerCase() === name.toLowerCase()) return true;
    return name.toLowerCase() === "count(*)" && /count\s*\(\s*\*\s*\)/i.test(item);
  });
  return index < 0 ? null : { index, desc };
}

function dayNumber(value: unknown): string | null {
  if (value == null) return null;
  if (value instanceof Date) return String(Math.round(value.getTime() / 86_400_000));
  if (typeof value === "bigint") return value.toString();
  const text = String(value);
  const date = /^(\d{4}-\d{2}-\d{2})(?:[T ].*)?$/.exec(text);
  if (!date) return text;
  const parsed = Date.parse(`${date[1]}T00:00:00Z`);
  return Number.isNaN(parsed) ? text : String(Math.round(parsed / 86_400_000));
}

function rowsOf(value: unknown): Cell[][] {
  if (!Array.isArray(value)) return [];
  return value.map((row) => (Array.isArray(row) ? row.map((cell) => dayNumber(cell)) : []));
}

function mask(rows: Cell[][], indexes: number[]): Cell[][] {
  if (indexes.length === 0) return rows;
  const skip = new Set(indexes);
  return rows.map((row) => row.map((cell, index) => (skip.has(index) ? "*" : cell)));
}

function multiset(rows: Cell[][]): string {
  return JSON.stringify(rows.map((row) => JSON.stringify(row)).sort());
}

function rankingAgrees(ours: Cell[][], duck: Cell[][], spec: { index: number }): boolean {
  if (ours.length !== duck.length) return false;
  const value = (row: Cell[]) => row[spec.index];
  const sequence = (rows: Cell[][]) => rows.map((row) => value(row));
  if (JSON.stringify(sequence(ours)) !== JSON.stringify(sequence(duck))) return false;
  if (ours.length === 0) return true;
  // The last order key is the cutoff. LIMIT may keep only some of the groups that share
  // it, so those rows may differ. Equal keys above the cutoff can also swap places.
  // The rows strictly above the cutoff are a unique set.
  const cutoff = value(duck[duck.length - 1]!);
  const above = (rows: Cell[][]) => rows.filter((row) => value(row) !== cutoff);
  return multiset(above(ours)) === multiset(above(duck));
}

function agrees(sql: string, ours: Cell[][], duck: Cell[][], rowCap: number): boolean {
  const selects = selectItems(sql);
  const approx = approxIndexes(selects);
  const order = orderSpec(sql, selects);
  const left = mask(ours, approx);
  const right = mask(duck, approx);
  if (order && approx.includes(order.index)) {
    return ours.length === duck.length && ours.every((row) => row.length === (duck[0]?.length ?? row.length));
  }
  if (!order) {
    if (left.length === right.length) return multiset(left) === multiset(right);
    const cap = rowCap > 0 ? rowCap : RESULT_ROW_CAP;
    if (left.length === cap && left.length < right.length) {
      const have = new Set(right.map((row) => JSON.stringify(row)));
      return left.every((row) => have.has(JSON.stringify(row)));
    }
    return false;
  }
  if (!rankingAgrees(left, right, order)) return false;
  return true;
}

async function runSuite(options: {
  maxNodes: number;
  hashReducers: number;
  minBytesPerNode?: number;
  requireMultiplex: boolean;
  groupSpill?: boolean;
}) {
  assert.ok(fs.existsSync(parquet), `missing ${parquet}`);
  const queries = loadQueries();
  const node = await loadNodeWasm();
  const coordinator = await loadCoordinatorWasm();
  const size = fs.statSync(parquet).size;
  const readAt = async (offset: number, length: number) => {
    const fh = await fs.promises.open(parquet, "r");
    try {
      const out = new Uint8Array(length);
      await fh.read(out, 0, length, offset);
      return out;
    } finally {
      await fh.close();
    }
  };
  const footer = await readParquetFooter(readAt, size);
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await duck.run(`
    CREATE VIEW hits AS
    SELECT * REPLACE (
      CAST(DATE '1970-01-01' + CAST(EventDate AS INTEGER) AS DATE) AS EventDate
    )
    FROM read_parquet('${parquet}')
  `);

  const unsupported: string[] = [];
  const failures: string[] = [];
  const multiplexed: string[] = [];
  let ran = 0;
  for (const { label, sql } of queries) {
    // Too many groups for a JSON answer once reducers send them: the ladder exports these.
    if (options.requireMultiplex && JSON_TOO_LARGE.has(label)) continue;
    let planned: ReturnType<typeof planJob>;
    try {
      planned = planJob(coordinator, {
          body: JSON.stringify({
            sources: { hits: { key: "http://example.invalid/hits.parquet" } },
            sql,
            output: { format: "json" },
            ...(options.groupSpill ? { groupSpill: true } : {}),
          }),
        footers: [{ fileSize: size, footer }],
        maxNodes: options.maxNodes,
        minBytesPerNode: options.minBytesPerNode,
        hashReducers: options.hashReducers,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (/unsupported|unexpected token/i.test(message)) {
        unsupported.push(label);
        continue;
      }
      failures.push(`${label} plan: ${message}`);
      continue;
    }
    if (planned.kind !== "json") {
      failures.push(`${label} planned ${planned.kind}`);
      continue;
    }
    try {
      const graph = planned.graph;
      let bytes: Uint8Array;
      if (graph.kind === "metadata_partial") bytes = graph.partial;
      else if (graph.edges.some((edge) => edge.partition !== undefined)) {
        const parts = graph.edges.filter((edge) => edge.partition !== undefined);
        const scans = new Set(parts.map((edge) => edge.from)).size;
        const reducers = new Set(parts.map((edge) => edge.partition)).size;
        const wantScans = ONE_ROW_GROUP.has(label) ? 1 : 2;
        if (options.requireMultiplex && (scans < wantScans || reducers < 2)) {
          failures.push(`${label} hash exchange is ${scans} scan(s) × ${reducers} reducer(s)`);
        } else multiplexed.push(`${label} ${scans}×${reducers}`);
        bytes = (await runLocalHashGraph({ node, coordinator }, graph, readAt)).bytes;
      } else bytes = await runGatherGraph({ node, coordinator }, graph, readAt);
      const data =
        graph.kind === "metadata_partial" || (graph.kind === "graph" && graph.rootOutput === "partial")
          ? { partial: bytes }
          : { rows: bytes };
      const parsed = JSON.parse(resultJson(coordinator, graph, data, planned.rowCap)) as { rows?: unknown[] };
      const ours = rowsOf(parsed.rows);
      const duckRows = rowsOf((await duck.runAndReadAll(sql)).getRows());
      ran++;
      if (!agrees(sql, ours, duckRows, planned.rowCap)) {
        failures.push(`${label} rows ${ours.length} vs ${duckRows.length}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      failures.push(`${label}: ${message}`);
    }
  }
  return { unsupported, failures, ran, multiplexed };
}

/**
 * `CounterID = 62` matches one row group of this subset, and row-group pruning reads only that
 * one: one unit is one scan, however many nodes are allowed.
 */
const ONE_ROW_GROUP = new Set(["Q36", "Q37", "Q38", "Q40", "Q41"]);

/** Unbounded GROUP BYs whose exchanged groups pass the JSON answer cap (exports in the ladder). */
const JSON_TOO_LARGE = new Set(["S7", "S10"]);

describe("clickbench on the local ABI host", { concurrency: 1 }, () => {
  it("runs every planner-supported query against hits_subset_500k", async () => {
    const { unsupported, failures, ran } = await runSuite({ maxNodes: 1, hashReducers: 1, requireMultiplex: false });
    assert.deepEqual(unsupported.sort(), ["Q18", "Q35", "Q42"]);
    assert.ok(ran >= 47, `ran ${ran}`);
    assert.deepEqual(failures, []);
  });

  it("runs them through a 2×2 hash exchange when the group key can be partitioned", async () => {
    const { unsupported, failures, ran, multiplexed } = await runSuite({
      maxNodes: 2,
      minBytesPerNode: 1,
      hashReducers: 2,
      requireMultiplex: true,
      groupSpill: true,
    });
    assert.deepEqual(unsupported.sort(), ["Q18", "Q35", "Q42"]);
    assert.ok(ran >= 47, `ran ${ran}`);
    assert.ok(multiplexed.length >= 8, `only ${multiplexed.length} queries multiplexed: ${multiplexed.join(", ")}`);
    assert.deepEqual(failures, []);
  });
});
