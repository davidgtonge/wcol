/**
 * Per-reducer hash budget on high-cardinality ClickBench queries.
 * Reducer count is pinned (`HASH_BUDGET_REDUCERS`, default 4). A flush-threshold
 * comparison should set it to 2 so the planner's power-of-two width stays fixed.
 *
 *   npx tsx scripts/hash-budget-bench.ts
 */
import fs from "node:fs";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { planJob, readParquetFooter, resultJson } from "../src/runtime/coordinator/plan-dispatch.ts";
import { runLocalHashGraph } from "../src/runtime/coordinator/run-local-hash-graph.ts";
import { loadCoordinatorWasm, loadNodeWasm } from "../src/runtime/host/wasm.ts";

const parquet = path.resolve(process.cwd(), process.env.HASH_BUDGET_PARQUET ?? "data/hits_subset_500k.parquet");
const QUERIES = (process.env.HASH_BUDGET_QUERIES ?? "Q15,Q16,Q33,S10").split(",").map((label) => label.trim()).filter(Boolean);
const REDUCERS = Number(process.env.HASH_BUDGET_REDUCERS ?? 4);
const MiB = 1024 * 1024;
const BUDGETS = [
  // Hard ceiling is above the flush target so one hash-table doubling can land.
  // A ceiling equal to the target fails when retained capacity overshoots it.
  { label: "16/32", target: 16 * MiB, hard: 32 * MiB },
  { label: "32/48", target: 32 * MiB, hard: 48 * MiB },
  { label: "32/64", target: 32 * MiB, hard: 64 * MiB },
  { label: "48/64", target: 48 * MiB, hard: 64 * MiB },
  { label: "64", target: 64 * MiB, hard: 64 * MiB },
].filter((budget) => {
  const only = process.env.HASH_BUDGET_ONLY;
  return !only || only.split(",").includes(budget.label);
});

function loadSql(label: string): string {
  const line = fs
    .readFileSync(path.join(process.cwd(), "scripts/clickbench-queries.tsv"), "utf8")
    .split("\n")
    .find((row) => row.startsWith(`${label}\t`));
  if (!line) throw new Error(`missing ${label}`);
  return line.split("\t")[1]!.trim().replace(/;$/, "");
}

async function readAt(offset: number, length: number): Promise<Uint8Array> {
  const fh = await fs.promises.open(parquet, "r");
  try {
    const out = new Uint8Array(length);
    await fh.read(out, 0, length, offset);
    return out;
  } finally {
    await fh.close();
  }
}

function cell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "bigint") return value.toString();
  return String(value);
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
  return match ? splitCommas(match[1]!) : [];
}

function orderIndex(sql: string): number | null {
  const match = sql.match(/\border\s+by\s+([\s\S]+?)(?:\blimit\b|\boffset\b|$)/i);
  if (!match) return null;
  const name = match[1]!.trim().replace(/\s+(?:asc|desc)\s*$/i, "").trim();
  const index = selectItems(sql).findIndex((item) => {
    const alias = item.match(/\bas\s+([A-Za-z_][A-Za-z0-9_]*)\s*$/i);
    if (alias && alias[1]!.toLowerCase() === name.toLowerCase()) return true;
    if (item.toLowerCase() === name.toLowerCase()) return true;
    return name.toLowerCase() === "count(*)" && /count\s*\(\s*\*\s*\)/i.test(item);
  });
  return index < 0 ? null : index;
}

function rowKey(row: string[]): string {
  return JSON.stringify(row);
}

/** Complete unordered match, truncated preview, ordered/ranking match, or a real mismatch. */
function compareResult(sql: string, got: string[][], duck: string[][]): string {
  const index = orderIndex(sql);
  if (index == null) {
    const counts = new Map<string, number>();
    for (const row of duck) counts.set(rowKey(row), (counts.get(rowKey(row)) ?? 0) + 1);
    const used = new Map<string, number>();
    for (const row of got) {
      const key = rowKey(row);
      const next = (used.get(key) ?? 0) + 1;
      if (next > (counts.get(key) ?? 0)) return "mismatch";
      used.set(key, next);
    }
    if (got.length === duck.length) return "unordered";
    if (got.length < duck.length) return "truncated-preview";
    return "mismatch";
  }
  const window = duck.slice(0, got.length);
  if (got.length !== window.length) return "mismatch";
  if (JSON.stringify(got) === JSON.stringify(window)) return "ordered";
  const value = (row: string[]) => row[index];
  if (JSON.stringify(got.map(value)) !== JSON.stringify(window.map(value))) return "mismatch";
  if (got.length === 0) return "ranking";
  const cutoff = value(window[window.length - 1]!);
  const above = (rows: string[][]) => rows.filter((row) => value(row) !== cutoff);
  const bag = (rows: string[][]) => JSON.stringify(rows.map(rowKey).sort());
  return bag(above(got)) === bag(above(window)) ? "ranking" : "mismatch";
}

async function main(): Promise<void> {
  const node = await loadNodeWasm();
  const coordinator = await loadCoordinatorWasm();
  const size = fs.statSync(parquet).size;
  const footer = await readParquetFooter(readAt, size);
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  const escaped = parquet.replaceAll("'", "''");
  await duck.run(`CREATE VIEW hits AS SELECT * FROM read_parquet('${escaped}')`);

  const reports: unknown[] = [];
  for (const label of QUERIES) {
    const sql = loadSql(label);
    const expected = (await duck.runAndReadAll(sql)).getRows().map((row) => row.map(cell));
    for (const budget of BUDGETS) {
      const planned = planJob(coordinator, {
        body: JSON.stringify({
          sources: { hits: { key: "http://example.invalid/hits.parquet" } },
          sql,
          output: { format: "json" },
          hashReducers: REDUCERS,
          groupSpill: true,
          targetGroupBytes: budget.target,
          ...(budget.hard != null ? { hardGroupBytes: budget.hard } : {}),
        }),
        footers: [{ fileSize: size, footer }],
        maxNodes: 4,
        minBytesPerNode: 1,
        batchRows: 2048,
        hashReducers: REDUCERS,
      });
      if (planned.kind !== "json" || planned.graph.kind !== "graph") {
        reports.push({ label, budget: budget.label, error: `planned ${planned.kind}` });
        continue;
      }
      const reducerProgram = planned.graph.tasks.find((task) => task.program[5] === 16)?.program;
      let encodedBudget = 0;
      let encodedCombined = 0;
      if (reducerProgram && reducerProgram.byteLength >= 22) {
        const view = new DataView(reducerProgram.buffer, reducerProgram.byteOffset, reducerProgram.byteLength);
        const base = reducerProgram.byteLength - 22;
        encodedBudget = Number(view.getBigUint64(base, true));
        encodedCombined = Number(view.getBigUint64(base + 9, true));
      }
      const edges = planned.graph.edges.filter((edge) => edge.partition !== undefined);
      const reducers = new Set(edges.map((edge) => edge.to)).size;
      const spillByReducer = new Map<number, number>();
      let peakWasm = 0;
      let peakGroupBytes = 0;
      let spillBoundaries = 0;
      let boundaryRun = 0;
      let boundaryGroups = 0;
      const started = performance.now();
      try {
        const result = await runLocalHashGraph(
          { node, coordinator },
          planned.graph,
          readAt,
          {
            onReducerProgress: (info) => {
              if (info.spillRuns != null) {
                spillByReducer.set(
                  info.taskId,
                  Math.max(spillByReducer.get(info.taskId) ?? 0, info.spillRuns),
                );
              }
              peakWasm = Math.max(peakWasm, info.wasmMemoryBytes);
              peakGroupBytes = Math.max(peakGroupBytes, info.spillPeakLiveBytes ?? 0);
              const boundaries = info.spillBoundaries ?? [];
              spillBoundaries = Math.max(spillBoundaries, boundaries.length);
              const last = boundaries[boundaries.length - 1];
              if (last) {
                boundaryRun = Math.max(boundaryRun, last.run);
                boundaryGroups = Math.max(boundaryGroups, last.liveGroups);
              }
            },
            onScanProgress: (info) => {
              peakWasm = Math.max(peakWasm, info.wasmMemoryBytes);
            },
          },
        );
        const parsed = JSON.parse(
          resultJson(coordinator, planned.graph, { rows: result.bytes }, planned.rowCap),
        ) as { rows?: unknown[][] };
        const got = (parsed.rows ?? []).map((row) => row.map(cell));
        const compare = compareResult(sql, got, expected);
        reports.push({
          label,
          budget: budget.label,
          reducers,
          encodedBudget,
          encodedCombined,
          spillRuns: [...spillByReducer.values()].reduce((sum, runs) => sum + runs, 0),
          spillBoundaries,
          boundaryRun,
          boundaryGroups,
          peakWasmMiB: Number((peakWasm / MiB).toFixed(1)),
          peakGroupMiB: Number((peakGroupBytes / MiB).toFixed(1)),
          wallMs: Math.round(performance.now() - started),
          oursRows: got.length,
          duckRows: expected.length,
          compare,
          match: compare !== "mismatch",
        });
      } catch (error) {
        reports.push({
          label,
          budget: budget.label,
          reducers,
          wallMs: Math.round(performance.now() - started),
          error: error instanceof Error ? error.message : String(error),
        });
      }
      console.log(JSON.stringify(reports[reports.length - 1]));
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
