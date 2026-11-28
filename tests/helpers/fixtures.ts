/**
 * The preamble most engine tests share: write a small Parquet file with DuckDB, read its
 * footer, plan a JSON job over it, and print the result rows as strings for comparison.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import type { ExecutionPlan } from "../../src/runtime/coordinator/execution-graph.ts";
import {
  planJob,
  readParquetFooter,
  resultJson,
  type JobPlanRequest,
  type ReadAt,
} from "../../src/runtime/coordinator/plan-dispatch.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";
import { fileRangeReader } from "./streams.ts";

export type Graph = Extract<ExecutionPlan, { kind: "graph" }>;

export type ParquetFile = {
  readonly path: string;
  readonly size: number;
  readonly footer: Uint8Array;
  readonly readAt: ReadAt;
};

/** Anything that runs cleanup when the test or suite ends (`t.after`, or `after` from node:test). */
export type Cleanup = { after(fn: () => unknown): void };

/** A DuckDB string literal. */
export const sqlString = (value: string): string => `'${value.replaceAll("'", "''")}'`;

/** An in-memory DuckDB, capped so a fixture can never take the machine down. */
export async function duckdb(): Promise<DuckDBConnection> {
  const instance = await DuckDBInstance.create(":memory:", { memory_limit: "1GB", threads: "2" });
  return instance.connect();
}

export async function openParquet(file: string): Promise<ParquetFile> {
  const readAt = fileRangeReader(file);
  const size = (await fs.promises.stat(file)).size;
  return { path: file, size, footer: await readParquetFooter(readAt, size), readAt };
}

/**
 * `select` written to a fresh Parquet file in its own temp directory, removed at cleanup.
 * `copyOptions` are extra DuckDB COPY options such as `ROW_GROUP_SIZE 2048`.
 */
export async function writeParquet(
  cleanup: Cleanup,
  select: string,
  copyOptions = "",
  duck?: DuckDBConnection,
): Promise<ParquetFile> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "wcol-test-"));
  cleanup.after(() => fs.promises.rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, "t.parquet");
  const options = ["FORMAT PARQUET", copyOptions].filter((option) => option.length > 0).join(", ");
  await (duck ?? (await duckdb())).run(`COPY (${select}) TO ${sqlString(file)} (${options})`);
  return openParquet(file);
}

export type JsonJob = {
  readonly graph: Graph;
  readonly rowCap: number;
};

/**
 * A JSON job over `file`, read as table `t` (or `table`). `body` adds request fields such as
 * `groupSpill`; `hashReducers` sets both the body field and the planner override.
 */
export function planJsonGraph(
  coordinator: WcolStreamWasm,
  file: ParquetFile,
  sql: string,
  options: Omit<JobPlanRequest, "body" | "footers"> & { body?: Record<string, unknown>; table?: string },
): JsonJob {
  const { body, table = "t", ...request } = options;
  const planned = planJob(coordinator, {
    ...request,
    body: JSON.stringify({
      sources: { [table]: { key: `http://example.invalid/${table}.parquet` } },
      sql,
      output: { format: "json" },
      ...(request.hashReducers != null ? { hashReducers: request.hashReducers } : {}),
      ...body,
    }),
    footers: [{ fileSize: file.size, footer: file.footer }],
  });
  if (planned.kind !== "json" || planned.graph.kind !== "graph") {
    throw new Error(`${sql} did not plan a JSON graph (${planned.kind}/${planned.graph.kind})`);
  }
  return { graph: planned.graph, rowCap: planned.rowCap };
}

type Cell = string | null;

const cell = (value: unknown): Cell => (value === null ? null : String(value));

/**
 * A finished job's rows, each value as its string form. Every row the graph produced: the
 * response cap (`job.rowCap`) is the API's business, not the engine's answer.
 */
export function jobRows(coordinator: WcolStreamWasm, job: JsonJob, rows: Uint8Array): Cell[][] {
  const json = JSON.parse(resultJson(coordinator, job.graph, { rows })) as { rows?: unknown[][] };
  return (json.rows ?? []).map((row) => row.map(cell));
}

/** DuckDB's answer, each value as its string form. */
export async function duckRows(duck: DuckDBConnection, sql: string): Promise<Cell[][]> {
  return ((await duck.runAndReadAll(sql)).getRows() as unknown[][]).map((row) => row.map(cell));
}

/** Rows in a fixed order, for comparing answers whose order the query leaves open. */
export function sortedRows(rows: Cell[][]): Cell[][] {
  return [...rows].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}
