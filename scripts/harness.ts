/**
 * What the build and the ClickBench runners share: the repository root, assertions, job
 * polling, and DuckDB's rows for the oracle.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export type HashSchedule = { coordinatorShuffleBytes?: number; openedEdges?: number };

/** What `POST /jobs` and `GET /jobs/:id` answer, as far as the runners read it. */
export type JobRecord = {
  jobId?: string;
  state?: string;
  error?: string;
  nodeCount?: number;
  rows?: unknown[][];
  truncated?: boolean;
  result?: { rows?: unknown[][]; truncated?: boolean };
  outputObjects?: number;
  outputBytes?: number;
  schedule?: HashSchedule;
};

export type GetJob = (jobId: string) => Promise<JobRecord>;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Poll until the job completes or fails; throws once `timeoutMs` passes first. */
export async function waitForJob(get: GetJob, jobId: string, timeoutMs: number, pollMs = 400): Promise<JobRecord> {
  const started = Date.now();
  for (;;) {
    const record = await get(jobId);
    if (record.state === "completed" || record.state === "failed") return record;
    if (Date.now() - started > timeoutMs) throw new Error(`job ${jobId} timed out in ${record.state ?? "unknown"}`);
    await sleep(pollMs);
  }
}

/** `timing.json` is written in the background after a job ends: poll briefly for its schedule. */
export async function scheduleOf(get: GetJob, jobId: string, waitMs = 5_000): Promise<HashSchedule | undefined> {
  const deadline = Date.now() + waitMs;
  for (;;) {
    const schedule = (await get(jobId)).schedule;
    if (schedule || Date.now() > deadline) return schedule;
    await sleep(250);
  }
}

export const jobRows = (record: JobRecord): unknown[][] => record.rows ?? record.result?.rows ?? [];

/** Only a grouped or distinct query hash-partitions; an ungrouped approx sketch does not. */
export function hashPartitions(sql: string): boolean {
  return /\bgroup\s+by\b/i.test(sql) || /count\s*\(\s*distinct\b/i.test(sql);
}

/** Cells as the oracle compares them. */
export function asText(rows: readonly unknown[][]): string[][] {
  return rows.map((row) => row.map((value) => (value === null ? "null" : String(value))));
}

/**
 * DuckDB over one local Parquet file as `hits`. Capped: an uncapped DuckDB oracle on full
 * hits once exhausted the machine.
 */
export async function localDuckDb(file: string): Promise<(sql: string) => Promise<string[][]>> {
  const { DuckDBInstance } = await import("@duckdb/node-api");
  const duck = await (await DuckDBInstance.create(":memory:")).connect();
  await duck.run(`SET memory_limit='${process.env.ORACLE_MEMORY ?? "4GB"}'`);
  await duck.run(`CREATE VIEW hits AS SELECT * FROM read_parquet('${file.replaceAll("'", "''")}')`);
  return async (sql) => asText((await duck.runAndReadAll(sql)).getRows() as unknown[][]);
}
