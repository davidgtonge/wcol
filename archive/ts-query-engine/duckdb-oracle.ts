import { execFile } from "node:child_process";
import os from "node:os";
import path from "node:path";

import { parsePartialQueryFromWire, type PartialOracleColumn } from "./partial-oracle.ts";
import type { ParquetPartialQuery } from "./parquet.ts";
import type { WcolStreamWasm } from "../../src/runtime/host/wasm-stream.ts";

/** DuckDB on the PATH, or where its installer puts it. */
const DUCKDB_BINS = ["duckdb", path.join(os.homedir(), ".duckdb/cli/latest/duckdb")];

/** Run a DuckDB CSV statement. Tries `duckdb` on PATH, then the local CLI install. */
export function duckdbCsv(statement: string): Promise<string[][]> {
  const attempt = (index: number): Promise<string[][]> => {
    const bin = DUCKDB_BINS[index];
    if (!bin) return Promise.reject(new Error("duckdb not found"));
    return new Promise((resolve, reject) => {
      execFile(bin, ["-csv", "-c", statement], { timeout: 180_000, maxBuffer: 8_000_000 }, (err, stdout, stderr) => {
        if (err && "code" in err && err.code === "ENOENT") {
          attempt(index + 1).then(resolve, reject);
          return;
        }
        if (err) reject(new Error((stderr || err.message).trim()));
        else {
          resolve(
            stdout
              .trim()
              .split("\n")
              .slice(1)
              .filter((line) => line.length > 0)
              .map((line) => line.split(",")),
          );
        }
      });
    });
  };
  return attempt(0);
}

/** Full-file check: the graph partial matches DuckDB on the same SQL over `url`. */
export async function partialMatchesDuckdb(
  wasm: WcolStreamWasm,
  columns: PartialOracleColumn[],
  partial: Uint8Array,
  sql: string,
  url: string,
): Promise<boolean> {
  const quoted = url.replaceAll("'", "''");
  const statement = sql.replace(/ FROM hits\b/, ` FROM read_parquet('${quoted}')`);
  const rows = await duckdbCsv(statement);
  return rowsMatch(sql, parsePartialQueryFromWire(wasm, columns, partial), rows);
}

function rowsMatch(sql: string, parsed: ParquetPartialQuery, rows: string[][]): boolean {
  if (/GROUP BY/i.test(sql)) {
    const groups = parsed.query.groups;
    if (!groups) return false;
    const got = new Map<string, number>();
    groups.keys.forEach((key, index) => {
      const count = groups.values[index]?.[0]?.count ?? 0;
      if (count !== 0) got.set(String(key), count);
    });
    if (got.size !== rows.length) return false;
    return rows.every((row) => got.get(row[0] ?? "") === Number(row[1]));
  }
  const row = rows[0];
  if (!row) return false;
  const aggs = parsed.query.aggregates;
  if (row.length >= 3) {
    const sum = aggs["sum(AdvEngineID)"];
    const count = aggs["count_star()"];
    const avg = aggs["avg(ResolutionWidth)"];
    if (!sum || !count || !avg) return false;
    return sum.sum === Number(row[0]) && count.count === Number(row[1]) && Math.abs(avg.mean - Number(row[2])) < 1e-6;
  }
  const count = aggs["count_star()"]?.count ?? parsed.rows;
  return count === Number(row[0]);
}
