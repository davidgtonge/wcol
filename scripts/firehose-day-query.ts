/**
 * Query one day of an analytics firehose (Parquet in an R2 bucket, `WCOL_ANALYTICS_BUCKET`).
 *
 *   npx tsx scripts/firehose-day-query.ts --date 2026/10/27
 *   npx tsx scripts/firehose-day-query.ts --date 2026-10-27 --sql "SELECT name, count(*)::int AS n FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 15"
 *
 * Requires `wrangler login`. Objects are downloaded with `wrangler r2 object get --remote`.
 */

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";
import { firehoseDayPrefix, listR2Objects } from "./r2-cloudflare-api.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BUCKET = process.env.WCOL_ANALYTICS_BUCKET;
if (!BUCKET) throw new Error("set WCOL_ANALYTICS_BUCKET to the firehose bucket");

const DEFAULT_SQL = `SELECT name, count(*)::int AS events
FROM t
GROUP BY 1
ORDER BY 2 DESC
LIMIT 25`;

function parseArgs(argv: string[]): { date: string; sql: string; keepFiles: boolean } {
  let date = "";
  let sql = DEFAULT_SQL;
  let keepFiles = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--date" && argv[i + 1]) date = argv[++i]!;
    else if (a === "--sql" && argv[i + 1]) sql = argv[++i]!;
    else if (a === "--keep") keepFiles = true;
    else if (a === "--help" || a === "-h") {
      console.log(`Usage: firehose-day-query.ts --date yyyy/MM/dd [--sql "..."] [--keep]`);
      process.exit(0);
    }
  }
  if (!date) throw new Error("--date is required (e.g. 2026/10/27)");
  return { date, sql, keepFiles };
}

function downloadObject(key: string, dest: string): void {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  const remote = `${BUCKET}/${key}`;
  const result = spawnSync(
    "npx",
    ["wrangler", "r2", "object", "get", remote, "--file", dest, "--remote"],
    { cwd: path.join(ROOT, "cloudflare"), encoding: "utf8" },
  );
  if (result.status !== 0) {
    throw new Error(`wrangler get ${remote} failed: ${result.stderr || result.stdout}`);
  }
}

async function main(): Promise<void> {
  const { date, sql, keepFiles } = parseArgs(process.argv.slice(2));
  const prefix = firehoseDayPrefix(date);
  console.log(JSON.stringify({ event: "list", bucket: BUCKET, prefix }));
  const objects = await listR2Objects(BUCKET, prefix);
  if (objects.length === 0) {
    throw new Error(`no objects under ${prefix}`);
  }
  const totalBytes = objects.reduce((s, o) => s + o.size, 0);
  console.log(
    JSON.stringify({
      event: "listed",
      files: objects.length,
      totalMiB: +(totalBytes / (1024 * 1024)).toFixed(3),
    }),
  );

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "wcol-firehose-"));
  const paths: string[] = [];
  try {
    for (const obj of objects) {
      const base = path.basename(obj.key);
      const dest = path.join(tmp, base);
      downloadObject(obj.key, dest);
      paths.push(dest);
    }
    const db = await DuckDBInstance.create();
    const conn = await db.connect();
    const listLiteral = paths.map((p) => `'${p.replace(/'/g, "''")}'`).join(", ");
    await conn.run(`CREATE VIEW t AS SELECT * FROM read_parquet([${listLiteral}])`);
    const result = await conn.runAndReadAll(sql);
    console.log(JSON.stringify({ event: "query", date, columns: result.columnNames(), rows: result.getRows() }, null, 2));
  } finally {
    if (!keepFiles) fs.rmSync(tmp, { recursive: true, force: true });
    else console.log(JSON.stringify({ event: "kept", dir: tmp }));
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
