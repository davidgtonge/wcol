#!/usr/bin/env node
/**
 * Build pre-aggregated trends rollups from version_downloads_daily.parquet.
 * Matches demo/data/query-dates.ts (last 30d ends 2026-06-03).
 */
import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dataDir = join(root, "data");
const daily = join(dataDir, "version_downloads_daily.parquet");
const crate30d = join(dataDir, "trends_crate_downloads_30d.parquet");
const serdeVersions = join(dataDir, "trends_serde_version_downloads.parquet");

const TRENDS_LAST_30D = "2026-05-04";

if (!existsSync(daily)) {
  console.error(`Missing ${daily} — run npm run prepare:parquet first.`);
  process.exit(1);
}

console.log("==> trends_crate_downloads_30d.parquet");
execSync(
  `duckdb -c "COPY (SELECT crate_name, CAST(SUM(downloads) AS BIGINT) AS downloads FROM read_parquet('${daily}') WHERE date >= DATE '${TRENDS_LAST_30D}' GROUP BY crate_name ORDER BY downloads DESC) TO '${crate30d}' (FORMAT PARQUET)"`,
  { stdio: "inherit" }
);
console.log("==> trends_serde_version_downloads.parquet");
execSync(
  `duckdb -c "COPY (SELECT version, CAST(SUM(downloads) AS BIGINT) AS downloads FROM read_parquet('${daily}') WHERE crate_name = 'serde' GROUP BY version ORDER BY downloads DESC) TO '${serdeVersions}' (FORMAT PARQUET)"`,
  { stdio: "inherit" }
);
console.log("\nRollup parquet written. Encode with: npm run encode:rollups");
