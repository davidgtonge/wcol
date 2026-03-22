#!/usr/bin/env node
/**
 * Explain why a filtered query still scans slowly — chunk min/max vs filter bounds.
 * Usage: npm run diagnose:queries
 */
import { openCratesFile, runPlan } from "../tests/helpers/wcol-node.ts";

function epochDays(iso) {
  return Math.floor(Date.parse(`${iso}T00:00:00Z`) / 86_400_000);
}

async function sampleParquetRowGroups() {
  try {
    const { execSync } = await import("node:child_process");
    const py = `
import pyarrow.parquet as pq
pf = pq.ParquetFile('data/version_downloads_daily.parquet')
for i in [0, 1, pf.num_row_groups - 1]:
    col = pf.read_row_group(i, columns=['date']).column('date')
    import pyarrow.compute as pc
    print(f'rg {i}: min={pc.min(col).as_py()} max={pc.max(col).as_py()} rows={pf.metadata.row_group(i).num_rows}')
`;
    execSync(`python3 -c ${JSON.stringify(py)}`, { cwd: new URL("..", import.meta.url).pathname, stdio: "pipe" });
    return true;
  } catch {
    return false;
  }
}

const trendsPath = "data/version_downloads_daily.wcol";
const file = await openCratesFile(trendsPath);
const nchunks = file.header.nchunks;
const rows = Number(file.header.totalRows);

console.log(`Trends file: ${rows.toLocaleString()} rows · ${nchunks} chunks (~${Math.round(rows / nchunks).toLocaleString()} rows/chunk)\n`);

const cutoff = epochDays("2026-05-04");
const plans = [
  { label: "group-by (no filter)", plan: { limit: 10, groupBy: { keys: ["crate_name"], value: "downloads" }, aggregates: [{ column: "downloads" }], groupOrderByCount: true } },
  { label: `date >= ${cutoff} (numeric epoch)`, plan: { limit: 10, filters: [{ column: "date", op: ">=", value: cutoff }], groupBy: { keys: ["crate_name"], value: "downloads" }, aggregates: [{ column: "downloads" }], groupOrderByCount: true } },
  { label: 'date >= "2026-05-04" (ISO string)', plan: { limit: 10, filters: [{ column: "date", op: ">=", value: "2026-05-04" }], groupBy: { keys: ["crate_name"], value: "downloads" }, aggregates: [{ column: "downloads" }], groupOrderByCount: true } },
  { label: "select 5 recent rows", plan: { limit: 5, filters: [{ column: "date", op: ">=", value: cutoff }], select: ["date", "crate_name", "downloads"] } },
];

console.log("Query timings (single-threaded Wasm):");
for (const { label, plan } of plans) {
  const { result, ms } = await runPlan(file, plan);
  const count = result.groups?.keys?.length ?? result.rows?.length ?? 0;
  console.log(`  ${ms.toFixed(0).padStart(6)} ms  ${label}  → ${count} results`);
}

console.log("\nParquet row-group date span (source layout):");
if (await sampleParquetRowGroups()) {
  console.log("  (see lines above — if min/max span the full dump, chunk min/max cannot skip)");
} else {
  console.log("  (install pyarrow to sample row groups, or inspect with DuckDB)");
}

console.log(`
Why filters feel slow on trends
──────────────────────────────
1. Chunk skip uses per-chunk min/max on the filter column (see wcol-decoder filter_possible).
   Skip only when max < cutoff (for >=) or min > cutoff (for <=).

2. version_downloads_daily.parquet row groups currently span ~Mar–Jun in EACH group
   (data not sorted by date). So almost every chunk looks like it "might" match.

3. ISO date strings ("2026-05-04") used to become NaN → cast to 0 for U16 compares,
   so the row filter matched nearly everything AND chunk stats stayed unknown.
   Fixed in wcol normalizeValue (epoch day parse); rebuild browser bundle to pick up.

4. Even with a correct numeric date filter, group-by still touches every chunk:
   ~${nchunks} chunks × decode/filter/aggregate ≈ tens of seconds on 35M rows.

5. top_depended_crates has NO filter — 27M full scan (~9s) is expected.

Fixes
─────
• Rebuild parquet sorted: npm run prepare:parquet (ORDER BY date added)
• Re-encode trends .wcol and npm run prepare:datasets
• Rebuild Wasm from ../wcol after normalizeValue fix: cd ../wcol && npm run build:browser
`);
