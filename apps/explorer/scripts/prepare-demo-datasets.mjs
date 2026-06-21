#!/usr/bin/env node
/**
 * Stage .wcol fixtures into demo/data/ for the static demo bundle.
 * Five minimal datasets are committed in git; larger tables can be copied from data/.
 */
import { copyFileSync, existsSync, mkdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(appRoot, "../..");
const dataDir = join(repoRoot, "data");
const demoData = join(appRoot, "demo/data");

mkdirSync(demoData, { recursive: true });

/** @type {{ dest: string; sources: string[] }[]} */
const STAGE = [
  {
    dest: "crates_categories.wcol",
    sources: [join(dataDir, "crates_categories.wcol"), join(demoData, "crates_categories.wcol")],
  },
  {
    dest: "crate_maintainers.wcol",
    sources: [join(dataDir, "crate_maintainers.wcol"), join(demoData, "crate_maintainers.wcol")],
  },
  {
    dest: "trends_crate_downloads_30d.wcol",
    sources: [
      join(dataDir, "trends_crate_downloads_30d.wcol"),
      join(demoData, "trends_crate_downloads_30d.wcol"),
    ],
  },
  {
    dest: "trends_serde_version_downloads.wcol",
    sources: [
      join(dataDir, "trends_serde_version_downloads.wcol"),
      join(demoData, "trends_serde_version_downloads.wcol"),
    ],
  },
  {
    dest: "hits_subset_500k.wcol",
    sources: [
      join(dataDir, "hits_subset_500k.refactor.wcol"),
      join(dataDir, "hits_subset_500k.wcol"),
      join(dataDir, "hits_subset_500k.plan_impl.1t.wcol"),
      join(demoData, "hits_subset_500k.wcol"),
    ],
  },
];

function mb(path) {
  return `${(statSync(path).size / (1024 * 1024)).toFixed(1)} MB`;
}

let staged = 0;
let skipped = 0;

for (const { dest, sources } of STAGE) {
  const out = join(demoData, dest);
  if (existsSync(out) && statSync(out).size > 0) {
    console.log(`keep  ${dest} (${mb(out)})`);
    staged += 1;
    continue;
  }
  const src = sources.find((p) => existsSync(p) && statSync(p).size > 0);
  if (!src) {
    console.warn(`skip  ${dest} — none of: ${sources.map((p) => p.replace(repoRoot + "/", "")).join(", ")}`);
    skipped += 1;
    continue;
  }
  copyFileSync(src, out);
  console.log(`copy  ${dest} ← ${src.replace(repoRoot + "/", "")} (${mb(out)})`);
  staged += 1;
}

console.log(`\nStaged ${staged} dataset(s) in demo/data/ (${skipped} missing source).`);
if (skipped > 0) {
  console.log("Encode parquet under data/ or copy committed fixtures from demo/data/.");
}
