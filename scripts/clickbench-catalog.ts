/**
 * The ClickBench queries WCOL runs, from `scripts/clickbench-queries.tsv`. The ladder, the
 * local run and the console all read them here.
 */
import fs from "node:fs";
import path from "node:path";

export type CatalogQuery = { name: string; sql: string };

/** ClickBench queries the planner does not support yet. */
export const UNSUPPORTED = new Set(["Q18", "Q35", "Q42"]);

/** Every query in the TSV, supported or not: ClickBench's `Q…` and WCOL's own. */
export function tsvQueries(root: string): CatalogQuery[] {
  return fs
    .readFileSync(path.join(root, "scripts/clickbench-queries.tsv"), "utf8")
    .split("\n")
    .filter((line) => line.includes("\t"))
    .map((line) => {
      const [name, sql] = line.split("\t");
      return { name: name!, sql: sql!.trim().replace(/;$/, "") };
    });
}

/** Every query in the TSV the planner supports. */
export function catalogQueries(root: string): CatalogQuery[] {
  return tsvQueries(root).filter((query) => !UNSUPPORTED.has(query.name));
}

/** The supported ClickBench queries alone, in ClickBench order. */
export function clickbenchQueries(root: string): CatalogQuery[] {
  return catalogQueries(root)
    .filter((query) => /^Q\d+$/.test(query.name))
    .sort((a, b) => Number(a.name.slice(1)) - Number(b.name.slice(1)));
}
