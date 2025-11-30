import type { ExplorerQueryDef } from "./explorer-queries.ts";
import {
  ASPIRATIONAL_EXPLORER_QUERIES,
  RUNNABLE_EXPLORER_QUERIES,
} from "./explorer-queries.ts";
import { DEPENDENCY_EXPLORER_QUERIES } from "./dependency-queries.ts";
import { CATEGORIES_EXPLORER_QUERIES } from "./categories-queries.ts";
import { MAINTAINERS_EXPLORER_QUERIES } from "./maintainers-queries.ts";
import { TRENDS_EXPLORER_QUERIES } from "./trends-queries.ts";
import { HITS_EXPLORER_QUERIES } from "./hits-queries.ts";
import {
  defaultCategoriesFixture,
  defaultCratesFixture,
  defaultDepsFixture,
  defaultHitsFixture,
  defaultMaintainersFixture,
  defaultTrendsFixture,
} from "./helpers/wcol-node.ts";

export type QuerySuiteDef = {
  id: string;
  label: string;
  rowsHint: string;
  fixture: () => Promise<string>;
  queries: ExplorerQueryDef[];
};

export const QUERY_SUITES: QuerySuiteDef[] = [
  {
    id: "crates",
    label: "crates_versions.wcol",
    rowsHint: "~2.4M",
    fixture: defaultCratesFixture,
    queries: RUNNABLE_EXPLORER_QUERIES,
  },
  {
    id: "dependencies",
    label: "crates_dependencies.wcol",
    rowsHint: "~27M",
    fixture: defaultDepsFixture,
    queries: DEPENDENCY_EXPLORER_QUERIES,
  },
  {
    id: "categories",
    label: "crates_categories.wcol",
    rowsHint: "~237k",
    fixture: defaultCategoriesFixture,
    queries: CATEGORIES_EXPLORER_QUERIES,
  },
  {
    id: "maintainers",
    label: "crate_maintainers.wcol",
    rowsHint: "~307k",
    fixture: defaultMaintainersFixture,
    queries: MAINTAINERS_EXPLORER_QUERIES,
  },
  {
    id: "trends",
    label: "version_downloads_daily.wcol",
    rowsHint: "~35M",
    fixture: defaultTrendsFixture,
    queries: TRENDS_EXPLORER_QUERIES,
  },
  {
    id: "hits",
    label: "hits_subset_500k.wcol",
    rowsHint: "500k",
    fixture: defaultHitsFixture,
    queries: HITS_EXPLORER_QUERIES,
  },
];

export type CatalogQuery = ExplorerQueryDef & {
  suiteId: string;
  dataset: string;
};

export const ALL_RUNNABLE_QUERIES: CatalogQuery[] = QUERY_SUITES.flatMap((suite) =>
  suite.queries.map((query) => ({
    ...query,
    suiteId: suite.id,
    dataset: suite.label,
  }))
);

export const ALL_ASPIRATIONAL_QUERIES = ASPIRATIONAL_EXPLORER_QUERIES;

export function queryCountBySuite(): Record<string, number> {
  return Object.fromEntries(QUERY_SUITES.map((s) => [s.id, s.queries.length]));
}
