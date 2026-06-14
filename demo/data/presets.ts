import { buildPlan } from "../wcol-query.ts";
import type { DatasetKind, PresetDef } from "../arch/types.ts";
import type { DatasetId } from "./datasets.ts";
import { TRENDS_JUNE_CUTOFF, TRENDS_LAST_30D_CUTOFF } from "./query-dates.ts";

export const CRATES_PRESETS: PresetDef[] = [
  {
    id: "topCrates",
    label: "Most downloaded crates",
    description: "Which crates have the highest total downloads across all versions?",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
    }),
  },
  {
    id: "byLicense",
    label: "Crates by license",
    description: "How are downloads distributed across SPDX licenses?",
    chartHint: "bar-v",
    plan: buildPlan({
      groupBy: { keys: ["license"] },
      aggregates: [{ column: "downloads" }],
    }),
  },
  {
    id: "mitLicense",
    label: "Popular MIT crates",
    description: "Top crates published under the MIT license",
    chartHint: "bar-h",
    plan: buildPlan({
      filters: [{ column: "license", op: "=", value: "MIT" }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
    }),
  },
  {
    id: "yankedCrates",
    label: "Crates with yanked versions",
    description: "Which crates have the most yanked versions in the index?",
    chartHint: "bar-h",
    plan: buildPlan({
      filters: [{ column: "yanked", op: "=", value: true }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
    }),
  },
  {
    id: "editionYanked",
    label: "Edition × yanked",
    description: "How do Rust editions and yanked flags affect download totals?",
    chartHint: "grouped",
    plan: buildPlan({
      groupBy: { keys: ["edition", "yanked"] },
      aggregates: [{ column: "downloads" }],
    }),
  },
  {
    id: "select",
    label: "Mega-download versions",
    description: "Browse individual versions with more than 10M downloads",
    chartHint: "table",
    plan: buildPlan({
      filters: [{ column: "downloads", op: ">", value: 10_000_000 }],
      select: ["crate_name", "license", "downloads", "version"],
    }),
  },
  {
    id: "filter",
    label: "High-download versions",
    description: "Find versions with unusually high download counts (>1M)",
    chartHint: "rows",
    plan: buildPlan({
      filters: [{ column: "downloads", op: ">", value: 1_000_000 }],
    }),
  },
];

export const DEPS_PRESETS: PresetDef[] = [
  {
    id: "topDependencies",
    label: "Most depended-on crates",
    description: "Which crates appear most often as dependencies?",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["dep_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "dependsOnSerde",
    label: "Crates that depend on serde",
    description: "Rank parent crates by serde dependency edges",
    chartHint: "bar-h",
    plan: buildPlan({
      filters: [{ column: "dep_crate_name", op: "=", value: "serde" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "tokioDependents",
    label: "Who depends on tokio?",
    description: "Parent crates with a dependency edge to tokio",
    chartHint: "bar-h",
    plan: buildPlan({
      filters: [{ column: "dep_crate_name", op: "=", value: "tokio" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "optionalDeps",
    label: "Optional dependencies",
    description: "Browse optional dependency edges",
    chartHint: "rows",
    plan: buildPlan({
      filters: [{ column: "optional", op: "=", value: true }],
      limit: 50,
    }),
  },
  {
    id: "browseEdges",
    label: "Browse dependency edges",
    description: "Sample parent → dependency pairs",
    chartHint: "table",
    plan: buildPlan({
      select: ["parent_crate_name", "dep_crate_name", "optional", "kind"],
      limit: 50,
    }),
  },
];

export const CATEGORIES_PRESETS: PresetDef[] = [
  {
    id: "topCategories",
    label: "Top categories by downloads",
    description: "Which crates.io categories drive the most download totals?",
    chartHint: "bar-v",
    plan: buildPlan({
      groupBy: { keys: ["category_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "webProgramming",
    label: "Web programming crates",
    description: "Most downloaded crates tagged Web programming",
    chartHint: "bar-h",
    plan: buildPlan({
      filters: [{ column: "category_slug", op: "=", value: "web-programming" }],
      groupBy: { keys: ["crate_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "browseCategories",
    label: "Browse crate categories",
    description: "Sample crate × category rows with download totals",
    chartHint: "table",
    plan: buildPlan({
      select: ["crate_name", "category_name", "category_slug", "crate_downloads"],
      limit: 50,
    }),
  },
];

export const MAINTAINERS_PRESETS: PresetDef[] = [
  {
    id: "topMaintainers",
    label: "Most prolific maintainers",
    description: "Which GitHub owners maintain the most crates?",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["owner_login"], value: "crate_id" },
      aggregates: [{ column: "crate_id" }],
      groupOrderByCount: true,
    }),
  },
  {
    id: "dtolnayCrates",
    label: "Crates by dtolnay",
    description: "Portfolio of crates associated with dtolnay",
    chartHint: "table",
    plan: buildPlan({
      filters: [{ column: "owner_login", op: "like", value: "dtolnay" }],
      select: ["crate_name", "owner_login", "crate_downloads"],
      limit: 50,
    }),
  },
  {
    id: "browseMaintainers",
    label: "Browse maintainer roster",
    description: "Sample crate × owner rows with download totals",
    chartHint: "table",
    plan: buildPlan({
      select: ["crate_name", "owner_login", "owner_name", "crate_downloads"],
      limit: 50,
    }),
  },
];

const TRENDS_FASTEST_GROWING_PLAN = buildPlan({
  filters: [{ column: "date", op: ">=", value: TRENDS_LAST_30D_CUTOFF }],
  groupBy: { keys: ["crate_name"], value: "downloads" },
  aggregates: [{ column: "downloads" }],
  groupOrderByCount: true,
});

const TRENDS_SERDE_VERSIONS_PLAN = buildPlan({
  filters: [{ column: "crate_name", op: "=", value: "serde" }],
  groupBy: { keys: ["version"], value: "downloads" },
  aggregates: [{ column: "downloads" }],
  groupOrderByCount: true,
});

export const TRENDS_CRATE_30D_PRESETS: PresetDef[] = [
  {
    id: "fastestGrowing",
    label: "Fastest-growing crates",
    description: "Top crates by total downloads in the last 30 days (pre-aggregated rollup)",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    }),
  },
];

export const TRENDS_SERDE_VERSIONS_PRESETS: PresetDef[] = [
  {
    id: "serdeVersionAdoption",
    label: "Serde version adoption",
    description: "Total downloads per serde version (pre-aggregated rollup)",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["version"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    }),
  },
];

export const TRENDS_PRESETS: PresetDef[] = [
  {
    id: "fastestGrowing",
    label: "Fastest-growing crates",
    description: "Crates with the most downloads in the last 30 days of daily stats",
    chartHint: "bar-h",
    plan: TRENDS_FASTEST_GROWING_PLAN,
  },
  {
    id: "serdeVersionAdoption",
    label: "Serde version adoption",
    description: "Which serde versions still receive daily downloads?",
    chartHint: "bar-h",
    plan: TRENDS_SERDE_VERSIONS_PLAN,
  },
  {
    id: "browseTrends",
    label: "Browse daily download rows",
    description: "Sample version × date download facts from the trends table",
    chartHint: "table",
    plan: buildPlan({
      filters: [{ column: "date", op: ">=", value: TRENDS_JUNE_CUTOFF }],
      select: ["date", "crate_name", "version", "downloads"],
      limit: 50,
    }),
  },
];

export const HITS_PRESETS: PresetDef[] = [
  {
    id: "filter",
    label: "Filter + preview",
    description: "Point lookup on CounterID with row preview",
    chartHint: "rows",
    plan: buildPlan({
      filters: [{ column: "CounterID", op: "=", value: 38 }],
    }),
  },
  {
    id: "select",
    label: "SELECT columns",
    description: "Late column materialization after filter",
    chartHint: "table",
    plan: buildPlan({
      filters: [{ column: "CounterID", op: "=", value: 38 }],
      select: ["CounterID", "EventDate", "URL"],
    }),
  },
  {
    id: "group1",
    label: "Group by CounterID",
    description: "Group-by aggregation on ClickBench hits",
    chartHint: "bar-h",
    plan: buildPlan({
      groupBy: { keys: ["CounterID"], value: "ResolutionWidth" },
      aggregates: [{ column: "ResolutionWidth" }],
    }),
  },
];

export function presetsForKind(kind: DatasetKind | null, datasetId?: DatasetId | null): PresetDef[] {
  if (kind === "hits") return HITS_PRESETS;
  if (kind === "dependencies") return DEPS_PRESETS;
  if (kind === "categories") return CATEGORIES_PRESETS;
  if (kind === "maintainers") return MAINTAINERS_PRESETS;
  if (kind === "trends") {
    if (datasetId === "trends-crate-30d") return TRENDS_CRATE_30D_PRESETS;
    if (datasetId === "trends-serde-versions") return TRENDS_SERDE_VERSIONS_PRESETS;
    return TRENDS_PRESETS;
  }
  return CRATES_PRESETS;
}

export function presetById(
  kind: DatasetKind | null,
  id: string,
  datasetId?: DatasetId | null
): PresetDef | undefined {
  return presetsForKind(kind, datasetId).find((p) => p.id === id);
}

export function defaultPresetId(kind: DatasetKind | null, datasetId?: DatasetId | null): string {
  if (kind === "hits") return "filter";
  if (kind === "dependencies") return "topDependencies";
  if (kind === "categories") return "topCategories";
  if (kind === "maintainers") return "topMaintainers";
  if (kind === "trends") {
    if (datasetId === "trends-serde-versions") return "serdeVersionAdoption";
    return "fastestGrowing";
  }
  return "topCrates";
}
