import { HttpRangeSource } from "../wcol-query.ts";
import type { DatasetKind } from "../arch/types.ts";

export type DatasetId =
  | "crates-versions"
  | "crates-dependencies"
  | "crates-categories"
  | "crate-maintainers"
  | "version-downloads-daily"
  | "trends-crate-30d"
  | "trends-serde-versions"
  | "clickbench-hits-500k";

export type DemoDataset = {
  id: DatasetId;
  kind: DatasetKind;
  title: string;
  label: string;
  description: string;
  filename: string;
  rowsHint: string;
  sizeHint: string;
  /** Primary sample button in the load panel. */
  featured?: boolean;
};

/** Bundled demo datasets (staged into demo/data/ by prepare-demo-datasets.mjs). */
export const DEMO_DATASETS: DemoDataset[] = [
  {
    id: "crates-versions",
    kind: "crates",
    title: "Crates.io versions",
    label: "crates_versions.wcol",
    description: "Every published crate version — downloads, license, edition, yanked flag.",
    filename: "crates_versions.wcol",
    rowsHint: "~2.4M versions",
    sizeHint: "~71 MB",
    featured: true,
  },
  {
    id: "crates-dependencies",
    kind: "dependencies",
    title: "Crate dependency graph",
    label: "crates_dependencies.wcol",
    description: "27M dependency edges — who depends on whom, optional vs required.",
    filename: "crates_dependencies.wcol",
    rowsHint: "~27M edges",
    sizeHint: "~242 MB",
  },
  {
    id: "crates-categories",
    kind: "categories",
    title: "Crate categories",
    label: "crates_categories.wcol",
    description: "Crate × category memberships — rank crates within a category.",
    filename: "crates_categories.wcol",
    rowsHint: "~237k rows",
    sizeHint: "~3.5 MB",
  },
  {
    id: "crate-maintainers",
    kind: "maintainers",
    title: "Crate maintainers",
    label: "crate_maintainers.wcol",
    description: "Crate owners and teams — search by GitHub login, browse portfolios.",
    filename: "crate_maintainers.wcol",
    rowsHint: "~307k rows",
    sizeHint: "~9 MB",
  },
  {
    id: "version-downloads-daily",
    kind: "trends",
    title: "Daily download trends",
    label: "version_downloads_daily.wcol",
    description: "35M version × day download facts — browse rows, weekly spikes, and date filters.",
    filename: "version_downloads_daily.wcol",
    rowsHint: "~35M rows",
    sizeHint: "~467 MB",
  },
  {
    id: "trends-crate-30d",
    kind: "trends",
    title: "Crate totals (last 30 days)",
    label: "trends_crate_downloads_30d.wcol",
    description: "Pre-aggregated downloads per crate since 2026-05-04 — fast “fastest growing” rankings.",
    filename: "trends_crate_downloads_30d.wcol",
    rowsHint: "~271k crates",
    sizeHint: "~4 MB",
    featured: true,
  },
  {
    id: "trends-serde-versions",
    kind: "trends",
    title: "Serde version totals",
    label: "trends_serde_version_downloads.wcol",
    description: "Total downloads per serde version — instant version adoption chart.",
    filename: "trends_serde_version_downloads.wcol",
    rowsHint: "~315 versions",
    sizeHint: "~4 KB",
  },
  {
    id: "clickbench-hits-500k",
    kind: "hits",
    title: "ClickBench hits (500k)",
    label: "hits_subset_500k.wcol",
    description: "Web analytics event log — filters, group-bys, and late SELECT projection.",
    filename: "hits_subset_500k.wcol",
    rowsHint: "500k events",
    sizeHint: "~35 MB",
  },
];

export function datasetById(id: string): DemoDataset | undefined {
  return DEMO_DATASETS.find((d) => d.id === id);
}

/** Resolve a bundled sample id (or legacy `"sample"`) to a range-fetchable source. */
export function resolveSampleSource(id: string): { byteSource: HttpRangeSource; label: string } {
  const normalized = id === "sample" ? "crates-versions" : id;
  const ds = datasetById(normalized) ?? DEMO_DATASETS[0];
  const url = new URL(`../data/${ds.filename}`, import.meta.url).href;
  return { byteSource: new HttpRangeSource(url), label: ds.label };
}

/** Worker OpenSource string from a dataset id. */
export function sampleSourceToken(id: DatasetId): string {
  return `sample:${id}`;
}

export function parseSampleSourceToken(source: string): string | null {
  if (source === "sample") return "crates-versions";
  if (source.startsWith("sample:")) return source.slice("sample:".length);
  return null;
}
