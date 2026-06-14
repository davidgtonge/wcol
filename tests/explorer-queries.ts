import type { QueryPlan } from "../apps/explorer/demo/query/plan-types.ts";

/**
 * Catalog of crates.io explorer questions we want the product to answer.
 * Used by Node/Wasm smoke tests (no browser) and as a roadmap for UI presets.
 */
export type ExplorerQueryCategory =
  | "ranking"
  | "search"
  | "browse"
  | "profile"
  | "audit"
  | "compare"
  | "aspirational";

export type ExplorerQueryExpect = {
  /** Minimum result rows or groups returned. */
  minResults?: number;
  /** Max wall time for the query in Node smoke tests. */
  maxMs?: number;
  /** At least one group label should include this substring (case-insensitive). */
  topLabelIncludes?: string;
  /** Result should include projection columns. */
  projectionIncludes?: string[];
  /** Human note when not yet implemented or needs another dataset. */
  skip?: string;
};

/** Pre-aggregated .wcol used instead of the suite default fixture. */
export type TrendsRollupFixture = "crate_downloads_30d" | "serde_version_downloads";

export type ExplorerQueryDef = {
  id: string;
  question: string;
  category: ExplorerQueryCategory;
  plan: QueryPlan;
  expect: ExplorerQueryExpect;
  /** Trends rollup table (see scripts/prepare-trends-rollups.mjs). */
  rollup?: TrendsRollupFixture;
};

const topK = (n: number) => n;

export const EXPLORER_QUERIES: ExplorerQueryDef[] = [
  // --- Rankings (killer charts) ---
  {
    id: "top_crates_by_downloads",
    question: "Top 25 crates by total downloads across all versions",
    category: "ranking",
    plan: {
      limit: topK(25),
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 25, maxMs: 15_000, topLabelIncludes: "spider" },
  },
  {
    id: "top_licenses",
    question: "How are downloads distributed across SPDX licenses?",
    category: "ranking",
    plan: {
      limit: topK(20),
      groupBy: { keys: ["license"] },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 15_000, topLabelIncludes: "MIT" },
  },
  {
    id: "popular_mit_crates",
    question: "Most downloaded crates under the MIT license",
    category: "ranking",
    plan: {
      limit: topK(25),
      filters: [{ column: "license", op: "=", value: "MIT" }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 20_000 },
  },
  {
    id: "crates_with_yanked_versions",
    question: "Which crates have the most yanked versions?",
    category: "audit",
    plan: {
      limit: topK(25),
      filters: [{ column: "yanked", op: "=", value: true }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 20_000 },
  },
  {
    id: "edition_x_yanked",
    question: "Downloads by Rust edition and yanked flag",
    category: "ranking",
    plan: {
      limit: topK(50),
      groupBy: { keys: ["edition", "yanked"] },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 4, maxMs: 20_000 },
  },
  {
    id: "top_by_edition",
    question: "Which editions dominate download totals?",
    category: "ranking",
    plan: {
      limit: topK(10),
      groupBy: { keys: ["edition"] },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 15_000 },
  },
  {
    id: "top_apache2_crates",
    question: "Most downloaded crates under Apache-2.0",
    category: "ranking",
    plan: {
      limit: topK(20),
      filters: [{ column: "license", op: "=", value: "Apache-2.0" }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 20_000 },
  },
  {
    id: "edition_2024_popular",
    question: "Popular crates published with Rust 2024 edition",
    category: "ranking",
    plan: {
      limit: topK(25),
      filters: [{ column: "edition", op: "=", value: 2024 }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 20_000 },
  },
  {
    id: "most_versions_per_crate",
    question: "Which crates have published the most versions?",
    category: "audit",
    plan: {
      limit: topK(25),
      groupBy: { keys: ["crate_name"], value: "version_id" },
      aggregates: [{ column: "version_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 20_000 },
  },

  // --- Search ---
  {
    id: "search_crate_name_serde",
    question: "Find versions for crates matching “serde”",
    category: "search",
    plan: {
      limit: topK(50),
      filters: [{ column: "crate_name", op: "like", value: "serde" }],
    },
    expect: { minResults: 10, maxMs: 10_000 },
  },
  {
    id: "search_spider",
    question: "Find crates with “spider” in the name",
    category: "search",
    plan: {
      limit: topK(25),
      filters: [{ column: "crate_name", op: "like", value: "spider" }],
    },
    expect: { minResults: 1, maxMs: 10_000 },
  },
  {
    id: "search_reqwest",
    question: "Find versions for crates matching “reqwest”",
    category: "search",
    plan: {
      limit: topK(40),
      filters: [{ column: "crate_name", op: "like", value: "reqwest" }],
    },
    expect: { minResults: 1, maxMs: 10_000 },
  },
  {
    id: "high_download_versions",
    question: "Versions with unusually high download counts (>1M)",
    category: "search",
    plan: {
      limit: topK(100),
      filters: [{ column: "downloads", op: ">", value: 1_000_000 }],
    },
    expect: { minResults: 25, maxMs: 10_000 },
  },
  {
    id: "mega_download_versions",
    question: "Individual versions with more than 10M downloads",
    category: "search",
    plan: {
      limit: topK(50),
      filters: [{ column: "downloads", op: ">", value: 10_000_000 }],
    },
    expect: { minResults: 5, maxMs: 10_000 },
  },

  // --- Browse / table ---
  {
    id: "browse_mega_downloads",
    question: "Browse name, license, and downloads for 10M+ versions",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [{ column: "downloads", op: ">", value: 10_000_000 }],
      select: ["crate_name", "version", "license", "downloads", "yanked"],
    },
    expect: {
      minResults: 5,
      maxMs: 15_000,
      projectionIncludes: ["crate_name", "downloads", "version"],
    },
  },
  {
    id: "browse_mit_sample",
    question: "Sample MIT-licensed versions with download counts",
    category: "browse",
    plan: {
      limit: topK(40),
      filters: [{ column: "license", op: "=", value: "MIT" }],
      select: ["crate_name", "version", "license", "downloads"],
    },
    expect: { minResults: 20, maxMs: 15_000, projectionIncludes: ["crate_name", "license"] },
  },
  {
    id: "browse_yanked_high_downloads",
    question: "Yanked versions that still have high lifetime downloads",
    category: "audit",
    plan: {
      limit: topK(30),
      filters: [
        { column: "yanked", op: "=", value: true },
        { column: "downloads", op: ">", value: 100_000 },
      ],
      select: ["crate_name", "version", "downloads", "yanked"],
    },
    expect: { minResults: 5, maxMs: 15_000, projectionIncludes: ["crate_name", "version"] },
  },

  // --- Crate profile (per-crate drill-down) ---
  {
    id: "profile_serde_versions",
    question: "Version history and downloads for crate `serde`",
    category: "profile",
    plan: {
      limit: topK(100),
      filters: [{ column: "crate_name", op: "=", value: "serde" }],
      select: ["version", "license", "downloads", "yanked", "edition"],
    },
    expect: { minResults: 10, maxMs: 10_000, projectionIncludes: ["version", "downloads"] },
  },
  {
    id: "profile_tokio_versions",
    question: "Version history and downloads for crate `tokio`",
    category: "profile",
    plan: {
      limit: topK(100),
      filters: [{ column: "crate_name", op: "=", value: "tokio" }],
      select: ["version", "downloads", "yanked"],
    },
    expect: { minResults: 10, maxMs: 10_000 },
  },
  {
    id: "profile_clap_versions",
    question: "Version history and downloads for crate `clap`",
    category: "profile",
    plan: {
      limit: topK(100),
      filters: [{ column: "crate_name", op: "=", value: "clap" }],
      select: ["version", "downloads", "yanked"],
    },
    expect: { minResults: 5, maxMs: 10_000 },
  },

  // --- Compare-oriented (filter to a set of crates) ---
  {
    id: "compare_serde_tokio_reqwest",
    question: "Compare download totals for serde, tokio, and reqwest",
    category: "compare",
    plan: {
      limit: topK(10),
      filters: [{ column: "crate_name", op: "in", value: ["serde", "tokio", "reqwest"] }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 15_000 },
  },

  // --- Aspirational (product north star; may need new columns/datasets) ---
  {
    id: "fastest_growing_crates",
    question: "Fastest-growing crates in the last 30 days",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/trends-queries.ts — load version_downloads_daily.wcol" },
  },
  {
    id: "download_spikes",
    question: "Crates with unusual download spikes this week",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/trends-queries.ts — load version_downloads_daily.wcol" },
  },
  {
    id: "dependency_graph_serde",
    question: "What crates depend on serde?",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/dependency-queries.ts — load crates_dependencies.wcol" },
  },
  {
    id: "maintainer_search",
    question: "Crates by maintainer or owner",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/maintainers-queries.ts — load crate_maintainers.wcol" },
  },
  {
    id: "category_rankings",
    question: "Top crates in a crates.io category",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/categories-queries.ts — load crates_categories.wcol" },
  },
  {
    id: "version_adoption_chart",
    question: "Which versions of a crate still receive downloads?",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Implemented in tests/trends-queries.ts — load version_downloads_daily.wcol" },
  },
  {
    id: "transitive_dependency_tree",
    question: "Full transitive dependency tree for a crate",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Needs recursive graph traversal beyond flat dependency edges" },
  },
  {
    id: "maintainer_download_portfolio",
    question: "Rank maintainers by total downloads across their crates",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Needs join between crate_maintainers.wcol and crates_versions.wcol" },
  },
  {
    id: "category_download_growth",
    question: "Which categories grew fastest in the last 30 days?",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Needs join between crates_categories.wcol and version_downloads_daily.wcol" },
  },
  {
    id: "license_mismatch_audit",
    question: "Crates whose dependency licenses conflict with their own",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Needs join dependencies + versions with license columns" },
  },
  {
    id: "dep_popularity_vs_downloads",
    question: "Do highly-depended-on crates also have the most downloads?",
    category: "aspirational",
    plan: { limit: 25 },
    expect: { skip: "Needs join crates_dependencies.wcol with crates_versions.wcol" },
  },
];

export const RUNNABLE_EXPLORER_QUERIES = EXPLORER_QUERIES.filter((q) => !q.expect.skip);

export const ASPIRATIONAL_EXPLORER_QUERIES = EXPLORER_QUERIES.filter((q) => q.expect.skip);
