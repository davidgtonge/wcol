import type { ExplorerQueryDef } from "./explorer-queries.ts";
import {
  TRENDS_JUNE_CUTOFF,
  TRENDS_LAST_30D_CUTOFF,
  TRENDS_LAST_WEEK_CUTOFF,
  TRENDS_MAY_CUTOFF,
  TRENDS_MID_MAY_CUTOFF,
} from "../apps/explorer/demo/data/query-dates.ts";

const topK = (n: number) => n;

/** Daily download trends — version_downloads_daily.wcol */
export const TRENDS_EXPLORER_QUERIES: ExplorerQueryDef[] = [
  {
    id: "fastest_growing_crates",
    question: "Fastest-growing crates in the last 30 days of daily stats",
    category: "ranking",
    rollup: "crate_downloads_30d",
    plan: {
      limit: topK(25),
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 3_000, topLabelIncludes: "syn" },
  },
  {
    id: "serde_version_adoption",
    question: "Which serde versions still receive daily downloads?",
    category: "profile",
    rollup: "serde_version_downloads",
    plan: {
      limit: topK(20),
      groupBy: { keys: ["version"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 1_000 },
  },
  {
    id: "tokio_recent_daily",
    question: "Recent daily download rows for tokio",
    category: "browse",
    plan: {
      limit: topK(25),
      filters: [
        { column: "crate_name", op: "=", value: "tokio" },
        { column: "date", op: ">=", value: TRENDS_MAY_CUTOFF },
      ],
      select: ["date", "crate_name", "version", "downloads"],
    },
    expect: { minResults: 5, maxMs: 15_000, projectionIncludes: ["crate_name", "downloads"] },
  },
  {
    id: "browse_recent_trends",
    question: "Browse recent daily download facts",
    category: "browse",
    plan: {
      limit: topK(25),
      filters: [{ column: "date", op: ">=", value: TRENDS_JUNE_CUTOFF }],
      select: ["date", "crate_name", "version", "downloads"],
    },
    expect: { minResults: 10, maxMs: 10_000, projectionIncludes: ["crate_name", "version"] },
  },
  {
    id: "download_spikes_week",
    question: "Crates with the most downloads in the latest week of stats",
    category: "ranking",
    plan: {
      limit: topK(20),
      filters: [{ column: "date", op: ">=", value: TRENDS_LAST_WEEK_CUTOFF }],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 30_000 },
  },
  {
    id: "compare_serde_tokio_daily",
    question: "Compare recent daily downloads for serde vs tokio",
    category: "compare",
    plan: {
      limit: topK(10),
      filters: [
        { column: "date", op: ">=", value: TRENDS_MAY_CUTOFF },
        { column: "crate_name", op: "in", value: ["serde", "tokio"] },
      ],
      groupBy: { keys: ["crate_name"], value: "downloads" },
      aggregates: [{ column: "downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 2, maxMs: 30_000 },
  },
  {
    id: "clap_recent_daily",
    question: "Recent daily download rows for clap",
    category: "profile",
    plan: {
      limit: topK(40),
      filters: [
        { column: "crate_name", op: "=", value: "clap" },
        { column: "date", op: ">=", value: TRENDS_MID_MAY_CUTOFF },
      ],
      select: ["date", "crate_name", "version", "downloads"],
    },
    expect: { minResults: 5, maxMs: 15_000, projectionIncludes: ["version", "downloads"] },
  },
  {
    id: "reqwest_daily_table",
    question: "Browse reqwest daily download facts",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [{ column: "crate_name", op: "=", value: "reqwest" }],
      select: ["date", "crate_name", "version", "downloads"],
    },
    expect: { minResults: 5, maxMs: 15_000, projectionIncludes: ["crate_name", "version"] },
  },
];
