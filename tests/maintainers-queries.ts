import type { ExplorerQueryDef } from "./explorer-queries.ts";

const topK = (n: number) => n;

export const MAINTAINERS_EXPLORER_QUERIES: ExplorerQueryDef[] = [
  {
    id: "top_maintainers_by_crate_count",
    question: "Which maintainers own the most crates?",
    category: "ranking",
    plan: {
      limit: topK(25),
      groupBy: { keys: ["owner_login"], value: "crate_id" },
      aggregates: [{ column: "crate_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 10_000 },
  },
  {
    id: "dtolnay_portfolio",
    question: "Is dtolnay a maintainer of serde?",
    category: "search",
    plan: {
      limit: topK(5),
      filters: [
        { column: "owner_login", op: "like", value: "dtolnay" },
        { column: "crate_name", op: "=", value: "serde" },
      ],
      select: ["crate_name", "owner_login", "crate_downloads"],
    },
    expect: { minResults: 1, maxMs: 10_000 },
  },
  {
    id: "dtolnay_crate_list",
    question: "Browse dtolnay’s crate portfolio",
    category: "browse",
    plan: {
      limit: topK(50),
      filters: [{ column: "owner_login", op: "like", value: "dtolnay" }],
      select: ["crate_name", "owner_login", "crate_downloads"],
    },
    expect: { minResults: 10, maxMs: 10_000 },
  },
  {
    id: "search_maintainer_like",
    question: "Maintainers matching “rust”",
    category: "search",
    plan: {
      limit: topK(20),
      filters: [{ column: "owner_login", op: "like", value: "rust" }],
      groupBy: { keys: ["owner_login"], value: "crate_id" },
      aggregates: [{ column: "crate_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 10_000 },
  },
  {
    id: "browse_maintainer_rows",
    question: "Browse crate owner roster",
    category: "browse",
    plan: {
      limit: topK(25),
      select: ["crate_name", "owner_login", "owner_name", "crate_downloads"],
    },
    expect: { minResults: 10, maxMs: 5_000 },
  },
  {
    id: "top_maintainers_by_downloads",
    question: "Which maintainers oversee the highest total crate downloads?",
    category: "ranking",
    plan: {
      limit: topK(20),
      groupBy: { keys: ["owner_login"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 15_000 },
  },
  {
    id: "team_owned_crates",
    question: "Crates owned by teams (not individual users)",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [{ column: "owner_kind", op: "=", value: 1 }],
      select: ["crate_name", "owner_login", "owner_name", "crate_downloads"],
    },
    expect: { minResults: 5, maxMs: 10_000, projectionIncludes: ["crate_name", "owner_login"] },
  },
  {
    id: "serde_maintainers",
    question: "Who maintains serde?",
    category: "profile",
    plan: {
      limit: topK(10),
      filters: [{ column: "crate_name", op: "=", value: "serde" }],
      select: ["crate_name", "owner_login", "owner_name", "crate_downloads"],
    },
    expect: { minResults: 1, maxMs: 5_000 },
  },
];
