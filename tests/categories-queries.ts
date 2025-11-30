import type { ExplorerQueryDef } from "./explorer-queries.ts";

const topK = (n: number) => n;

export const CATEGORIES_EXPLORER_QUERIES: ExplorerQueryDef[] = [
  {
    id: "top_categories_by_downloads",
    question: "Which categories drive the most crate downloads?",
    category: "ranking",
    plan: {
      limit: topK(20),
      groupBy: { keys: ["category_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 10_000 },
  },
  {
    id: "web_programming_crates",
    question: "Top crates in Web programming",
    category: "ranking",
    plan: {
      limit: topK(15),
      filters: [{ column: "category_slug", op: "=", value: "web-programming" }],
      groupBy: { keys: ["crate_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 10_000 },
  },
  {
    id: "browse_category_rows",
    question: "Browse crate category memberships",
    category: "browse",
    plan: {
      limit: topK(20),
      select: ["crate_name", "category_name", "category_slug", "crate_downloads"],
    },
    expect: { minResults: 10, maxMs: 5_000 },
  },
  {
    id: "search_serde_category",
    question: "Categories that include serde",
    category: "search",
    plan: {
      limit: topK(10),
      filters: [{ column: "crate_name", op: "=", value: "serde" }],
      select: ["crate_name", "category_name", "crate_downloads"],
    },
    expect: { minResults: 1, maxMs: 5_000 },
  },
  {
    id: "database_category_crates",
    question: "Top crates in the Database category",
    category: "ranking",
    plan: {
      limit: topK(15),
      filters: [{ column: "category_slug", op: "=", value: "database" }],
      groupBy: { keys: ["crate_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 10_000 },
  },
  {
    id: "command_line_utilities",
    question: "Top crates tagged command-line-utilities",
    category: "ranking",
    plan: {
      limit: topK(15),
      filters: [{ column: "category_slug", op: "=", value: "command-line-utilities" }],
      groupBy: { keys: ["crate_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 10_000 },
  },
  {
    id: "parser_implementations",
    question: "Top parser-implementation crates by downloads",
    category: "ranking",
    plan: {
      limit: topK(15),
      filters: [{ column: "category_slug", op: "=", value: "parser-implementations" }],
      groupBy: { keys: ["crate_name"], value: "crate_downloads" },
      aggregates: [{ column: "crate_downloads" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 10_000 },
  },
];
