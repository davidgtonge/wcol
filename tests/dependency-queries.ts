import type { ExplorerQueryDef } from "./explorer-queries.ts";

const topK = (n: number) => n;

/** Runnable queries against crates_dependencies.wcol */
export const DEPENDENCY_EXPLORER_QUERIES: ExplorerQueryDef[] = [
  {
    id: "top_depended_crates",
    question: "Which crates are depended on most often?",
    category: "ranking",
    plan: {
      limit: topK(25),
      groupBy: { keys: ["dep_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, topLabelIncludes: "serde", maxMs: 30_000 },
  },
  {
    id: "depends_on_serde",
    question: "What crates depend on serde?",
    category: "search",
    plan: {
      limit: topK(25),
      filters: [{ column: "dep_crate_name", op: "=", value: "serde" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 30_000 },
  },
  {
    id: "depends_on_tokio",
    question: "Which crates depend on tokio?",
    category: "search",
    plan: {
      limit: topK(20),
      filters: [{ column: "dep_crate_name", op: "=", value: "tokio" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 30_000 },
  },
  {
    id: "browse_serde_edges",
    question: "Browse serde dependency edges",
    category: "browse",
    plan: {
      limit: topK(20),
      filters: [{ column: "dep_crate_name", op: "=", value: "serde" }],
      select: ["parent_crate_name", "dep_crate_name", "optional", "kind"],
    },
    expect: { minResults: 10, maxMs: 30_000 },
  },
  {
    id: "optional_dependencies",
    question: "Sample optional dependency edges",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [{ column: "optional", op: "=", value: true }],
      select: ["parent_crate_name", "dep_crate_name", "optional"],
    },
    expect: { minResults: 10, maxMs: 30_000 },
  },
  {
    id: "clap_dependencies",
    question: "What does clap depend on?",
    category: "profile",
    plan: {
      limit: topK(30),
      filters: [{ column: "parent_crate_name", op: "=", value: "clap" }],
      groupBy: { keys: ["dep_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 30_000 },
  },
  {
    id: "depends_on_syn",
    question: "Which crates depend on syn?",
    category: "search",
    plan: {
      limit: topK(20),
      filters: [{ column: "dep_crate_name", op: "=", value: "syn" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 30_000 },
  },
  {
    id: "depends_on_clap",
    question: "Which crates depend on clap?",
    category: "search",
    plan: {
      limit: topK(20),
      filters: [{ column: "dep_crate_name", op: "=", value: "clap" }],
      groupBy: { keys: ["parent_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 5, maxMs: 30_000 },
  },
  {
    id: "dev_dependencies",
    question: "Most common dev-dependencies",
    category: "ranking",
    plan: {
      limit: topK(25),
      filters: [{ column: "kind", op: "=", value: 1 }],
      groupBy: { keys: ["dep_crate_name"], value: "dependency_id" },
      aggregates: [{ column: "dependency_id" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 10, maxMs: 30_000 },
  },
  {
    id: "serde_optional_edges",
    question: "Optional serde dependency edges",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [
        { column: "dep_crate_name", op: "=", value: "serde" },
        { column: "optional", op: "=", value: true },
      ],
      select: ["parent_crate_name", "dep_crate_name", "optional"],
    },
    expect: { minResults: 5, maxMs: 30_000 },
  },
];
