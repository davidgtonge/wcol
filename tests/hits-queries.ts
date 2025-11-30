import type { ExplorerQueryDef } from "./explorer-queries.ts";

const topK = (n: number) => n;

/** ClickBench hits subset — hits_subset_500k.wcol */
export const HITS_EXPLORER_QUERIES: ExplorerQueryDef[] = [
  {
    id: "filter_counter",
    question: "Filter events for CounterID 38",
    category: "search",
    plan: {
      limit: topK(25),
      filters: [{ column: "CounterID", op: "=", value: 38 }],
    },
    expect: { minResults: 1, maxMs: 5_000 },
  },
  {
    id: "group_by_counter",
    question: "Group hits by CounterID",
    category: "ranking",
    plan: {
      limit: topK(20),
      groupBy: { keys: ["CounterID"], value: "ResolutionWidth" },
      aggregates: [{ column: "ResolutionWidth" }],
      groupOrderByCount: true,
    },
    expect: { minResults: 3, maxMs: 10_000 },
  },
  {
    id: "browse_url_sample",
    question: "Browse URL and event date for a counter",
    category: "browse",
    plan: {
      limit: topK(30),
      filters: [{ column: "CounterID", op: "=", value: 38 }],
      select: ["CounterID", "EventDate", "URL", "ResolutionWidth"],
    },
    expect: { minResults: 1, maxMs: 5_000, projectionIncludes: ["URL"] },
  },
];
