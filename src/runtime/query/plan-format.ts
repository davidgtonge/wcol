/**
 * Plan format: filters and aggregations the runtime understands.
 * Use this shape when outputting a query plan (e.g. from SQL parser or API).
 *
 * Pass the result to file.query(plan).
 *
 * Filter operators: "=" | "==" | "eq" | "!=" | "neq" | "<" | "lt" | "<=" | "lte" | ">" | "gt" | ">=" | "gte" | "between" | "in"
 * - For "in", value must be an array of scalars (e.g. [1, 2] or ["a", "b"] for dict columns).
 * - For "between", use value and value2.
 * - Column: string (schema name) or number (column id).
 *
 * Aggregates: one entry per column; runtime computes count, sum, min, max, mean for each.
 * Column must be numeric (no string/dict columns).
 *
 * GroupBy: keys = array of column refs (up to 2); optional value column.
 *
 * select: column refs to return as row values (filter + limit queries only; no groupBy/aggregates).
 * Values are materialized after the scan via a second pass over only the chunks that contain result rows.
 */

import type { AggregateSpec, FilterSpec, GroupBySpec, QueryPlan } from "../core/types.ts";

export type { FilterSpec, AggregateSpec, GroupBySpec, QueryPlan };

/** Filter op tokens the runtime accepts (see ops.ts normalizeOp). */
export const FILTER_OPS = [
  "=",
  "==",
  "eq",
  "!=",
  "neq",
  "<",
  "lt",
  "<=",
  "lte",
  ">",
  "gt",
  ">=",
  "gte",
  "between",
  "in",
  "like",
  "not_like",
] as const;

/**
 * Build a QueryPlan from a minimal spec. Use this to normalize output before passing to file.query(plan).
 * Column refs can be string (name) or number (id). Filters and aggregates are applied in array order.
 */
export function buildPlan(spec: {
  limit?: number;
  filters?: FilterSpec[];
  combine?: (number | string)[];
  groupBy?: GroupBySpec;
  aggregates?: AggregateSpec[];
  groupOrderByCount?: boolean;
  select?: (number | string)[];
}): QueryPlan {
  return {
    limit: spec.limit,
    filters: spec.filters?.length ? spec.filters : undefined,
    combine: spec.combine?.length ? spec.combine : undefined,
    groupBy: spec.groupBy,
    aggregates: spec.aggregates?.length ? spec.aggregates : undefined,
    groupOrderByCount: spec.groupOrderByCount,
    select: spec.select?.length ? spec.select : undefined,
  };
}

/**
 * Example plan (JSON-serializable). Emit this shape from Rust/API so the runtime can run it.
 *
 * {
 *   "limit": 10,
 *   "filters": [
 *     { "column": "CounterID", "op": "=", "value": 62 },
 *     { "column": "EventDate", "op": ">=", "value": "2013-07-01" },
 *     { "column": "EventDate", "op": "<=", "value": "2013-07-31" },
 *     { "column": "TraficSourceID", "op": "in", "value": [-1, 6] }
 *   ],
 *   "combine": ["AND"],
 *   "groupBy": { "keys": ["URLHash", "EventDate"] },
 *   "aggregates": [
 *     { "column": "ResolutionWidth" },
 *     { "col": "UserID" }
 *   ]
 * }
 */
export const EXAMPLE_PLAN: QueryPlan = {
  limit: 10,
  filters: [
    { column: "CounterID", op: "=", value: 62 },
    { column: "EventDate", op: ">=", value: "2013-07-01" },
    { column: "EventDate", op: "<=", value: "2013-07-31" },
    { column: "TraficSourceID", op: "in", value: [-1, 6] },
  ],
  combine: ["AND"],
  groupBy: { keys: ["URLHash", "EventDate"] },
  aggregates: [{ column: "ResolutionWidth" }, { col: "UserID" }],
};
