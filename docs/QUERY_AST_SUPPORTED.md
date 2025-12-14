# Query AST — supported surface (historical)

> **Not in the product API.** The runtime uses **`QueryPlan` only** (`buildPlan` + `WcolFile.query`). This page documents a removed AST layer kept for design history.

Single-page reference for the former `QueryAst` / `validateQueryAst` surface.

## Top-level fields

| Field | Supported | Notes |
|-------|-----------|--------|
| `where` | Yes | `FilterExpr` tree |
| `groupBy` | Yes | Max 2 string column names; requires `aggregates` |
| `aggregates` | Yes | `count`, `sum`, `avg`, `min`, `max` |
| `limit` | Yes | |
| `select` | Yes | Passed through to plan where applicable |
| `rows` | Yes | `preview` \| `sample` + positive `count` |
| `orderBy` | **No** | Validation error |

## Filter operators (per column)

| Form | Maps to |
|------|---------|
| literal / shorthand | `$eq` |
| `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` | plan filter ops |
| `$in` | `in` |
| `$contains`, `$startsWith` | `like` (substring / prefix) |
| `$and`, `$or`, `$not` | combine tokens on plan |

## Rejected at validation

`join`, `joins`, `from`, `having`, `union`, `with`, `subquery`, `sql`, more than 2 group keys, `groupBy` without aggregates, unknown scalar ops.

## Caveats

- **`queryAstToPlan`** currently passes aggregate **columns** only; aggregate **function** selection follows `buildPlan` defaults until explicitly wired.
- **`queryPlanToAst`** is lossy on aggregate function names (defaults to `sum`).
- Execution path is always: AST → `QueryPlan` → WASM plan (same as `file.query(buildPlan(...))`).

See also [query-ast-and-engine-plan.md](./query-ast-and-engine-plan.md).
