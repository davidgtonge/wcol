# Row query graph: execution scope (ClickBench Q19, Q24–Q27)

**Status:** Active brief (2026-10-29)  
**Parent:** [PHASE6_ROWS_EXPORT_JOINS.md](./PHASE6_ROWS_EXPORT_JOINS.md) §6.4 (narrow slice)  
**Related:** [LATE_MATERIALIZATION.md](./LATE_MATERIALIZATION.md) — performance follow-up, **not** in this scope  

**Goal:** Make the **existing** WCOL physical row graph (node `OP_NODE_FRAGMENT` + coordinator `OP_COORDINATOR_FRAGMENT`) correctly execute five real ClickBench row queries on `hits_subset_500k.parquet`. No new SQL paths, no planner expansion for its own sake, no Parquet export / joins / reduction-tree work.

**Principles (unchanged):**

- The **physical plan is the program**; node and coordinator wire programs contain **no SQL**.
- The host only moves bytes (range GETs, WCS on graph edges).
- Correctness is judged against **DuckDB** on **1, 2, and 4** nodes.

---

## Milestones

| # | Milestone | Done when |
|---|-----------|-----------|
| **M1** | **Rust correctness** | Q19 and Q24–Q27 return correct rows via the fragment/coordinator graph at 1/2/4 nodes; programs contain no SQL; host reads reconcile with plan metrics (see [Reads](#reads-and-plan-metrics)). |
| **M2** | **WCS transport correctness** | Production transport is **per-edge decode**, not sequential complete streams. Independent producer streams are consumed **without byte corruption**, **dictionary scope leakage**, or **premature EOF**—for **all** graph edges that fan in to one consumer, not only `RootOutputKind::Rows`. |
| **M3** | **End-to-end validation** | TypeScript `runPlannedQuery` runs all five queries and compares decoded results to DuckDB, including the [formal tie rules](#duckdb-comparison-and-tie-rules). |

**Out of scope for this brief:** Parquet row export, joins, new SQL parsing paths, reduction-tree / multi-stage aggregate plumbing, late materialization (see separate doc).

---

## 1. WCS multiplexing is a graph-edge correctness issue

### Problem

If a runner **interleaves arbitrary byte chunks** from independent WCS producers into **one** ingress byte stream, frames break:

```text
Producer A: [FRAME HEADER][PAYLOAD A]
Producer B: [FRAME HEADER][PAYLOAD B]

Invalid interleaving:
[HEADER A][HEADER B][PAYLOAD A][PAYLOAD B]
```

That is invalid WCS regardless of whether the sink is a **partial reducer** or a **row coordinator**. Treating “drain each producer sequentially” as a **row-query-only** workaround hides a protocol bug on any fan-in edge.

### Additional subtlety

Dictionary IDs, stream headers, and EOF markers are **scoped to the producer**. Concatenating two individually valid streams is not necessarily one valid stream unless the consumer knows **stream boundaries** and keeps **separate decoder state** per edge.

### Required direction

**Do not** special-case `RootOutputKind::Rows` alone.

| Approach | Notes |
|----------|--------|
| **Per-edge decode (production)** | One **decoder state per input edge**; the host delivers bytes from any ready port. Producers may run concurrently; the reducer merges **decoded frames**. Sequential byte-concat is a test helper only. |

**Aggregates:** Worker partials are merged by `ReduceOp` after each producer’s `FRAME_PARTIAL`. Arrival order is not a query result. Any multi-producer WCS ingress uses per-port `WcsIngress`, never one parser over concatenated bytes.

**Rust tests:** Coordinator and reduce tests feed **one wire per port**. Chunk order and producer completion order may vary; assertions compare semantics (ordering, multiplicity, FP tolerance), not concat bytes.

**References:** `wcol/src/runtime/stream/ready-port-scheduler.ts`, `graph-runner.ts`, `rust/wcol-engine/src/exec/wcs_port.rs`, `rust/wcol-engine/src/exec/operators/wcs_frame.rs`.

---

## 2. ORDER BY columns must survive until TopK finishes

Queries like Q25–Q27 are the guardrails:

```sql
SELECT SearchPhrase
FROM hits
WHERE SearchPhrase <> ''
ORDER BY EventTime
LIMIT 10
```

`EventTime` is required for ordering but **not** in the `SELECT` list.

### Correct pipeline shape

Conceptually:

```text
Scan(SearchPhrase, EventTime)
    ↓
Filter …
    ↓
Local TopK by EventTime          -- on each node
    ↓
Exchange
    ↓
Global TopK / Sort + Limit       -- coordinator
    ↓
Project(SearchPhrase)            -- drop sort keys here only
    ↓
Rows sink
```

**Rules:**

- **No node-side `Project`** that drops sort keys before **local TopK** (physical plan audit / `clickbench_rows` already asserts this for some queries).
- **Coordinator `Project`** runs **after** global ordering/limit, not before.
- **Multi-column sort** (Q27): all sort keys stay live through both TopK stages; comparator must match **locally and on the coordinator** (types, string byte order, dictionary resolution, NULL ordering — DuckDB default **NULLS LAST** unless the query says otherwise).

**Verification:** Extend plan + runtime tests so Q25–Q27 cannot pass with premature projection.

---

## 3. DuckDB comparison and tie rules

Informal “ties are arbitrary” is right; encode it so tests distinguish **legitimate tie choice** from **TopK bugs**.

### Q19 (unordered)

- Compare **multisets** of full result rows, including **duplicate multiplicity**.

### Ordered queries (`ORDER BY … LIMIT n`)

For WCOL result `R` and DuckDB reference:

1. **Row count:** `|R| = n` (or expected count when `OFFSET` exists — not in these five queries).
2. **Sort order:** Rows in `R` are **non-decreasing** in the query’s sort key(s) under the same comparator used in operators (including NULL placement).
3. **Strictly better rows:** Let `K` be the sort key vector of the **n-th** row in DuckDB’s total order (cutoff). Every row in DuckDB’s result whose key is **strictly less** than `K` (in sort order) must appear in `R` with the **same multiplicity** as in DuckDB’s full ranked list truncated to keys `< K`.
4. **Cutoff equivalence class:** Remaining rows in `R` must be drawn from DuckDB’s set of rows whose key **equals** `K`, with multiplicities consistent (no row outside that class; no missing row from the class beyond what `LIMIT` allows).
5. **Full rows:** Compare **complete projected rows** (all output columns), not sort-key columns alone.

The existing `assert_topk` in `rust/wcol/tests/runtime/clickbench_rows.rs` (ranked keys + `full_last` tie class) is the template; align M3 TS tests with the same rules.

---

## 4. Node order is not a query requirement

- **Unordered** queries (Q19): arrival order of node streams must not affect the **multiset** result.
- **Ordered** queries: **global** TopK/sort on the coordinator defines order; node completion order must not.

Production fan-in decodes each edge independently. Node completion order is not a query result.

**Direction:** Concurrent producers + per-edge decode (M2); tests with **permuted node completion order** must pass (see regression table).

The `reverse_nodes` flag in `clickbench_rows` is a useful **stress test** for transport, not a semantic requirement—document it as such in tests.

---

## 5. Targeted regression tests

Add or keep these **in this scope** (Rust and/or TS as noted):

| Test | What it catches |
|------|------------------|
| Conflicting dictionary IDs across nodes | Cross-stream dictionary state merged incorrectly |
| `ORDER BY` on a non-output column | Premature projection before TopK |
| Identical sort keys split across nodes | Wrong global TopK merge |
| WCS frames split into **tiny chunks** (1-byte or sub-frame) | Byte-stream parser assuming chunk = frame |
| Q24 `SELECT *` with wide rows | `WCS_MAX_PAYLOAD` / batch row caps; full schema preserved |
| Different **node completion orders** | Accidental dependence on arrival order |
| `no_sql(program)` on node + coordinator bytes | SQL leaked into wire programs |
| Host **reads vs plan metrics** | Wrong fetch set or double-count (see below) |

Existing tests to preserve or extend: `conflicting_dictionary_scopes_stay_with_their_producer`, `order_by_a_non_output_column_keeps_the_key_through_topk`, `clickbench_row_queries_match_duckdb_on_one_two_and_four_nodes`.

---

## Reads and plan metrics

Keep assertions that **actual** host `ReadAt` count and bytes match the physical plan per node.

If any metric is an **estimate** or coalesced fetch span (not 1:1 with logical reads), document the mapping and assert **expected vs observed** separately—do not silently treat estimates as exact in tests.

---

## Queries in scope

From `scripts/clickbench-queries.tsv`:

| Id | SQL (abbrev) |
|----|----------------|
| Q19 | `SELECT UserID FROM hits WHERE UserID = …` |
| Q24 | `SELECT * … URL LIKE '%google%' ORDER BY EventTime LIMIT 10` |
| Q25 | `SELECT SearchPhrase … ORDER BY EventTime LIMIT 10` |
| Q26 | `SELECT SearchPhrase … ORDER BY SearchPhrase LIMIT 10` |
| Q27 | `SELECT SearchPhrase … ORDER BY EventTime, SearchPhrase LIMIT 10` |

DataFusion optimized plan for Q24 still shows a wide `TableScan` projection; WCOL lowering may decode widely until [late materialization](./LATE_MATERIALIZATION.md)—**correctness first**, performance second.

---

## Suggested implementation order

1. **M1 Rust:** Fix any remaining graph_host / TopK / projection ordering bugs; green `clickbench_rows` at 1/2/4 nodes with formal tie checks.
2. **M2 Transport:** Per-edge WCS decoding in Rust coordinator graph (if not already); align TS `runPlannedQuery` / fan-in with frame-safe merging; audit aggregate fan-in paths for the same invariant.
3. **M3 TS:** `runPlannedQuery` e2e for all five queries vs DuckDB on hits subset.

---

## What we are explicitly not doing

- Late materialization / narrow decode (separate doc).
- Parquet export sink, joins, broadcast/shuffle exchange beyond what these five queries need.
- Expanding DataFusion lowering or new SQL execution modes.
- Making producer **arrival order** part of the plan contract.
