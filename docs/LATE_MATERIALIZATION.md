# Late materialization for row queries

**Status:** Design notes (2026-10-29) — **follow-up after** [ROW_QUERY_GRAPH_SCOPE.md](./ROW_QUERY_GRAPH_SCOPE.md)  
**Context:** [PHASE6_ROWS_EXPORT_JOINS.md](./PHASE6_ROWS_EXPORT_JOINS.md), ClickBench Q24–Q27  
**Problem:** Row queries today decode every column listed on `Scan` before `Filter` / `TopK`. For `SELECT * … ORDER BY … LIMIT n` (Q24), that is ~105 columns × ~500k rows per node shard—while DuckDB finishes in tens of milliseconds by reading predicate and sort keys first and widening rows only for survivors.

**Prerequisite:** The physical row graph must be **correct** on Q19 and Q24–Q27 (plan-driven execution, WCS fan-in, tie-aware DuckDB checks) before investing in a second decode path or planner phases.

This document collects approaches that fit WCOL’s constraints: **the physical plan is the program**, the host only does range GETs and WCS I/O, and correctness is checked against DuckDB on 1/2/4 nodes.

---

## What we do today

1. **DataFusion optimized plan** for Q24 is already narrow at the expression level (`required_source_columns` → `EventTime`, `URL`) but the `TableScan` still carries a **full `projection=[…]`** because of `SELECT *`.
2. **`lower_logical`** expands the scan to **all footer column ids** when the output is `*`, then `cover()` adds filter and sort columns (already included).
3. **Physical planning** builds `Decode { groups }` with that full column list per row group; the node runs `Decode → Filter → TopK → …`.
4. **Exchange / coordinator** may receive **wide WCS row batches** (all projected columns) even when only sort keys are needed for merge.

So the gap is not “DataFusion doesn’t know”—it’s that **our logical and physical layers treat scan columns as “columns to decode now”**, not “columns needed for this pipeline stage.”

---

## Design goals

| Goal | Notes |
|------|--------|
| Cut **bytes fetched / decompressed** on ordered row queries | Especially Q24; Q25–Q27 already decode a small set |
| Keep **plan = program** | New stages appear as explicit operators or decode phases in the plan, not SQL rediscovery at runtime |
| Preserve **distributed semantics** | Per-node TopK + coordinator merge; tie handling unchanged |
| Stay within **decode memory budgets** | Narrow phases reduce peak resident columns per batch |
| **Verify** with existing DuckDB tests | Same multiset / top-k tie rules |

Non-goals for a first slice: global optimization across nodes, spilling sort, arbitrary expression evaluation on row handles, changing aggregate partial paths (already column-pruned).

---

## Core idea: separate “pipeline columns” from “output columns”

Introduce two sets at logical lowering time (names illustrative):

- **`read_columns`** — minimum set to evaluate filters and sort keys (and any expressions in `WHERE` / `ORDER BY` not yet supported elsewhere).
- **`output_columns`** — final `SELECT` list (all ids for `*`, or the project list).

Today `Scan { columns }` is the union used for decode. Late materialization means **only `read_columns` ride through Filter / TopK**; **`output_columns \ read_columns`** are fetched in a second phase for rows that survive.

For Q24:

| Set | Example ids (hits subset) |
|-----|---------------------------|
| `read_columns` | URL (13), EventTime (4) |
| `output_columns` | 0..104 |
| **Phase-2 decode** | ~103 columns × **≤10 rows** (per node before exchange), not × 500k |

---

## Row identity: how to point at a row without materializing it

Any multi-phase plan needs a stable **row handle** within a Parquet object. Candidates:

### A. `(row_group, row_index)` — local to object

- **Pros:** Small (8–12 bytes), matches how decode already walks row groups; no need to decode extra columns for identity.
- **Cons:** Coordinator merge must know **object id + row_group + row_index**; after exchange, rematerialize reads must target the correct node’s object (today one object per table part).

Best fit for **single-object** ClickBench files and per-node shards where each node owns disjoint row groups of the same object key.

### B. `(fetch_id, row_index)` or logical read id

- Tie to planner **`logical_reads`** / piece ids already in the physical plan audit trail.
- **Pros:** Aligns with existing fetch accounting and `ReadAt` tickets.
- **Cons:** More opaque in WCS payloads; must document mapping in the plan.

### C. Surrogate key column when present

- ClickBench hits has natural keys (e.g. `WatchID`) but they are not guaranteed unique per query; do not rely on them for correctness.

**Recommendation:** Plan-carried **`RowLoc { object_id, row_group, row_index }`** (or column ids + indices) stored in a lightweight **`I64` tuple column** or a small fixed struct in the batch metadata side channel. TopK heap entries hold `RowLoc + sort key columns` (keys duplicated for compare) instead of full rows.

---

## Execution patterns

### Pattern 1 — Two decode operators on the node (minimal concept change)

Physical shape on each node:

```text
R2Fetch → Decode(read_columns) → Filter → TopK(n, keys)   // narrow
       → Rematerialize(output_columns, row_locs)            // wide, bounded by n
       → WCS egress (wide batch, ≤ n rows)
```

- **`Rematerialize`** (new `OpKind` or fragment step): given a batch of `RowLoc` and optional already-decoded columns, issue **targeted decodes** for missing output columns for those row indices only.
- Decoder changes: allow **row-index subset decode** (see below) or **second pass** over the same row group with a row bitmap / index list.

Planner emits **two decode groups** per row group: one group for `read_columns`, one for `output_columns` with **`mode: subset(rows from TopK)`** dependency on the TopK operator id.

**Tests:** Q24 1-node: `metric fetched_bytes` drops massively vs today; result still matches DuckDB.

### Pattern 2 — TopK on narrow WCS; materialize only at coordinator

Node sends **narrow frames** (sort keys + `RowLoc` + any columns in `read_columns`) to the coordinator. Coordinator merge sort → **Limit** → **Rematerialize** once for final 10 rows.

- **Pros:** Minimizes bytes on the wire between nodes; best for multi-node Q24.
- **Cons:** Coordinator must perform **random access decodes** into Parquet (or delegate “fetch these locs” back to nodes via a second RPC—probably out of scope for “host only moves bytes” unless the coordinator program embeds fetch programs).

For Cloudflare-style deployment, **coordinator rematerialize with range GETs** is acceptable if the plan lists exact byte ranges for the winning rows (planner can precompute column chunk locations from footer; execution passes row indices into a decoder API).

### Pattern 3 — Lazy columns in `OwnedWcolBatch`

Keep one decode pass structurally but add **column slots that are “lazy”**:

- Batch carries `RowLoc` per row and a map **column id → not loaded**.
- `Filter` / `TopK` touch only loaded columns.
- Project or sink calls **`ensure_column(id)`**, which decodes that column for rows still in the batch.

- **Pros:** Incremental; operators stay batch-shaped.
- **Cons:** Easy to accidentally touch a wide column too early; harder to prove peak memory in audit.

Useful as a stepping stone behind explicit plan operators (Pattern 1).

### Pattern 4 — Planner uses DataFusion `required_source_columns` for scan width

At lowering time:

```text
read_columns = required_source_columns(optimized_plan) ∪ keys_for_sort
output_columns = SELECT list (or all for *)
```

Do **not** set `Scan.columns = output_columns` when `output_columns ⊃ read_columns` and the query is a **row pipeline** (no aggregate, or before exchange).

DataFusion’s optimized Q24 scan still lists full projection; we **intentionally narrow** the WCOL scan for phase 1 and attach **`Project { columns: output_columns }`** after materialization. The WCOL logical plan becomes:

```text
Limit → Sort → Project(output) → Materialize(output \ read) → TopK? 
  → Filter → Scan(read)
```

Exact ordering depends on whether TopK sits before or after project in the distributed plan; today TopK is on nodes before exchange, project on coordinator—**materialize split should mirror that** (narrow through TopK; wide only after global limit or on finalists).

---

## Decoder / Parquet API ideas

Today decode is **row-group batch oriented** (fill batch with all columns in the group). Late materialization needs at least one of:

1. **Bitmap / index list decode:** `decode_columns_for_rows(group, column_ids, row_indices[])`.
2. **Two-speed ingest groups:** same row group, first ingest group `{URL, EventTime}`, second ingest group `{all others}` with **`depends_on`** survivor list from prior operator.
3. **Column-at-a-time with selection:** reuse `SelectionMask` from filter so phase 2 only decodes **passed rows**—still batch-friendly if filter is applied before TopK shrinks the set.

Dictionary columns: phase 1 may load URL dictionary + ids; phase 2 for wide BYTE_ARRAY columns must either **reuse dict scope** from phase 1 or reload dictionary pages (plan should list dict reads once per row group per phase).

**Memory:** Phase 2 for Q24 still touches wide pages (Title, etc.) but for **10 rows** the existing per-column window logic should stay far below group-wide full decode pressure.

---

## Physical plan / dispatch changes

| Area | Change |
|------|--------|
| `LogicalOp::Scan` | Optional `read_columns` vs `Project` output list; or explicit `Materialize { columns, input }` |
| `OpKind::Decode` | `columns` = phase columns; optional `phase: Read \| Output` |
| New `OpKind::Materialize` | Inputs: survivor batch + row locs; outputs: wide batch |
| `plan_query_dispatch` | Node program may include two decode/materialize steps; metrics split **narrow_bytes** vs **wide_bytes** |
| WCS | Optional **narrow row frame** profile (keys + loc only) vs **wide frame**; version byte in 6.1 row batches |
| Audit | `fetched_bytes` for Q24 should approach **URL+EventTime column totals**, not full file |

---

## Distributed ordered queries (Q24–Q27)

Current: each node **TopK(10)** on wide rows → coordinator **Sort** → **Limit** → **Project**.

Proposed:

1. Node: narrow **TopK(10 + tie slack)** on `(sort keys, RowLoc)`—tie slack optional if we keep DuckDB tie rule at coordinator only.
2. Exchange: ship **O(nodes × k)** narrow rows.
3. Coordinator: global sort/limit on keys → **Materialize(output_columns)** for **≤ fetch (+ ties)** rows.
4. Coordinator **Project** if sort keys should not appear in output (already the case for Q25–Q27).

For **Q27** (two sort keys), `read_columns` = `{SearchPhrase, EventTime}`; output = `{SearchPhrase}`—materialize is a no-op if project drops EventTime after sort (EventTime still in read set).

---

## Single-node fast path

When `nodes == 1`, skip exchange entirely; still use **narrow → TopK → materialize → sink**. Optionally fold limit into TopK without coordinator sort (same as today’s semantics).

This is the first milestone to implement and benchmark against DuckDB latency.

---

## Rollout phases

### Phase A — Planner-only narrow scan (no new operators)

- Lower `Scan.columns` to `read_columns` only for row queries.
- Temporarily **break Q24** unless combined with Phase B; good for Q25–Q27 if output ⊆ read (already true).

### Phase B — Node `Materialize` after TopK

- Unblocks Q24 on one node.
- Metric: `clickbench_rows` + `fetched_bytes` assertion on plan.

### Phase C — Narrow WCS + coordinator materialize

- Multi-node Q24 performance and wire size.

### Phase D — Lazy batch columns (optional refactor)

- Reduce duplicate decode logic between Materialize op and export/join paths.

---

## Verification

- Existing **`clickbench_rows`** tests (Q19, Q24–Q27) on 1/2/4 nodes.
- Plan golden: Q24 physical plan shows **two decode phases** or materialize op; **`fetched_bytes` << file size**.
- Property: for queries where `output_columns ⊆ read_columns`, materialize is elided (optimizer rule).
- Regression: **`streaming_compressed_window`**, **`bounded_memory`**—phase 2 must respect same byte budgets; wide columns only for tiny row counts.

---

## Risks and open questions

1. **RowLoc size on the wire** vs repeating sort keys in TopK heap (duplicate key storage vs indirection).
2. **Rematerialize I/O pattern:** many small range reads vs one read of whole column chunk + decode 10 rows (likely still cheaper than full group decode).
3. **SELECT *** without `ORDER BY` / `LIMIT`:** late materialization still helps if filter is selective; if not, wide output is required for all passed rows—phase 2 equals “decode rest for filtered set.”
4. **Join probe/build:** row handles may need **(scan side, row_group, row_index)** namespacing—defer until phase 6.7.
5. **Alignment with DataFusion:** we may never execute DataFusion’s physical plan; narrowing WCOL scan while DF still shows full projection is fine if documented in `CLICKBENCH_PLANS.md` for Q24.

---

## Related code (starting points)

| Piece | Location |
|-------|----------|
| SQL → DF logical | `wcol/src/plan/sql/frontend.rs` (`plan_sql`) |
| DF → WCOL logical | `wcol/src/plan/sql/wcol_logical.rs` (`cover`, `Scan { columns }`) |
| Column requirement analysis | `wcol/src/plan/sql/required.rs` |
| Physical decode groups | `wcol/src/plan/physical_plan.rs` (`resolve`, `lower_fragment`) |
| Batch decode / budgets | `wcol/src/formats/parquet/batch_decode.rs` |
| Row TopK / exchange | `PHASE6_ROWS_EXPORT_JOINS.md` §6.2, §6.4 |
| Q24 reference plans | Regenerate with `cargo run -p wcol-sql --features native --bin dump-clickbench-plans` |

---

## Summary

Late materialization for WCOL means **splitting decode into a narrow phase (filter + sort + TopK) and a wide phase (output columns for survivors only)**, with **stable row handles** carried through the pipeline and **explicit materialize steps in the physical plan**. The highest leverage target is **Q24**; the smallest proof is **1-node Q24** with **`read_columns = {URL, EventTime}`** and **`Materialize` after TopK** before WCS egress or coordinator project.
