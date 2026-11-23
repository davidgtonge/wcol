# Physical planner fixes

**Status:** Implemented (2026-10-29)  
**Code:** `rust/wcol/src/plan/physical_plan.rs` (`lower_to_physical`), `rust/wcol-sql/src/wcol_logical.rs` (DataFusion → WCOL logical), `rust/wcol-sql/src/bin/dump_clickbench_plans.rs`.  
**Output today:** [CLICKBENCH_PLANS.md](./CLICKBENCH_PLANS.md). 21 of 52 queries reach a physical plan.

The planner is deterministic, pure, and its byte ranges match the footer: for `AdvEngineID` each read starts at the dictionary page and spans `total_compressed_size`. This plan fixes what the review found before [PHYSICAL_PLAN_EXECUTION.md](./PHYSICAL_PLAN_EXECUTION.md) makes the planner the source of execution.

Each fix below lands with its own tests. Regenerate `CLICKBENCH_PLANS.md` after each one and read the diff: it is the review artefact.

## 1. Balanced node placement

**Problem:** `assign` walks row-group units in file order and moves to the next node as soon as adding a unit would pass `remaining / nodes_left`. The node that stops early is light, and the last node absorbs the rest.

| Query | Node 0 bytes | Node 1 bytes | Best contiguous split |
|-------|-------------:|-------------:|----------------------|
| Q1 | 6,374 | 18,514 | 12,771 / 12,117 |
| Q2 | 29,247 | 86,063 | ≈ 58k / 57k |

**Fix:** contiguous linear partition that minimises the largest node's bytes.

- Units stay in canonical order (object, offset, row group), so adjacent row groups stay on one node and fetch coalescing still works.
- Binary-search the smallest feasible max load `L` (greedy fill: a node takes units while `used + w ≤ L`). That is `O(n log Σw)`, deterministic, and needs no floating point.
- Tie-break: among splits with the same max load, the greedy fill gives the earliest cut points. State that in the doc comment so it is part of the determinism contract.
- More nodes than units: nodes past the last unit get nothing and emit no operators. The metric lists only nodes with work, as now.
- Weight stays query bytes (`Σ` read lengths, at least 1). Keep a note that decode cost tracks rows as well as bytes; revisit only with measurements.

**Tests:**
- Property test against a brute-force partition for up to 8 units and 1–4 nodes: same max load.
- Q1 and Q2 balance pinned in `CLICKBENCH_PLANS.md`.
- Permuting the input `chunks` order gives the same plan bytes.

## 2. Fetches never exceed `max_request_bytes`

**Problem:** coalescing allows one fetch per column chunk even when the chunk is larger than the cap (`legal_oversize`). S10's `Title` reads are about 5.5 MB per fetch on the 500k subset, and full-size hits row groups make that worse. Criterion 6 says fetches are bounded.

**Fix:** split, don't exempt.

- A logical read maps to an ordered list of fetch pieces: `LogicalRead.fetches: Vec<FetchPiece { fetch_id, fetch_offset, read_offset, length }>`, covering the read exactly once, in byte order.
- Coalescing works on pieces:
  1. Cut each read into pieces of at most `max_request_bytes`. Cut at fixed multiples from the read start so plans stay deterministic; the decoder streams pages, so a cut can fall mid-page.
  2. Merge neighbouring pieces with the existing gap and size rules. A merged fetch may hold the tail of one read and the head of the next.
- Delete `legal_oversize`. `audit` then checks `fetch.length ≤ max_request_bytes` for every fetch, and that each read's pieces tile it without gaps or overlap.
- `canonical_text` prints the piece list on each `read` line.

**Tests:**
- A read of `3.5 × max_request_bytes` becomes 4 pieces in 4 fetches, and the pieces reassemble the read.
- Two small reads separated by a gap under `max_gap_bytes` still merge.
- Random reads, gaps and caps (property test): every fetch is within the cap, every read is tiled exactly, and over-read equals the sum of merged gaps.

## 3. Explicit exchange on every cross-node edge

**Problem:** `gather` returns a lone producer directly, so with one node the coordinator's `final_aggregate` reads a node operator with no `exchange` between them. The plan is meant to state its exchange boundaries.

**Fix:**
- `gather` always emits `Exchange`, even for one producer.
- `audit` rule: an operator's inputs run on the same node as the operator, or the input is an `Exchange` (whose own inputs may be anywhere). This also catches a join or sort fed directly from a node.

**Tests:**
- Single-node plans for an aggregate, a row query and a join each show one exchange per side.
- `audit` rejects a hand-built plan with a bare cross-node edge.

## 4. Metadata-only `COUNT(*)`

**Problem:** Q0 emits decode operators with `columns=[] reads=[]` on every node. The current engine answers `COUNT(*)` without filters from footer row counts.

**Fix:**
- When the scan needs no columns and has no filter, emit one coordinator `MetadataCount { rows }` operator (rows summed from the catalog) and no node work.
- Keep the rule narrow: any predicate or column reference falls back to the normal path.

**Test:** Q0's plan is one operator with `rows=500000` and zero requests.

## 5. Row queries: per-node top-k

**Problem:** for `ORDER BY … LIMIT n` on rows, nodes send every row through the exchange and the coordinator sorts all of them.

**Fix:**
- When a `Sort` is directly followed by `Limit { skip, fetch: Some(n) }` over a row scan, each node fragment ends with `TopK { keys, n: skip + n }`.
- The coordinator keeps `Sort` and `Limit` above the exchange.
- Grouped `ORDER BY COUNT(*) LIMIT` does not push down: groups are incomplete on a node.

**Tests:** plans with and without a limit; `skip` is folded into the node bound; the coordinator still applies `skip`.

## 6. Lowering coverage (DataFusion → WCOL logical)

31 queries stop before a physical plan. Most are lowering or catalog gaps; the current SQL path runs 48 of the 52 against DuckDB. Fix in this order, regenerating the doc after each:

| Gap | Queries | Fix |
|-----|---------|-----|
| `COUNT(DISTINCT x)` | Q4, Q5, Q8, Q10, Q11, Q13, S2 | DataFusion rewrites it to `Aggregate(group+alias1) → Aggregate(count(alias1))`. Recognise that two-level shape and lower it to the engine's `COUNT(DISTINCT)` (which already runs as a hidden last group key). |
| Date literals on `EventDate` | Q36–Q38, Q40, Q41, D1, D2, D4 | The catalog gives DataFusion `Int32`. Expose Parquet `DATE` logical types as `Date32` so `'2013-07-01'` casts; lower `Date32` literals to the day count the scan produces. |
| `approx_count_distinct` | A1–A4 | Register it as an alias of DataFusion's `approx_distinct` and lower that to the HLL aggregate. |
| `LIKE` / `NOT LIKE` | Q20, S4, S5, S9, S11 | Lower to the existing `Predicate::Like`; the batch engine already runs it. |
| `__common_expr_2` | Q29 | Lower a `Projection` of column references through the common-subexpression rewrite, or disable that optimizer rule for now. |
| `COUNT(...)` with options | Q9 | Look at what option DataFusion attaches; lower plain `DISTINCT`, reject the rest. |
| `CASE` from `IS NOT NULL` | S8 | Simplify `CASE WHEN x IS NOT NULL THEN true END` to a null check, or reject with a clear message. |
| `todatetime`, `date_trunc`, expressions | Q18, Q35, Q42, S3 | Real feature work. Out of scope here; keep the reasons in the doc table. |

**Parity gate:** for every query both frontends accept, the WCOL logical plan from DataFusion and the one from the existing `sql_parser` (once it lowers to WCOL logical, see the execution plan) produce identical physical plans.

## 7. Tooling

- `dump-clickbench-plans` writes the doc in place. Add `--out <path>` (default: print to stdout), and make the doc path explicit in the regenerate command.
- Add a test that regenerates into memory and compares with the checked-in `CLICKBENCH_PLANS.md`, so planner changes show up as a doc diff in review.

## Order

1 and 2 first: they change plan shape and every later diff reads better after them. Then 3, 4 and 5 (small, independent). Then 6, one gap at a time. Then 7, or alongside 1.

## Out of scope

- Distributed joins. Both sides gather to the coordinator, which is correct; co-partitioned or broadcast joins come later.
- Cost-based placement (decode cost, node speed, cache locality between queries).
- Page-index-based pruning and row-group statistics pruning. Both change what is read, not how it is placed; add them as a resolve step later.
