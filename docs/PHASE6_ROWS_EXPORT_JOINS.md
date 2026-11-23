# Phase 6: row queries, row export and joins from the physical plan

**Status:** Planned (2026-10-29)  
**Parent plan:** [PHYSICAL_PLAN_EXECUTION.md](./PHYSICAL_PLAN_EXECUTION.md) phase 6, plus the rest of phase 5.  
**Active slice:** [ROW_QUERY_GRAPH_SCOPE.md](./ROW_QUERY_GRAPH_SCOPE.md) — Q19, Q24–Q27 on the existing fragment/coordinator graph (correctness + WCS fan-in + `runPlannedQuery`), before export/joins/late materialization.  
**Goal:** every query shape the planner lowers runs from `lower_to_physical`: aggregates (done), row queries, row export to Parquet, and joins. After this, the opcode-10 SQL path, `plan_row_export` and the row-group sharding helpers can be deleted.

## Where things stand

Phases 1–4 run aggregate queries from the plan. 47 ClickBench queries run as `OP_NODE_FRAGMENT` programs on 1, 2 and 4 nodes, and `check-duckdb-results.py --plan-nodes N` gives 48 ok. Everything else still goes through SQL.

| Shape | Planner today | Execution today | Gap |
|-------|---------------|-----------------|-----|
| Row query (`SELECT cols … WHERE … [ORDER BY … LIMIT n]`) | Lowers to `Filter → Project → [TopK] → Exchange → [Sort] → [Limit]` | Opcode 10 builds `Filter → [Sort] → Project → ParquetEncode` from SQL, on one node | `open_fragment_graph` rejects `Project` and `TopK`; nodes have no row output; no coordinator row stage |
| Row sort | `SortKey` on any column | `SortOp` compares FLOAT/DOUBLE keys only; integer `ORDER BY` is rejected; it buffers every row | Integer, string and dictionary keys; a memory bound; `TopK` |
| Rows between nodes | `Exchange` | WCS `FRAME_DATA_BATCH`, but `projection_body_from_owned_batch` encodes `I64` columns only | `Utf8`, `DictId` and `F64` columns on the wire |
| Parquet sink | n/a | `ParquetEncode` writes INT64 only; refuses FLOAT, DOUBLE and UTF-8 | Write each column with its source physical type |
| Row export | n/a | `plan_row_export` shards by `row_groups_per_job` with `partition_ingest_groups`; one SQL row graph and one Parquet object per part; manifest checks row-group coverage | Placement from the plan; sink described in the plan |
| Join | DataFusion lowers `Join`; the planner gathers both sides' raw rows to a coordinator `Join` | `HashJoinOp` (build/probe, 48 MiB build cap) and `HashPartitionOp` exist; nothing runs a planned join | Column-id namespacing; executing the join; distributing it |

ClickBench in `scripts/clickbench-queries.tsv` has one row query (Q19). The upstream row queries Q24–Q27 (`SELECT * … ORDER BY EventTime LIMIT 10`, `SELECT SearchPhrase … ORDER BY …`) are not in the file yet and are the main correctness targets here.

## Principles (unchanged)

- The plan is the program. Node and coordinator programs come from `lower_to_physical`; nothing re-derives work from SQL or row-group counts.
- The host only moves bytes: range GETs, WCS frames between stages, R2 writes.
- Every step is checked against DuckDB, on 1, 2 and 4 nodes.

## 6.1 Typed row frames

Everything else in this phase moves rows, so this comes first.

- Extend the WCS `FRAME_DATA_BATCH` body to carry every batch column type:
  - `I64`: values plus validity, as today.
  - `F64`: values plus validity.
  - `Utf8`: offsets, bytes, validity.
  - `DictId`: ids plus validity, plus a dictionary reference per frame scope. The dictionary itself is sent once per scope as a `FRAME_DICTIONARY`; reuse `dict_scope` and `stream/dictionary.rs`.
- A column tag per column, plus a body version byte. Version 1 bodies (i64 only) still decode until opcode 2 / `ParquetWcsProjection` is retired.
- `WcsRowStream::push_batch` and the row ingress (`WcsIngress`) round-trip all four types.
- **Tests:**
  - round-trip property test over random batches mixing all four types, NULLs, empty strings and multi-byte UTF-8
  - a dictionary shared across frames
  - frames split at every byte boundary through the incremental reader

## 6.2 Sort and TopK for every type

- One row comparator over the batch column types:
  - integers numerically
  - floats by `float_key`, so NaN is highest and `-0.0` equals `0.0`
  - strings by bytes, which is DuckDB's default binary collation for UTF-8
  - dictionary ids by their resolved values
  - NULLs last in both directions unless `nulls_first`
  - multi-key, with each key's own direction
- `SortOp` uses it for any key type and drops the float-only restriction. The hand parser stops rejecting integer and string `ORDER BY`.
- `TopKOp { keys, n }`: a bounded heap holding at most `n` rows. A node keeps the best `skip + fetch` rows, and the coordinator still sorts and applies `Limit`.
- **Memory:** `SortOp` keeps a byte budget (default 64 MiB on a Durable Object profile). Past it, it fails with `SORT_TOO_LARGE` and a clear message. Spilling is out of scope. `TopK` with a bounded `n` never hits the budget.
- **Ties:** with `LIMIT`, rows tied on the sort key at the cut are an arbitrary choice in DuckDB too. Result checks use the rule in [Verification](#verification).
- **Tests:**
  - comparator brute-forced against a reference sort over random rows of every type, both directions, explicit NULL ordering
  - `TopK` equals sort-then-limit for every `n` up to the input size
  - the `SORT_TOO_LARGE` error

## 6.3 Parquet sink for every type

- `ParquetEncode` writes each output column with its source physical type: INT32, INT64, BOOLEAN, FLOAT, DOUBLE, BYTE_ARRAY (UTF-8 logical type when the source has it), FIXED_LEN_BYTE_ARRAY.
- The schema comes from the catalog's leaves and keeps their logical types (`DATE` for `EventDate`). `compact-parquet-thrift` already encodes all of these, including dictionary pages for bytes and floats.
- Dictionary-encoded inputs are written as dictionary pages when the column was dictionary-encoded at the source. Otherwise they're written PLAIN; no re-dictionarying.
- Row-group rotation by `ParquetOutputPolicy::target_row_group_rows`.
- **Tests:** write every type, read back with DuckDB (`read_parquet`) and with our scan, and check exact equality, NULLs included.

## 6.4 Row queries on the fragment path

- **Node fragment:** `open_fragment_graph` accepts `Project` and `TopK` steps. A fragment without `PartialAggregate` emits row batches. `GraphHost` sends them on the output port as typed row frames (6.1), not PWCL.
- **Coordinator fragment:** a new program kind, `OP_COORDINATOR_FRAGMENT` (13), built from the operators with `node = coordinator`:
  - ingress: concatenated row frames from every node (one `WcsIngress` per node edge, merged in node order)
  - steps: `Sort`, `Limit`, `Project` (from the plan)
  - sink: rows as typed frames (interactive results) or Parquet bytes (`ParquetEncode`)

  The coordinator never sees SQL either.
- **Dispatch:** `plan_query_dispatch` gains `QueryDispatch::Rows { nodes, coordinator }`. TS `runPlannedQuery` streams node outputs into the coordinator program with bounded buffering. This mirrors `reducePartialFromWcsStream`: a frame at a time, never the whole stream.
- The physical plan gains an explicit sink operator, `OpKind::Sink { kind: Rows | Parquet { policy } }`, so the result format is part of the plan rather than a dispatcher choice.
- **Tests:**
  - Q19 and new Q24–Q27 rows in the TSV
  - row queries with and without `ORDER BY`/`LIMIT`/`OFFSET` on 1, 2 and 4 nodes
  - node programs contain no SQL
  - host reads equal plan metrics

## 6.5 Row export on the plan

- Export is a row query whose sink is `Parquet { policy }` at the nodes, with no coordinator merge of rows:
  - Each node writes its own part, rotating files at `target_file_bytes`.
  - The coordinator collects part records into the manifest.
- **Placement** is the planner's byte-balanced split, replacing `partition_ingest_groups` and `row_groups_per_job`. The worker count comes from the existing estimate (`estimate_output_bytes`, `workers_for_target_file`), passed to the planner as the node count.
- **Manifest check:** `verify_row_export_manifest` checks coverage against the plan's decode groups: every planned row group in exactly one published part.
- **Ordered export** (`ORDER BY` with an export sink): the nodes do `TopK` or plain rows, and the coordinator sorts and writes a single file. Bounded by the sort budget; larger ordered exports are out of scope.
- **Cloudflare:** `row-export-route.ts` keeps its task shape (`{taskId, wireProgram, objectKey}`). `cf-row-export.ts` and `cf-row-export-full.ts` build tasks with `plannedTaskJson`, plus the sink policy.
- **Tests:**
  - the local export writes parts whose union, read with DuckDB, equals the DuckDB query result (`EXCEPT` both ways, row counts equal)
  - manifest coverage
  - task ids are stable across a retry

## 6.6 Joins

### 6.6.1 Column namespacing (prerequisite)

- Today both join inputs number columns from 0 within their own table, and `HashJoinPlan::equijoin` takes one key list for both sides. Make the join's output schema explicit:
  - left columns keep their ids
  - right columns are offset by the left table's leaf count
- `LogicalOp::Join` records both widths. Operators above the join refer to the combined ids.
- Update together:
  - DataFusion lowering (`lower_join`)
  - the planner's `require_one_side`
  - `HashJoinPlan` (separate `build_keys` and `probe_keys`, already fields; plus an output id map)
- **Tests:** a join of two tables that share column names and ids; group by and aggregate on columns from each side.

### 6.6.2 Coordinator hash join (correct first)

- Both sides run node fragments that filter and project, and emit typed row frames.
- The coordinator builds a hash table from the side with fewer planned bytes, probes with the other, then runs `Aggregate`/`Sort`/`Limit` from the plan.
- **Semantics:** inner equijoin first. NULL keys never match. Duplicate keys produce every pair.
- **Memory:** the build side is capped (`DEFAULT_JOIN_BUILD_MEMORY_LIMIT`, 48 MiB). Past it, the query fails with `JOIN_BUILD_TOO_LARGE`; that's the trigger for 6.6.4.
- **Tests:** DuckDB comparisons on a generated two-table fixture: a fact table plus a small dimension, skewed keys, NULL keys, no matches.

### 6.6.3 Broadcast join

- When the build side's planned bytes are under `broadcast_threshold_bytes` (a new planner config, default 8 MiB), the planner sends the build rows to every probe node:
  1. a coordinator→node broadcast exchange
  2. each node joins locally
  3. a following aggregate runs as a partial aggregate on the node, with the coordinator merging partials as today
- The decision is deterministic and recorded in the plan (`Exchange { kind: Broadcast }`).
- **Tests:** results equal 6.6.2; host reads per node equal the plan; probe-side partials merge correctly on 1, 2 and 4 nodes.

### 6.6.4 Partitioned (shuffle) hash join

- For two large sides, both repartition by the join key into `P` partitions with `HashPartitionOp` (`Exchange { kind: Hash { keys, partitions } }`). Partition `i` of both sides goes to node `i`, which joins locally.
- Needs node-to-node row exchange. On Cloudflare that's `inter-do-wcs-pipe.ts`; locally, in-process channels.
- `typed_hash` already gives a stable hash version, so the partition placement is deterministic and part of the plan.
- **Tests:** results equal 6.6.2 at every partition count; skew within the build cap; repartition traffic matches the plan's byte estimates within a stated tolerance.

## 6.7 Finish phase 5

Once 6.4 and 6.5 are green:
- **Rust:**
  - `plan_row_export`, `RowExportPartPlan`, `partition_ingest_groups`, `verify_partition_coverage`, `task_id_for_shard` (after page compaction moves to plan task ids)
  - `ColocatedGraphPlan::open_row_parquet`
  - the opcode-10 SQL body
  - `FilterOp::open(sql)` and `AggregateOp::open(sql)` outside the frontends and tests
  - `ParquetWcsProjection` (opcode 2), once typed row frames replace it
- **TS:**
  - `partitionIngestGroups`, `verifyPartitionCoverage`, `taskIdForShard*`, `rowGroupsPerJob`
  - SQL-carrying `encodeVerticalSlicePlan`
  - `scanColumnsForStreamSql`
  - the `ReadAtPool` / `read_at_hints` prefetch (no SQL path left to use it)
- **Tests that use the SQL path as their oracle:** switch to DuckDB, or to stored expected results.
- **Done when:** a grep for SQL text in worker or coordinator wire programs, `row_groups_per_job` and `partitionIngestGroups` finds nothing outside frontends and tests.

## Verification

For each step, on `data/hits_subset_500k.parquet`, and on the full hits file before deleting the SQL path:

| Check | How |
|-------|-----|
| Row results | Compare with DuckDB on 1, 2 and 4 nodes. Formal rules: [ROW_QUERY_GRAPH_SCOPE.md § DuckDB comparison](./ROW_QUERY_GRAPH_SCOPE.md#3-duckdb-comparison-and-tie-rules) (multiset for unordered; ordered = sort + strictly-better multiplicities + cutoff equivalence class on **full rows**). |
| WCS fan-in | No interleaved raw bytes from independent producers into one ingress parser; per-edge framing or sequential complete streams — [ROW_QUERY_GRAPH_SCOPE.md § WCS](./ROW_QUERY_GRAPH_SCOPE.md#1-wcs-multiplexing-is-a-graph-edge-correctness-issue). Applies to rows **and** any multi-producer WCS ingress. |
| Export | Union of parts read by DuckDB `EXCEPT` the DuckDB query, both ways, is empty; row counts are equal; manifest covers every planned row group exactly once |
| Joins | DuckDB on generated fixtures, every strategy and node count; NULL and duplicate keys |
| Reads | Host requests and bytes per node equal plan metrics, for every new shape |
| Memory | Peak per node and coordinator within the Durable Object budget; `SORT_TOO_LARGE` and `JOIN_BUILD_TOO_LARGE` trip where expected, not earlier |
| No SQL | Node and coordinator programs never contain SQL text |
| Cloudflare | `cf-row-export*.ts` and a join run against a deployed worker (manual; needs credentials) |

## Order and dependencies

```
6.1 typed row frames ──┬─> 6.4 row queries ──> 6.5 row export ──┐
6.2 sort / TopK ───────┤                                        ├─> 6.7 finish phase 5
6.3 parquet sink ──────┘                                        │
6.6.1 join ids ──> 6.6.2 coordinator join ──> 6.6.3 broadcast ──> 6.6.4 shuffle
```

6.1, 6.2, 6.3 and 6.6.1 are independent of each other and can run in parallel. 6.4 needs all three of 6.1–6.3. Joins need 6.1 and 6.4's coordinator fragment.

## Risks

- **Sort memory.** A full `ORDER BY` without `LIMIT` on a large result exceeds a Durable Object. The budget error makes it explicit; spilling to R2 is future work.
- **Dictionary scopes on the wire.** Row frames from different nodes carry different dictionaries for the same column. The coordinator must not mix ids across scopes. Resolve to values at the coordinator, or merge dictionaries per scope; the 6.1 tests must cover two nodes sending the same column.
- **Join skew.** One hot key can exceed the build cap even when partitioned. The error is the guard; salting is future work.
- **Row frame size.** A wide `SELECT *` batch can exceed `WCS_MAX_PAYLOAD` (32 MiB). Cap the batch rows per frame from the column widths in the plan.

## Out of scope

- Outer, semi and anti joins; non-equi joins.
- Spilling sorts and joins.
- Expressions in projections (`SELECT a + 1`) and `CASE`; the planner does not lower them yet.
- Window functions.
