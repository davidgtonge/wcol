# Executing the WCOL physical plan

**Status:** In progress (2026-10-29). Phases 1–4 done for aggregate queries; phase 5 partly done; phase 6 not started.

- **Phase 1 (done):** both frontends lower to `WcolLogicalPlan`. The hand parser lowers 49 ClickBench queries, aggregate offsets included. `wcol-sql/tests/frontend_parity.rs` checks that the 43 queries both frontends accept produce identical physical plans (predicate order aside).
- **Phase 2 (done):** `NodeFragment` is self-contained (codec on each read, coordinator limit and ordering, `fetch_depth`) and round-trips through `encode_node_fragment` / `decode_node_fragment`. `OP_NODE_FRAGMENT` (opcode 12) runs a fragment in GraphHost with operators built from plan structures; the program carries no SQL.
- **Phase 3 (done):** fragment programs read only the plan's fetches through `FetchStage` (bounded prefetch, buffers freed by consumed bytes, fetches larger than the input ring delivered in parts). Host reads equal the plan's per-node requests and bytes. The TS `ReadAtPool` / `read_at_hints` prefetch stays for the SQL path (opcode 10), which still uses decoder-driven reads.
- **Phase 4 (done for aggregates):** `plan_query_dispatch` / Wasm `wcol_plan_query` turn SQL plus the Parquet footer into one program per node slot, or the final partial for a footer-only `COUNT(*)`. TS `runPlannedQuery` (local and HTTP) and `plannedTaskJson` (Cloudflare coordinator tasks) dispatch from it; the coordinator merges node partials with the streaming `ReduceOp`. Task ids hash the source, node and program bytes. `runDistributedHitsExecGroup` and the four aggregate Cloudflare scripts use the plan.
- **Phase 5 (partly done):** removed the Rust shard planner (`plan_distributed_row_group_query`, `WorkerJobPlan`, `CoordinatorPlan`, `DistributedRuntimeConfig`, `JobAttempt`) and the half-built opcode-10 spec payload (`ingest_spec_wire`). Still in place: `partition_ingest_groups` and TS `partitionIngestGroups` (row export), the opcode-10 SQL path (single-node runs and the tests' SQL oracle), `rowGroupsPerJob` (sizes the default node count only).
- **Phase 6** (row queries, row export, joins on the fragment path) not started.

**Verified:** `wcol/tests/runtime/plan_execution_equivalence.rs` runs 47 ClickBench queries as fragment programs on 1, 2 and 4 nodes, equal to the SQL path, with host reads equal to plan metrics. `scripts/check-duckdb-results.py … --plan-nodes N` gives 48 ok, 0 mismatch on 1, 2 and 4 nodes. `tests/plan-dispatch.test.ts` checks the TS path. The Cloudflare scripts are type-checked but were not run against a deployed worker.  
**Depends on:** [PHYSICAL_PLANNER_FIXES.md](./PHYSICAL_PLANNER_FIXES.md) items 1–4 (balanced placement, bounded fetches, explicit exchanges, metadata count).  
**Goal:** `lower_to_physical` is the only place that decides what runs, where, and which bytes it reads. Every node and the coordinator execute the operators it emitted, and nothing else re-derives that work from SQL or row-group counts.

## Where decisions are made today

The physical plan is printed but not executed. The work it describes is decided in several other places:

| Decision | Made today by | Replaced by |
|----------|---------------|-------------|
| SQL → operators on a worker | Worker programs (opcode 10) carry the SQL text; `FilterOp::open` and `BatchQuery::open` re-parse it on every node with `sql_parser` | Node fragment with structured operators (phase 2) |
| Which row groups a worker scans | Rust `partition_ingest_groups` / `plan_distributed_row_group_query` (`row_groups_per_job: 7`), and TS `partitionIngestGroups` in `hits-exec-group.ts` | `Decode` groups on each node (phase 4) |
| Which bytes are read, in what order | `ParquetDecode::host_read_request`: per-column ReadAt of up to `HOST_READ_LIMIT` (256 KiB), at most 2 outstanding; host `ReadAtPool` / `read_at_hints` prefetch | Plan `fetches`, served to the decoder from fetched buffers (phase 3) |
| Coordinator tail | `CoordinatorPlan { sql, catalog }`: Reduce → Finalize built from SQL again | Coordinator fragment from the plan root (phase 4) |
| Task identity | `task_id_for_shard(source, sql, shard_index)` | Hash of (source version, plan bytes, node id) (phase 4) |
| Frontend | `sql_parser` (48 of 52 ClickBench queries against DuckDB) and DataFusion (21 of 52 reach a physical plan) | Both lower to `WcolLogicalPlan`, then one physical planner (phase 1) |

## Principles

- **The plan is the program.** A node receives its fragment (operators, decode groups, fetch list) and runs exactly that. It never sees SQL.
- **The host only moves bytes.** It performs the plan's fetches and carries exchange frames. It does not choose ranges, split work or interpret operators.
- **Same operators everywhere.** The batch `FilterOp`, `AggregateOp`, `ReduceOp`, `FinalizeOp`, `SortOp`, `HashJoinOp` and the PWCL partial format stay as they are. This work changes how they are constructed and fed, not the kernels.
- **Determinism carries through.** Identical inputs give identical plan bytes, identical fragments and identical task ids, so a retry on another Durable Object reruns the same fragment.
- **Parity at every phase.** Each phase is gated on ClickBench results matching DuckDB through the physical-plan path. Run with 1, 2 and 4 nodes, and get the same answers on all three.

## Phase 1 — One frontend contract

Make `WcolLogicalPlan` the only input to physical planning, from either frontend.

- Add `sql_parser::QueryPlan → WcolLogicalPlan` lowering in `wcol` (the hand parser already resolves columns, filters, aggregates, offsets, `COUNT(DISTINCT)`, HLL, LIKE).
  - All 48 queries the engine runs today then reach `lower_to_physical` immediately, without waiting for DataFusion coverage.
- DataFusion keeps lowering through `wcol-sql/src/wcol_logical.rs` and gains coverage via planner-fixes item 6.
- **Parity test:** for every query both frontends accept, the two `WcolLogicalPlan`s are equal, or at least produce byte-identical physical plans.
- `WcolLogicalPlan` gets a canonical serialisation (it has `canonical_text` for display; add a binary form) so the coordinator can hash and ship it.

**Done when:** `CLICKBENCH_PLANS.md` shows a physical plan for all 48 queries via the hand-parser frontend, and DataFusion plans match it where both exist.

## Phase 2 — Node fragments without SQL

Serialise what each node runs, and build operators from it.

- **`NodeFragment`** (Rust, versioned wire format next to opcode 10):
  - `decode: Vec<DecodeGroup>` (object, row group, rows, columns, read ids)
  - the `logical_reads` and `fetches` for this node
  - `steps`: `Filter`, `Project`, `PartialAggregate`, `TopK` (from planner-fixes item 5)
  - the output edge (PWCL partial or row batches) and its exchange id
- **Typed constructors** that take plan structures instead of SQL:
  - `FilterOp::from_predicates`
  - `BatchQuery::from_aggregate(group_by, aggregates)`
  - `ProjectOp::from_columns`

  `ingest_spec.rs` (`IngestQuerySpec`) already started this for opcode 10; converge on one structured spec rather than keeping both.
- A new GraphHost opcode (or an opcode-10 payload variant) runs a `NodeFragment`: source → decode → steps → output port. The SQL payload variant stays only until phase 5.
- **Equivalence tests:** for each ClickBench query and fixture, the operators built from the fragment produce PWCL byte-identical to the ones built from SQL today.

**Done when:** a worker can run any lowered query from its fragment alone, and the SQL text is not present in the bytes sent to the node.

## Phase 3 — Fetches drive the reads

The decoder still asks for column-chunk bytes as it needs them. Those requests are now served from the plan's fetches instead of reaching R2 one by one.

- **`FetchStage`** in Rust, inside GraphHost:
  - Holds the node's `FetchRequest` list in plan order.
  - Asks the host for one fetch at a time, through the existing ticketed ReadAt ABI with the fetch's exact range, keeping at most `fetch_depth` in flight.
  - Stores completed fetch bytes in a buffer table keyed by fetch id.
- **Demux:** `ParquetDecode` keeps its per-column ReadAt requests. Instead of crossing to the host, they are satisfied by slicing the buffered fetch that covers them, using `LogicalRead.fetches` pieces (planner-fixes item 2).
  - A request that spans two fetch pieces is assembled from both.
  - The decoder's compressed-window credit logic is unchanged.
- **Release:** a fetch buffer is freed when every logical read it holds has been fully consumed. Count bytes handed out per read; the plan says exactly how many each read needs.
- **Memory budget:**
  - Resident fetch bytes on a node ≤ `fetch_depth × max_request_bytes` plus the decoder windows.
  - Add `fetch_depth` and the memory profile to `PhysicalPlannerConfig`, so the planner and runtime agree. The planner rejects a config whose `max_request_bytes` cannot fit the node budget.
- **Order:** fetches are issued in plan order (object, offset). Row groups decode in plan order too, so the decoder's demand follows the fetch order. If the decoder needs a range whose fetch is not yet in flight, that fetch jumps the queue; the plan stays deterministic, only timing changes.
- **Host side (TS, Cloudflare):** `readAt` stays a plain R2/HTTP range GET. `ReadAtPool` and `read_at_hints` prefetch are deleted: the plan already says what to prefetch.
- **Observability:** each node reports actual R2 requests and bytes. A test asserts they equal the plan's per-node `requests` and `fetched` metrics.

**Done when:** on the 500k hits file, every ClickBench query matches DuckDB, and observed R2 requests and bytes per node equal the plan's metrics.

## Phase 4 — Coordinator and dispatch from the plan

- **Coordinator fragment:** built from the operators with `node=coordinator`:
  - `Exchange` → `FinalAggregate` (`ReduceOp` + `FinalizeOp`)
  - `Sort` / `Limit`
  - `Join` (`HashJoinOp`) → `Aggregate`
  - `MetadataCount` (no node work at all)

  The coordinator no longer reads SQL. Exchange inputs are worker output ports, carried as WCS partial frames, merged frame by frame (the streaming reduce).
- **Dispatch (TS coordinator Durable Object):**
  - Calls the planner (Wasm) once, with the cached footers as `ParquetMetadataCatalog` and the node slots for this query.
  - Sends each node its `NodeFragment` and feeds exchange outputs to the coordinator fragment.
  - `concurrent_jobs` and `max_total_outstanding_reads` become scheduling limits on how many node fragments run at once. They don't change what a fragment contains.
- **Node identity:** a `NodeId` is a slot in the plan, not a specific Durable Object. A slot runs on any worker. If a worker fails, the same fragment is retried elsewhere, with task id = hash(source version, plan bytes, node id).
- **More units than parallelism:** the planner gets the slot count from the coordinator, typically `ceil(query bytes / per-node target)` capped by the account limit. It does not get the number of live workers.
- **Metadata catalog:** footers are cached per object version (R2 key + etag). Planning stays I/O-free; fetching and caching footers is a separate step before planning.

**Done when:**
- `hits-exec-group` (local) and `graph-coordinator-run` (Cloudflare) run every lowered ClickBench query from the physical plan, on 1, 2 and 4 node slots.
- Results match DuckDB and are identical across slot counts: exact for integer and string aggregates, within the documented float tolerance for float SUM/AVG.

## Phase 5 — Delete the other decision makers

Once phase 4 is green on Cloudflare:

- Rust: `partition_ingest_groups`, `verify_partition_coverage`, `plan_distributed_row_group_query`, `WorkerJobPlan`, `CoordinatorPlan`, `DistributedRuntimeConfig.row_groups_per_job`, `task_id_for_shard`.
- TS: `partitionIngestGroups`, `verifyPartitionCoverage`, `taskIdForShard*`, `rowGroupsPerJob`, `ReadAtPool` prefetch hints, and the SQL-carrying plan encoders (`encodeVerticalSlicePlan` with SQL, `scanColumnsForStreamSql`).
- GraphHost: the SQL payload variant of opcode 10, and `FilterOp::open(sql)` / `BatchQuery::open(sql)` outside the frontends and tests.
- `sql_parser` stays only as a frontend producing `WcolLogicalPlan`, until DataFusion covers its queries. Then it can go too.

**Done when:** a grep for `row_groups_per_job`, `partitionIngestGroups` and SQL strings in worker wire programs finds nothing outside tests and the frontend.

## Phase 6 — Row queries, export, joins

Extend the same path to the remaining shapes:

- **Row queries:** nodes run `Project` and optional `TopK`. The coordinator runs `Sort` / `Limit` and the row sink.
- **Row export to Parquet:** today `plan_row_export` sizes parts. Move part sizing into the physical plan as a sink property of each node fragment, so export uses the same placement and fetches.
- **Joins:** keep the coordinator join from the planner. A later planner change can co-partition or broadcast; execution already handles exchange edges generically.

## Verification matrix

For each phase, on `data/hits_subset_500k.parquet`, and on the full hits file for phases 3–5:

| Check | How |
|-------|-----|
| Results | `scripts/check-duckdb-results.py` through the physical-plan path; float queries with the NaN-aware comparison |
| Node-count independence | Same query on 1, 2 and 4 slots gives identical results (exact for integers and strings) |
| Reads match plan | Observed R2 requests and bytes per node equal plan metrics |
| Memory | Peak Wasm memory per node within the Durable Object budget, with `max_request_bytes` and `fetch_depth` at their defaults |
| Determinism | Plan bytes, fragment bytes and task ids identical across two planning runs and across machines |
| Performance | `query_bench` and the Cloudflare bench within noise of today on numeric queries; R2 request count lower on wide string queries |

## Risks

- **Decoder demand vs. fetch order.** If a node's row groups interleave columns far apart, buffered fetches wait longer before release, raising memory. Bound it with `fetch_depth`, and measure on the full hits file before deleting `read_at_hints`.
- **Two frontends drifting.** The phase 1 parity test is the guard; keep it running until `sql_parser` is retired.
- **Plan size.** Full hits has many row groups and about 100 columns. Fragments carry only their node's reads and fetches, not the whole plan. Measure fragment bytes for the widest ClickBench query.
