# Distributed row-group jobs (physical planner)

**Status:** Planner + worker mode + Rust coordinator `Reduce`/`Finalize`; host job runner + **HTTP acceptance passed** (2026-10-28, clean wasm build via [`scripts/verify-distributed-provenance.sh`](../scripts/verify-distributed-provenance.sh)).

### Milestone record

| Milestone | Status |
|-----------|--------|
| Distributed Q3 correctness | **Passed** — 226 RG vs monolithic + oracle |
| Distributed Q8 `GROUP BY` correctness | **Passed** — 226 RG vs monolithic + oracle |
| Partition coverage | **Passed** — 33×7 + 1×2 tail |
| Duplicate partial rejection | **Passed (host)** — registry + `finalizeForReduce` |
| Distributed performance improvement | **Established (Q8 HTTP)** — ~4.3× wall at 16 jobs; memory scales with concurrency |
| Full restrictive memory plateau (I-09) | **Pass (HTTP)** — `npm run test:restrictive-plateau-hits` (226 RG, `memoryProfile=1`, replay 0) |
| Failed-job retry guarantees | **Passed (host v1)** — `runTaskWithRetries`; failures not merged; missing tasks block reduce |
| Cloudflare shard lifecycle | **Passed (Miniflare preview)** — `npm run test:cf-shard`: Q3 and Q8, 226 RG, injected shard failure retried, duplicate partial rejected |
| Production Cloudflare Q3/Q8 | **Passed** — `https://wcol-stream.<your-subdomain>.workers.dev`, `npm run test:cf-production`. Concurrency 4/8/16, 226 RG, parity with local monolithic, 0 failed attempts. Q8 wall 11.9s / 4.6s / 2.5s. Q3 wall 20.1s / 9.9s / 6.3s. Version `5f075a92-911e-4df7-8575-4f9f544969ca` |
| Distributed string `GROUP BY` (Q12 slice) | **Rust** — `distributed_string_group` with `HITS_PARQUET`; **HTTP** — Q12 in [`hits-http-distributed-acceptance.test.ts`](../tests/hits-http-distributed-acceptance.test.ts) via `wcol_partials_wire_equivalent` |
| Distributed high-cardinality string keys (Q33) | **Planned** — full 226-RG reducer memory; hash-partitioned reducers when single reduce exceeds budget |

Acceptance: [`hits-http-distributed-acceptance.test.ts`](../tests/hits-http-distributed-acceptance.test.ts) with `HITS_HTTP_DISTRIBUTED=1`.

**Provenance (2026-10-28):** `npm run verify:distributed-provenance` — release `wcol.wasm` SHA-256 `c695f1a66d9135b45b06dd31eac80803a271fa31a53fe05b0bbc803da1c32225`, then 4/4 acceptance (~166s).

**Prerequisite:** Ticketed ReadAt depth **2** + `complete_read(ticket)` on opcode **10** ingest (see [`batch_decode.rs`](../../rust/wcol/src/formats/parquet/batch_decode.rs)). Sharded row-group jobs should show **`page_replay_count == 0`** on columns decoded via owned `PageCursor` (flat `max_def ≤ 1`, PLAIN / dict RLE — **`ENABLE_OWNED_PAGE_CURSOR` is on** in [`numeric_scan.rs`](../../rust/wcol/src/formats/parquet/numeric_scan.rs)).

## Principle

Distribute the **existing** colocated execution graph. No second TypeScript query engine, no ad-hoc WPP1 merge helpers in the host.

```text
                 Rust physical planner
                         │
              partition IngestGroup[] (disjoint, complete)
                         │
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
   Worker job 0      Worker job 1     Worker job N
   RG slice          RG slice         RG slice
       │                 │                 │
  GraphHost op10   GraphHost op10    GraphHost op10
  WorkerShard      WorkerShard       WorkerShard
  Scan→Agg         Scan→Agg          Scan→Agg
       │                 │                 │
   typed partial    typed partial     typed partial
   (PWCL edge)      (PWCL edge)       (PWCL edge)
       └─────────────────┼─────────────────┘
                         │
              Coordinator (same Reduce → Finalize ops)
                         │
                   JSON / Parquet sink
```

Placement (one process, threads, or separate DOs) is **outside** the planner; operators and merge semantics stay the same.

## Adaptive reduction topology

Row-group sharding is the default: each worker runs **Scan → Filter → Aggregate** and emits a typed partial; the coordinator runs **`ReduceOp` → `FinalizeOp`**. That is correct for `GROUP BY` even when the same key appears on many workers — merge is by group key, not a sum of local distinct counts. **`COUNT(DISTINCT)`** stays on the two-level partial plan; **`ReduceOp`** remains the reference merge.

Do **not** take per-worker Top K on grouped queries before global merge (a locally popular phrase can lose to a key that is never local Top 1). Global **`ORDER BY … LIMIT`** runs after full merge, or after a hash-partitioned second stage when reducer state is too large.

| Estimated aggregate state | Topology |
|---------------------------|----------|
| Small (low-cardinality groups) | Single Rust reducer (today’s Q3/Q8 path) |
| Medium | Hash-partition **partial aggregates** across 4–16 reducer nodes (`HashPartitionOp` on group keys — future planner wiring) |
| Large | Spill or staged partitioning; not required for every query |

Track three budgets when choosing topology:

| Budget | Controls |
|--------|----------|
| Scan-node | Local aggregation, partial size per worker (`ScanLimits`, `peakMemoryPerJobBytes`) |
| Reducer | Merged group count × key width (string PWCL v4) |
| Exchange | Buffered WCS1 bytes and concurrent streams (`bounded-byte-pipe`, `concurrent_jobs`) |

Hash partitioning is an **optimization for scaling reduction**, not a prerequisite for distributed `GROUP BY`.

## Rust API (authoritative)

| Type | Module | Role |
|------|--------|------|
| [`DistributedRuntimeConfig`](../../rust/wcol/src/plan/distributed.rs) | `plan::distributed` | Row groups/job, concurrent jobs, ReadAt depth, global read budget |
| [`plan_distributed_row_group_query`](../../rust/wcol/src/plan/distributed.rs) | `plan::distributed` | Partition + worker wire programs + coordinator descriptor |
| [`IngestExecutionMode`](../../rust/wcol/src/plan/physical.rs) | `plan::physical` | Trailing plan byte: `Monolithic` (0) vs `WorkerShard` (1) |
| [`coordinator_reduce_partials`](../../rust/wcol/src/plan/distributed.rs) | `plan::distributed` | Merge worker PWCL via `ReduceOp` (same as monolithic tail) |
| [`coordinator_reduce_finalize_rows`](../../rust/wcol/src/plan/distributed.rs) | `plan::distributed` | Reduce then `FinalizeOp` for row sinks |
| [`ColocatedGraphPlan::ingest_query_worker_shard`](../../rust/wcol/src/exec/colocated_plan.rs) | `exec::colocated_plan` | Opens graph **without** local `Reduce` |

Integration: [`distributed_row_group_jobs.rs`](../rust/wcol/tests/runtime/distributed_row_group_jobs.rs) — workers + `coordinator_reduce_partials` ≡ monolithic opcode **10** on `flat.parquet`.

Host responsibilities only:

- Run `N` worker `wire_program` bytes through [`runWcolEngine`](../../src/runtime/stream/run-wcol-engine.ts).
- Collect one **complete** aggregate partial per accepted task attempt.
- Feed partials into coordinator `Reduce → Finalize` (Rust), not a bespoke merge.

## Runtime controls (independent)

| Setting | Default | Notes |
|---------|---------|--------|
| `row_groups_per_job` | 7 | Last job smaller; **226 RGs → 33 jobs** (32×7 + 1×2), not 32×7=224 |
| `concurrent_jobs` | 4 | Sweep 1,2,4,8,16,32 for wall time **and** aggregate CPU / HTTP wait |
| `read_at_depth_per_job` | 2 | Maps to host `readAtConcurrency` per worker |
| `max_total_outstanding_reads` | 16 | Budget across all in-flight workers |

ReadAt overlap is **not** a prerequisite for row-group jobs; both should be configurable.

## Task identity and retries

```rust
JobAttempt {
    task_id: TaskId,       // Stable across retries (shard + source + SQL)
    attempt: u32,          // Increments on retry
    row_groups: Vec<u32>,
    source: SourceVersion,
    wire_program: Vec<u8>,
}
```

Coordinator rules (v1):

- Accept **at most one successful attempt** per `task_id`.
- **Do not** merge failed or partial attempts into global state.
- Each worker emits **one complete** aggregate partial when its shard finishes.
- Missing job → explicit failure (never silent partial success).

## Merge path

**Production:** Worker PWCL → [`partial_state_from_wcs_bytes`](../../rust/wcol/src/exec/runtime/wcol_stream/wpp1_edge.rs) → [`coordinator_reduce_partials`](../../rust/wcol/src/plan/distributed.rs) (ordinary [`ReduceOp`](../../rust/wcol/src/ops/reduce.rs)); optional [`coordinator_reduce_finalize_rows`](../../rust/wcol/src/plan/distributed.rs) for row sinks.

**Transitional:** Benchmarks may compare via [`partialsEquivalent`](../../src/runtime/stream/partial-oracle.ts) / WPP1 views — parity only, delete when hits distributed acceptance is green.

## Wire format

Opcode **10** body ends with `execution_mode: u8` after SQL:

- `0` — monolithic (Reduce on host edge) — TS [`encodeVerticalSlicePlan`](../../src/runtime/stream/vertical-slice-plan.ts) appends `0`.
- `1` — worker shard (aggregate partial only).

Legacy plans without the trailing byte default to monolithic.

## Acceptance criteria (implementation checklist)

| Test | Requirement |
|------|-------------|
| Row-group coverage | Every group exactly once (`verify_partition_coverage`) |
| Q3 | SUM / COUNT / AVG match monolithic opcode **10** |
| Q8 | Group counts match; global ORDER BY after Reduce |
| Q12 (string key, slice) | Partial merge via `ReduceOp`; compare with `encoded_partials_equivalent` / `wcol_partials_wire_equivalent` |
| Q33 (full hits, string key) | Reducer memory milestone; optional count-only check before global Top K |
| Placement | Same operators locally or remote |
| Duplicate attempt | No double counting | **Demonstrated** (Reduce is additive; host must dedupe by `task_id`) |
| Failed attempt | No partial accepted |
| Missing job | Hard failure |
| Memory | Per-worker budget respected |
| Backpressure | Coordinator can throttle workers |
| Performance | Wall time, aggregate CPU, HTTP wait (not wall time alone) |

## Q8 distributed concurrency sweep

Command: `npm run bench:hits-distributed-jobs` (226 RG, 7 RG/job → 33 worker jobs; **226** total HTTP range reads).

Baseline **monolithic** (same session): **22.3s** wall, peak Wasm **~12.4 MiB**.

| `concurrentJobs` | Wall | vs mono | Σ HTTP wait | Peak Wasm (single Node host) |
|------------------|------|---------|-------------|------------------------------|
| 1 | 23.1s | 0.96× | 16.3s | ~12.4 MiB |
| 2 | 11.4s | 1.95× | 18.0s | ~30.4 MiB |
| 4 | 7.9s | 2.80× | 25.1s | ~54.2 MiB |
| 8 | 5.5s | 4.04× | 37.6s | ~83.7 MiB |
| 16 | 5.2s | 4.28× | 75.5s | ~165 MiB |

Wall time improves through 16 jobs on HTTP-bound Q8; peak Wasm scales with concurrent workers in one process—cap `concurrent_jobs` per isolate memory. Coordinator reduce &lt; 2ms.

### Memory-aware concurrency (host)

Do not use a fixed `concurrentJobs: 16` in memory-constrained placements. The HTTP host caps parallelism with [`memoryAwareConcurrentJobs`](../../src/runtime/stream/coordinator-tasks.ts):

```text
concurrency = min(
  requested,
  floor((available_memory - headroom) / peak_memory_per_job)
)
```

Set `DistributedHitsConfig.availableMemoryBytes` and `peakMemoryPerJobBytes` (default **13 MiB** from Q8 sweep; refine from observed `maxWasmMemoryBytes` per query). Separate Cloudflare isolates budget **per worker**, not aggregate Node heap.

### Roadmap (post-milestone)

| Priority | Work | Acceptance |
|----------|------|------------|
| **P0** | Task identity + retry (HTTP coordinator) | **Done:** `runTaskWithRetries`, `finalizeForReduce`, `taskIdForShardIndex`; failed attempts never merged; [`HITS_HTTP_DISTRIBUTED=1`](../../tests/hits-http-distributed-acceptance.test.ts) |
| **P0** | Full restrictive 226-RG plateau | **Done (HTTP):** `npm run test:restrictive-plateau-hits` — 226 RG, replay 0, wasm linear flat after row group 64 |
| **P1** | Placement-specific concurrency | Single-host vs per-isolate DO limits |
| **P1** | Distributed Q3 benchmark | `npm run bench:hits-distributed-jobs -- --query q3` CPU-heavy scaling |
| **P2** | Generic graph placement | Row-group sharding in Rust planner, not HITS-only TS |

Task keys (HTTP coordinator v1): [`taskIdForShardIndex`](../../src/runtime/stream/coordinator-tasks.ts) (`sourceKey`, SQL, **shard index** after [`partitionIngestGroups`](../../src/runtime/stream/hits-exec-group.ts)). Rust planner: [`task_id_for_shard`](../../rust/wcol/src/plan/distributed.rs) + [`task_id_hex`](../../rust/wcol/src/plan/distributed.rs) on `SourceVersion.object_key_hash`. Row-group-set hashing ([`taskIdForShard`](../../src/runtime/stream/coordinator-tasks.ts)) remains for ad-hoc tests. Attempt number is separate. Coordinator merge uses only registry-accepted partials before [`coordinatorReducePartials`](../../src/runtime/stream/coordinator-reduce.ts) (`ReduceOp`).

### Coordinator attempt contract

Node HTTP: [`runDistributedHitsExecGroup`](../../src/runtime/stream/hits-exec-group.ts). Cloudflare: [`WcolCoordinator`](../../cloudflare/src/shard-coordinator.ts) + [`WcolNode`](../../cloudflare/src/worker.ts) — see [**Distributed streaming graph execution**](./DISTRIBUTED_STREAMING_GRAPH_EXECUTION.md) for the unified target (async `/task/start`, single `GraphJob` control plane). Today: synchronous `/shard` partials and coordinator-side `ReduceOp`.

**Cloudflare R2 reads (single object):** `POST /distributed` accepts **`objectKey`** instead of **`url`** (exactly one). The coordinator validates the key and requires the matching R2 binding (`OUTPUT` → `wcol-hits-exports`, **`ANALYTICS` → an optional second bucket, not bound in the tracked configs; add it to a local config to use it**); pass optional **`r2Bucket`**: `"output"` (default) or `"analytics"`. Each `WcolNode` `/shard` uses [`R2RangeSource`](../../cloudflare/src/r2-range-source.ts) for ReadAt. For **firehose** day partitions (`firehose/yyyy/MM/dd/*.parquet`), use [`scripts/firehose-day-query.ts`](../scripts/firehose-day-query.ts) (DuckDB over all parts) until multi-key ingest exists on opcode 10. Production hits validate: `WCOL_R2_QUERY_KEY` on `npm run test:cf-production`.

1. Each shard gets a stable [`taskIdForShardIndex`](../../src/runtime/stream/coordinator-tasks.ts). Rust [`JobAttempt`](../../rust/wcol/src/plan/distributed.rs) carries the same idea (`task_id`, `attempt`, `wire_program`) for the native planner.
2. Worker runs the shard wire program with bounded ReadAt depth and returns a partial.
3. [`CoordinatorTaskRegistry`](../../src/runtime/stream/coordinator-tasks.ts) accepts one completed partial per task and records failed attempts without merging them.
4. `finalizeForReduce` then [`coordinatorReducePartials`](../../src/runtime/stream/coordinator-reduce.ts).
5. Default **3** attempts per task.

## Performance note

Parallel jobs must not **hide** decoder replay (~9× amplification on Q3). A correct `PageCursor` should get faster with sharding; replay-heavy decode only multiplies cost across workers.

## Next engineering steps

1. ~~Host job runner~~: [`runDistributedHitsExecGroup`](../../src/runtime/stream/hits-exec-group.ts) + [`coordinatorReducePartials`](../../src/runtime/stream/coordinator-reduce.ts) (`wcol_reduce_open` / `push` / `finish`); CLI `--distributed` on [`hits-http-exec-group.ts`](../../tests/hits-http-exec-group.ts); acceptance [`hits-http-distributed.test.ts`](../../tests/hits-http-distributed.test.ts).
2. ~~HTTP acceptance test: full hits Q3/Q8 vs monolithic~~ — **done** via `verify:distributed-provenance` + `HITS_HTTP_DISTRIBUTED=1`.
3. ~~Task dedup + failed-job rejection in coordinator host~~ — **done** (`CoordinatorTaskRegistry`). Per-placement memory budget (not single-process 16× Wasm) remains P1.
4. Delete transitional WPP1-only merge once production path drops oracle-only comparisons.

Benchmark-only TS helpers (e.g. `shardHitsJobs`) may exist for sweeps; **production sharding stays in** `plan_distributed_row_group_query`.
