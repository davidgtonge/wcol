# wcol — Distributed Streaming Graph Execution

**Status:** Target architecture (authoritative for Cloudflare orchestration)  
**Boundary:** [RUST_FIRST_EXECUTION.md](./RUST_FIRST_EXECUTION.md)  
**Host types:** [`graph-job.ts`](../src/runtime/stream/graph-job.ts), [`graph-coordinator.ts`](../src/runtime/stream/graph-coordinator.ts)  
**Implementation:** [`shard-coordinator.ts`](../cloudflare/src/shard-coordinator.ts), [`stream-task-node.ts`](../cloudflare/src/stream-task-node.ts), [`graph-coordinator-run.ts`](../cloudflare/src/graph-coordinator-run.ts)

## Objective

Extend wcol's existing Cloudflare execution architecture into a **generic, recoverable, distributed streaming graph runtime**.

The same infrastructure must execute:

- Interactive analytical queries
- Distributed aggregations and reductions
- Streaming Parquet transforms and exports
- Parquet page-copy compaction
- Future multi-stage queries and data processing graphs

**There must not be separate orchestration systems for queries and transforms.** They are all physical execution graphs with different operators and output sinks.

Use the existing Rust `GraphHost`, WCOL/WCS1 protocols, operator implementations, `WcolNode` Durable Objects and Cloudflare coordinator.

Do not introduce a second query engine, scheduler, data-plane implementation or TypeScript operator pipeline.

## 1. Architecture

```text
Client
  │
  │ Submit physical graph
  ▼
Coordinator DO
  │
  ├── Persist graph and execution state
  ├── Partition graph into node tasks
  ├── Schedule tasks within concurrency budgets
  ├── Receive heartbeats and completion reports
  ├── Detect failed/expired tasks
  └── Retry / restart affected graph fragments

       Control plane only
                │
                ▼

WcolNode A ── WCS1 ──► WcolNode D
   │                        │
   │                        ▼
WcolNode B ── WCS1 ──► ReduceOp
   │                        │
   │                        ▼
WcolNode C ── WCS1 ──► Output sink
                             │
                   Client / R2 / manifest
```

**Control plane:** Coordinator RPC messages for placement, scheduling, heartbeats, completion, failure and cancellation.

**Data plane:** Direct streams between WcolNodes. The coordinator must not forward bulk data.

**Execution:** Rust GraphHost owns operators and graph advancement. TypeScript only supplies Cloudflare-specific I/O and execution lifecycle plumbing.

## 2. Generic physical execution model

Three concepts (see [`graph-job.ts`](../src/runtime/stream/graph-job.ts)):

```ts
interface GraphJob {
  jobId: string;
  epoch: number;
  tasks: GraphTask[];
  edges: GraphEdge[];
}

interface GraphTask {
  taskId: string;
  stageId: string;
  nodeId?: string;
  plan: Uint8Array;
  inputs: string[];
  outputs: string[];
}

interface GraphEdge {
  edgeId: string;
  fromTask: string;
  fromPort: number;
  toTask: string;
  toPort: number;
}
```

Keep partitioning and operator semantics in **Rust**. TypeScript must not plan SQL, interpret Parquet, or branch on opcodes in the coordinator.

Edge kinds: colocated WCOL, remote WCS1, external `ReadAt`, external `WritePort`.

## 3. Node execution lifecycle

| WcolNode route | Purpose |
|----------------|---------|
| `POST /task/start` | Persist assignment; `waitUntil` execution; **202 accepted** |
| `GET /task/status` | Local state + `heartbeatAt` |
| `GET /task/wcs-egress?taskId=&jobId=&epoch=&attempt=` (DO) | One-shot claim of live task WCS stream when `wcsEgress` on `/task/start` |
| `WcolNode.openTaskWcsEgress(identity)` (RPC) | Same claim path as HTTP; enable with host env **`WCOL_INTER_DO_WCS_RPC=1`** |
| `GET /task/result` | Partial when complete (base64 JSON) |
| `POST /task/cancel` | Best-effort abort (storage flag checked during `runWcolEngine`) |

Implemented in [`stream-task-node.ts`](../cloudflare/src/stream-task-node.ts). Heartbeats to coordinator during `runWcolEngine` yields (~10s). **`waitUntil()` does not guarantee indefinite execution** — use durable ledger + alarm recovery.

**Task identity (v0):** `/task/start` requires `coordinator.jobId` and `coordinator.epoch`. Durable records, cancel flags, and WCS egress sessions use `streamTask:{jobId}:{epoch}:{taskId}`. Poll routes require matching `jobId`, `epoch`, and `taskId` query params. Worker DO routing uses `wcolTaskNodeName(jobId, taskId)` (`shard:{jobId}:{taskId}`). Legacy reduce partials use `task:{jobId}:{taskId}`.

Synchronous `POST /shard` remains for legacy `/distributed` → `/run` until Milestone D retires it.

## 4. Streaming transport and backpressure

**Milestone B (transport v0):** [`bounded-byte-pipe.ts`](../src/runtime/stream/bounded-byte-pipe.ts) caps in-flight bytes between producer and consumer. Two **`WcolNode`** DOs exchange data via `fetch` + `ReadableStream` body ([`wcs-bench-node.ts`](../cloudflare/src/wcs-bench-node.ts)):

| Route | Role |
|-------|------|
| `GET /wcs/bench/source` | Bounded pipe → response body |
| `GET /wcs/bench/wcs-source` | Valid WCS1 frames through bounded pipe |
| `POST /wcs/bench/drain` | Pull from another node's source |
| `POST /wcs/bench/consume-wcs` | Consumer runs **`runWcolStream`** with **`PLAN_BYTE_PIPE`** (`0xBE`) + WCS guard + copy ([`wcol_ffi_session.rs`](../rust/wcol/src/exec/wcol_ffi_session.rs)) |
| `POST /wcs/bench` / `POST /wcs/bench/wcs` (worker) | Runs consumer DO → producer DO |

Verify: `npm run test:cf-wcs-bench`, `tests/bounded-byte-pipe.test.ts`. Producer **`peakBufferedBytes`** is persisted on the source DO and checked on drain (`backpressureOk`: peak ≤ `bufferCap + chunk`).

**Shared byte pool (hash shuffle):** [`SharedBytePool`](../src/runtime/stream/shared-byte-pool.ts) is the authoritative in-isolate budget for queued WCS bytes. Scanners demux tagged WCS locally and `acceptScanEdge` reducer DOs; the coordinator does not carry shuffle bytes. Empty partitions complete via `completeScan`/`seal`. See [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md) §8a. Stress: `tests/stream-pool-scale.test.ts` (1000→1 ingress; 16×1000 hash in-process); Durable Object transport: `npm run test:cf-hash-fanout`. Do not interpret `WCOL_DISPATCH_CONCURRENCY` (default 6) as a Cloudflare Durable Object RPC hard cap.

**Producer contract:** GraphHost WCS bytes must be written with **`pipeWorkerShardWcsToWritable`** (or equivalent `WritableStreamDefaultWriter.write` loop). Do not wrap `pipeWorkerShardWcs` in a `ReadableStream` whose `start()` enqueues asynchronously — that bypasses pipe backpressure and can buffer unbounded JS memory ahead of the bounded pipe.

### Stable routes (Milestone F)

| Route | Role |
|-------|------|
| `POST /wcs/egress/shard` (DO) | GraphHost bounded WCS producer |
| `GET /wcs/egress/metrics?runId=` (DO) | Producer pipe metrics |
| `POST /wcs/ingress/shard` (DO) | Consumer: fetch producer + Rust WCS guard |
| `POST /wcs/ingress/shard-rpc` (DO) | Consumer: **`openInterDoShardWcsEgress`** RPC + Rust guard |
| `POST /wcs/ingress/copy` (DO) | Consumer: synthetic/guard+copy bench |
| `POST /wcs/stream/shard` (worker) | Orchestrates consumer DO for shard stream |
| `POST /wcs/stream/shard-rpc` (worker) | Same via **`native_rpc_wcs`** transport |
| `POST /wcs/bench/rpc-pipe` (worker) | Raw-byte RPC backpressure smoke |

Legacy `/wcs/bench/*` paths remain for CI. Constants: [`inter-do-wcs-paths.ts`](../src/runtime/stream/inter-do-wcs-paths.ts).

**GraphHost producer path:** **`POST /wcs/stream/shard`** (fetch) or **`POST /wcs/stream/shard-rpc`** (RPC) stream one WorkerShard plan over HTTP ReadAt through a bounded pipe ([`pipeWorkerShardWcs`](../src/runtime/stream/cf-shard-job.ts), [`inter-do-wcs-pipe.ts`](../src/runtime/stream/inter-do-wcs-pipe.ts)); strict WCS guard verified with `WCS_BENCH_GRAPH=1`.

**Milestone F (RPC + WCS reduce ingress):** `WcolNode.openInterDoShardWcsEgress`; async reduce **`taskPartialTransport: "wcs_stream"`** uses **`GET /task/wcs-egress`** + Rust **`WCS_PARTIAL_REDUCE_PLAN`** (`CF_SHARD_WCS_INGRESS=1`). Default transport remains **`json`**. Feeding multiple worker WCS streams into one reduce pipe without re-open is not wired yet.

**Milestone G (reducer-as-node):** **`taskPartialTransport: "wcs_reduce_stage"`** starts a reducer **`WcolNode`** before scanners. The reducer claims each scanner edge via **`openTaskWcsEgress`** (RPC when **`WCOL_INTER_DO_WCS_RPC=1`**, else **`GET /task/wcs-egress?…&attempt=`**). One-shot stream transfer; **`edge consumed`** (WCS END through `WcsPartialReduce`) is distinct from **`scanner completed`**. Verify: `CF_REDUCE_WCS_STAGE=1 npm run test:cf-shard`; claim tests: `tests/task-wcs-egress-claim.test.ts`. **Production RPC egress:** `npm run test:cf-production-distributed-rpc` (deploys with `WCOL_INTER_DO_WCS_RPC=1`, reduce stage only).

## 5. Coordinator: scheduling and backpressure

[`WcolCoordinator`](../cloudflare/src/shard-coordinator.ts) SQLite storage:

| Key | Content |
|-----|---------|
| `job:{jobId}` | [`JobRecord`](../src/runtime/stream/graph-coordinator.ts) (includes optional **`coordinatorKey`** for DO routing when it differs from `jobId`) |
| `ledger:{jobId}:{taskId}` | [`TaskLedgerRow`](../src/runtime/stream/graph-coordinator.ts) |
| `task:{jobId}:{taskId}` | Legacy accepted partial (reduce idempotency, scoped per job) |

**Worker HTTP (unified job API v0):**

| Route | Behavior |
|-------|----------|
| `POST /jobs` | **202** + `jobId`; coordinator runs in `waitUntil` |
| `GET /jobs/{jobId}` | Job + task ledger |
| `GET /jobs/{jobId}/result` | Reduce partial, or **`manifestKey`** when `kind` is `page-compact` / `row-export` |
| `POST /jobs/{jobId}/cancel` | Mark cancelled |
| `POST /distributed` | Sync reduce (existing `/run`) |

Async path uses `/task/start` + poll ([`graph-coordinator-run.ts`](../cloudflare/src/graph-coordinator-run.ts)).

**Manifest jobs (`page-compact` / `row-export`) on `POST /jobs`:**

```json
{
  "kind": "row-export",
  "jobId": "optional-uuid",
  "rowExport": {
    "url": "https://…/source.parquet",
    "manifestKey": "exports/run/manifest.json",
    "tasks": [{ "wireProgram": "…", "sourceRowGroups": [0, 1] }]
  }
}
```

Legacy async routes accept the same bodies with `"async": true`. Unless **`coordinatorJob: false`** or **`WCOL_LEGACY_GRAPH_JOB_STATUS=1`**, they forward to **`POST /jobs`**. Poll with **`GET /jobs/{jobId}`**, **`GET /jobs/{jobId}/result`**, or **`GET /row-export/status?jobId=`** / **`GET /page-compact/status?jobId=`** (coordinator by default; **`?coordinator=0`** for R2 `graph-job-status/*`).

Benchmark scripts (`page-compact-common`, `cf-row-export*`) use the same path when **`WCOL_COORDINATOR_JOBS=1`** (202 + poll + R2 manifest fetch). Default remains synchronous POST for backward compatibility.

## 6. Heartbeats and progress

[`TaskHeartbeat`](../src/runtime/stream/graph-coordinator.ts) on `POST /heartbeat` (coordinator). Nodes send via `coordinator: { jobKey, jobId, epoch }` on task start. Stale **epoch** heartbeats rejected (409).

Defaults: node ~10s, watchdog alarm 30s, task lease 60s — [`DEFAULT_COORDINATOR_TIMING`](../src/runtime/stream/graph-coordinator.ts).

## 7. Coordinator watchdog

`WcolCoordinator.alarm()` scans active jobs, marks **lease expired** tasks failed, and **resumes orphaned distributed-reduce executors** from persisted [`graphPlan`](../../src/runtime/stream/graph-coordinator.ts) when `dispatch.executingGeneration` is set but `lastProgressAt` is stale (coordinator lost an in-flight `waitUntil`). Resume clears worker ledger/partials for that job and redispatches; `dispatch.resumeCount` increments. Test: `npm run test:cf-coordinator-dispatch` (`WCOL_COORDINATOR_TEST_HOOKS=1`).

Executor progress is refreshed on task ledger updates via [`touchJobDispatchProgress`](../../src/runtime/stream/coordinator-dispatch.ts). Does not run Rust graphs inside the alarm handler beyond scheduling `waitUntil(runJob…)`.

**Execution phase ledger (resume-safe):** `JobRecord.executionPhase` is `"workers" | "reduce_stage" | "finalize"` while a distributed reduce runs; cleared on completion. Monotonic **`workerAttempt`** / **`reduceStageAttempt`** bump on each alarm-driven resume (`markDispatchResume`) so scanner→reducer replay does not merge stale partials. Task lease expiry on `worker_shard` or `reduce_wcs` ledger rows triggers the same full replay from persisted **`graphPlan`**.

**Cancel:** `POST /jobs/cancel` sets `cancelRequested`, marks the job cancelled, and **`POST /task/cancel`** on each worker task DO (epoch-scoped); in-flight egress claims return `CANCELLED`.

## 8. Correctness, retries and reduction

Existing [`CoordinatorTaskRegistry`](../src/runtime/stream/coordinator-tasks.ts): one accepted partial per logical task; failed attempts not merged. Epoch fencing on heartbeats. Full reducer attempt isolation and stage replay — **Milestone C/E**.

No general mid-stream checkpointing in Milestone A.

## 9. Unified output semantics

Terminal operators differ; scheduler must not. Examples:

```text
SQL:     ParquetDecode → … → Reduce → ResultSink
Export:  ParquetDecode → … → ParquetEncode → R2Sink
Compact: ParquetPageCopy → ParquetFooter → R2Sink
```

Sync vs async is an **API delivery** choice (`POST /distributed` vs `POST /jobs`), not a different engine.

## 10. Implementation sequence

| Milestone | Scope | Status |
|-----------|--------|--------|
| **A** | Job/task records, async ack, background run, heartbeats, alarm, missing-task detection | **Mostly done** — `/jobs` async reduce, Miniflare proof in `test:cf-shard`; page-compact `?async=1` + status |
| **B** | Two-node byte stream + bounded buffer; WCS1 on wire | **Done (v1)** — `test:cf-wcs-bench`; GraphHost shard wire passes strict WCS guard (`WCS_BENCH_GRAPH=1`) after `EmitPartial` END |
| **C** | Q3/Q8 multi-scan → reducer, attempt isolation | **Mostly done (sync + async)** — `CoordinatorTaskRegistry` + `test:cf-shard` (`failFirstAttempt` + duplicate replay); async `/jobs` records `failedTaskAttempts` and retry `attemptId` on ledger |
| **D** | Export + compact on same coordinator lifecycle | **Done (v1)** — async submit + status default to coordinator; partial task failures surface as **`exportPartial`** + sync **502** `{ failed, published }` for retry/reuse (`test:cf-failure-matrix` `coordinatorExportPartial`) |
| **E** | Failure injection matrix | **Done (Miniflare v1)** — [DISTRIBUTED_FAILURE_MATRIX.md](./DISTRIBUTED_FAILURE_MATRIX.md); `test:cf-failure-matrix`; shard retry/duplicate in `test:cf-shard` |
| **G** | Reducer-as-node WCS stage (`wcs_reduce_stage`) | **Done (v1)** — `CF_REDUCE_WCS_STAGE=1 npm run test:cf-shard` |
| **H** | Durable dispatch + alarm resume from `graphPlan` | **Done (v1)** — `npm run test:cf-coordinator-dispatch` (Miniflare); remote async + reduce stage: `npm run test:cf-production-distributed` |
| **F** | Native RPC vs `fetch` inter-DO WCS | **In progress** — RPC producer + `/wcs/stream/shard-rpc` parity; streaming reduce ingress from remote WCS remains |

## 11. Acceptance criteria

See spec checklist in repo history; key gates: one coordinator, no bulk relay, async client disconnect ≠ cancel, recovery after DO restart, no double-counted partials, Q3/Q8 + compaction parity preserved.

## 12. Scope discipline

No separate export/compaction schedulers, no TS operators, no mandatory D1/R2 intermediate store, no general checkpoint barriers in v1.

**Next:** Reducer ingress from inter-DO WCS (replace PWCL **`GET /task/result`** when reduce graphs accept remote WCS); production HITS **`cf-row-export-full --fail-worker --retry`** with coordinator jobs default.

## Related

- [DISTRIBUTED_ROW_GROUP_JOBS.md](./DISTRIBUTED_ROW_GROUP_JOBS.md)
- [INTER_DO_WCS_TRANSPORT.md](./INTER_DO_WCS_TRANSPORT.md)
- [PAGE_COMPACTION.md](./PAGE_COMPACTION.md)
- [STREAMING_EXECUTION.md](./STREAMING_EXECUTION.md)
