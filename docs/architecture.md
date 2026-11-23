# Architecture

WCOL runs a SQL query over Parquet in R2 as a graph of tasks spread over Durable Objects. All planning, decoding and query logic is Rust. TypeScript is the host: it moves bytes and streams between the Wasm and Cloudflare's APIs, and it runs the task graph that the planner produced.

## The pieces

| Piece | Where | Does |
|---|---|---|
| Front Worker | `cloudflare/src/worker.ts` | Serves the console, checks the API token, and routes `/jobs` to the job's coordinator |
| Coordinator | `WcolCoordinator` DO, one per job, coordinator Wasm | Reads the source footers, plans the job, runs the graph, finalizes the result and publishes it |
| Nodes | `WcolNode` DOs, node Wasm | Run scan and reducer tasks. A slot number names each one, so a job reuses the same objects |
| Container reducers | `WcolReducer` DO and container, `rust/wcol-reducer` | A hash reducer with the instance's memory and local-disk spill, for large group state ([containers](containers.md)) |
| R2 | one bucket, bound as `OUTPUT` | Source Parquet, spill runs, async results, Parquet output and manifests |

The node Wasm has no planner and the coordinator Wasm has no scan code, so each stays small (the node module is about 1.1 MB, 430 KB gzipped). The build script fails if the node Wasm grows past its budget or exports a planner symbol.

## A query, end to end

1. `POST /jobs` reaches the coordinator for that job. A client `idempotencyKey` names the job, so a retried request reaches the same coordinator.
2. The coordinator Wasm validates the body and lists its sources. The host reads each source's Parquet footer from R2 and pins its etag. A source that changes during the job fails it.
3. `wcol-plan` parses the SQL and builds a logical plan, then a physical plan: which columns each scan reads, which row groups each statistics check can skip, where each operator runs, and how rows move between tasks. Every task carries a compiled program for the engine.
4. The planner picks one of three shapes:
   - **Metadata only.** `COUNT(*)`, or `MIN`/`MAX` of an integer column whose every chunk has statistics: answered from the footer. Nothing is scanned.
   - **Gather.** Scans aggregate their share, and the coordinator merges what they send. This is used when the group state fits in one place.
   - **Hash.** Scans partition their partial aggregates by key hash, and send each partition to its reducer. Reducers merge, and the coordinator's finalizer reads their outputs. The planner chooses a power-of-two reducer count from its estimate of group state.
5. The coordinator runs the graph. Scans claim row groups at run time: each scan has a queue dealt by cost, and steals from the fullest queue when its own runs out. Slow scans therefore shed their tails to fast ones.
6. The finalizer sorts, limits and formats. A `json` job answers inline if it finishes within the sync budget, and otherwise stores the result in R2 for `GET /jobs/{id}`. Parquet output is committed by publishing a manifest.

## Inside a task

`GraphHost` (in `wcol-engine`) runs one task's fragment: a small operator graph such as decode → filter → partial aggregate → hash partition. The host feeds it bytes and streams, and the engine owns the poll loop, buffer sizes, end-of-input and errors.

- **Decode.** Parquet pages are decoded straight into WCOL batches. Dictionary-encoded strings stay as dictionary indexes for as long as possible. A top-k over wide rows reads its output-only columns in a second pass, for the winning rows only.
- **Transport.** An edge between tasks is a stream of WCS frames, the engine's wire format for batches and partial aggregate states, carried over Durable Object RPC. Fan-in is pulled per edge, with at most one pending read and one ready chunk per port, so a slow consumer applies backpressure all the way to the R2 reads.
- **Memory.** A node reducer keeps its group table within a target (16 MiB by default), and a hard limit that is twice that. Past the target it flushes a sorted run to R2, and at the end it merges its runs. A Durable Object's isolate has 128 MB, shared with the engine's buffers, so these limits are deliberately small. Container reducers have the same engine with a larger budget and local-disk runs.

## Attempts and recovery

A job runs as numbered attempts. An attempt claims a lease in the coordinator's SQLite storage, and every task and edge carries the attempt number. A node refuses work from an attempt that is not current, so a superseded attempt cannot commit. Each attempt writes outputs under its own keys. Publication (the result record, or the Parquet manifest) is the commit, and it is a separate step that can be retried.

When a task fails, the attempt fails as a whole. Recovery is a planning step: the coordinator reads the persisted graph and starts a new attempt through the same runner. A stall watchdog fails an attempt that makes no progress. A job fails after three attempts.

## Idle cost

Nothing runs between jobs. The coordinator clears its alarm when a job ends, nodes clear theirs once their work is done, and all of them hibernate. A hibernated Durable Object becomes active again in under 200 ms, so WCOL does not keep objects warm. Containers are destroyed as soon as their output has been read.

## Code layout

```text
rust/
  wcol-engine/   formats/parquet, exec/graph_host, ops, spill, stream (WCS)
  wcol-plan/     plan/sql_parser, plan/logical, plan/physical_plan, plan/job_plan
  wcol/          facade and tests (tests/runtime)
  wcol-node-wasm, wcol-coordinator-wasm   the two cdylibs and their FFI exports
  wcol-reducer/  native reducer: http.rs, engine.rs, spill.rs
src/runtime/
  host/          Wasm loading and imports, stream pools, ports, R2 spill driver
  coordinator/   plan dispatch, local graph runners, run-time work claims, attempt replay
cloudflare/src/
  coordinator/   submit, plan, run, gather and hash execution, finish and publish, live view
  node/          node DO, scans, reducers, container reducers
cloudflare/console/   the console page
```

The local runners in `src/runtime/coordinator` run the same graphs in one process for tests, with the same Wasm.

## Conventions

[AGENTS.md](../AGENTS.md) sets out the rules the code follows: a pure planning core with I/O at the edges, one implementation of each concept, explicit types, and bounded concurrency.
