# TypeScript after the host cut

Rust owns the job: planning, operators, ports, buffering, backpressure, and WCS.
TypeScript does two things.

1. **Adapters.** Each physical stream implements one interface. RPC, R2, HTTP range, and SQLite are the same kind of object.
2. **One run loop.** Load Wasm for a program, hand it those streams, then ask Rust what to read or write until the program finishes.

Nothing in TypeScript chooses a port, parses a frame, demuxes a hash partition, prefetches a range, or retains a leftover chunk.

Success is those responsibilities leaving TypeScript, not a line count. The ~2,300-line host is a plausible size, not the goal.

Rust owns execution. The Cloudflare coordinator still owns the durable job: attempts, fencing, publication, and recovery.

Do not delete the old runners, demux, or signal router until a real 2×2 direct-RPC path passes with the same empty-edge and failed-scan guarantees. The browser and file runtime stays until that migration is done; archiving it is a separate compatibility decision.

## Constraints

**Lazy ports.** A coordinator must not build a `Map` of 1,000 RPC streams up front. It registers a `PortFactory`. Rust `Open` carries the port id; the factory resolves the descriptor and creates the `BytePort`. An incoming RPC stream is attached to a Rust input port. It is not opened by the receiver.

**ReadAt stays for Parquet.** `read(maxBytes)` is a sequential stream. Existing Parquet ingestion keeps ticketed random-access reads (`FLAG_READ_AT`: offset in `extra`, source in `aux`). Where a bounded range is a natural stream, Rust may `Open` that range as its own sequential port.

**Close is not scan completion.** Sixteen scanners and one reducer can see twelve streams close and still not know whether the other four finished empty or have not started. The coordinator reports producer completion. Rust records which expected producers are done and whether each opened stream reached a valid WCS end. Transport `Close` only closes the byte pipe. `scan-edge-session.ts` goes away only after that barrier lives in Rust.

**Publication is not an R2 write.** `ports/r2-write.ts` writes one object or fails. The coordinator decides when that object is the authoritative result, so a retired attempt cannot publish over a newer one. `job-recover.ts` restarts an attempt under the same fencing. It does not rerun a stored program with no attempt identity.

**The run loop stays the tested ABI loop.** One serialized Wasm thread, batched actions, concurrent host I/O, ticketed completions, one wake, explicit yield and cancel. The 32-byte action and 16-byte completion records stay. The sketch below is the shape, not a replacement for `host-loop.ts`.

**Host memory stays accounted.** Rust credits do not see JS copies, stream remainders, multipart staging, or outstanding writes. Adapters keep a host budget. Each node object owns its Wasm instance (`NodeSessionLease.wasm`) and drops it after each task and between attempts; the isolate holds none, because objects the runtime places in one isolate would otherwise share one linear memory and the engine's globals. Wasm memory does not shrink because Rust freed an allocation.

| Rust | TypeScript |
| --- | --- |
| Plans the graph | Instantiates Wasm |
| Runs operators | Copies bytes |
| Routes partitions | Resolves a port resource when Rust says `Open` |
| Buffering and I/O credits | Bounds JS-owned buffers |
| Logical stream state, including producer completion | RPC connection lifecycle |
| Spill and merge | SQLite and R2 adapters |
| Task bytes | Attempt fencing, durable publication, recovery |

## Target

```
src/runtime/host/
  byte-port.ts      BytePort
  abi.ts            32-byte action / 16-byte completion (matches Rust ABI v1)
  loop.ts           poll → dispatch → complete. No WCS.
  run.ts            instantiate Wasm, bind ports, call the loop, return counters
  wasm.ts           load the module, read the last Rust error

  ports/readable.ts     Web ReadableStream
  ports/writable.ts     Web WritableStream
  ports/r2-read.ts      R2 or HTTP range → ReadAt
  ports/r2-write.ts     R2 multipart sink
  ports/rpc.ts          Durable Object RPC body, one stream per port
  ports/sqlite.ts       DO SQLite as a byte stream (insert batches stay inside this file)
  ports/memory.ts       in-memory streams for tests

cloudflare/src/
  worker.ts             POST /jobs, GET status. Routes to the coordinator DO.
  coordinator.ts        job record, call planner Wasm, start node DOs, hibernate
  node.ts               accept one program + port bindings, call run()
  wasm.ts               instantiate node and coordinator modules
  r2.ts                 bucket, key, conditional put — used only by the R2 ports
```

About **1,500 lines** in `src/runtime/host/` and **800 lines** in `cloudflare/src/`.
Today the stream runtime plus `graph-task-rpc.ts` is roughly **11,000 lines**.

### Ports

```ts
interface BytePort {
  read?(maxBytes: number): Promise<Uint8Array | null>; // null = EOF
  write?(bytes: Uint8Array): Promise<void>;
  close(): Promise<void>;
  abort(reason?: unknown): Promise<void>;
}

interface PortFactory {
  open(resource: ResourceDescriptor): Promise<BytePort>;
}
```

`read` and `write` are optional because a port is one direction. Parquet keeps `FLAG_READ_AT` on the Read action. The R2 adapter fetches that offset and length. It does not choose the next range.

### The loop

`host/loop.ts` is today's `host-loop.ts`: completions are applied, then one poll, then each action is dispatched. Completions never re-enter Wasm. If the batch asked the host to yield, the loop yields. If it dispatched work, it continues. If nothing is runnable, it waits on the single wake. The first poll runs with an empty completion queue.

`run.ts` instantiates Wasm for one program and gives the loop a factory plus the coordinator's resource table. It does not know the plan. Rust `Open` is what creates a physical stream.

## What is not TypeScript anymore

| Today | After |
| --- | --- |
| `ReadyPortScheduler`, `nextReady`, ingress window | Rust issues `Read(port)`. The readable port blocks on its own stream. |
| `ReadAtPool`, prefetch hints | Rust issues overlapping `Read` + `FLAG_READ_AT`. |
| `leftover` / `subarray` in the driver | Rust slot holds the committed prefix. The port keeps a tight copy of the tail. |
| `writePort(0)` and `demuxPartitionStream` | Rust `Write(partition)`. The RPC port for that id is the reducer stream. Empty partitions are never opened. |
| `wcs-frame-codec`, `wcs-transport`, fan-in mux | Rust. TypeScript copies bytes. |
| `spill-host.ts` SQL opcodes | `ports/sqlite.ts`. Rust reads and writes records. SQL stays inside the adapter. |
| Hash exchange, opening limiters, shared byte pools as schedulers | A byte cap on the port, if the transport needs one. Not a scheduler. |
| Signal router, tickets, wire frames | Direct RPC or R2 streams, after Rust tracks producer completion. `Open` creates a stream. `Close` ends it. Scanner completion is a separate coordinator report. |
| `graph-runner.ts` placing tasks and merging partials | Coordinator calls planner Wasm, then `node.run` per task. Reduce is a program, not a TS merge. |

## Stream runtime — `src/runtime/stream/`

| File | Lines | Verdict |
| --- | --- | --- |
| `run-wcol-engine.ts` | 472 | **Fold** into `host/run.ts`. Already the ABI v1 loop. Drop spill-opcode and `nextReady` branches when those ports exist. |
| `host-loop.ts` | 193 | **Keep** as `host/loop.ts`. |
| `host-io-abi.ts` | 178 | **Keep** as `host/abi.ts`. |
| `port-host-io.ts` | 163 | **Fold** into `ports/readable.ts` + `ports/writable.ts`. |
| `port-ingress.ts` | 140 | **Temporary.** Per-port reader with no mux. Delete once each input is a `BytePort` opened by Rust `Open`. |
| `sqlite-byte-streams.ts` | 367 | **Keep** as `ports/sqlite.ts`. |
| `memory-host-io.ts` | 164 | **Keep** as `ports/memory.ts`. |
| `wasm-stream-driver.ts` | 212 | **Split.** Wasm exports and `run()` stay. `runWcolStream` becomes `ports/readable` + `ports/writable`. |
| `wasm-last-error.ts` | 23 | **Fold** into `host/wasm.ts`. |
| `stream-wasm.ts` | 84 | **Fold** into `cloudflare/src/wasm.ts` (load) and `host/wasm.ts` (types). |
| `r2-multipart-write-port.ts` | 212 | **Keep** as `ports/r2-write.ts`. |
| `bounded-byte-pipe.ts` | 197 | **Keep only if** an RPC body needs a bounded buffer inside `ports/rpc.ts`. It is not a scheduler. |
| `owned-reader.ts` | 70 | **Fold** into `ports/readable.ts`. |
| `chunk-stream.ts` | 395 | **Delete** with the signal/R2 edge mux. A port writes the RPC or R2 body directly. |
| `ready-port-scheduler.ts` | 260 | **Delete.** |
| `read-at-pool.ts` | 60 | **Delete.** |
| `hash-exchange.ts` | 734 | **Delete.** `demuxPartitionStream`, `pushPartitionedScan`, `runHashExchangeSchedule`. |
| `graph-runner.ts` | 744 | **Delete.** Local graph placement moves to the coordinator calling `run()`. |
| `dispatch-limits.ts` | 215 | **Delete.** Caps become port buffer sizes. |
| `shared-byte-pool.ts` | 100 | **Delete** with the demux. |
| `graph-edge-transport.ts` | 74 | **Fold** the RPC open into `ports/rpc.ts`. |
| `scan-edge-session.ts` | 200 | **Keep until Rust owns the producer barrier.** Then delete. `Close` does not mean the scanner is finished. |
| `wcs-frame-codec.ts` | 147 | **Delete.** |
| `wcs-transport.ts` | 97 | **Delete.** |
| `wcs-fan-in-multiplexer.ts` | 42 | **Delete.** |
| `inter-do-wcs-pipe.ts` | 139 | **Delete.** |
| `inter-do-wcs-paths.ts` | 28 | **Delete.** |
| `spill-host.ts` | 322 | **Delete** after `ports/sqlite.ts` is what GraphHost opens. |
| `spill-wire.ts` | 10 | **Delete** with `spill-host.ts`. |
| `signal-router-core.ts` | 230 | **Delete.** |
| `signal-client.ts` | 271 | **Delete.** |
| `signal-wire.ts` | 90 | **Delete.** |
| `signal-tickets.ts` | 63 | **Delete.** |
| `signal-local.ts` | 79 | **Delete.** |
| `execution-graph.ts` | 136 | **Delete** from the host. The planner Wasm owns the graph. A test may decode a fixture. |
| `plan-dispatch.ts` | 444 | **Delete.** `runNodeProgram` is `run()`. SQL → programs is a Wasm export the coordinator calls once. |
| `job-replay.ts` | 53 | **Delete.** |
| `coordinator-tasks.ts` | 213 | **Delete.** |
| `coordinator-reduce.ts` | 64 | **Delete.** |
| `cf-shard-job.ts` | 342 | **Delete.** Shard jobs are `run()` plus an R2 read port. |
| `hits-exec-group.ts` | 235 | **Delete** from the host. Hits benches call `run()`. |
| `task-identity.ts` | 29 | **Fold** into `coordinator.ts` if a status row still needs it. |
| `partial-oracle.ts` | 374 | **Tests only.** Calls Rust to compare PWCL. Not on the run path. |
| `partial-wire.ts` | 14 | **Tests only**, or delete if Rust reports the format. |
| `duckdb-oracle.ts` | 75 | **Tests only.** |
| `stream-sql-plan.ts` | 102 | **Delete** from the host. Planner Wasm replaces it. |
| `index.ts` | 119 | **Replace** with exports of `run` and `BytePort`. Stop re-exporting the deleted modules. |

## Cloudflare — `cloudflare/src/`

| File | Lines | Verdict |
| --- | --- | --- |
| `worker.ts` | 57 | **Keep.** `POST /jobs` and status. |
| `coordinator-entry.ts` | 7 | **Keep.** Re-export the coordinator DO. |
| `node-entry.ts` | 6 | **Keep.** Install Wasm, re-export the node DO. |
| `shard-coordinator.ts` | 233 | **Fold** into `coordinator.ts`. Drop `RouterCore` and the signal WebSocket. |
| `wcol-node.ts` | 177 | **Fold** into `node.ts`. One method: run this program with these bindings. |
| `graph-task-rpc.ts` | 1,135 | **Delete.** This is the second runtime: hash fan-out, ingress scheduler, spill telemetry, `writePort`. |
| `graph-jobs.ts` | 284 | **Fold** submit/status into `coordinator.ts`. |
| `job-run.ts` | 376 | **Delete.** Orchestration is planner Wasm + `node.run`. |
| `job-record.ts` | 195 | **Keep**, slim. Status rows only. |
| `job-publish.ts` | 196 | **Keep** in the coordinator. An R2 write is not publication. Fence attempts here. |
| `job-recover.ts` | 155 | **Keep.** Restart the attempt under the same fencing. Do not rerun a bare stored program. |
| `job-pin.ts` | 294 | **Keep** the pin that stops hibernation mid-`run()`. Delete hash-lease machinery once a running `run()` is the only thing holding the DO. |
| `node-session.ts` | 56 | **Fold** into `node.ts`. |
| `node-pool.ts` | 37 | **Keep.** Map a slot to a DO id. |
| `node-wasm.ts` | 30 | **Fold** into `wasm.ts`. |
| `coordinator-wasm.ts` | 19 | **Fold** into `wasm.ts`. |
| `wasm-bindings.ts` | 26 | **Fold** into `wasm.ts`. |
| `host-execution-hints.ts` | 29 | **Delete.** Rust decides memory and concurrency. |
| `edge-signals.ts` | 67 | **Delete** with the signal stack. |
| `spill-sql.ts` | 1 | **Delete.** `ports/sqlite.ts` takes the DO `sql` binding. |
| `r2-buckets.ts` | 18 | **Fold** into `r2.ts`. |
| `r2-binding-session.ts` | 66 | **Fold** into `ports/r2-read.ts`. |
| `r2-range-source.ts` | 26 | **Fold** into `ports/r2-read.ts`. |
| `r2-chunk-store.ts` | 22 | **Delete** with chunked signal edges. |
| `object-key.ts` | 11 | **Keep** inside `r2.ts`. |
| `bounded-storage.ts` | 41 | **Keep.** Coordinator status storage cap. |
| `hash-fanout-test-worker.ts` | 152 | **Delete** after fan-out tests call `POST /jobs`. |
| `wcs-bench-node.ts` | 646 | **Delete.** Benches call `run()`. |
| `worker.bench.ts` | 165 | **Delete** or keep as a bench entry that only calls `run()`. |
| `wcol-node-bench.ts` | 108 | **Delete** with the bench DO methods. |
| `node-entry.bench.ts` | 6 | **Delete** with the bench build. |
| `coordinator-entry.bench.ts` | 7 | **Delete** with the bench build. |

`cloudflare/do-start/` (~1,180 lines) and `cloudflare/df-planner/worker.ts` are probes. They are not part of the host. `cloudflare/r2-multipart/worker.ts` folds into `ports/r2-write.ts` if that experiment is still the writer; otherwise it goes.

## Browser and file runtime — later, not this cut

These files are a TypeScript query engine. They are not the distributed host. Leave them in place until the 2×2 path and the host migration are done, then check whether anything still calls them before archiving or dropping the package exports.

| Path | Lines | Verdict |
| --- | --- | --- |
| `src/runtime/core/` | ~390 | **Archive.** `wcol.ts` still exports `executePlanFromPlan`. |
| `src/runtime/query/` | ~310 | **Archive.** |
| `src/runtime/schema/` | ~250 | **Archive.** |
| `src/runtime/io/` (`header`, `pages`, `sources`, most of `byte-source`) | ~630 | **Archive**, except an HTTP range reader if `ports/r2-read.ts` wants to share it. |
| `src/runtime/parquet.ts` | 768 | **Archive.** Decode lives in Rust. |
| `src/runtime/parquet-job.ts` | 432 | **Archive.** |
| `src/runtime/wasm/` | ~450 | **Archive** with the browser engine. The host loads Wasm in `host/wasm.ts`. |
| `src/runtime/workers/` | ~320 | **Archive.** |
| `src/index.ts`, `src/public.ts`, `src/browser.ts` | ~20 | **Stop** exporting the archived engine from the host package. |

Do not move this tree as part of the host refactor.

## Scripts

| Keep | Delete when the host is the only runner |
| --- | --- |
| `build-workers.ts`, `build-cloudflare-prod.ts`, `deploy-worker.ts`, `report-node-worker-bundle.ts` | |
| `cf-production-jobs.ts`, `cf-q15-discard.ts`, `cf-hash-fanout.ts`, `cf-workerd-*.ts` as callers of `POST /jobs` | `cf-wcs-stream-bench.ts`, `cf-spill-*.ts`, `spill-insert-*`, `bench-distributed.ts`, `clickbench.ts` once they only POST a job |
| `r2-cloudflare-api.ts` if deploy still needs it | `firehose-day-query.ts` if it drives the archived engine |

## Tests that go with the deleted files

Delete with the module: `ready-port-scheduler`, `dispatch-limits`, `hash-exchange` / demux, `signal-router`, `scan-edge-session`, `chunk-stream`, `graph-runner-placement`, `job-replay`, `execution-graph`, `inter-do-wcs-pipe`, `wcs-frame-codec`, `spill-host` (after sqlite-port tests cover spill).

Keep, renamed onto `src/runtime/host/`:

- `host-loop.test.ts` — many ports, slow port, cancel, no re-entry.
- `sqlite-byte-streams.test.ts` — the SQLite port.
- `run-wcol-engine-remainder.test.ts` — a short host chunk is not dropped.
- One Wasm test that a real module copies bytes through `run()`.
- Cloudflare job tests that POST `/jobs` and read status. They must not import a scheduler.

## Order

The ABI v1 loop is already in `host-loop.ts`. Keep it.

1. **Reducer ingress.** Rust `Read(port)` replaces `ReadyPortScheduler`. Producer completion stays: a scanner that sends nothing is still reported finished.
2. **2×2 direct RPC.** Port-aware hash writes plus that ingress, no TypeScript demux and no signal router on this path. This passes before any large deletion.
3. **SQLite and R2 adapters.** Same byte ports, including ReadAt for Parquet and a host buffer budget. Existing spill and Parquet tests stay green.
4. **Coordinator and node.** One `node.run`, placement, fencing, publication, recovery, and the producer barrier.
5. **Delete** the old runners, demux, signal router, and scheduling pools only after that coverage exists.
6. **Production.** 16×16, empty edges, cancel, 16×1,000, then Q15 discard with isolate memory.
