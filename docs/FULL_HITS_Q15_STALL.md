# Full-hits Q15 stall

Notes from the November 2026 Cloudflare runs of ClickBench Q15 on `hits.parquet`. The query is

```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
```

with `hashReducers: 16` unless a run says otherwise, `groupSpill: true`, and `targetGroupBytes` of 16 MiB. DuckDB's top row on the full file is user `1313338681122956954` with count `29097`.

The same query on `hits-500k.parquet` matches DuckDB. The failure is specific to the full file under a wide hash exchange.

## What failed first

Job `k-8fc8c24dee50683021bb2c277086d862` died in about 35 seconds, three attempts, `nodeCount` 0. The job record stored

`ReadableStream received over RPC disconnected prematurely`

That string is the coordinator abort (`abortHashAttempt`). It is not the first error.

R2 task errors, attempt 1:

| Where | Error |
| --- | --- |
| Scan task 6 | `Durable Object is overloaded. Requests queued for too long.` at `acceptScanEdge` |
| Scan tasks 2 and 7 | `write to closed bounded pipe` |
| Most reducers 32–47 | `transport EOF before FRAME_END` |

Attempt 2 repeated the overload on tasks 6 and 7. Attempt 3 no longer showed the overload string; the stored job error stayed the disconnect. Cloudflare's overload message means an incoming request waited too long to enter the isolate. Those requests should not be retried immediately. The transport EOF and the closed pipe are what happens after the streams are aborted.

Scan task 0's last progress snapshot before the failure, not a finished total:

| Field | Value |
| --- | --- |
| Compressed parquet read | 8.7 MB |
| Framed rows already written | 96 MB |
| Wasm linear memory | 122,224,640 bytes (116.6 MiB, 1865 pages) |
| Resident groups | 0 |
| Spill runs | 0 |

Hash scans do not aggregate. The scan fragment decodes, projects, and hash-partitions raw rows. `residentGroups: 0` on a scan is expected. The ~11× expansion is compressed dictionary-encoded `UserID` values turned into uncompressed framed i64 rows. `wasmMemoryBytes` is the grown linear memory, which does not shrink. Reducers on that attempt had started spill: 7 runs, 229,376 groups flushed, 12 MiB peak live.

That 116.6 MiB figure is close to a 128 MiB isolate, but it is not what failed `acceptScanEdge`. Reducing scan memory (fewer I/O slots, less codec scratch, not copying a decoded batch into 16 partition buffers) is a separate change.

## What the host was doing

`acceptScanEdge` only checks the in-memory lease, registers the byte stream, wakes the host loop, and returns. It does not wait for Rust or SQLite. The overload happened because the reducer isolate could not enter the handler while it was inside synchronous Wasm or SQLite.

A Durable Object runs synchronous JavaScript on one thread. Incoming RPCs do not interleave until that turn yields. `await Promise.resolve()` is a microtask and does not let a queued RPC in. `scheduler.wait(0)` does. The engine's yield is checked at the start of an I/O poll, then `GraphHost::poll` could run up to 256 graph steps before returning to JavaScript. SQLite spill inserts were already bounded at 50 rows / 256 KiB. `performance.now()` does not advance during uninterrupted CPU on deployed Workers, so a short `wallMs` on a reducer snapshot is not evidence that the isolate was idle.

The scan start window is not the same as dispatch concurrency. `WCOL_MAX_ACTIVE_SCANS` and `WCOL_MAX_ACTIVE_INGRESS` cap how far ahead scans may be scheduled. `WCOL_DISPATCH_CONCURRENCY` (default 6) wraps each scan for its whole lifetime, so only 6 scans actually run.

## What we tried

### Observability

Workers logs and traces were off (`observability: null`). Both workers now have logs and traces enabled, sampled at 1, and persisted. The Wrangler OAuth token can tail, and can list R2. It gets 403 from the Workers Logs query API.

### Yield every 8 host ops, intending 32 scanners

Deployed node `45ba57fd-917f-440e-adb0-79b0ff785424`, coordinator `213d55db-4c3c-495b-9bba-fb9991ced1f2`, Wasm `23305a9adc9da333`.

| Knob | Value |
| --- | --- |
| `WCOL_YIELD_AFTER` | 8 |
| `WCOL_MAX_ACTIVE_SCANS` | 32 |
| `WCOL_MAX_ACTIVE_INGRESS` | 32 |
| `WCOL_DISPATCH_CONCURRENCY` | still 6 |
| Spill | SQLite, unchanged |

Job `k-b4a6b8e1b09c9b719a351102e86e3529`. This was a 6-scanner run. Every reducer recorded exactly 6 accepts.

`acceptScanEdge` returned in 9–42 ms. There was no overload error. Each reducer then published one snapshot: about 15 polls, `yieldCount` 1, 8–24 KB ingested, no output, about 24 MB of Wasm, `spillRuns` 0, and then nothing else. Scanners 0 and 2–5 kept yielding (53–94 times), wrote 43–64 MB, and at about 106 seconds failed with `write to closed bounded pipe`. Reducer `performance.now()` was still about 3 seconds. The coordinator abort, `ReadableStream received over RPC disconnected prematurely`, landed about 360 ms later. The job record eventually stored that disconnect after 3 attempts.

Attempt 2 of that job froze with no further progress and no error objects. Scan writes sat in the 2 MB pipe (`buffered > cap`, pipe not closed), so the coordinator never observed a failure until the leases ran out. The yield fires only when an I/O poll starts. One poll could still stay inside the 256-step graph pump, and the reducers crossed the 8-op threshold once and did not come back.

### Same build, 500k

Job `k-726872c959c8a1c6bcc8d12137d334f7` completed in 5.5 seconds, 21 nodes, 80 opened edges, and matched the DuckDB top 10. The stall is not "any hash reduction of `UserID`".

### One graph step per poll, local 500k

`PUMP_BUDGET` was set to 1. Local Q15, one scan and two reducers, SQLite spill on, finished in 719 ms and matched DuckDB. The longest synchronous poll was 9 ms on the scan and 3 ms on each reducer. The reducers took about 35,000 polls each, and every poll returned.

So the poll wrapper is not an unconditional loop. One `GraphHost` step can still drain every buffered frame, and a scan step can still decode until the next parquet read. The 500k file never hit a step that failed to return. The 8–24 KB figure on the full-file reducers is the last progress snapshot published before the poll that did not come back. Spill stats are published after a flush returns, so `spillRuns = 0` is compatible with being inside the first flush.

### One scan, two reducers, full file, one step per poll

Deployed node `b834f358-4115-4df9-8073-7fea00b25cb0`, coordinator `230e84fe-f5e9-4625-be9d-3ac655001f67`, Wasm `d3e695b354a4bc59`. `WCOL_REDUCER_TRACE=1` calls a synchronous `wcol.wcol_trace` import on frame entry, consume entry/exit, and flush entry/exit. A live tail did not show those lines: they stay inside the Durable Object invocation until it finishes.

Job `k-d23b75af19300d714087c1ba2799c057`, `maxNodes: 1`, `hashReducers: 2`. The early stall did not repeat. Attempt 1 ran about four minutes. Each reducer took about 50,000 polls and 3,000 yields, ingested about 80 MB, and flushed about 3 million groups across 90 spill runs. `consume` and `flush` were returning. The scan then failed with `write to closed bounded pipe` after its Wasm memory reached 205 MB. The reducer error with that text is the coordinator abort. The job then used its remaining attempts and ended `failed` with that pipe error. There is no `exceededMemory` or `exceededCpu` on the task errors.

This run changed the pump budget and the fan-in together, so it does not say which of those removed the 24 KB stall.

### Six scanners, sixteen reducers, one step per poll

Deployed node `33d22359-7b6b-444a-91bf-b40a24e64484`, coordinator `ec02b497-e878-4954-89ce-f8fcda7c4a1c`, same Wasm `d3e695b354a4bc59`. `WCOL_REDUCER_TRACE=0`. Yield, spill, admission, and dispatch concurrency were unchanged from the stalled six-scanner run. The only compute change was the pump budget, already 1 in that Wasm.

Job `k-907d23a3b0357f9aa7e69ed1e92c496d`. It failed on attempt 1 after about 140 seconds. The job record is `stalled: no task progress for 90000ms`. That string was not replaced by the later pipe error.

Each reducer that published an admit recorded all six scan accepts, within about a second, and then one progress snapshot: 15 polls, `yieldCount` 1, 24 KB ingested, `spillRuns` 0. Scanners 0–3 kept going longer: 242–286 polls, 6–8 MB of parquet read, 46–55 MB framed out, Wasm linear memory 79–99 MB, still no resident groups. Their stored error is `write to closed bounded pipe`. Reducer task errors are the stall string from the abort. No task error says `exceededMemory` or `exceededCpu`.

So one graph step per poll does not remove the wide-fan-in stall. The 256-step pump is not what separated this from the two-reducer run. A single `GraphHost` step can still drain every buffered frame or run a flush. The 24 KB snapshot is still the last publish, not a proof that only 24 KB was consumed.

### Phase breadcrumb, same six-scanner run

Slot 32 is the only reducer that writes the breadcrumb. After six scan inputs have attached, it upserts one SQLite row on phase changes for the next 20 polls, capped at 80 writes. A thrown request on a probe object kept that row (`phaseName: PROBE`), so the commit survives the request failing.

Job `k-e5c354a5d44ba648d3c8508258d9b336`, node `c7f8724e-b235-4142-9c15-8de7b523b547`, Wasm `11524115ca75f5b2`. Three attempts, and slot 32's row was the same each time:

| Field | Value |
| --- | --- |
| phase | `HOST_WAIT` |
| poll | 15 |
| task | 32 |
| group bytes on that row | 0, because the host-wait record does not copy Rust group state |

`HOST_WAIT` is written in TypeScript after `wcs_io_poll` has returned, immediately before the macrotask yield or the wake that waits for an I/O completion. Poll 15 entered and exited. The reducer was not inside Wasm when it stopped. Attempt 3's progress for that task is the familiar 15 polls, one yield, 24 KB in, no spill. The job record is again `stalled: no task progress for 90000ms`. Attempts 1 and 2 ended sooner, with scanner `write to closed bounded pipe` and reducer disconnects; their phase rows are still `HOST_WAIT` at poll 15.

### Which await, and which tickets

The host loop now records the wait it is about to enter. A return from that wait keeps the same phase and adds `returned` plus the post-return counters, so the stall abort cannot erase the outstanding tickets. Rust was not changed. Wasm is still `11524115ca75f5b2`.

Job `k-47fb29b64f9aa28fc962176391fc3921`, node `06889a82-68a4-423b-9d50-030f4594d21a`, coordinator `7a4d9d2f-eddf-49d3-a4e4-dcb5c33c16ad`. Slot 32, attempt 1:

| Field | Value |
| --- | --- |
| phase | `WAIT_IO` |
| poll | 15 |
| pending | 16 |
| completions queued | 0 |
| runnable | false |
| TypeScript completions | 6 |
| delivered to Rust | 6 |

All 16 outstanding operations are `read`, ports 6 through 21, tickets 7 through 22. Ports 6–15 were issued in one poll. Ports 16–21 were issued over the next three seconds, one as each earlier read finished. The six completions that Rust did receive match the six live scans, ports 0–5, and the 24 KB of reducer input. Nothing is queued ahead of Rust, so this is not a lost wake: `tsCompleted` and `rustDelivered` are equal until the abort.

The row then records `returned: WAKE_RETURNED` about 121 seconds later, with `pending: 0` and `tsCompleted: 22`. Those extra 16 TypeScript completions were the abort rejecting the reads. They were not delivered to Rust. The wake returned because the watchdog cancelled the streams, not because a scanner produced bytes.

Scanners 0 and 5 had kept running for that whole wait: about 230 and 207 polls, 6.5 MB of parquet in, 46 MB and 43 MB framed out, then `write to closed bounded pipe` once the reducer side was aborted. The reducer had stopped reading them.

`IO_SLOT_COUNT` is 16, so those sixteen reads are every host slot. The reducer is prepared with one input port per planned scan (`scanIds.length`, up to `WCOL_MAX_NODES`, 32). Dispatch concurrency is still 6, so only scans 0–5 are running. After their first read completes, the freed slots are spent on ports 6–21. Those scans have not been started. The reducer will not read ports 0–5 again until a slot frees, the running scans block in the 2 MB pipe, and the coordinator never reaches scan 6.

The macrotask yield is not this stall. `yieldCount` is 1 because an earlier yield returned. The await that lasted until the watchdog was `wake.wait()`.

A pending `reader.read()` no longer holds a Wasm copy slot. The host keeps at most one ready chunk per attached port, inside an 8 MiB budget, and posts `PortAttached`, `PortReady`, or `PortSealed` through the existing completion queue. Rust issues a stream `Read` only for a ready chunk, and only for as many bytes as the input ring can take. The host drops its byte charge for the copied prefix; a remainder stays host-owned and is announced again. Seal of a port that was never attached is absence. EOF of an attached stream is not.

`ready_chunks_are_copied_while_planned_ports_wait` covers that admission. `tests/reducer-unattached-ports.test.ts` passes both ways: the first six scanners finish through 64 KiB pipes before the other ports attach, and ports 16–31 finish while ports 0–15 stay attached and idle. Idle reads stay pending and do not occupy copy slots. `IO_SLOT_COUNT` and the byte budget are unchanged. Wasm build `0a6e446b78bc01f4`.

### Completed exchange frames are dropped

`RowBatchWcsEgress` used to keep every encoded frame. `poll_write` only advanced an index, so a scanner retained a second copy of every byte it had already handed to the host. That is what took the one-scan run to 205 MB of Wasm. There is still no 200 MB scan budget. Wasm linear memory does not shrink; the live ownership is the frames not yet copied.

The egress now stores frames in a `VecDeque` and `pop_front`s a frame only after that frame has been copied into the host buffer. `egressPendingBytes` is the bytes still waiting, `egressPeakPendingBytes` is the high-water mark of that queue, and `wasmMemoryBytes` stays the linear-memory high-water mark. `wcs_egress_releases_completed_frames` drains 16 partitions through a 5-byte buffer and checks that a partial frame stays, a finished frame is released, and the peak is one batch. `tests/egress-memory-plateau.test.ts` runs one local scanner across eight row groups.

Deployed node `8bf58ba9-06d3-4020-a13e-aadf4d93073b`, coordinator `98b561c6-297c-4d87-9a74-ed922ee32175`, Wasm `2d83a8df6361b74c`. Job `k-f1e4ab9fb504e303a1922484259b868f`, `maxNodes: 1`, `hashReducers: 2`, pump budget still 1.

On attempt 2 the scan's linear memory stayed at 40,566,784 bytes while framed output went from 42 MB to 185 MB. `egressPeakPendingBytes` stayed 2,172,216. `egressPendingBytes` returned to 0 between batches. The same plateau, 40,566,784 bytes, is on the first snapshot of every attempt. The reducers are a different curve: their Wasm grew with spill, to about 80 MB and 98 spill runs, and their egress pending stayed 0.

The job still failed, three attempts, stored error `failed after 3 attempts`. The scan error on each attempt is `write to closed bounded pipe`, at framed sizes of about 42 MB, 185 MB, and 141 MB. Those sizes are not a scan-memory cliff. Attempt 1's reducers recorded `transport EOF before FRAME_END` and `input stream failed`, which is the stream abort after the pipe closed. That failure stays with the reducer overload investigation.

## Still open

The 16-slot deadlock is not what failed job `k-f8bfed3d463cf29602a041fb5d816e91` (wasm `0a6e446b78bc01f4`, pump budget 1, six running scans, sixteen reducers). Slot 32 reached poll 1467 in `WAIT_IO` with one copy-read in flight (port 2, ticket 487) and 486 host read completions. It did not stop at poll 15 with reads of ports 6–21. The job still failed, in about 15 seconds and three attempts, with `Durable Object is overloaded. Requests queued for too long.` It did not reach a stored aggregate. One scanner and two reducers still get through tens of megabytes, because that plan has one scan port. After frame reclamation the scan's linear memory plateaus near 39 MiB; the closed pipe on job `k-f1e4ab9fb504e303a1922484259b868f` still happens, at unrelated framed sizes, and still has no `exceededMemory` or `exceededCpu` outcome.

### Which RPC, which object

Job `k-204e276f569a0d28a445535c90b7f1f3`, same wasm, node `594d01e0-0a6d-498f-89eb-598d7c47510a`, coordinator `e2a0cfb9-54ee-42f5-8437-f64ecb9985d7`. The coordinator now writes `first-rpc.json` and `first-overload.json` before an abort can replace the message. There is no `blockConcurrencyWhile` in the repo. Reducer `wcs_io_complete` failures now keep the Rust last-error string; the earlier `unknown Rust stream error` was that string being dropped.

| Attempt | First coordinator failure | First overload |
| --- | --- | --- |
| 1 | `runTask.body` from scan task 5 slot 5, `write to closed bounded pipe`, after 16 successful `acceptScanEdge` calls (slowest 1102 ms) | later `abortHashAttempt` to reducer task 34 slot 34, queued 223 ms |
| 2 | `waitConsumeReady` for scan index 6, reducer task 36 slot 36, queued 684 ms | same call |
| 3 | `waitConsumeReady` for scan index 8, reducer task 45 slot 45, rejected in 15 ms | same wave, reducer task 41 slot 41 |

Attempt 1's overload is the cancellation RPC. The attempt's own first record is the closed pipe. Attempts 2 and 3 fail on `waitConsumeReady` before that scan's `runTask` starts. `readyToAccept` has already marked every slot consume-ready, so the handler returns as soon as it is admitted. The 15 ms rejection on attempt 3 means the queue was already past the limit. Scans 6 and 7 on attempt 3 then recorded the overload on `acceptScanEdge` itself (16 admits, slowest 1344 ms and 1509 ms).

The targeted reducers were already aggregating. Attempt 1 task 34: 17,005 polls, 18 host yields, 4 spill runs, resident groups 0, 22 MB in. Attempt 2 task 36: 12,669 polls, 12 yields, 3 spill runs. Attempt 3 tasks 41 and 45: about 18k–22k polls, 16–23 yields, 4–5 spill runs. A host yield is the only macrotask turn (`scheduler.wait(0)`). Rust counts one toward `WCOL_YIELD_AFTER` when it issues an I/O operation, not on every poll, so thousands of polls that only copy a ready chunk or spill run inside one JavaScript turn. Spill uses `storage.transactionSync`. Incoming `waitConsumeReady` and `acceptScanEdge` wait on that turn. Scanners in the same attempt were at 78–122 MB of wasm and had not reached the older 205 MB figure.

### Dispatch concurrency 2

Same plan, 32 planned scans and 16 reducers, `WCOL_DISPATCH_CONCURRENCY=2`. Job `k-54b52c547cab009419de23b6eb839275` still failed, at about 60 seconds, with the same overload string. It did reach spill.

| Attempt | First coordinator failure | Reducer progress |
| --- | --- | --- |
| 1 | `waitConsumeReady` for scan index 4, reducer task 40 slot 40, rejected in 13 ms | 14k–18k polls, 12–16 yields, 3–4 spill runs, 18–23 MB in |
| 2 | `runTask.body` scan task 3, `write to closed bounded pipe` after 14 s. No coordinator overload record | every reducer at 5 spill runs, 20 yields, 19k–21k polls, 25–27 MB in |
| 3 | `runTask.body` scan task 4, the overload string, after 2.0 s. The scan's own error is that string with 16 admits | 13k–15k polls, 3–4 spill runs |

Two running scans still leave the reducers inside a synchronous stretch long enough that a later scan cannot enter `waitConsumeReady` or `acceptScanEdge`. The stream ownership model, 16 copy slots, and 8 MiB budget were not changed.

### Poll quantum

The host loop now macrotask-yields after 64 completed polls, whether or not Rust issued I/O. `WCOL_YIELD_AFTER` still counts issued I/O operations inside Rust. Each SQLite insert batch is one `transactionSync`, and the adapter awaits `scheduler.wait(0)` after the callback returns. `prepareReducer` is the readiness barrier, so the scan loop no longer calls `waitConsumeReady`.

Job `k-f3a5e77660125c1e9e83556ebf3dc7dc`, wasm `2d83a8df6361b74c`, dispatch concurrency 2. No attempt wrote `first-overload.json`. Reducer snapshots show `maxPollsBetweenYields` 2 and `sqliteBatchesBetweenYields` 1, with 5,000–12,000 host yields across 20,000–59,000 polls. Spill reached 13–18 runs and 55–75 MB ingested while later scans still recorded 16 admits. The job then failed for a different reason: attempt 2 reducer task 45, `Durable Object's isolate exceeded its memory limit and was reset`, with reducer wasm already 93–109 MB. Attempt 1's first record is still `hash attempt is not the active node session` on scan 11 after 16 admits. The earlier `write to closed bounded pipe`, before any cancellation, is still unexplained when it is the first record.

The coordinator fails an attempt that publishes no newer task progress for 90 seconds, records `stalled: no task progress for …`, and does not wait for the abort past five seconds. A stall is not retried on its own. Earlier non-stall failures in the same job still are.

Not tried yet:

- A drain-only reducer, and six scanners into two reducers.
- One scanner, pump budget 256, sixteen reducers.
- Why the one-scan two-reducer pipe still closes after the scanner has stopped retaining frames. Reducer Wasm on that run kept growing with spill, up to about 80 MB.

## Deployed experiment settings

These are still on the workers. They are not tuned defaults.

| Worker | Setting |
| --- | --- |
| Node | `WCOL_YIELD_AFTER=8`, `WCOL_REDUCER_TRACE=0`, `WCOL_PHASE_SLOT=32`, pump budget 1 |
| Coordinator | `WCOL_MAX_ACTIVE_SCANS=32`, `WCOL_MAX_ACTIVE_INGRESS=32`, `WCOL_FIXED_NODE_POOL=0`, stall deadline 90s |
| Both | logs and traces on, head sampling 1, persisted |

`WCOL_DISPATCH_CONCURRENCY` is pinned to 2 for the memory diagnostic. The code default remains 6.

### Spill boundaries

Each completed spill run now records wasm linear memory, the allocator's live and peak bytes, group-table bytes, hash-index capacity, interner capacity, the last page buffer, host ready bytes, and SQLite append bytes. `releaseIfOwner` drops a session only when that execution id is still current, and `hash attempt is not the active node session` names the rpc job, the current execution, and why the previous one ended. A late `finally` is covered by `tests/session-fence.test.ts`. Yields were not retuned.

Job `k-88da417dbfacce521256012456bd87df`, wasm `6d5db91357c681a6`, dispatch 2, `targetGroupBytes` 16 MiB. Node `93ddfa66-7963-46cd-9175-e2256396f627`, coordinator `b4f5e2a8-b49a-4b7d-9343-40ac1deb1e5d`. Failed after 3 attempts, `input stream failed`. Attempt 1 reducers 33–45 and 47 reset with `Durable Object's isolate exceeded its memory limit`. No overload file. Reducer 32, 41 spill runs:

| | Run 1 | Run 41 |
| --- | --- | --- |
| Wasm linear memory | 38.8 MB | 188.3 MB |
| Group table after reset | 7.14 MB | 7.14 MB |
| Hash index capacity | 57,344 | 57,344 |
| Interner | 0 | 0 |
| Last page buffer | 147,456 | 183,360 |
| Host ready bytes | 8 KB | 0 |
| SQLite bytes appended | 3.47 MB | 142.1 MB |

The table, index, interner, and page buffer do not grow. Wasm grows about 3.7 MB per run, in step with the 3.47 MB appended to SQLite on that run.

Job `k-30b2580f5c903884523960390f030b9f`, wasm `4c56b7a7b3740fa2`, same dispatch, `targetGroupBytes` 8 MiB. Node `5adba202-29f4-41a1-99d6-d9c92e7b693f`, coordinator `0c080036-9d91-4358-a05a-cb38afebb216`. It failed after 3 attempts with `write to closed bounded pipe`. Attempt 1's first RPC is `runTask.body` with that message. No overload file. Reducer 32 then ends with `input stream failed` after 16 runs:

| | Run 1 | Run 16 |
| --- | --- | --- |
| Wasm linear memory | 33.0 MB | 67.0 MB |
| Live allocator bytes | 26.8 MB | 56.0 MB |
| Allocator peak | 27.8 MB | 58.1 MB |
| Group table after reset | 3.57 MB | 3.57 MB |
| Live groups | 16,384 | 16,384 |
| Last page payload | 40,704 | 40,704 |
| Host ready bytes | 8 KB | 25 KB |
| SQLite bytes appended | 1.73 MB | 27.8 MB |

Halving the group budget halves the steady table (7.14 MB to 3.57 MB) and halves the bytes per run. It does not stop the climb. Live allocator bytes rise with the spilled payload, and the peak stays within about 2 MB of live, so this is retained heap rather than a wasm buffer that merely refuses to shrink. The 16 MiB target is not the working set that hits the isolate limit. The spill path is keeping about one copy of every flushed page. Host ready bytes stay in the tens of kilobytes. The local regression `tests/reducer-spill-memory.test.ts` fills and spills a small key set and checks that wasm stops growing after warmup; that case does not retain a full-query spill.

Reducer progress now carries stage snapshots per completed spill run: `liveBefore` and group bytes at flush entry, `serializeBytes` and `appendBufCap` when Rust hands a page to the host, `hostPendingOps`, `hostPendingSqlBytes`, and `sqliteWriteBytes` when SQLite accepts a chunk, `rustQueuedSpillBytes` and `rustInflightSpillBytes` peaks for the run, and `liveAfter` after the group table reset. `tests/reducer-spill-plateau.test.ts` runs 50+ same-sized spills and asserts post-flush live bytes plateau plus bounded queued/inflight/host SQL payload counters.

The host clears `spill_append_buf` after SQLite ack. **`fill_spill_exec` now `take_pending`s once per op** (guarded by `spill_op`) and moves append bytes instead of cloning on every poll; the I/O bridge drops encoded SQLite record buffers with `Vec::new()` after each append completes. Bounded pipes record the first close/abort owner on later `write to closed bounded pipe` errors.

### Instrumented run

Job `k-89d6a4d8c0084995ea18f3b64de4b189`, wasm `666599c045cbc79a`, dispatch 2, `targetGroupBytes` 16 MiB. Node `e20922ab-ebab-4058-81c4-288add52f278`, coordinator `414e43d3-ca55-4df4-ae62-6e876d5ac440`. Failed after 3 attempts with `Durable Object is overloaded. Requests queued for too long.`

| Attempt | First coordinator record | Reducer 32 instrumented spill |
| --- | --- | --- |
| 1 | `runTask.body` overload | 1 boundary, then `transport EOF before FRAME_END` |
| 2 | (in progress when sampled) | 34 runs: `liveBefore` 31.4 MB → 148.8 MB; `serializeBytes` / `appendBufCap` / `ioEncodeCap` flat at ~114 KB / 192 KB / 117 KB per run |
| 3 | overload (stored job error) | phase slot 32 at poll 11423 in `WAIT_IO` with six scan reads pending |

The stage fields confirm the climb is in cumulative `liveBefore` / `liveAfter`, not in per-run serialize or I/O encode capacity. Overload returned before a memory reset on this job; attempt 2 reducer 32 was already near 150 MB live at 34 spill runs.

### Post ownership-fix validation (take_pending + payload counters)

Deployed wasm `030ffaa442a32456`, node `aab174b0-2c10-45ed-91d0-ad0b83150125`, coordinator `cd589f55-f787-445c-8174-6fcd1986f8c7`, dispatch 2, `targetGroupBytes` 16 MiB. Job `k-ae179896466b4d53b734fb51c06253f1` failed after 3 attempts with `input stream failed` (attempt 3 reducer 32 phase trace: `WAIT_IO` with one spill sqlite `write` still pending).

| Attempt | Reducer 32 spill runs | `liveBefore` run 1 → last | `rustInflightSpillBytes` (peak/run) | Cumulative SQLite append |
| --- | --- | --- | --- | --- |
| 1 | 41 | 31.3 MB → 174.8 MB (`liveAfter`) | flat ~228 KB | ~142 MB |
| 2 | 34 | (same pattern as pre-fix instrumented run) | flat ~228 KB | (partial progress) |
| 3 | 18 | climbing ~3.6 MB/run | flat ~228 KB | (partial progress) |

Per-run **`rustQueuedSpillBytes` / `rustInflightSpillBytes` stay page-sized** (~114 KB queued + ~228 KB inflight peak, consistent with one append page plus encode buffer). **`liveBefore` / `liveAfter` still rise ~3.5–3.6 MB per completed spill run**, in step with cumulative SQLite bytes (~3.47 MB/run). The handshake change removed per-poll append clones and proved the in-flight spill payload is not the cumulative leak; retained live heap still tracks total bytes persisted to SQLite on another path (not `serializeBytes`, `ioEncodeCap`, group table, or hash index).

### Per-run lifecycle checkpoints

Each spill boundary now adds:

| Checkpoint | Field | Meaning |
| --- | --- | --- |
| `FLUSH_ENTER` | `liveBefore` | Before serializing the run |
| `SERIALIZED` | `liveSerializedPeak` | Peak live while encoding spill pages |
| `SQL_ACKED` | `liveSqlAcked` | After every page ack, before group-table reset |
| `GROUP_RESET` | `liveAfter` | After `reset_live_after_run` |
| Cross-run | `liveBefore[N+1] − liveAfter[N]` | Retention between runs |

`mergeScanBegins` / `mergeSteps` on each boundary should stay **zero during ingest flushes** (merge only starts in `begin_finish_chunks` after scans seal). That rules out merge as the source of the first 41 flushes on Q15.

Local **`tests/reducer-spill-lifecycle.test.ts`**: real reducer, host SQLite byte port, **DELETE after each insert batch** (discard-on-ack), 50+ flushes — **passes** (post-reset live and cross-run gap bounded). That suggests the ~3.5 MB/run retention on full Q15 may need **larger per-run spill volume** (16 MiB group target, many pages per flush) to reproduce outside production; next step is scale the lifecycle test toward Q15 page counts or run native `dhat` on a multi-page host-path harness. Native file spill probe only: `scripts/run-spill-retention-probe.sh`.

### Scaled local lifecycle results (Mar 2026)

Command: `npm run test:spill-lifecycle-scale` (or `--test-name-pattern "MiB"`). Synthetic parquet: `printf('user-%d')` keys, `GROUP BY` + `COUNT(*)`, `hashReducers: 2`.

| Case | Spill runs | Bytes acked (host) | `liveAfter` run 1 → last | Per-run `liveAfter` Δ | Q15-like (~3.5 MiB/run)? |
| --- | --- | --- | --- | --- | --- |
| 32 KiB discard | 50+ | (small) | plateaus | ~0 | No |
| 1 MiB discard | 64 | ~84 MiB | 65.0 MiB → 63.2 MiB | **−17 KiB/run** (falls) | No |
| 16 MiB discard | 64 | ~317 MiB (~4.9 MiB/run acked) | 85.0 MiB → 85.3 MiB | **+11 KiB/run** | No |
| 16 MiB retain SQLite | 64 | ~317 MiB | 85.0 MiB → 85.3 MiB | **+11 KiB/run** | No |

Observations:

- **`mergeScanBegins` / `mergeSteps` stay 0** on all ingest flushes (merge not involved).
- **`liveSqlAcked` matches `liveAfter`** (no leak between SQL ack and group reset).
- **~85 MiB post-reset baseline** at 16 MiB target is mostly stable capacity (not cumulative 3.5 MiB/run steps like Cloudflare job `k-ae179896…`).
- **Retaining vs discard-on-ack SQLite does not change Rust live** — the CF climb is not explained by host SQL payload retention alone in this harness.
- **Gap to Q15:** multi-reducer RPC ingress (16×), real `hits`/`UserID` partial encoding, or coordinator/DO timing — not reproduced locally yet. Set `HITS_PARQUET=/path/to/hits.parquet` to run the hits variant when available. Superseded by [Current hypothesis](#current-hypothesis-november-2026) below: real `UserID` partials at the 8 MiB spill volume stay flat in Node and Miniflare.

### Controlled fan-in experiment (Mar 2026)

**`npm run test:spill-fanin`** — real `UserID` u64 path, WCS hash exchange, WASM host, retaining SQLite spill.

| Case | Planned scans | Hash reducers | Active scans | Dispatch |
| --- | --- | --- | --- | --- |
| **A** | 1 | 2 | 1 | 1 |
| **FULL** | 32 (env `SPILL_FANIN_SCANNERS`) | 16 | 2 (`SPILL_FANIN_ACTIVE_SCANS`) | 2 (`SPILL_FANIN_DISPATCH`) |

**FULL** runs the graph **once** and records both **ref reducer** (task id / slot **32**, same as CF traces) and **worst** post-warm `liveAfter` slope. Per-reducer metrics: `liveAfter`, cumulative **WCS input** bytes, cumulative **SQLite append** bytes, **ingress fan-in retained** peak, `spillRuns`.

On full hits, asserts DuckDB reference top row **`1313338681122956954` / `29097`**. **`SPILL_FANIN_STRICT=1`** enables the 40+ spill red gate on ref + worst.

**Data:** full `hits.parquet` is not in git. Local FULL uses HTTP range reads (default [`hits_compatible/hits.parquet`](https://datasets.clickhouse.com/hits_compatible/hits.parquet)) via `HITS_URL` / `HITS_PARQUET`. Production R2 objects use the same bytes through Miniflare/coordinator (`sources.hits.key` on bucket OUTPUT). Quick check: `npm run test:spill-fanin-probe`. Full run (long): `npm run test:spill-fanin-full`.

If FULL stays flat on full hits → shift repro to Miniflare DO; if ref/worst climb ~3.5 MiB/run → profile Rust/host ownership locally.

**Host fixes for FULL over HTTP:** `SPILL_FANIN_HOST_BYTES` (default 512 MiB for FULL) is the TS ledger/pipe pool, not Rust `liveAfter`. Wasm linear memory above ~2 GiB returned signed-looking pointers from FFI; host I/O now uses `wasmMemoryU8` (`ptr >>> 0`) so slot/complete views do not throw `Start offset -2045419224…`.

### Local metric validation (Mar 2026)

The Node hash harness used to run **all reducers and scans on one `loadNodeWasm()` singleton**. Rust spill boundaries (`wcs_spill_boundary_*`) are **per Wasm module**, not per task — so every reducer could report **identical** `liveAfter` / spill rows while WCS input differed, and **`maxScanWasmBytes` tracked cumulative linear memory** (~2.5 GiB) instead of per-scan egress peaks (~40 MiB after `RowBatchWcsEgress` reclamation).

Fan-in runs now default to **`isolatedNodeWasm`** (one fresh instance per reducer and per scan). Set **`SPILL_FANIN_SHARED_WASM=1`** to reproduce the old aliasing. Output includes **`metricValidation`** (Wasm instance ids, `suspiciousIdenticalReducerSummaries`) and **`scanIngress.maxScanWasmPeakOverStartBytes`**.

**Strict coverage:** `SPILL_FANIN_STRICT=1` **fails** when ref/worst see **&lt; 40** spill runs — no plateau pass on insufficient data (`tests/helpers/spill-coverage.ts`).

**Next repro (no full hits read):** `npm run test:spill-do-focused` — **~1–2 min** local: one scan, one reducer, synthetic **`UserID` u64**, **`isolatedNodeWasm`**, **40+ spills** (`SPILL_FAST_GROUPS` / `SPILL_FAST_TARGET_BYTES`), strict plateau + **`spillOwnershipFromBoundaries`**. Optional DO smoke: `npm run test:spill-do-focused:mf` (~30s, `cf-spill-quick` scale — not 40-spill coverage).

**Scope check:** `npm run test:spill-metrics-scope` (topology **A**, shared vs per-isolate Wasm).

## Current hypothesis (November 2026)

The +3.5 MiB/run `liveAfter` climb is a property of the production Cloudflare isolate on full-hits Q15. It is not a mis-decoded spill record, not shared-Wasm accounting, and not "one retained copy of every flushed page" on any runtime we can execute here. The deployed binary that climbed in production (`030ffaa442a32456`) stays flat on the short fixture, and the current binary stays flat on real `UserID` partials at the same per-run SQLite volume as the 8 MiB Cloudflare job.

What is ruled out:

| Idea | Result |
| --- | --- |
| A. Local metrics were cumulative because every task shared one Wasm | True for the old FULL harness, and fixed. Later runs use `isolatedNodeWasm`. The production climb was measured per isolate, so this does not explain it. |
| B. Durable Object `storage.sql` retains the spilled bytes inside the Wasm heap | Not reproduced. Miniflare with the same fixture as Node stays within tens of KiB/run, including the deployed Wasm and the real-`UserID` prefix. |
| C. The short synthetic fixture was too small or too unique to spill like Q15 | The real-`UserID` prefix below spills **~1.74 MiB/run** at an 8 MiB group target, matching job `k-30b2580f…` (1.73 MiB on run 1). `liveAfter` still does not climb. |

Still open: full hits on the production isolate (16 reducers, 32 scans, 16 MiB target, isolate memory pressure). A local or Miniflare full scan is the wrong next measurement. A capped job on that isolate is the one that can still show the climb.

### Wire and Wasm

Deployed node worker `aab174b0-2c10-45ed-91d0-ad0b83150125` is Wasm `030ffaa442a32456` (929,278 bytes). It writes **23 × u32 = 92 bytes** per spill record, ending at `rustInflightSpillBytes`. The working tree writes **27 × u32 = 108 bytes** (`liveSerializedPeak`, `liveSqlAcked`, `mergeScanBegins`, `mergeSteps`). Dist after the rebuild is `4f2f5838167f5b0b` (929,799 bytes).

`readSpillBoundaries` picks the stride from the byte length (`spillBoundaryStride` in `src/runtime/host/run.ts`). A 27-word reader on a 23-word wire shifts every record after the first: the bogus run id `1785856` was `liveTableBefore` of the real second spill. Job `k-e27663eda3db8e85d1477c6a5db4f9f4` was decoded that way. Do not cite it.

`sqliteWriteBytes` (word 19) is the **last page** of the run (21,888 or 43,776 bytes here), not the run total. Run volume is host `bytesAcked` or the worker's `sqliteAppendBytes`.

### Shared Wasm on the local full fan-in

`npm run spill-fanin-full-run` on full hits (about 22 minutes) matched the DuckDB top row `1313338681122956954` / `29097`. It is not a memory result. One `loadNodeWasm()` served every reducer and every scan, and the spill boundary is a **per-module global**:

- 5 spill runs, 437,760 bytes appended to SQLite. Too few to call a plateau. Strict mode now requires 40 runs (`tests/helpers/spill-coverage.ts`).
- All 16 reducers reported the same ~1.34 GiB `liveAfter`.
- Scanner `memory.buffer.byteLength` peaked at 2,871,328,768, next to 2,665,897,730 bytes of cumulative WCS output. The buffer does not shrink.

Fan-in now defaults to one Wasm instance per reducer and per scan. `SPILL_FANIN_SHARED_WASM=1` restores the singleton.

### Short unique-`UserID` fixture

220,000 distinct `UserID`s, 4 MiB group target, one row group, `hashReducers: 2`, `maxNodes: 1`. In-flight spill bytes stay **228,004** on every run. Post-warm `liveAfter` growth:

| Runtime | Wasm | Spills | SQLite | `liveAfter` growth |
| --- | --- | --- | --- | --- |
| Node `test:spill-cf-signature` | `4f2f5838167f5b0b` | 14 | 23,320,000 acked (~1.67 MiB/run if shared, ~0.83 MiB each) | **−3 KiB/run** (20.86 MiB flat) |
| Miniflare, completed ~10s, job `k-a0e1f14aa66e37c11c7b6affbe324dca` | same | 8+ | task 1: 11,628,200; task 2: 11,686,500 | **−7,150** and **−13,291 B/run** |
| Miniflare, cap 8, job `k-a849991bc4f7cbaeba7cc93c53508f51` | same | 8 | 6,943,000 each | **+13,869** and **+122 B/run** (`liveAfter` 21.01 → 21.08 MiB and 21.07 → 21.08 MiB) |
| Node, `WCOL_NODE_WASM_FILE` = deployed file | `030ffaa442a32456`, 23-word stride | 14 | 23,320,000 acked (~1.67 MiB/run) | **−3,235 B/run** |
| Miniflare, cap 8, job `k-1ee04af597f08376ac165bebe31b636d` | deployed file, reader rebuilt into `node-worker.js` | 8 | 6,943,000 each | **+13,840** and **+94 B/run** (21.01 → 21.08 MiB and 21.07 → 21.08 MiB) |

Commands: `npm run test:spill-cf-signature`, `npx tsx scripts/spill-do-probe.mts` with `SPILL_DO_PROBE_CAP=8`. The cap is a Miniflare binding (`WCOL_SPILL_RUN_CAP`). It is not set on the deployed worker.

### Real `UserID` prefix at the 8 MiB spill volume

`npx tsx scripts/spill-hits-prefix.mts`. Source: first 2,200,000 rows of `data/hits_subset.parquet` (332,102 distinct `UserID`s), rewritten as ~122,880-row groups. Two row groups of `hits_subset_2m.parquet` are only ~17,000 distinct users, which fills an 8 MiB table about once, so they cannot show an 8-run slope. Group target 8 MiB, `hashReducers: 2`, isolated Wasm, stop at 8 ingest spills (`mergeScanBegins` stayed 0). About 6 seconds in Node, about 22 seconds in Miniflare.

Per reducer the host acked **~1.74 MiB/run**. That is the 8 MiB Cloudflare job (`k-30b2580f5c903884523960390f030b9f`: 1.73 MiB on run 1, live 26.8 → 56.0 MiB over 16 runs), not the 16 MiB job's 3.47 MiB/run. Last page was 43,776 bytes. In-flight stayed 228,004.

| Runtime | Reducer | `liveAfter` (post-warm → last) | Growth | SQLite at the cap |
| --- | --- | --- | --- | --- |
| Node | task 9 | 23.46 → 23.43 MiB | **−6.7 KiB/run** | 27,782,600 acked, shared by both reducers |
| Node | task 10 | 23.46 → 23.43 MiB | **−6.0 KiB/run** | same counter |
| Miniflare `k-358890bafabb8ae3e84d342d817c45cb` | task 9 | 23.92 → 24.07 MiB | **+29 KiB/run** | `sqliteAppendBytes` 13,891,300 |
| Miniflare, same job | task 10 | 24.15 → 24.13 MiB | **−3.7 KiB/run** | `sqliteAppendBytes` 13,891,300 |

Node slopes are from run 3 through run 8. The script printed task 10's summary one callback early (+0.7 KiB/run over 7 runs); run 8 then landed at 23.43 MiB. Neither reading is a climb.

Miniflare used dist Wasm `4f2f5838167f5b0b` and `WCOL_SPILL_RUN_CAP=8`. The cap message is `perRunLiveGrowthPostWarm=28773` and `-3714` with `liveAfter=23923118->24066983` and `24150083->24131514`.

### Next implementation (approved)

**Replace SQLite group spill with immutable sorted R2 runs** — see [R2_GROUP_SPILL.md](./R2_GROUP_SPILL.md). Do not repair DO SQLite spill; per-group row writes drove **>287M DO SQLite rows written** (~$238). R2 spill must still prove bounded `liveAfter` in its own memory tests; switching storage alone is not assumed to fix the full-hits climb.
