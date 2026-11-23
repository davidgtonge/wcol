# Cloudflare Q33 2M — post-spill hang status

Status notes for getting ClickBench **Q33** through **real Durable Objects** with R2 sorted-run group spill. Written 2026-11-05. Complements [FULL_HITS_Q15_STALL.md](./FULL_HITS_Q15_STALL.md) (older SQLite-spill / overload history) and [R2_GROUP_SPILL.md](./R2_GROUP_SPILL.md) (spill design).

## Goal

Run Q33 on `hits-2m.parquet` on production Cloudflare Workers / Durable Objects and meet acceptance:

| Check | Required |
| --- | --- |
| Result | DuckDB parity (same top rows) |
| Isolate | No DO isolate kill / OOM reset |
| Spill | R2 sorted-run flush + commit + merge path exercised |
| Metrics | Progress / spill boundaries published |
| Cleanup | No leftover `wcol-spill/…` objects for the job |
| Shutdown | Soft shutdown after each run (`deleteAlarm` + release Wasm; not `purgeStorage`) |

Acceptance script: `scripts/cf-q33-production.ts`  
Coordinator: `https://wcol-coordinator.<your-subdomain>.workers.dev`

## Constraints (locked)

- No legacy restore (ABI v1 / byte-pipe / `wcs_io_*` path stays gone).
- No stream-cap / WCS / topology redesign without new evidence.
- Flush / hard / retained budgets stay: **16 MiB flush**, **32 MiB hard**, **8 MiB retained low-water**.
- Freeze hash budget / spill state machine / WCS shape unless a concrete bug forces a change.
- Do not commit unless asked. Git root for this work: `wcol/`.

## Update 2026-11-05 (evening): first green real-DO Q33

Job `k-13260090783a4270545f31f1fce8ec73` completed on attempt 1: DuckDB parity, 26 spill runs (13 per reducer), peak Wasm 68.9 MiB, wall 148s. Two root causes, neither in the spill state machine:

1. **Reducer DO instance swapped mid-job.** `prepareReducer` and `acceptScanEdge` returned immediately, so the reducer had no request in flight while it ingested. Within ~6–10s the runtime routed new requests to a **fresh instance** of the same DO id. That instance had no session, so it rejected later scans' accepts (`hash attempt is not the active node session … no prior end`). Meanwhile the original instance kept running as a zombie. The rejected caller never settled, so scans 2+ hung and the job hit the 300s watchdog. No error status is recorded for this (Workers Logs and Analytics show only `ok`/`canceled`, with zero memory, CPU or fatal errors). **Fix:** `acceptScanEdgeOnNode` holds the RPC open until the edge has been consumed (relay `pipeTo`).
2. **Spill run keys collided across attempts and jobs.** Rust names runs `wcol-spill/{partition+1:016x}/run-N.bin` (`graph_host.rs`, `session = partition + 1`). Every attempt, every job and any zombie reducer wrote the same keys. Once (1) was fixed, merges failed in a different way on each attempt (`unsupported canonical key version`, `run read stalled`, `wcs_sp_spill_complete failed`). **Fix:** `WcolHost.spillKeyPrefix`, applied by the pool spill driver. The node sets it to `_jobs/{job}/a{attempt}/t{task}/`, which also makes per-job cleanup a prefix delete.

Also kept: `run-hash-job.ts` now surfaces the original failure instead of waiting out the stall (regression test in `tests/hash-job-abort-propagation.test.ts`), plus a prefix test in `tests/spill-r2-pool-read.test.ts`.

Disproved on the way: a lost wake in the stream pool, a reducer stuck after the first spill, shared-isolate memory, event-loop blocking (yielding after every step did not help) and a finalizer abort.

**Miniflare follow-up (2026-11-06).** `CLICKBENCH_ONLY=Q33 scripts/cf-clickbench-miniflare.ts` failed with `hash attempt is not the active node session … last end … reason stream settled`. Cause: the reducer finishes and releases its execution as soon as every port reaches EOF, which can happen before the coordinator's `sealReducer` arrives. Holding accepts open made that ordering more likely. **Fix:** `sealReducer` is a no-op when this attempt's execution has already ended with `stream settled`. The reducer's result or failure still travels on its body stream. After the fix, Miniflare Q33 matches DuckDB and the full Miniflare ClickBench suite gives 53 matches and the 3 expected unsupported queries. Deployed (node `a927748b`, coordinator `bca5a6d9`). Real-DO Q33 2M job `k-e44c6e4a08f56aab7cc2cec7b5fbed19` completed on attempt 1: DuckDB parity, 26 spill runs, peak Wasm 65.2 MiB, wall 68s.

The temporary `DIAG(q33)` code is removed (`wcs_sp_diag`, `GraphHost::diag`, `StreamPool.diagnostics()` (now `stats()`, the profile counters only), the `runProgram` watchdog, the edge send/accept logs, isolate counters, the coordinator `reducer_abort` log and `WCOL_LOCAL_RECHUNK`). The stream cancel on a rejected accept stays: `refuseScanEdge` is what fails a refused scan promptly (`tests/scan-edge-refusal.test.ts`). Still to do: run the spill leftover check with an R2 listing token (`leftoverSpillListing: skipped-no-token`) and a soft-shutdown audit.

## Where we are (one line)

**Local R2-spill Q33 passes. Every real-DO Q33 run still dies the same way: first spill completes (`spillRuns: 1`), then the reducer advance loop goes silent until the 300s stall watchdog.**

A real-DO Q33 2M run has now succeeded (see the evening update above).

## What works locally

`scripts/local-q33-r2-spill.ts` (in-memory R2 bucket, same host spill driver as CF):

- Completes in ~49–54s.
- Multiple spill runs / objects (order of ~24 spill objects in one measured run).
- Wasm peaks in the ~60–70 MiB range after reclaim work (earlier climbs toward 100+ MiB were addressed in retained-capacity / flush cooperative paths).

So the **Rust spill SM + TS `createPoolSpillDriver` one-shot put path are viable outside a Durable Object**. The remaining failure is **DO / RPC / single-threaded scheduling specific**.

## What fails on real DOs

### Symptom (stable across builds)

1. Job accepts; reducers prepare; scans open edges; data flows.
2. Each reducer publishes progress with **`spillRuns: 1`**, one spill boundary, Wasm ~36–40 MiB, wall ~13–17s.
3. Then **no further progress JSON updates** for the stall window.
4. Coordinator records `stalled: no task progress for 300000ms` (`WCOL_STALL_MS`).
5. `nodeCount` often still `0` on the job record (progress is read from R2, not that field).

### Latest measured job (pipe-decoupling deploy)

| Field | Value |
| --- | --- |
| Job | `k-e794fe5a08fdc1934a288398992e0d44` |
| Error | `stalled: no task progress for 300000ms` |
| Deploy | coordinator version `71922c54-a949-4f75-ad31-ab9c09eaab84` |
| Wasm build id | `063edf8b9989dee5` (~964477 bytes) |
| Reducer tasks | `t10`, `t11` (2 hash reducers) |

Example reducer snapshot (`t10`):

- `wallMs` ≈ 12953, `pollIterations` **2**, `yieldCount` **418**
- `spillRuns` **1**, one `spillBoundaries` row (`run: 1`, `liveAfter` ~10 MiB, `tableAfter: 0`)
- `inputBytes` / `outputBytes` reported as 0 on that snapshot (progress fields; not proof that no ingress occurred)
- Then silence until stall

Earlier jobs with the same shape: `k-9f3e9f0f…`, `k-ee1e8f62…`, `k-72df9e22…` (some also hit session / spin errors under more aggressive host-loop experiments).

### What the stall watchdog measures

`startRenewal` in `cloudflare/src/coordinator/renewal.ts` lists `_jobs/{jobId}/a{attempt}/*-progress.json` and stalls if the newest `at` stamp is older than `WCOL_STALL_MS` (default **300000**). Heartbeats only help while the isolate is still entering `yieldToEventLoop` / spill `onProgress`. After the freeze, those stops fire too.

## Work already done (this arc)

### Spill / memory (mostly settled; keep frozen unless new evidence)

- Hash group flush / hard / retained budgets documented in [HASH_GROUP_BUDGET.md](./HASH_GROUP_BUDGET.md).
- Retained-capacity reclaim after spill so Wasm live group bytes drop instead of climbing forever on local runs.
- R2 sorted-run spill replaces DO SQLite per-group inserts (cost / overload history in Q15 notes).

### Post-spill hang fixes attempted (Rust + host)

These are in tree and deployed on the latest wasm/host; **they did not clear the real-DO hang**:

| Area | Change | Intent |
| --- | --- | --- |
| `batch/mod.rs` | `spill_flush_incomplete` includes finish-run; cooperative `AwaitHost` only while incomplete | Avoid parking a resume batch after a fully committed run |
| `graph_host.rs` | Do not report `NeedInput` while spill resume is pending; clear stranded `host_inflight` | Stop false idle that freezes ingress |
| `stream_pool.rs` (Rust) | `NeedInput` + full ingress ports → keep stepping; `Yield` increments work and returns | Avoid parking while mailboxes still hold frames; return to JS on Yield |
| `stream-pool.ts` | Yield every 8 `SpillExec`; if `wcs_sp_spill_op ≠ 0`, keep advancing even without `STEP_PROGRESS` | Heartbeats + don’t drop pending spill host ops |
| `spill-r2-pool-driver.ts` | **One-shot** `bucket.put` of full run body (no chunked `FixedLengthStream`) | Avoid DO deadlock: producer write waits on put reader that can’t run until same isolate awaits |
| `spill/r2/put.rs` | Wait on `pending_request()`, not `has_pending()` / stranded `host_inflight` | Allow COMMIT after PUT when host already took the op |
| `spill/host.rs` | `clear_stranded_inflight` | Recover stuck bridge bit |
| Scans | `eventLoopYield` / `scheduler.wait(0)` on scan stream pool | Let queued RPCs enter between steps |
| Coordinator | `WCOL_STALL_MS` configurable (300s default) | Fail hung jobs instead of leasing forever |

### Aggressive experiments that were reverted

- Forcing `hasBufferedWork` / awaiting `accept` in ways that produced **hundreds of thousands of yields** and `hash attempt is not the active node session` — reverted.

### Scan → reducer pipe decoupling (latest try; did not fix)

In `cloudflare/src/node/scan.ts` `onPartitionStream`:

- Open `createBoundedBytePipe({ maxBufferedBytes: PIPE_BYTES })` (2 MiB).
- Call `acceptScanEdge` with `pipe.readable` immediately.
- `pumpReadableToWriter(body, pipe.writable)` asynchronously.

Motivation (from celld / [FULL_HITS_Q15_STALL.md](./FULL_HITS_Q15_STALL.md)): overlapping streamed `acceptScanEdge` + scan write can deadlock when the reducer is inside sync Wasm and cannot enter the accept RPC. The local pipe absorbs up to 2 MiB while accept is in flight.

**Result on real DO:** same post-spill freeze (`spillRuns: 1`). Accepts and first spill still happen, so this was not the gate for the current hang (it may still be useful for earlier overload / closed-pipe classes of failure).

Celld’s stronger escape hatch `WCOL_BUFFER_SCAN_EDGES=1` (buffer whole edge, then RPC bytes) is **not** viable for 2M production edges inside a 128 MiB isolate.

## Hypotheses

### Strong / still open

1. **Advance loop stops after first spill on DO only**  
   Local multi-spill success vs DO `spillRuns: 1` + silence. Something about the first `SpillExec` completion / resume / `NeedInput` / full-port interaction leaves `wcs_sp_step` returning no progress and `spill_op == 0`, so TS `runAdvance` exits and never wakes again—even though scans may still be blocked on full pipes.

2. **Single-threaded DO + long sync Wasm / R2 await interleaving**  
   Documented in Q15 notes: incoming RPCs only run after a real macrotask yield (`scheduler.wait(0)`). Microtask yields do not admit RPCs. If the host stops calling `yieldToEventLoop`, both heartbeats and accepts die together.

3. **Wake lost after spill**  
   Ingress may still have bytes, but nothing calls `pool.wake()` / `requestAdvance()` after the first spill path returns to idle. Full-port `NeedInput` continue helps only while `runAdvance` is still looping.

### Weakened / unlikely for *this* hang

- “Never got a successful local spill path” — false; local Q33 R2 spill works.
- “Stall is only a false watchdog” — false; progress stamps really stop updating.
- “First-edge accept deadlock alone” — weakened for current failure: we routinely reach `spillRuns: 1` with hundreds of yields first.
- “Chunked R2 upload deadlock” — mitigated with one-shot put; hang remained.

## Architecture context (current path)

```
Coordinator DO
  → prepareReducer (node DO, stream-pool GraphHost, input latch)
  → runTask scans (node DOs)
       → onPartitionStream → acceptScanEdge(stream) on reducer DO
  → reducer runWcolEngine / createStreamPool
       → wcs_sp_step → SpillExec → createPoolSpillDriver (R2 put/commit/read)
  → gather / JSON result
```

Legacy ABI v1 I/O bridge, ScratchPool-era host, and byte-pipe pipeline were removed in this branch arc (`STRUCTURE.md` updated). Stream pool is the only production host path.

Spill object keys: `wcol-spill/{session:016x}/run-{seq:06}.bin` (session id, **not** job id). Cleanup checks in `cf-q33-production.ts` filter leftover keys by job id when API listing is available—session-scoped keys may need a different join for strict leftover asserts.

## How to reproduce

```bash
cd wcol
npx tsx scripts/deploy-worker.ts
WCOL_BASE_URL=https://wcol-coordinator.<your-subdomain>.workers.dev \
  TIMEOUT_MS=900000 \
  npx tsx scripts/cf-q33-production.ts
```

Inspect reducer progress (task ids for 2-reducer Q33 are typically **10 and 11**, not 0–1):

```bash
npx wrangler r2 object get \
  wcol-hits-exports/_jobs/<jobId>/a1/t10-progress.json \
  --remote --pipe -c cloudflare/wrangler.toml
```

Local baseline (should pass):

```bash
npx tsx scripts/local-q33-r2-spill.ts
```

## Open work (priority order)

1. **Pin why `runAdvance` stops after the first DO spill**  
   Distinguish: Rust returns perpetual idle `NeedInput` with empty ports; spill resume parked without `SpillExec`; host never re-wakes after put/commit; output backpressure; or isolate stuck in a non-yielding await that also blocks progress puts.

2. **Reproduce NeedInput + full-port / post-spill idle under a DO-like single-thread harness**  
   Local memory R2 is insufficient; need a test that forces accept RPC / yield scheduling similar to a Durable Object (or celld with streaming edges, not buffered).

3. **Only after a green real-DO Q33**  
   Spill leftover cleanup assert, soft-shutdown audit across fixed node pool, then deferred sort/Parquet follow-ups.

## Related files

| Path | Role |
| --- | --- |
| `scripts/cf-q33-production.ts` | Real-DO acceptance |
| `scripts/local-q33-r2-spill.ts` | Local R2 spill baseline |
| `scripts/deploy-worker.ts` | Build workers + deploy node then coordinator |
| `cloudflare/src/node/{program,scan,reducer}.ts` | Reducer/scan host, progress, pipe decoupling |
| `src/runtime/host/stream-pool.ts` | Advance / SpillExec / yield policy |
| `src/runtime/host/spill-r2-pool-driver.ts` | One-shot R2 put / commit / read |
| `rust/wcol-engine/src/exec/stream_pool.rs` | NeedInput+full ports, Yield → progress |
| `rust/wcol-engine/src/exec/graph_host.rs` | Spill resume vs NeedInput |
| `rust/wcol-engine/src/ops/query/batch/mod.rs` | Cooperative flush / incomplete finish |
| `rust/wcol-engine/src/spill/r2/put.rs` | PUT/COMMIT wait on pending request |
| `docs/FULL_HITS_Q15_STALL.md` | Prior DO overload / closed-pipe / yield history |
| `docs/R2_GROUP_SPILL.md` | Why R2 spill replaced SQLite |

## Bottom line

The production gap is no longer “can we spill on R2” or “can Q33 finish in-process.” It is specifically: **after the first successful R2 spill run on a Durable Object reducer, the stream-pool host stops advancing and stops heartbeating.** Local multi-spill success and the list of deployed hang fixes above are the starting point for the next diagnosis—not another budget or topology change.
