# Replace SQLite group spill with sorted R2 runs

**Status:** Approved direction (Nov 2026). Do not repair the SQLite spill path; replace it.

**Default:** hash reducers spill to R2 by default, for every aggregate including `COUNT(DISTINCT)`. A job sends `"groupSpill": false` to keep reducers in memory and fail on the group budget instead. The `WCOL_GROUP_SPILL` environment variable is gone.

**Run memory:** a flush encodes its pages straight into one run body, sized once from the flush's row count, and hands that body to the host for a single `PUT`. Ingest waits until the run is uploaded and committed, so a reducer holds at most one live table plus one run body (and the host's copy of it during the upload). Pages must arrive in canonical key order; the store rejects them otherwise instead of re-sorting.

**Merge reads:** each run read takes an equal share of an 8 MiB budget across the open runs, from 256 KiB to 8 MiB (`run_read_chunk`), so a merge of few runs makes few R2 round trips and a merge of many stays bounded. A spilled reducer with `COUNT(DISTINCT)` merges consecutive count chunks and sends one per 4096 groups; merged pairs arrive grouped, so sending every 256-pair chunk would make the finalizer merge thousands of tiny partials.

**Context:** Q15 development on Cloudflare incurred **>287M Durable Object SQLite rows written** (~$238 overage). Per-group `INSERT INTO wcol_spill_rows` on DO SQLite is the cost driver. The unexplained **~3.5 MiB/run Rust `liveAfter` climb** on full hits may or may not disappear with this change — R2 spill must prove memory reclamation in its own tests ([FULL_HITS_Q15_STALL.md](./FULL_HITS_Q15_STALL.md)).

## Objective

Replace SQLite-backed aggregation spill with **immutable sorted runs in Cloudflare R2** for ClickBench Q15:

```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
```

Reference top row: `UserID = 1313338681122956954`, `COUNT(*) = 29097`.

Preserve:

- Rust `GraphHost` owns aggregation, spill decisions, sorting, merge semantics, scheduling.
- TypeScript owns physical I/O via ticketed `Open` / `Read` / `Write` / `Close` / `Abort`.
- Cloudflare DOs run scanner/reducer tasks; scanners hash-partition to reducers over existing RPC streams.
- No new TS query executor, partition scheduler, or aggregation logic.

**Guiding principle:** Rust owns the algorithm; R2 owns persisted bytes; TypeScript only moves bounded byte streams between them.

---

## 1. Target architecture

Each reducer keeps a bounded Rust hash table. When the group budget is reached:

1. Freeze current aggregate entries.
2. Sort by canonical group key (prefer sorting indices, not cloning every key/state).
3. Serialize a sorted run `(key, partial aggregate state)`.
4. Stream the run into **bounded TypeScript-owned memory** (handoff), not R2 latency on the Rust critical path.
5. **Handoff complete:** Rust releases encoded run buffers and resets the group table; aggregation continues.
6. TypeScript starts **`bucket.put()` asynchronously**; merge / job success wait until every run upload has **committed**.

Example keys:

`wcol-spill/{jobId}/{attemptId}/{reducerId}/{executionId}/run-000001.bin`

**No second hash partition inside the reducer.** The distributed exchange already routes each group key to one reducer.

---

## 2. Rust spill implementation

### Replace (do not extend)

| Area | Current location |
| --- | --- |
| Group spill flush / reset | `rust/wcol-engine/src/ops/query/batch/group_spill.rs` |
| Spill store trait (append + ordered scan) | `rust/wcol-engine/src/spill/store.rs` |
| DO SQLite bridge | `rust/wcol-engine/src/spill/host.rs` (`HostSpillStore`, `fill_spill_exec`) |
| Graph host wiring | `rust/wcol-engine/src/exec/graph_host.rs` (`enable_group_spill`, `HostSpillBridge`) |
| Encode / canonical keys | `rust/wcol-engine/src/spill/encode.rs`, `canonical.rs` |
| Boundaries / telemetry | `rust/wcol-engine/src/ops/query/batch/mod.rs` (`SPILL_BOUNDARY_*`) |

### New logical surface (small, not a generic storage framework)

- **`SpillRunWriter`** — emits a sorted run through existing byte-oriented host ABI.
- **`SpillRunReader`** — sequential read of a committed sorted run.
- **`SpillRun`** — immutable descriptor: object key, run sequence, byte length, format version, record count.

### Run format

Versioned binary sorted-run format: independently readable; metadata for key type, aggregate-state format, record boundaries, completion. Reuse WCOL/WCS encoding where appropriate; avoid unnecessary dictionary overhead.

**Invariants:**

- Do **not** accumulate the full encoded run in a growing `Vec`.
- Emit bounded pages through reusable buffers; release each page after ownership transfers to the host.
- No collection retains completed spill pages or grows with cumulative bytes persisted.
- Sorting must not duplicate the entire group table.
- Active hash table + sort workspace + page buffers stay bounded.
- Record **live Rust allocation** separately from WASM linear-memory high-water.

Production group budget is locked at **16 MiB flush / 32 MiB hard**, with an **8 MiB** empty-table retained-capacity low-water mark after each pressure flush ([HASH_GROUP_BUDGET.md](./HASH_GROUP_BUDGET.md)). Do not raise the flush target to avoid spilling without new isolate evidence.

### Sorting / aggregates

Q15: key `UserID` (`u64`), state count. Representation must support other spill-compatible aggregate states later — do not silently treat all states as counts. Preserve unsigned comparison, null grouping, composite keys where already supported.

---

## 3. R2 write adapter (TypeScript host)

Use existing **`Open` / `Write` / `Close` / `Abort`** lifecycle — not a new spill-specific RPC protocol.

**Two events (do not couple them):**

| Event | Meaning |
| --- | --- |
| **Handoff complete** | Rust has transferred the full sorted run to the TS host; Rust releases spill encode buffers and resumes aggregation. |
| **Upload committed** | R2 `put()` succeeded; the run is durable and eligible for merge reads. |

For typical ~3.5 MiB Q15 runs, stream each sorted run through `createR2SpillPut()` (`src/runtime/host/ports/r2-spill-put.ts`) from the stream-pool spill driver; `close()` awaits the PUT before Rust advances.

Initial defaults per reducer session:

| Setting | Value |
| --- | --- |
| Max concurrent R2 uploads | 2 |
| Max pending spill bytes (queued + uploading) | 8 MiB |
| Backpressure | Block spill `Open` when the byte/count budget is exhausted |

At **`runWcolEngine` completion**, `waitAll()` drains outstanding uploads before merge reads rely on R2. A failed upload fails the attempt.

Streaming **`FixedLengthStream`** (`createR2SpillPut`) remains for larger runs; same handoff vs commit split applies on `Close`.

Do **not** treat R2 upload latency as part of the Rust spill critical path under normal load.

Multipart fallback only if needed (`src/runtime/host/ports/r2-write.ts`, `rust/wcol-plan` 5 MiB non-final part rules). Prefer single-object uploads for ~3.5 MiB Q15 runs.

Existing R2 helpers: `cloudflare/src/r2.ts`, `src/runtime/host/ports/r2-write.ts`.

---

## 4. R2 read adapter

`bucket.get(key).body` as physical readable port:

- Incremental read, backpressure, bounded chunks.
- Partial reads; records may span chunks.
- Reject missing/truncated/EOF errors.
- Close/cancel when done.

Do not load full objects into WASM or TS memory.

**Merge read concurrency:** configurable; start with **8 concurrent readers per reducer**.

---

## 5. Streaming sorted merge

After all scanners complete and ingress is sealed:

1. Flush final in-memory table to a run if non-empty.
2. **K-way merge** (k ≤ 8): smallest key, merge partials with **same semantics as today’s reducer**, emit once, advance readers.
3. If **> 8 runs**, multi-pass merge (e.g. 41 → 6 intermediate runs → final). Intermediates are also immutable R2 objects via the same writer.

No SQLite merge index. No materializing all records.

**ORDER BY / LIMIT:** key-ordered merge emits `(UserID, count)`; maintain bounded **top-10 heap** per reducer; existing global top-K combines reducer outputs.

---

## 6. Manifests, attempts, cleanup

- Small **run manifest** of successfully committed R2 descriptors only (not R2 list as source of truth).
- Coordinator **attempt identity / fencing** — late writes from abandoned attempts must not visible to a new attempt.
- On failure: cancel uploads/reads, abort multipart, fence before retry; incomplete objects are not valid runs.
- R2 for immutable run data + manifests; **DO storage for small control metadata only** — **zero per-group spill rows**.
- Cleanup temp spill objects on success, cancel, expired attempts; R2 lifecycle policy as safety net.

---

## 7. SQLite group spill (removed)

Production group spill is **R2 sorted runs only** (`createR2SpillPut` via the stream-pool spill driver). The former `ports/sqlite.ts` spill adapter and `host.spillSql` path are deleted.

DO `storage.sql` is not used for spill storage on graph nodes.

---

## 8. Required tests (before full-hits CF job)

| Test | Intent |
| --- | --- |
| **A. Sorted-run round trip** | Known keys/partials → R2 → incremental read; partial buffers, empty run, truncate, failed upload |
| **B. Cross-run aggregation** | Repeated keys across ≥50 runs; one- and multi-pass merge; no lost/duplicate groups |
| **C. Memory plateau** | ≥100 MiB spill across many R2 runs; live Rust + pending host bytes bounded; not cumulative spill volume |
| **D. Real host integration** | WASM `GraphHost` + ticketed loop + R2 adapter; slow I/O, cancel, failed PUT, backpressure vs RPC admission |
| **E. SQL correctness** | Q15 forced spill on `hits_subset_500k.parquet`; multi scanner/reducer; DuckDB parity |
| **F. CF smoke** | Small DO deploy, forced spill, real R2 binding; objects, merge, cleanup, bounded memory |

**Do not launch another full-hits Cloudflare job until A–F pass.**

Local spill tests to retire/replace: `tests/reducer-spill-*.ts`, `tests/sqlite-spill-port.test.ts`, `scripts/spill-*`.

---

## 9. Cost and observability

Per job / reducer metrics (extend spill boundaries or parallel progress):

- `spillRunsCreated`, `spillBytesUploaded`, `spillBytesDownloaded`
- `spillR2Puts`, `spillR2Gets`, `spillR2Deletes`
- `activeMergeReaders`, `mergePasses`, `spillTemporaryObjects`
- `rustLiveBytes`, `hostBufferedBytes`, `spillUploadPendingBytes`

Configurable limits: R2 ops, total spill bytes, merge passes → **non-retryable** on exhaustion.

Diagnostic jobs: default **one attempt** (avoid automatic 3× expensive retries).

**Acceptance:** aggregation spill generates **zero SQLite group-row writes**.

---

## 10. Implementation order

1. Sorted-run format + Rust streaming writer/reader (native tests first).
2. R2 write/read byte ports + real R2 tests (`FixedLengthStream` proof in DO).
3. Hash-table flush → sorted-run writer.
4. Bounded k-way aggregation merge (+ multi-pass).
5. Final top-K integration.
6. Delete SQLite spill execution path.
7. Failure recovery, cost budgets, cleanup.
8. Correctness + memory tests (A–E).
9. Cloudflare smoke (F).

**Out of scope for this change:** scanner partitioning, WASM slot counts, RPC scheduling, yield policy, coordinator architecture refactors.

---

## Acceptance criteria

- Q15 correct with forced spill.
- Every spill run = immutable sorted R2 object.
- Multi-run merge exact aggregate semantics.
- Rust/TS memory **not** proportional to total spilled bytes (proven in tests).
- Zero SQLite group spill rows.
- Retries cannot corrupt another execution’s objects.
- Temp objects cleaned up.
- Existing ticketed-ABI / streaming tests green.

---

## Proof points before full integration

1. **`FixedLengthStream` + concurrent `put()` + bounded pipe** in a real DO (reuse publish pattern).
2. **Memory plateau test** must pass on R2 path alone — do not assume storage switch fixes `liveAfter` climb.
