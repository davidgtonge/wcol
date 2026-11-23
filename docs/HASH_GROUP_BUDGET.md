# Hash group budget (production default)

**Status:** Locked (Nov 2026). Do not change the flush target, hard ceiling, retained-capacity low-water mark, spill state machine, or WCS wire format without new evidence.

## Production default

| Setting | Value | Role |
| --- | --- | --- |
| Flush target | **16 MiB** | Start a pressure spill when accounted group state reaches this. |
| Retained-capacity low-water | **8 MiB** | After a pressure flush, an empty table above this is recreated so retained capacity cannot sit above the flush lock. |
| Hard ceiling | **32 MiB** | Abort the reducer if accounted group state exceeds this (headroom for one hash-table doubling). |

Defaults live in `HashAggregateConfig` (`rust/wcol-plan`) and `SpillConfig` (`rust/wcol-engine`). An explicit `targetGroupBytes` without `hardGroupBytes` still sets hard = target (a single ceiling). The 16/32 pair is the default only.

After the Nov 2026 retained-capacity fix, smaller tables remain faster than larger ones at a fixed reducer count. Budget tuning is parked.

## Gather or hash

The planner keeps a grouped aggregate on the gather plan (each scan aggregates its rows; the coordinator merges the partials) while the estimated group state is within `GATHER_GROUP_BYTES`, **64 MiB** at 384 bytes a group, even above one reducer's flush target. Only partial rows then cross the network, where a hash exchange ships every source row. A key with no footer bound (the row-count fallback) is always partitioned. Full-hits RegionID, a footer bound of 131K groups, ran in 2.6–3.5 s gathered against 4.0–4.4 s through 16 reducers. The reducer budgets above are unchanged; the gather merge has no run-time group budget.

## Local performance baseline

Recorded on `hits_subset_2m.parquet`, 2 hash reducers, group spill on, flush/hard **16/32 MiB**, after the retained-capacity fix. Reducer count is pinned so the planner’s power-of-two width does not confound the budget.

| Metric | Q15 | Q33 |
| --- | --- | --- |
| Rows | 2M | 2M |
| Reducers | 2 | 2 |
| Flush / hard | 16/32 MiB | 16/32 MiB |
| Committed runs | 6 | 27 |
| Wall time | 15.1 s | 53.9 s |
| Peak Wasm | 49.6 MiB | 61.7 MiB |
| Peak group table | 22.6 MiB | 17.1 MiB |

Reproduce:

```bash
HASH_BUDGET_PARQUET=data/hits_subset_2m.parquet \
HASH_BUDGET_QUERIES=Q15,Q33 \
HASH_BUDGET_REDUCERS=2 \
HASH_BUDGET_ONLY=16/32 \
npx tsx scripts/hash-budget-bench.ts
```

Machine-readable copy: [`baselines/hash-group-budget-2m.json`](./baselines/hash-group-budget-2m.json).

Same day, clean 16/32 vs 32/48 (same file, 2 reducers): 16/32 was **2.07×** faster on Q15 and **1.67×** faster on Q33, with substantially lower peak Wasm (Q33 61.7 MiB vs 128.1 MiB).

## Invariants (unit-tested)

1. After a completed pressure flush drains to an empty grouping table, `resident_state_bytes()` stays **under the flush threshold** (and under the 8 MiB low-water mark).
2. Thousands of small post-flush batches must **not** each commit a new run; final aggregates match an unspilled baseline across host suspend/resume.

Tests: `empty_table_stays_under_flush_threshold_after_pressure_flush`, `pressure_flush_releases_capacity_across_host_suspend` in `rust/wcol-engine/src/ops/query/batch/retain_capacity.rs`.

## Production checks (still open)

Local Miniflare and in-process benches are not enough for isolate lifetime. Both checks below must pass on **real Cloudflare Durable Objects** before treating high-cardinality spill as production-complete.

### 1. Real Durable Object — Q33, 2M rows

Acceptance:

- Exact DuckDB result parity.
- No isolate termination or unexpected restart.
- Spill runs commit and merge correctly (local baseline ~27; exact count may vary with placement).
- Record peak Wasm linear memory, host-buffer / process memory if available, and wall time.
- No leftover multipart uploads or spill objects after successful cleanup.

Entry point for hosted jobs: `scripts/cf-production-jobs.ts` (`STAGE=b` for 2M). Extend or follow with an explicit Q33-only run that records the metrics above.

### 2. Repeated jobs on the same Durable Object

Run Q33 **10–20 times** without recreating the DO or Wasm instance.

Acceptance:

- Wasm linear-memory high-water and Rust live allocations recorded before and after every job.
- Memory **plateaus** across jobs (growth is irreversible for the instance lifetime; released allocations must be reusable).
- A failed or cancelled job followed by a successful one leaves no unbounded climb and no leaked spill objects.

## Next engineering (after the two checks)

Distributed global sort and production-scale Parquet output. Do not reopen hash-budget, spill lifecycle, or WCS framing work without new evidence.