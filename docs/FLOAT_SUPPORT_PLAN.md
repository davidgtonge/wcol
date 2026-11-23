# Float and double columns

**Status:** Planned (2026-10-29)  
**Depends on:** `cleanup-src-minimal` landing (one page driver, `reject_float_columns` in `ops/query/mod.rs`).  
**Goal:** Queries over Parquet FLOAT (physical 4) and DOUBLE (physical 5) columns return DuckDB's answers. ClickBench `hits` has no float columns, so this is for files from other writers (pyarrow, Spark, Trino), which is the serverless Trino path.

## Where things stand

| Layer | Float support today |
|-------|---------------------|
| Parquet decode | `PageCursor` decodes PLAIN float and double. Dictionary pages hold them as `ValueRef::Float`. |
| Scan → batch | PLAIN values land in the `I64` lane as IEEE bit patterns (`dictionary.rs`: `v.to_bits() as i64`). Dictionary-encoded floats fail with `"float dictionary entry in numeric column"`. |
| Batch | `WcolColumn` has `I64`, `DictId`, `Utf8`. No float column. |
| Filter kernels | `Pred` is integer-only: `Range { lo, hi }`, `NotEq`, `InSmall`, `InSet` over `i64`. |
| Aggregate kernels | `Acc { count, sum: i128, min: i64, max: i64 }`. |
| GROUP BY | `GroupKey { nulls, values: [i64; MAX_KEYS] }`. |
| Partial state (PWCL) | Has floats already: `PartialStat` carries `sum_f`, `min_f`, `max_f`, and PWCL stores them as bit columns (`StatField::SumFBits` and friends). Flag bit 1 marks an integer stat. `merge_partial_stat` has a float branch. Group keys have `KeyPart::Float(u64)`. These are left over from the wcol-file engine. |
| Result readers | `partial-oracle.ts` and `query_bench --print-groups` read `sum_f` for non-integer stats and compute AVG as `sum_f / count`. The JSON sink writes `sum_f`, `min_f`, `max_f`. |
| Planner | `reject_float_columns` refuses any query that touches a float column. |

So the wire format and the merge already work for floats. The missing pieces are the batch column, the kernels, and the scan writing real values.

## Semantics to match (DuckDB)

- FLOAT widens to DOUBLE exactly. Carry one `f64` lane for both physical types.
- **NaN:** DuckDB treats NaN as equal to NaN and greater than every other value, including `+inf`.
  - `x = 'NaN'::DOUBLE` matches NaN rows.
  - `x > 1e308` matches NaN rows.
  - `MAX` returns NaN if any row is NaN.
  - `MIN` ignores NaN unless every row is NaN.
  - GROUP BY puts every NaN in one group.

  Rust's `f64::min` and `f64::max` drop NaN, so they are wrong for `MAX`. Use a total order.
- **Signed zero:** `-0.0 = 0.0` is true, and they group together. The result key prints as `0.0` (check DuckDB's output for `-0.0` once and pin it in a test).
- **NULL** stays separate from NaN. Validity masks work as they do for integers.
- **Literals:**
  - Integer literals against a float column compare as `f64`.
  - Float literals against an integer column keep the current `number_pred` path, which already handles out-of-range values and NaN.
- **SUM of floats** is a plain `f64` sum in DuckDB. It is order dependent, so per-shard sums merged in a different order can differ in the last bits. See [Testing](#testing).
- **AVG** is `sum_f / count`. The partial readers already compute it this way for non-integer stats.
- `COUNT`, `COUNT(DISTINCT)` and `approx_count_distinct` hash the normalised bit pattern (NaN and `-0.0` canonicalised as for grouping).

## Canonical key

One helper, used everywhere a float becomes a key or a hash input:

```rust
/// Bits that order like DuckDB's DOUBLE order and make equal values identical:
/// -0.0 → 0.0, every NaN → one NaN that sorts above +inf.
fn float_key(v: f64) -> u64
```

It maps to the standard total-order encoding: flip all bits for negatives, set the sign bit for positives. NaN is normalised first, so it lands above `+inf`. Comparing two `float_key` values as `u64` then matches DuckDB's order. That gives GROUP BY, DISTINCT, HLL, IN-sets and range predicates one rule.

## Phases

Each phase relaxes `reject_float_columns` only for the positions it makes correct. The guard becomes "refuse float columns where the engine can't run them yet" and is deleted in the last phase. Between phases a float query either runs correctly or fails at plan time. It never returns a wrong number.

### Phase 1 — Float lane in the batch

- Add `WcolColumn::F64 { column, values: &[f64], nulls }` and the owned counterpart in `OwnedWcolBatch`.
- Add `ColumnPhysical::F64` alongside `I64`, `DictId` and `Utf8`.
- Scan: for physical 4 and 5 in `MaterializedI64` mode, write `f64` values to the F64 lane instead of bit patterns.
  - This takes a scan value mode or a per-column flag. Pick whichever keeps the integer loop untouched.
  - Remove the `to_bits` arm from `dictionary.rs`.
- Dictionary-encoded floats:
  - store `ValueRef::Float` entries in a `DictEntries::Float(Vec<f64>)`
  - support both dictionary ids (for filter pass tables and group slots) and materialising to the F64 lane
- `cols_of` and the `view_with` / `export` helpers learn the new variant. Sinks that write rows (row export, JSON) write floats as floats.
- **Guard:** unchanged. Nothing reads the lane yet.
- **Tests:**
  - `legacy_encodings.rs` asserts real `f32`/`f64` values, not bits.
  - New dictionary-encoded float fixture (pyarrow with dictionary on).
  - NaN, `±inf`, `-0.0` and NULL in both PLAIN and dictionary pages.

### Phase 2 — Filters

- Add a float predicate family next to the integer `Pred` so the integer kernel keeps its shape:
  - `FRange { lo: u64, hi: u64 }` over `float_key`, so `<`, `<=`, `>`, `>=`, `=` and NaN all become one inclusive key range
  - `FNotEq(u64)`, `FIn(FxHashSet<u64>)`
  - reuse `AnyValid` and `Never`
- Compile literals with `float_key`. Strict `<` and `>` step to the adjacent key; at the ends of the range, use `Never` or `AnyValid`.
- For dictionary pages, evaluate the predicate once per entry, as integer and string dictionaries already do.
- **Guard:** allow float columns in `WHERE`.
- **Tests:** brute-force oracle over random batches with NaN, `±inf`, `±0.0`, NULL, subnormals and values one ULP apart. Every comparison operator, `IN` and `NOT IN`, literals given as integers and as floats.

### Phase 3 — Aggregates

- `FAcc { count, sum: f64, min_key: u64, max_key: u64 }`, chosen per aggregate column when the column is float.
  - Track min and max as `float_key` so the NaN rule falls out of integer comparison.
  - Convert back when building the partial.
- Build `PartialStat` with flag bit 1 clear, and fill `sum_f`, `min_f` and `max_f`.
- `merge_partial_stat` float branch: replace `f64::min` / `f64::max` with the `float_key` order, so `MAX` keeps NaN and `MIN` skips it. This fixes the existing merge code, which currently drops NaN.
- The readers (`partial-oracle.ts`, `query_bench`) already compute AVG from `sum_f` for non-integer stats. Check them with an empty group and an all-NaN group, and check `min_f` / `max_f` are read rather than `min_i` / `max_i`.
- **Summation:**
  - Inside a batch, sum in row order, like DuckDB, so a single-shard result matches exactly.
  - Across shards the order is the merge order. Don't add compensated summation unless the DuckDB comparison shows a need: it would make single-shard results differ from DuckDB.
- **Guard:** allow float columns as aggregate arguments.
- **Tests:**
  - Brute force against an `f64` row loop for SUM, MIN, MAX, AVG, COUNT.
  - Shard-merge test that splits the same data three ways and compares within a relative tolerance.
  - Exact compare for MIN, MAX and COUNT.

### Phase 4 — GROUP BY, DISTINCT, approx

- Float group keys go into the `i64` lanes of `GroupKey` as `float_key` bits, with a per-key type mask like the existing text-key mask. Output converts them back to `KeyPart::Float`.
- Check that `KeyPart::Float` ordering and equality in `ops/query/mod.rs` use the same normalised bits. The partial merge must not split NaN or `±0.0` groups that one worker merged.
- `COUNT(DISTINCT f)` runs through the hidden-key path unchanged, once float keys work.
- `approx_count_distinct(f)` hashes `float_key(v)` with the existing xxh3 HLL.
- **Guard:** allow float columns in GROUP BY and DISTINCT.
- **Tests:**
  - brute force with duplicate NaNs, `0.0` and `-0.0` in one batch and across shards
  - `ORDER BY COUNT(*)` with float keys
  - PWCL round trip

### Phase 5 — Row projection and removing the guard

- `SELECT f FROM t WHERE …` and `ORDER BY f` on row queries, using the F64 lane and `float_key` for ordering.
- Delete `reject_float_columns`, and replace `float_columns_are_rejected_wherever_a_query_touches_them` with a test that the same queries run.

## Testing

- **Fixtures:**
  - The existing pyarrow fixture.
  - A new dictionary-encoded float fixture.
  - A generated file of about 300k rows with NaN, `±inf`, `±0.0`, NULL and mixed magnitudes. DuckDB writes it with `PARQUET_VERSION v2`; check it uses PLAIN, not BYTE_STREAM_SPLIT, which is still unsupported.
- **DuckDB comparison (`scripts/check-duckdb-results.py`):**
  - It already compares non-integer values with `rel_tol 1e-9`. Keep that for SUM and AVG, and make MIN, MAX, COUNT and group keys exact.
  - Add F-prefixed queries to `scripts/clickbench-queries.tsv` covering each phase.
  - Teach `query_bench --print-groups` to print NaN and `±inf` the way DuckDB's CSV does (`nan`, `inf`, `-inf`), so the script can compare them.
- **Shard independence:** run each F query with `QUERY_BENCH_SPLIT` at 1 and 3. Exact aggregates must match bit for bit; SUM and AVG within tolerance.
- **Performance:** run `query_bench` on hits before and after each phase. The integer kernels must not slow down, because floats get their own predicate and accumulator types instead of a branch inside the integer loop.

## Out of scope

- BYTE_STREAM_SPLIT encoding, which is the usual float encoding in newer pyarrow and Spark output. It's a separate decoder in `PageCursor`; do it right after phase 1 if the target files use it.
- INT96 timestamps and DECIMAL (`FIXED_LEN_BYTE_ARRAY` / INT32 / INT64 with a decimal logical type). DECIMAL needs scaled integers, not floats.
- Float expressions (`SUM(f * 2)`, `ROUND`). The planner has no expression support for any type yet.
- Compensated or pairwise summation.
