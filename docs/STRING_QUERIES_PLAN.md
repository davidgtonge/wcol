# Plan: string queries on streaming WCOL batches

Status: proposal (2026-10-28). Follows the numeric batch engine (`rust/wcol/src/ops/query/batch/`),
which today runs filters and aggregates over `i64` and integer-dictionary columns and matches
DuckDB on 23 ClickBench-style queries (`scripts/clickbench-queries.tsv`).

## Goal

Run the string ClickBench queries on the same streaming path — Parquet edge → `WcolBatch` →
`FilterOp` / `AggregateOp` → partial state → reduce — with the query engine still knowing
nothing about Parquet. Done means each query below matches DuckDB through
`scripts/check-duckdb-results.py` on `data/hits_subset_500k.parquet`, and the batch engine
stays at least as fast as the row kernel it replaces.

26 of the 43 ClickBench queries touch a string column. Grouped by what they need:

| Need | Queries |
| --- | --- |
| String filter only (`= ''`, `<>`, `LIKE`) + numeric aggregates | Q20, Q30, Q31 |
| String `GROUP BY` key | Q12, Q14, Q16, Q17, Q33, Q36, Q37, Q38 |
| String key + `COUNT(DISTINCT UserID)` | Q10, Q11, Q13 |
| `COUNT(DISTINCT string)` | Q5 |
| `MIN(string)` | Q21, Q22 |
| Row-returning `SELECT … ORDER BY … LIMIT` | Q23, Q24, Q25, Q26 |
| Expressions (`STRLEN`, `HAVING`, `REGEXP_REPLACE`, `CASE`, `extract`, `GROUP BY 1`) | Q18, Q27, Q28, Q34, Q39 |

## What the data looks like

From `parquet_metadata('data/hits_subset_500k.parquet')`:

| Column | Encoding per row group | Compressed |
| --- | --- | --- |
| `SearchPhrase` | RLE_DICTIONARY | 0.7 MB |
| `MobilePhoneModel` | RLE_DICTIONARY | 5 KB |
| `URL` | RLE_DICTIONARY **and** PLAIN | 7.4 MB |
| `Title` | RLE_DICTIONARY **and** PLAIN | 22 MB |
| `Referer` | RLE_DICTIONARY **and** PLAIN | 8.6 MB |

Writers fall back from dictionary to PLAIN when a row group's dictionary grows too large, so
the high-cardinality columns arrive in both forms, and the encoding can change between row
groups of the same column. The engine has to accept both shapes in any batch.

## Where things stand

- **Batches:** `WcolColumn` is `I64` or `DictId`, and `DictionaryValues` holds only
  `int_entries: Arc<[i64]>`. There is no string column and no string dictionary.
- **Parquet edge:** `validate_leaf` rejects physical types 6/7 ("string batches are not
  implemented"). `compact-parquet-thrift` already decodes PLAIN `ByteArrays` /
  `FixedByteArrays`, and `DictionaryStore` already stores `Entry::Bytes`; only
  `copy_int_entries` refuses them. The store has an entry budget but no byte budget.
- **Planner:** `Scalar::Text` literals and `Filter::Like` are already planned; `ColumnKind::Text`
  exists. Aggregates and `GROUP BY` on text are rejected in `reject`.
- **Partial state:** group key parts are `Null | Int(i128) | Float(u64)`. PWCL key columns are
  `i64`; WPP1 key tags are 0/1/2. Stats have no string min/max.
- **Exchange:** projection payloads (`stream/projection.rs`) carry `i64` columns only.
- **Old engine kernels worth reusing** (`archive/rust/wcol-file`): dictionary LIKE id sets
  (`build_like_id_set`), SIMD substring search (`decode/decode/simd.rs::find_subslice`), the
  128-bit xxh3 group-key-with-collision-check idea, and `hll_insert_bytes`. Its raw-string LIKE
  mask is built for the `.wcol` front-coded layout (shared prefix + suffix blob); that trick
  only carries over to Parquet `DELTA_BYTE_ARRAY` pages, not PLAIN.

## Design

### 1. Batch representation

Add to `WcolColumn` / `OwnedColumn`:

```rust
Utf8 { column: u32, offsets: &'a [u32], bytes: &'a [u8], nulls: &'a [u8] }  // rows + 1 offsets
```

and let a dictionary carry bytes:

```rust
pub enum DictEntries {
    Int(Arc<[i64]>),
    Bytes { offsets: Arc<[u32]>, bytes: Arc<[u8]> },
}
```

`DictionaryValues.int_entries` becomes `entries: DictEntries`. Dictionary-encoded row groups
keep emitting `DictId` columns (no string materialisation); PLAIN row groups emit `Utf8`.
Strings are raw bytes: comparisons are byte-wise, which is what DuckDB does for `VARCHAR`
without a collation.

This touches every exhaustive match on `WcolColumn` (about 40 sites in join, exchange, encode,
project, owned batch). Where a site has no meaning for strings yet (hash join keys, typed
exchange hashing) it returns a clear "string columns not supported here" error rather than
guessing.

### 2. Parquet edge

- Lift the 6/7 rejection in `validate_leaf`; add `ValueDecoder::ByteArray` (and fixed-length)
  in `page_cursor.rs` over the library's `ByteArrays`.
- `ScanValueMode::DictionaryIds` for byte dictionaries exports `DictEntries::Bytes`
  (a `copy_byte_entries` beside `copy_int_entries`).
- PLAIN pages append to a per-scan `offsets/bytes` buffer, sliced per batch like the `i64`
  path, so a batch never copies a string twice.
- **Memory:** add a dictionary **byte** budget and a per-batch string byte budget to
  `ScanLimits` / `IngestMemoryProfile`. When a batch's strings would exceed it, emit a shorter
  batch. Long `Title`/`URL` values make row-count-only batch sizing unsafe in a Worker.
- Mixed encodings per row group are already planned per chunk (`dict_encoded` on
  `IngestGroupChunk`); keep that, it is exactly the fallback case above.

### 3. String identity across batches

Dictionary ids only mean something inside one dictionary scope (one column chunk). Anything
that outlives a batch — group keys, distinct values, min/max — must be keyed by value. The
engine gets a per-query **string interner**: an arena of bytes plus an
`FxHashMap<hash, Vec<u32>>` from xxh3-128 to interned ids, comparing bytes on a hash hit (no
silent collisions). Interned ids are `u32` and slot straight into today's fixed-width
`GroupKey` value lanes, so the group table, sort and partial builder stay integer-only inside
the engine.

Per-dictionary caches avoid hashing per row: `dict id → interned id` built once per dictionary
(the same `Arc::ptr_eq` cache the numeric `dict_slots` already uses). PLAIN rows hash once each.

### 4. Filters

All compile to the existing packed-mask kernels (`ops/query/batch/filter.rs`):

| Predicate | Dictionary column | Utf8 column |
| --- | --- | --- |
| `= 'x'`, `<> 'x'`, `IN (...)` | pass table per dictionary entry (one compare per entry) | per row: length check, then `memcmp` |
| `<> ''` / `= ''` | pass table | per row: `offsets[r+1] - offsets[r]` only |
| `LIKE` / `NOT LIKE` | pass table per entry (port `build_like_id_set`) | per row matcher |

LIKE patterns compile once into a matcher: exact, `prefix%`, `%suffix`, `%needle%` (port
`find_subslice`, SIMD on wasm `simd128`), and a general `%`/`_` matcher. Patterns are
case-sensitive, as in DuckDB. NULL never matches, including under `NOT LIKE`.

Q20, Q30 and Q31 need nothing else: their aggregates are numeric.

### 5. Aggregates and keys over strings

- **`GROUP BY` string:** interned id as the key lane (above).
- **`COUNT(DISTINCT string)`:** the existing two-level plan already makes the distinct column a
  hidden key, so it works once string keys do (Q5, and Q10/Q11/Q13 combine a string key with
  `COUNT(DISTINCT UserID)`).
- **`approx_count_distinct(string)`:** `Hll::insert_hash(xxh3_64(bytes))`; per-dictionary hash
  cache.
- **`MIN` / `MAX(string)`:** a byte-string stat (min and max interned ids compared by bytes).
  `SUM`/`AVG` on strings stay rejected.

### 6. Partial state and wire formats

Strings must leave the engine as bytes, never interned ids (ids are per worker).

- `PartialKeyPart::Bytes(Vec<u8>)` and a byte min/max on `PartialStat` (`Option<Vec<u8>>`),
  merged by byte comparison.
- **PWCL v4:** string key columns and string stat columns as `offsets + bytes` sections, the
  same shape as the v3 sketch columns. v1–v3 still decode.
- **WPP1:** key tag `3` (length-prefixed bytes) and a string min/max block for string
  aggregates only, so numeric partials stay byte-identical for the TS reader.
- `finalize_groups` and `query_bench --print-groups` emit strings.
- The BTreeMap key order the row kernel defines (`Null < Int < Float`) gains `Bytes` last;
  string parts sort by bytes.

### 7. Row-returning queries

Q23–Q26 return rows: filter, then `ORDER BY … LIMIT n`. `ProjectOp` already projects rows for
row export; add a top-k sink (bounded heap of `n + offset` rows keyed by the `ORDER BY`
columns, partial = the worker's top-k, merge = re-run top-k) and string columns in projected
row batches. `SELECT *` (Q23) needs every column, so it depends on the full batch type set.

### 8. Expressions (separate, after the above)

Q18, Q27, Q28, Q34 and Q39 need planner expressions this plan does not cover: `STRLEN`,
`HAVING`, `REGEXP_REPLACE`, `CASE`, `extract(minute …)`, `GROUP BY 1`. The numeric Q35
(`ClientIP - 1` keys) and Q42 (`DATE_TRUNC`) belong to the same piece of work: computed
columns evaluated per batch into new `WcolColumn`s before filter/aggregate.

## Phases

Each phase ends with tests green, the DuckDB check extended with its queries, and a commit.

| Phase | Work | Unlocks |
| --- | --- | --- |
| 1. Types and decode | `Utf8` column, `DictEntries::Bytes`, PLAIN/dict byte-array decode, byte budgets, every `WcolColumn` match updated | decoding all hits string columns (no new queries) |
| 2. String filters | `=`, `<>`, `IN`, `= ''`, `LIKE`/`NOT LIKE` on both shapes | Q20, Q30, Q31 |
| 3. String keys | interner, string `GROUP BY` keys, `PartialKeyPart::Bytes`, PWCL v4, WPP1 tag 3, finalize | Q5, Q10–Q14, Q16, Q17, Q33, Q36–Q38 |
| 4. String aggregates | `MIN`/`MAX(string)`, `approx_count_distinct(string)` | Q21, Q22 |
| 5. Row queries | top-k sink, string projection | Q23–Q26 (and numeric Q19) |
| 6. Expressions | computed columns in the planner and engine | Q18, Q27, Q28, Q34, Q39 (and Q35, Q42) |
| 7. Host and transport | TS WPP1 reader for string keys (display/oracle), `wcol_partials_wire_equivalent`, string columns in the WCS exchange projection | distributed string `GROUP BY` through the Worker |

**Phase 7 acceptance (correctness before more infra):**

- Rust: [`distributed_string_group.rs`](../rust/wcol/tests/runtime/distributed_string_group.rs) — Q12 on first 14 row groups, `HITS_PARQUET` set.
- HTTP: Q12 slice in [`hits-http-distributed-acceptance.test.ts`](../tests/hits-http-distributed-acceptance.test.ts) with `HITS_HTTP_DISTRIBUTED=1` (partial compare via `wcol_partials_wire_equivalent`, not the legacy numeric-only WPP1 view).
- Q33 full-file distributed: track merged group cardinality and reducer memory; do not use per-worker Top K before merge.

Phases 2 and 3 are the bulk of the value: they cover 15 of the 26 queries.

## Testing

- **Fixtures:** a DuckDB-written file with a dictionary string column, a PLAIN string column, a
  column that switches encoding between row groups, NULLs, empty strings and non-ASCII bytes;
  add it beside `tests/fixtures/duckdb_nulls.parquet` with tests against DuckDB's answers.
- **Kernels:** randomised parity of every string filter and key path against a brute-force
  evaluation over the same batches (as `ops/query/batch/tests.rs` does for numbers), with
  dictionaries changing between batches so ids are never reused as values.
- **Distributed:** split batches across two engines, merge partials, compare with one pass.
- **DuckDB:** add each phase's queries to `scripts/clickbench-queries.tsv` and teach
  `check-duckdb-results.py` to compare strings.
- **Memory:** extend `tests/runtime/bounded_memory.rs` so string batches stay within the byte
  budgets with long values.

## Performance targets

- Dictionary columns never materialise strings for filters or keys: one compare / hash per
  dictionary entry, then integer work per row.
- PLAIN columns are compared in place from the page buffer; hashing happens once per row only
  when the row is grouped.
- Measure with `query_bench` per phase (`--engine batch`), comparing to the decode-only time;
  string decode itself (snappy + offsets) is expected to dominate for `Title`/`URL`.

## Risks and open questions

- **Partial size for high-cardinality string keys.** `GROUP BY URL` on full `hits` produces
  millions of distinct URLs per worker; partials carry every one. A per-worker top-k cut is not
  correct for `ORDER BY COUNT(*) LIMIT` across shards. The correct fix is a hash-partitioned
  second aggregation stage, which needs string columns in the exchange (phase 7).
- **Memory in Workers.** Dictionaries for `Title`/`URL` can be large; the byte budgets in
  phase 1 must be enforced before any string query is enabled on Cloudflare.
- **TS host.** Until phase 7, string partials reach a TS reader that does not know key tag 3;
  it must fail loudly, not misread. Add a test for that in phase 3.
- **Semantics to pin with DuckDB:** byte-wise ordering, `''` versus NULL, LIKE escape
  characters (`\`), and `_` matching one byte versus one UTF-8 character (DuckDB: one
  character). Decide `_` explicitly and test non-ASCII.
