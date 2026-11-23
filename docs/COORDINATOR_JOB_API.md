# Coordinator job API

**Status:** Running (2026-10-30). `POST /jobs` takes this body and nothing else. The coordinator Wasm validates it (`job_sources`), reads the source footers, and plans it (`plan_job`); the graph runs on node Durable Objects. `json` answers inline within the sync budget (`WCOL_JOB_SYNC_MS`, default 10 s; `0` is always async) and otherwise returns `202` with the result kept in R2 for `GET /jobs/{jobId}`. `parquet`, `concat`, and model training return `202` at once. `idempotencyKey` names the job, so a repeat returns the first job. Verified on workerd by `scripts/cf-workerd-jobs.ts` against DuckDB. Not executed yet: `partitionBy` (a planning error, "partitionBy is not executed yet"), and superseded-attempt cleanup.

One request describes a job: a table map, an optional query, and a sink. SQL chooses the rows. The sink chooses how those rows leave the graph. `partitionBy` is a field on the sink. It is not part of the SQL text.

```text
Job request
  sources + SQL + output
        |
        v
DataFusion  ->  WCOL logical plan  ->  WCOL physical plan
        |
        v
Placement + exchanges + sink operators
        |
        +-- json     return rows
        +-- parquet  write parts
        +-- model    retain scan partitions for iterative training
        |
concat
  page-compaction planner (no SQL, no WCOL query plan)
  copy pages
```

## Request

```json
{
  "idempotencyKey": "client-token",
  "sources": {
    "hits": { "bucket": "in", "key": "hits.parquet" }
  },
  "sql": "SELECT EventDate, UserID FROM hits WHERE CounterID = 1",
  "output": {
    "format": "parquet",
    "bucket": "out",
    "prefix": "hits/",
    "partitionBy": ["EventDate"]
  }
}
```

`sources` maps every table name to one object. The value is a bucket key or a file URL. A prefix, a glob, and a list of files are not a source. `FROM` must name a key in `sources`. A missing key is an error.

Day 1 runs a job only when `sources` has one table. A second entry is valid in the request and is rejected at execution until joins run.

`sql` is optional. With no `sql`, the job is an identity transform: every column and every row, written by the distributed Parquet sinks.

`idempotencyKey` is optional. A repeat of the same key returns the existing job instead of starting another.

## K-means model training

`CREATE MODEL` is a sinkless top-level SQL statement. It reuses the SELECT planner, then loads each planned scan partition once into a resident node session. Each global iteration sends centroids to those sessions, merges their partial sums and counts, commits the updated barrier in coordinator storage, and continues until convergence or `max_iterations`.

```json
{
  "idempotencyKey": "customer-segments-v1",
  "sources": {
    "customers": { "bucket": "in", "key": "customers.parquet" }
  },
  "sql": "CREATE MODEL customer_segments OPTIONS (model_type = 'kmeans', num_clusters = 12, max_iterations = 100, seed = 42, tolerance = 0.0001) AS SELECT income, spending, savings FROM customers WHERE income > 0"
}
```

The planner marks each worker fragment with a resident sink. `runWcolEngine` loads the fragment through the normal stream pool; SELECT batches enter the Rust sink directly in the same Wasm instance. Input completion parks the graph at a ready barrier. Round RPCs queue control messages and wake that same graph pump; explicit release, cancellation, or expiry closes its handle. There is no feature-data WCS transfer through JavaScript and no separate resident Wasm instance.

### Resident memory budget

Set `WCOL_RESIDENT_MEMORY_BYTES` on the **node Worker** to a positive 32-bit integer byte count (default `41943040`, or 40 MiB). The engine checks the budget in Rust before growing feature storage. It accounts conservatively for old/new raw buffers during reallocation and for overlapping raw f64 and normalized f32 matrices. Exceeding the budget fails the job without retrying or publishing a model. The runtime does not automatically repartition an oversized partition.

This is a **feature-storage budget**, not a limit on total Worker memory. Decode buffers, initialization samples, centroid/partial buffers, Wasm allocator overhead, and JavaScript/R2 buffers are additional. Each released session writes phase measurements to `_jobs/{jobId}/a{attempt}/t{taskId}-resident-memory.json`: load, normalization, and round feature accounting; Wasm linear-memory high-water; wall times; and round count. Feature peaks conservatively account for allocation overlap; Wasm bytes are observed allocated pages and exclude JavaScript memory. These telemetry writes are best effort and may be absent after node loss. See [local memory measurements](benchmarks/RESIDENT_MEMORY.md).

`npm run test:iterative-miniflare` verifies real multi-node training with R2 and SQLite Durable Objects. A test-only node terminates during round two; the jobs runner retries, reloads the partitions from R2, and resumes after the last committed barrier. The recovered model must match uninterrupted training. The suite also checks session and iteration fencing, stale-attempt rejection, cancellation, and idempotent release. Failed rounds settle all outstanding RPCs before releasing resident graphs; cleanup errors preserve the original execution failure.

The v1 kernel accepts numeric feature columns. Defaults are 32 clusters, seed 42, 100 iterations, and tolerance `1e-4`. Training computes global z-score normalization, mean-imputes nulls, and maps constant or all-null columns to zero. The create-once artifact is `OUTPUT:_models/{name}/model.json`. `GET /jobs/{jobId}` reports `modelName`, `modelKey`, and `modelSummary` when training completes. Prediction and table functions are not part of v1.

## Response lifecycle

| `output.format` | Response |
|---|---|
| `json` | The result body, when the query finishes inside the request. The caller supplies `LIMIT`, or the coordinator applies a row cap. |
| `parquet` | `{ "jobId": "..." }` immediately. `GET /jobs/{jobId}` reports `running` or `complete` with the manifest key. |
| `concat` | Same as `parquet`: a job id, then a completion check. |

Parquet and concat do not depend on the original HTTP connection staying open.

## Sinks

| `output.format` | SQL | What the job does |
|---|---|---|
| `json` | required | Interactive result. The plan's row sink. |
| `parquet` | optional | Query or transform. Parquet parts under `output.prefix`, named by the published manifest. |
| `concat` | absent | Page-copy compaction. Input is a list of object keys. Output is one Parquet key. No table names and no SQL. |

`json` output:

```json
{ "format": "json", "rowCap": 1000 }
```

`parquet` output:

```json
{
  "format": "parquet",
  "bucket": "out",
  "prefix": "hits/",
  "partitionBy": ["Day"],
  "targetFileBytes": 536870912,
  "targetRowGroupRows": 65536
}
```

`partitionBy` may be omitted. `targetRowGroupRows` defaults to 65_536.

`targetFileBytes` is advisory (default 512 MiB). The planner uses it to choose a worker count. It does not promise that any file stays under that size. A skewed partition can be larger, and adding workers does not fix that.

`concat` is a different request. It does not use `sources` or `sql`, and it does not enter the SQL or WCOL query planner:

```json
{
  "output": { "format": "concat", "bucket": "out", "key": "hits/merged.parquet" },
  "inputs": [
    { "bucket": "out", "key": "hits/part-0.parquet" },
    { "bucket": "out", "key": "hits/part-1.parquet" }
  ]
}
```

This is the page-compaction job. It copies row groups and does not read values, so it cannot filter, project, or partition.

An input may name `rowGroups: [start, end]` (half-open). That job is a split, not a compaction: it takes exactly one input and copies those row groups' column chunks byte for byte, whatever their codec or dictionary pages. The part's footer is the source footer rewritten for the part: only those row groups, offsets moved to where the chunks now sit, page-index and bloom-filter offsets dropped. `scripts/cf-partition-hits.ts` splits one object into N parts this way and checks the parts against DuckDB.

```json
{
  "output": { "format": "concat", "bucket": "out", "key": "hits-parts/part-0003.parquet" },
  "inputs": [{ "bucket": "out", "key": "hits.parquet", "rowGroups": [12, 16] }]
}
```

A concat job's create-once publish pointer is `<key>/_manifest.json`, so jobs writing different keys never collide.

## Where the Parquet sink sits

Physical lowering places the sink on the final rows.

| Query shape | Sink |
|---|---|
| Scan, filter, projection | One Parquet sink on each node that has row groups |
| No SQL (identity) | Same as a scan: distributed |
| Global aggregate or `GROUP BY` | One sink on the coordinator, after `FinalAggregate` |
| Global `ORDER BY` or `LIMIT` | One sink on the coordinator, after that step |
| Join | Rejected until joins execute |

A grouped query is the case that must not sink on the scan. Each node emits partial aggregates. Those are not the result. The sink reads the rows after the final reduction.

```sql
SELECT EventDate, COUNT(*) AS n
FROM hits
GROUP BY EventDate
```

`partitionBy` is legal on day 1 only for the distributed shapes: scan, filter, projection, and identity. An aggregate, `ORDER BY`, or `LIMIT` with `partitionBy` is a planning error. A partition writer after the final reducer can come later. A hash exchange of partition writers is not part of day 1.

`PhysicalPlannerConfig.parquet_row_group_rows` turns the sink on. `PhysicalPlannerConfig.partition_columns` holds the output ordinals from `bind_partition_columns`. A non-empty list is `partitionBy`, and it triggers the rejection above. Those ordinals are on the node fragment. Opening that fragment as one Parquet file returns an error: the host writes it with `PartitionDemux`.

## partitionBy

DataFusion parses `sql` only. The lowerer in `wcol-sql/src/wcol_logical.rs` turns a scan, filter, projection, aggregate, sort, limit, or join into a `WcolLogicalPlan`. It does not accept a `COPY` statement or a write option.

`PARTITION BY` inside SQL is a window clause. That plan is rejected. File layout is not expressed that way.

Names in `partitionBy` bind to the final output schema, including aliases and computed expressions. They do not bind to the source catalog.

```sql
SELECT CAST(EventTime AS DATE) AS Day, UserID
FROM hits
```

`partitionBy: ["Day"]` is valid. `partitionBy: ["EventTime"]` is not, because `EventTime` is not an output column. `SELECT UserID FROM hits` with `partitionBy: ["EventDate"]` fails for the same reason.

The writer demultiplexes by those output values. It cannot close a file when the value changes, because a later batch can contain that value again. Each open partition stays open until the task finishes or the cardinality cap is hit.

Day 1 fails the task when the number of distinct values on that node exceeds `max_open_partitions`. It does not spill and it does not reshuffle.

```rust
struct PartitionWriterConfig {
    max_open_partitions: usize,   // default 8
    max_buffered_bytes: usize,    // default 8 * 5 MiB
    multipart_part_bytes: usize,  // default 5 MiB
}
```

R2 requires non-final multipart parts of at least 5 MiB, and those parts must be equal-sized. Eight open writers therefore hold about 40 MiB of part buffers before Parquet encoding, WASM, and query state. The isolate limit is about 128 MiB and may be shared. `PartitionWriterConfig::validate` rejects a part size under 5 MiB and a buffer that cannot hold one part per open partition.

### Path values

`partition_path_value` is the canonical segment:

| Value | Segment |
|---|---|
| null | `__HIVE_DEFAULT_PARTITION__` |
| bool | `true` or `false` |
| int | decimal, including a leading `-` |
| date | `YYYY-MM-DD` (month 1–12, day 1–31) |
| text | unreserved bytes kept (`A–Z`, `a–z`, `0–9`, `-`, `_`, `.`); every other byte percent-encoded, so `/` is `%2F` and `=` is `%3D` |

## Attempts and the manifest

A logical task id is stable across retries. A physical attempt is not. Two attempts must not write the same R2 key: R2 limits writes per key, and a retry can overlap the first attempt.

```text
Temporary:  _jobs/<jobId>/<taskId>/<attempt>/part.parquet
Committed:  <prefix>_manifest.json
```

`attempt_object_key` builds the temporary key.

1. Each attempt writes only its own immutable objects.
2. A successful attempt reports output records.
3. The coordinator keeps the current successful attempt for each task and drops any other attempt of that task.
4. When the graph completes, the coordinator publishes the manifest. That publication is the commit.
5. Failed and superseded attempts are deleted later. Readers ignore `_jobs/` and read the manifest.

Two checks, not one:

- **Task input coverage** (`verify_scan_task_coverage`). Every planned source row group is processed by exactly one accepted scan task.
- **Output manifest** (`verify_output_manifest`). Every committed object is recorded once, with its partition values, row count, byte count, and originating task and attempt. One task may publish many objects. Two attempts of the same task cannot both appear.

```text
Source row group 42
    -> filter / projection
    -> EventDate=2013-07-01/part.parquet
    -> EventDate=2013-07-02/part.parquet
    -> EventDate=2013-07-03/part.parquet
```

All three objects are valid records of one scan of row group 42.

`verify_row_export_manifest` remains for an unpartitioned export, where each task writes one object. It is the two checks above applied to that shape. A partitioned job must not use it as a one-file-per-row-group rule.

## What day 1 does not do

- A bucket prefix or a glob as a table.
- More than one source table in an executed job.
- A partition exchange (hash by the partition column, then dedicated writers).
- `partitionBy` on an aggregate, `ORDER BY`, `LIMIT`, `json`, or `concat`.
- A guarantee that `targetFileBytes` is a maximum file size.
- Joins. The map may name two tables. Execution returns an error until phase 6.6.

## Changes

### In the planner now

- `export_placement` puts node sinks on scan, filter, and projection, and a coordinator sink after `FinalAggregate` or after a global sort or limit.
- `partition_columns` rejects `partitionBy` on an aggregate, sort, or limit, and the node fragment carries the output ordinals.
- `verify_scan_task_coverage` and `verify_output_manifest` replace the one-row-group-one-file rule for partitioned output.
- `PartitionWriterConfig`, `partition_path_value`, and `attempt_object_key` are the day-1 limits, path encoding, and attempt keys.
- `parse_job_request` checks the body: one object per table, one table for json and parquet, no SQL on concat, no globs.
- `job_output_fields` is the final schema. No SQL returns the catalog columns. `bind_partition_columns` maps `partitionBy` onto those names, including a `CAST(...) AS Day` alias.
- `AttemptLog` keeps the newest successful attempt per task. `publish` is the commit set. `manifest_object_key` is `<prefix>_manifest.json`.
- `IdempotencyTable` returns the first job id for a client key.
- `PartitionDemux` keeps one open object per value, hard-fails past the cardinality cap, and refuses to grow past the buffer cap when no part is large enough to upload.

### Built since day 1

- `POST /jobs` parses this body in the coordinator Wasm (`plan::job_plan`). The old job kinds (`tasks`, `row-export`, `page-compact`) and routes (`/row-export`, `/page-compact`, `/export`, `/compact`, `/distributed`) answer `410`.
- `GET /jobs/{jobId}`: state, attempts, node count, and the manifest key (`parquet`), output key (`concat`) or result (async `json`).
- `parquet` with no edges: every task writes `_jobs/<jobId>/<taskId>/<attempt>/part.parquet`, retrying alone on a fresh key; with a root (sort, limit, aggregate) the root writes the only object. The manifest at `<prefix>_manifest.json` is the commit.
- A grouped export's sink is the graph's finalizer: sort and limit, then the `SELECT` columns in order, then a typed Parquet sink. Counts and integer `SUM`/`MIN`/`MAX` are INT64 (an overflowing `SUM` fails the export; DuckDB writes HUGEINT as DOUBLE), `AVG` and float aggregates DOUBLE.
- The table name comes from `sources`; `FROM` must name it.
- `concat` runs the page-compaction planner with one output part, on one node, reading every input by index.

- A `json` row top-k over wide rows reads them late (`plan::late_rows`). A query like `SELECT * … ORDER BY … LIMIT k`, where the printed-only columns of a row group decode to more than 64 MiB, runs as two row graphs. The first decodes only the filter and sort columns and prints the winners' row ids (source, row group, row). The coordinator then plans scans that read the `SELECT` list back for those ids only, one per group of columns that fits a node, over the row groups that hold winners. It prints their rows in the first graph's order. `OFFSET + LIMIT` must be at most 1024. The body hint `lateWideBytes` lowers the 64 MiB threshold (tests use it on small files). The descriptor keeps the body, so a retry plans the read-back again from the pinned footers.

### Still to build

- Run `PartitionDemux` on the host for a fragment whose `partition_columns` are set, then store the attempt objects and publish them in the manifest.
- Cleanup of superseded `_jobs/` objects.
- `concat` writes `output.key` directly, so a retry rewrites that key after the failed attempt has stopped (attempts do not overlap, but they share the key).

### SQL

- Parse `sql` with DataFusion when it is present (`wcol-sql`).
- When `sql` is absent, build a logical scan of every leaf of that one table. That is the identity transform. It is not page-copy. Page-copy is `concat`.
- Do not add `PARTITION BY` to the SQL grammar or to `wcol_logical.rs`.

### Tests

- Grouped Parquet export: the sink is the coordinator root and its input is `FinalAggregate`. No node has a Parquet sink. `partitionBy` on that plan is an error.
- Scan or filter with `partitionBy`: Parquet sinks sit on the nodes.
- One scan task, three output objects: coverage passes and the output manifest passes. A second attempt of that task in the same manifest fails.
- One table, `json`, a small `LIMIT`: the body matches DuckDB. Not built yet.
- One table, `parquet`, no `partitionBy`: part files union to the DuckDB result. This is `tests/runtime/row_export.rs`.
- `partitionBy` on `CAST(EventTime AS DATE) AS Day` binds to output ordinal 0. `EventTime` is rejected. `PartitionDemux` keeps both rows for a repeated day on one object. The files are not yet written through the worker, so there is no DuckDB union of those parts yet.
- `concat` of two part files: the page-compaction planner already copies pages. The `concat` request parses. It is not wired to that planner yet.
