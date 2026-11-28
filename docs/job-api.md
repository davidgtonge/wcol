# Job API

`POST /jobs` takes a job, and `GET /jobs/{jobId}` reports on it. Every `/jobs` route needs `Authorization: Bearer <token>` matching the Worker's `WCOL_API_TOKEN` secret; a Worker without the secret answers 401 to every job (unless `WCOL_ALLOW_ANONYMOUS` is `"1"`, for local Miniflare runs only).

## Routes

| Route | Answer |
|---|---|
| `POST /jobs` | A `json` job that finishes within the sync budget returns its result. Any other job returns `202` with `{ "jobId": ... }` |
| `GET /jobs/{jobId}` | State (`accepted`, `running`, `completed`, `failed`), attempts, node count, timings and bytes read; plus the result, manifest key or output key once complete |
| `GET /jobs/{jobId}/live` | A snapshot of the running graph: tasks, edges, bytes and data centres. The console polls this |
| `GET /version` | The deploy id. The deploy script waits for it to change |
| `GET /recordings/{name}.json` | A recorded console run, bundled at build time, or `404`. Public: visitors without a token replay these |
| `POST /admin/shutdown` | Ops only. Needs header `x-wcol-shutdown` matching `WCOL_SHUTDOWN_SECRET` (`403` otherwise, and always when the secret is unset). Aborts and purges a job's node objects, aborts its container reducers and purges its coordinator. Body: optional `jobIds` (default: every job under `_jobs/`), `attempts` (default `[1, 2, 3]`), `slots` (default: found from the job's records in R2) and `timeoutMs` per call (default 5000) |
| `GET /` | The console |

The sync budget is `WCOL_JOB_SYNC_MS` (10 s by default). With `0`, every job is asynchronous.

## Request

```json
{
  "idempotencyKey": "client-token",
  "sources": { "hits": { "bucket": "output", "key": "hits.parquet" } },
  "sql": "SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID ORDER BY c DESC LIMIT 20",
  "output": { "format": "json", "rowCap": 1000 }
}
```

- `sources` maps each table name to one object. It can be `{ "bucket", "key" }`, `{ "bucket", "keys": [...] }` (several objects with one schema, read as one table), or a URL. A prefix or a glob is not a source. `bucket` names an R2 binding: Workers have one, `output`.
- `sql` is optional for a Parquet export. Without it, the export copies every row and column. `FROM` must name a table in `sources`. A job reads one table: joins are not supported yet.
- `idempotencyKey` is optional. A repeated key with the same body returns the first job instead of starting another; with a different body it answers `409`.
- `jobId` is optional and names the job (it must match `[A-Za-z0-9_-]{1,64}`, or the request gets `400`). Without it, the job id comes from `idempotencyKey`, or is random. Reusing a `jobId` with a different body answers `409`.

### Tuning

These are optional. Without them, the planner and the coordinator decide.

| Field | Meaning |
|---|---|
| `maxNodes` | The most scan tasks (default `WCOL_MAX_NODES`, 32) |
| `hashReducers` | A fixed reducer count, instead of the planner's estimate |
| `reducers` | `"auto"` (default), `"node"` or `"container"`: where hash reducers run |
| `reducerInstance` | The container instance type: `lite`, `basic` (default), `standard-1` to `standard-4` |
| `targetGroupBytes` | A reducer's group table size before it spills (16 MiB on nodes; on containers, the instance's memory less 128 MiB) |
| `groupSpill` | `false` makes a reducer fail at its hard limit instead of spilling |
| `minBytesPerNode` | The fewest planned scan bytes per scan task (64 KiB by default), so a small scan stays on few nodes |
| `maxActiveIngress` | Scan streams a reducer reads at once, 1 to 64 (default `WCOL_MAX_ACTIVE_INGRESS`, 8) |
| `failFirstAttemptTaskId` | Fault injection for retry tests: `"n<taskId>"` makes that task fail mid-stream on the first attempt only |

With `"reducers": "auto"`, the coordinator compares the planner's estimate of group state with what node reducers can hold in memory (128 of them × 16 MiB). It sends larger aggregates to 64 `basic` containers.

## Outputs

### `json`

```json
{ "format": "json", "rowCap": 1000 }
```

Returns rows. Without a `LIMIT`, the coordinator stops at `rowCap`.

### `parquet`

```json
{
  "format": "parquet",
  "bucket": "output",
  "prefix": "exports/hits/",
  "partitionBy": ["EventDate"],
  "targetFileBytes": 536870912,
  "targetRowGroupRows": 65536
}
```

Writes Parquet files, and then commits them by publishing `<prefix>_manifest.json`. The manifest lists every file with its size and task, and is how readers find them: the files themselves stay under the job's attempt keys (`_jobs/<jobId>/...`). A scan, filter or projection writes one file per scan task. An aggregate, sort or limit writes one file from the finalizer. `targetFileBytes` (default 512 MiB) guides the number of writers and is not a maximum.

`partitionBy` (optional) names output columns. Each scan task then writes one file per distinct value, under a Hive path such as `EventDate=15887/part-0.parquet`. The partition columns are in the path, not in the files, as Hive, Spark and DuckDB write them. Each manifest entry carries its `partitionValues` and `rows`. Path values are integers in decimal, text percent-encoded except `A-Z a-z 0-9 - _ .`, and `__HIVE_DEFAULT_PARTITION__` for null. Floating-point columns cannot partition.

- It applies to a scan, filter or projection. With an aggregate, sort or limit, it is a planning error.
- A task keeps at most 8 files open. A new value past that closes the least recently used one, and a later row with that value starts another file in the same directory. Data clustered by the partition column (dates in ClickBench's `hits`, say) writes one file per value per task; scattered values write more, smaller files. A task fails if it would write more than 4096.
- Open files share a 16 MiB budget for rows not yet written: past it, the largest writes a row group early. Each file uploads in 5 MiB parts.

### `CREATE MODEL`

```json
{
  "sources": { "customers": { "bucket": "output", "key": "customers.parquet" } },
  "sql": "CREATE MODEL segments OPTIONS (model_type = 'kmeans', num_clusters = 12) AS SELECT income, spending FROM customers"
}
```

Trains k-means over numeric columns. Each scan loads its partition once and keeps it resident. The coordinator then runs the rounds, and commits each one so that a retry resumes from the last committed round. The model is written to `_models/{name}/model.json`. Options are `num_clusters` (32), `max_iterations` (100), `seed` (42) and `tolerance` (1e-4). `WCOL_RESIDENT_MEMORY_BYTES` on the node Worker caps a partition's feature storage (40 MiB by default).

## Attempts and output keys

Each attempt writes under `_jobs/<jobId>/...`, keyed by task and attempt, so a retry never overwrites the attempt before it. Publishing the result record or the manifest is the commit. Readers should read the manifest rather than list `_jobs/`.

When a job completes or fails, the coordinator sweeps its scratch in the background. It deletes every attempt output the published result does not name (earlier attempts' files, and the shards a single-file export concatenated), and any spill runs a failed task left. The job's records stay: plan, timing, result and manifest. A sweep that fails is logged and leaves only objects no reader is pointed at.

## Errors

Planning errors (unsupported SQL, an unknown table or column, an invalid body) return `400` with `{ "error": ... }`, and the job is not retried. A reused `idempotencyKey` or `jobId` with a different body, or a `CREATE MODEL` whose model already exists, returns `409`. A failed task fails its attempt, and the job runs again as a new attempt, up to three attempts in all. A fatal error (one that would recur on the same bytes, such as a result too large for a JSON answer) or a stall (an attempt that makes no progress) fails the job at once. A job that fails reports `failed` with the last error; a synchronous `json` request that fails gets `500` with `{ "jobId", "error" }`.
