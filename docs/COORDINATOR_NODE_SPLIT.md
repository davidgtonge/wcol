# Coordinator / node split

**Status:** Phase 1 done. Phase 2 crate split landed (2026-10-29): `wcol-engine` and `wcol-plan`; `datafusion` is off by default on `wcol-plan` (and forwarded by the `wcol` facade). Not folded into that feature: `wcol-sql`, `wcol-sql-wasm`, `df-planner`, `df-planner-wasm` (`patches/` stays). `cargo test -p wcol-sql --features native,datafusion --test clickbench_plans_snapshot` is a pre-existing failure (`docs/CLICKBENCH_PLANS.md` was not regenerated after row queries). Phases 3–6 not started.  
**Finalizer (2026-10-30):** every aggregate graph now ends in a finalizer task (`OP_FINALIZE_FRAGMENT`): node partials stream in, it merges them with the streaming `ReduceOp`, turns the groups into rows, and runs the plan's sort and limit with the engine's `SortOp` / `LimitOp`. It emits row frames, like a row query's root. The coordinator only names columns and prints JSON (`wcol_result_json`, from the plan's result-column trailer). Checked against DuckDB at 1 and 4 nodes (`finalize_job`, `plan-dispatch.test.ts`). **Phase 5 (2026-10-30):** a multi-node graph's root gets its own node slot. `/jobs` runs SQL queries as the planned graph: one RPC to the finalizer's `WcolNode` (`runFinalizer`), which calls `runScanTask` on each producer and streams their WCS bytes into its program; only result rows return to the coordinator. Programs and streams are `Uint8Array` / `ReadableStream` over RPC, nothing is base64, and storage holds bounded status rows only. A failed attempt reruns the graph on fresh DOs, and a finalizer whose input ends before every producer's END fails rather than dropping groups. Verified in Miniflare on the 500k-row hits subset: Q15 matches DuckDB with an injected mid-stream failure retried, a 5-node GROUP BY and a 5-node row query match DuckDB, and every stored value is ≤ 64 KiB (`scripts/cf-workerd-query.ts`, `tests/graph-job-boundaries.test.ts`). **Phase 6 (2026-10-30):** `POST /jobs` is the only entry point (`docs/COORDINATOR_JOB_API.md`): the coordinator Wasm validates and plans `json`, `parquet` and `concat` bodies (`plan::job_plan`), and every kind runs as a graph on node DOs through three RPC methods (`runScanTask`, `runFinalizer`, `runTaskToObject`). Exports write attempt objects and commit a manifest; an aggregate export is written by its finalizer; `concat` is page compaction on one node. Sync `json` within `WCOL_JOB_SYNC_MS`, otherwise `202` and `GET /jobs/:id`. The old routes and job kinds answer `410` and their code is gone (`shard-worker`, `stream-task-node`, `reduce-stage-node`, `row-export-route`, `page-compact-route`, `graph-coordinator*`, and their tests and scripts); `scripts/deploy-worker.ts` deploys the two Workers. Verified on workerd by `scripts/cf-workerd-jobs.ts` (json across nodes with idempotency, a 5-part export, an aggregate export, concat, async json) and `scripts/cf-workerd-query.ts` (Q15 with retry), against DuckDB. The forward-for-one-release step was skipped: the routes went straight to `410`. Left: `partitionBy` execution, `_jobs/` cleanup, and the WCS transport benchmark routes (`wcs-bench-node.ts`, `cf-shard-job.ts`), the last base64 users (I14).
**Goal:** two Wasm binaries with different jobs. The **coordinator** parses and plans SQL, schedules the graph, and prints the result; it is the only place SQL text exists. The **node** executes plan fragments over Parquet bytes and knows nothing about SQL. DataFusion is an optional coordinator frontend behind a Cargo feature; the default frontend is our own parser, also coordinator-only.

The same plan closes the other gaps from the system review: hard-coded node count, partials stored in Durable Object storage, and a job API that is not yet the entry point.

## Where things stand

| Area | Today | Problem |
|------|-------|---------|
| Node Wasm (`wcol.wasm`) | `wcol` links DataFusion (`plan/sql`), the hand parser (`plan/sql_parser`), the physical planner and the engine; exports `wcol_plan_query` | 26.7 MB after `wasm-opt` (6.8 MB gzipped), up from about 600 KB. Every node pays for a planner it never runs. |
| Engine operators | Kernels take `ir`. SQL FFI (`wcol_columns_for_sql`, `wcol_plan_query`) lives in `wcol-plan`. Fragment / PWCL / WCS codecs live in `wcol-engine` (`program`). | Phase 1 and the crate split are done. The node Wasm is still the `wcol` facade cdylib, so the planner is still linked into the node binary until Phase 3. |
| Coordinator planning | `cloudflare/src/df-plan-graph.ts` loads a separate DataFusion Wasm (`wcol-sql-wasm`); `plan-dispatch.ts` calls `wcol_plan_query` in the node Wasm | Two planner binaries, two frontends, and the default path no longer accepts aggregate offsets (`SUM(ResolutionWidth + 1)` fails: "projection is not a column list"). |
| Crates | `wcol-engine`, `wcol-plan` (`datafusion` off by default), facade `wcol`. Still separate: `wcol-sql`, `wcol-sql-wasm`, `df-planner`, `df-planner-wasm`. | The engine no longer depends on the planner. The node Wasm crate does not exist yet, so the facade cdylib still exports both. |
| Deployment | One Worker, `wcol-stream`, with both `WcolNode` and `WcolCoordinator` and `globs = ["**/*.wasm"]` | Worker size limits apply to the whole bundle, so splitting Wasm alone does not shrink the node. The bundle also carries the legacy `wcol_wasm.simd.wasm` and `wcol_parquet.wasm`. |
| Node count | `shard-coordinator.ts` uses `const nodeCount = 2` for every `/jobs` query | Tiny scans pay for two isolates; large ones get two nodes. |
| Partials | Nodes return base64 partials in JSON; the coordinator stores them in DO SQLite ledger rows | High-cardinality GROUP BY partials exceed the value size limit (`SQLITE_TOOBIG`, ClickBench Q15). |
| Job entry | `POST /jobs` runs SQL-to-JSON; `/export` and `/compact` are separate routes; `docs/COORDINATOR_JOB_API.md` is parsed and planned but not connected | Three entry points with three orchestration paths. |

## Invariants

Each invariant has a check that fails CI when it breaks.

| # | Invariant | Enforced by |
|---|-----------|-------------|
| **I1** | The node binary contains no SQL parser, no DataFusion and no planner. | Crate graph: `wcol-node-wasm` depends only on `wcol-engine`. A test runs `cargo tree -p wcol-node-wasm -e normal --target wasm32-unknown-unknown` and fails on `datafusion`, `sqlparser`, `wcol-plan`. |
| **I2** | `wcol-engine` has no SQL entry points. Operators are built only from IR structures (`Predicate`, `AggExpr`, `SortKey`, column ids). | No `fn open(sql: &str, …)` in `wcol-engine` (source grep test, like `legacy_paths_removed`); the engine has no dependency on the parser module. |
| **I3** | Node programs never carry SQL text, on any path. | Existing "no SQL in program bytes" assertions, extended to every opcode the node accepts. Opcode 10's SQL body is removed from `GraphHost`. |
| **I4** | Node binary size budget: ≤ 1.0 MB after `wasm-opt`, ≤ 400 KB gzipped. | Size check in the Wasm build script; CI fails above the budget. |
| **I5** | The coordinator builds with our parser by default. DataFusion is the `datafusion` Cargo feature, off by default. | `cargo build -p wcol-coordinator-wasm` (no features) has no `datafusion` in `cargo tree`; CI also builds `--features datafusion`. |
| **I6** | Both frontends lower to the same `WcolLogicalPlan`, and for every query both accept, produce byte-identical physical plans (predicate order aside). | `frontend_parity` runs in CI with `--features datafusion`; it must cover at least the queries it covers today (43). |
| **I7** | The default frontend never regresses: every ClickBench query the coordinator accepted before still plans and runs. | `plan-dispatch.test.ts` and `check-duckdb-results.py --plan-nodes N` run against the coordinator binary with default features; a skip count above the recorded baseline fails. |
| **I8** | Coordinator and node share one wire contract (fragment, PWCL, WCS row frames), defined only in `wcol-engine`, with version bytes. A node rejects an unknown version instead of guessing. | Round-trip and version-rejection tests in `wcol-engine`; the coordinator encodes through the engine's codecs, never its own. |
| **I9** | Plans are deterministic: the same SQL, footer, config and node budget give the same bytes. | Existing snapshot tests (`CLICKBENCH_PLANS.md`) run against the coordinator crate. |
| **I10** | Node count is a planner output, chosen from planned bytes. Nothing in TS or the coordinator Durable Object picks it. | Grep test: no numeric `nodeCount` literal in `cloudflare/src` or `src/runtime/stream`; a planner test that a small scan plans one node and a large one several. |
| **I11** | No partial or row payload is written to Durable Object storage. Storage holds bounded records only (task state, attempts, epochs, byte counts), each ≤ 64 KiB. | A storage wrapper in the coordinator that rejects values over 64 KiB; Q15 (`UserID` GROUP BY) runs end to end. |
| **I12** | One job entry point. Query, export and compaction are `POST /jobs` requests with a sink; the coordinator plans and runs all three the same way. | Old routes return 410 or forward to `/jobs`; tests drive all three kinds through `/jobs`. |
| **I13** | Coordinator and node are separate Workers. The node Worker bundles only the node Wasm. | `wrangler.toml` per Worker with explicit Wasm imports (no `**/*.wasm` glob); a bundle-size check per Worker. |
| **I14** | Binary never travels as text. Programs, partials, row frames and plans move as `Uint8Array` or `ReadableStream` over DO RPC (or `application/octet-stream` bodies); nothing on an internal path is base64. JSON appears only at the client edge, as result rows. | Grep test: no `btoa`, `atob`, `bytesToBase64` or `base64ToBytes` under `cloudflare/src` or `src/runtime/stream`. |
| **I15** | Every graph ends in a finalizer task. Merging, sorting, limiting (and later joins) run as engine operators inside the graph, and the finalizer streams rows to a sink: Parquet, or row frames back to the coordinator. The coordinator never merges, orders or windows a result; JSON is serialization of the finalizer's rows at the client edge. | `finalize_job` and `plan-dispatch.test.ts`: aggregate roots are `OP_FINALIZE_FRAGMENT` with `rows` output, results match DuckDB at 1, 2 and 4 nodes; the engine has no JSON writer; `wcol_result_json` is only exported by the coordinator Wasm. |

## Target layout

```
rust/
  wcol-engine/          rlib    formats (Parquet decode/encode), stream (batches, WCS, PWCL),
                                ops, exec (GraphHost, FetchStage), ir (predicates, aggregates,
                                sort keys, fragments) + fragment/PWCL/WCS codecs
  wcol-plan/            rlib    logical + physical plan, lower_to_physical, node_fragments,
                                execution graph, job model, query dispatch
                                frontends: sql (our parser, always), datafusion (feature)
  wcol-node-wasm/       cdylib  wcol-engine + node FFI (wcs_open, poll, read/complete, ...)
  wcol-coordinator-wasm/cdylib  wcol-plan + wcol-engine (ReduceOp, coordinator fragments,
                                Finalize) + coordinator FFI (plan_query, reduce, run
                                coordinator fragment)
  wcol-tools/           bin     query_bench, dump-clickbench-plans (native, all features)
```

```
Workers
  wcol-coordinator   WcolCoordinator DO   wcol_coordinator.wasm   routes: POST /jobs, GET /jobs/:id,
                                                                  cancel, heartbeat
  wcol-node          WcolNode DO          wcol_node.wasm          RPC/fetch from the coordinator only
```

The coordinator Worker reaches node Durable Objects through a cross-script binding (`script_name = "wcol-node"`).

Retired: `wcol` (split into the two crates), `wcol-sql`, `wcol-sql-wasm`, `df-planner`, `df-planner-wasm`, `cloudflare/src/df-plan-graph.ts`, and the legacy `wcol_wasm.simd.wasm` / `wcol_parquet.wasm` routes in `worker.ts` (demo graphs).

## Phases

Each phase lands with its checks green. Phases 1–3 are the split; 4–6 close the other gaps and depend only on phase 1.

### Phase 1: Engine IR, no SQL in operators (I2)

- Create `ir` in the engine: `ColumnKind`, `Scalar`, `CmpOp`, `Predicate`, `AggKind`, `AggExpr` (with `offset`), `SortKey`, and the catalog column type. The physical plan's `Predicate`/`AggExpr` and the parser's `Filter`/`Aggregate` both convert to these; the batch kernels take only these.
- Replace `BatchFilter::from_filters(&[sql_parser::Filter])` and `BatchQuery::from_plan(&QueryPlan)` with constructors over IR. Delete `IngestQuerySpec`; it exists only to adapt plan types into parser types.
- Delete `FilterOp::open(sql)`, `AggregateOp::open(sql)`, `ColocatedGraphPlan`'s SQL constructors, `with_plan`, and opcode 10's SQL body. `query_bench`'s SQL path plans through the coordinator crate and runs fragments.
- **Done when:** nothing under the future engine modules imports `plan::sql_parser` or `plan::sql`; the grep test for I2 passes; all tests pass, with SQL-based tests rewritten to plan first.
- **Landed:** `ir` (`ColumnKind`, `Scalar`, `CmpOp`, `Predicate`, `AggKind`, `AggExpr`, `SortKey`, `CatalogColumn`). Kernels take IR (`BatchFilter::from_predicates`, `BatchQuery::from_ir`, `FilterOp::from_predicates`, `AggregateOp::from_aggregate`). `IngestQuerySpec` and `fn open(sql: &str, …)` are gone. SQL tests plan through `plan::bind`. Grep test: `engine_modules_have_no_sql_entry` in `tests/runtime/legacy_paths_removed.rs`. `cargo test -p wcol` passed. `wcol_columns_for_sql` still lives on the FFI in `exec` and calls `plan::columns_for_sql`; that edge is the first thing Phase 2 has to invert.

### Phase 2: Split the crates (I1, I5, I8, I9)

- Move `formats`, `stream`, `ops`, `exec` and `ir` to `wcol-engine`. Move `plan` (minus `ir`), both frontends and `query_dispatch` to `wcol-plan`.
- `wcol-plan` features: `datafusion` (off by default) gates `frontends::datafusion`. Our parser is always compiled, and only in `wcol-plan`.
- Frontend selection:
  - `plan_query_dispatch` takes a `Frontend` value. With the feature off, the only value is `Frontend::Sql`.
  - With it on, the coordinator can try DataFusion and fall back to our parser, or the reverse; the order is a coordinator config, not a node concern.
- Fold `wcol-sql`, `wcol-sql-wasm`, `df-planner` and `df-planner-wasm` into `wcol-plan`'s `datafusion` feature, keeping the `patches/` crates.
- Move the codecs the coordinator encodes with (fragment, PWCL, WCS) into `wcol-engine`; `wcol-plan` calls them.
- **Landed (2026-10-29):** `wcol-engine` (`formats`, `stream`, `ops`, `exec`, `ir`, `program` codecs) does not depend on `wcol-plan`, DataFusion, or sqlparser. `wcol-plan` depends on the engine; `plan::sql_parser` is always on; `plan::sql` is `--features datafusion` only. `plan_query_dispatch` takes `&[Frontend]` (try order). Feature off: only `Frontend::Sql`. Feature on: `Frontend::DataFusion` may precede or follow `Frontend::Sql`. The `wcol` crate is a facade re-export so existing hosts still link. `cargo test -p wcol-engine` (89) and `cargo test -p wcol-plan` (88) passed. `cargo test -p wcol --features native` passed, including `engine_modules_have_no_sql_entry`. `cargo test -p wcol-sql --features native,datafusion --test frontend_parity` passed. Default `cargo tree -p wcol-engine` and `cargo tree -p wcol-plan` contain no `datafusion` or `sqlparser`. `--features datafusion` on `wcol-plan` pulls `datafusion-sql` and `sqlparser` 0.62.0.
- **Not in this landing:** `wcol-sql`, `wcol-sql-wasm`, `df-planner`, and `df-planner-wasm` are still separate crates (default-members still omit them). `frontend_parity` still lives in `wcol-sql`, not as a `wcol-plan` integration test. `clickbench_plans_snapshot` still fails for the pre-existing reason above.
- **Done when:**
  - the `cargo tree` checks for I1 and I5 pass
  - `frontend_parity` passes with the feature on
  - snapshot and plan tests pass in `wcol-plan`
  - engine tests pass in `wcol-engine`

### Phase 3: Two Wasms, two Workers (I3, I4, I13)

- `wcol-node-wasm` exports only node FFI: `wcs_open` / poll / read / complete / cancel, output, errors. `wcol_plan_query` and the reduce exports leave the node.
- `wcol-coordinator-wasm` exports planning (`wcol_plan_query` with `Frontend` in the request), streaming reduce, and coordinator-fragment execution (sort, limit, project, finalize).
- Build scripts:
  - `npm run build:node-wasm` checks the size budget (I4).
  - `npm run build:coordinator-wasm` builds the default, and `-- --datafusion` adds the feature.
  - Both run cargo from `rust/` (pinned toolchain) with the `getrandom` cfg in one place.
- Cloudflare: two `wrangler.toml` files and two entry modules. The coordinator Worker imports the coordinator Wasm explicitly and binds `WCOL_NODE` with `script_name = "wcol-node"`; the node Worker imports only the node Wasm. Remove the `**/*.wasm` glob and the legacy demo routes.
- TS host: `wasm-stream-driver.ts` splits into node and coordinator interfaces. `plan-dispatch.ts`, `execution-graph.ts` and the graph runner take the coordinator Wasm for planning and reduce, and the node Wasm only for running fragments.
- **Done when:**
  - node Wasm ≤ 1.0 MB and contains no planner export
  - coordinator Wasm builds with and without `datafusion`
  - `plan-dispatch.test.ts` passes against the coordinator default build, including the aggregate-offset query (I7)
  - a Miniflare run of `POST /jobs` with two Workers returns the same result as today
  - each Worker's bundle size is reported

### Phase 4: Node count from the planner (I10)

- Add `PhysicalPlannerConfig::{min_bytes_per_node, max_nodes}`. `lower_to_physical` picks `clamp(ceil(planned_bytes / min_bytes_per_node), 1, max_nodes)` and places on that many slots. The coordinator passes `max_nodes`, an account or deployment limit, not a count.
- A one-node plan runs its fragment in the coordinator isolate (`EdgeTransport::Colocated`), with no node Durable Object.
- Remove `const nodeCount = 2`.
- **Done when:** Q1 (25 KB of reads) plans one colocated node; a full-file plan uses several; workerd timings for Q1, Q3, Q6 and Q7 drop to roughly native time plus read latency (measure and record).

### Phase 5: Stream partials, never store them (I11)

- Node output goes over the plan's edge (DO RPC byte stream of WCS frames) into the graph's finalizer task, which runs on a node DO, not in the coordinator. The finalizer's row frames stream back to the coordinator (or to a Parquet sink). A node reports completion as a bounded record: task id, attempt, epoch, output bytes, frame count.
- `/jobs` runs the planned graph for every node count; the legacy `runJob` distributed-reduce path, its base64 programs and partials, and `wcol_partial_first_sum_i` go (I14).
- Durable Object storage writes go through a wrapper that rejects values over 64 KiB.
- Retry on failure reruns the fragment and replaces that node's stream. The registry accepts one completed attempt per task, as now.
- **Done when:** Q15 runs end to end and matches DuckDB; no storage write exceeds 64 KiB in any test; a node failure mid-stream retries and still matches.

### Phase 6: One job entry point (I12)

- Connect `docs/COORDINATOR_JOB_API.md` to `POST /jobs`: sources, optional SQL, sink (`json`, `parquet` with `partitionBy`, `compact`).
- Short jobs hold the request open up to a deadline (config, for example 10 s); longer jobs return `202` with a job id and `GET /jobs/:id` returns status and, for JSON, the result.
- `/export` and `/compact` become `/jobs` requests; the old routes forward for one release, then return 410.
- **Done when:** query, export and compaction run through `/jobs` in Miniflare; sync and async both tested; the old routes are gone.

## Verification after each phase

| Check | Command |
|-------|---------|
| Results | `check-duckdb-results.py … --plan-nodes 1/2/4` against the coordinator build, default features and `--features datafusion` |
| Parity | `cargo test -p wcol-plan --features datafusion --test frontend_parity` |
| Engine | `cargo test -p wcol-engine` (no SQL anywhere in its tests) |
| Node purity | `cargo tree` checks (I1, I5), grep tests (I2, I3, I10) |
| Size | node Wasm budget (I4); each Worker's bundle size printed in CI |
| End to end | Miniflare two-Worker `POST /jobs` for query, export, compaction; workerd timings table refreshed |

## Risks

- **Moving the IR touches every operator.** Phase 1 is a large mechanical change. Do it in one session with the full test suite as the gate, and keep behaviour byte-identical (PWCL output unchanged).
- **Cross-Worker Durable Object bindings** add a deploy-order dependency: the node Worker must deploy first. Script the deploy.
- **Frontend fallback** can mask a DataFusion bug as a silent switch to our parser. Log which frontend planned each job, and keep the parity test as the guard.
- **Coordinator size with DataFusion** (about 25 MB uncompressed today) may still be near Cloudflare's per-Worker limit (check the current limit). The default build avoids it; measure the feature build before deploying it.

## Out of scope

- New SQL features in either frontend (expressions beyond aggregate offsets, `CASE`, date functions).
- Reduction trees and hash-partitioned aggregation; phase 5 is their prerequisite.
- Joins (`PHASE6_ROWS_EXPORT_JOINS.md` 6.6).
