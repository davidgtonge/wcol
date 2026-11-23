# Distributed failure injection matrix (Milestone E)

Miniflare proof: `npm run test:cf-failure-matrix`. Full shard retry + duplicate replay still lives in `npm run test:cf-shard` (requires HITS HTTP).

| Injection | How | Expected observable |
|-----------|-----|---------------------|
| **Shard attempt failure** | `failFirstAttemptTaskId` on `POST /distributed` or `POST /jobs` | `failedTaskAttempts ≥ 1`; ledger row with `attemptId ≥ 2`; job eventually `completed` with same partial as monolith |
| **Duplicate partial replay** | `replayDuplicate: true` on distributed reduce | Coordinator rejects second submit; `duplicateRejected: true` |
| **Client disconnect** | Close HTTP client after `202` on `/jobs` | Job continues (`waitUntil`); poll reaches `failed`/`completed`, not `cancelled` — `test:cf-failure-matrix` (`asyncSurvivesAfterAccept`) |
| **Explicit cancel** | `POST /jobs/{jobId}/cancel` during `testHoldMs` (CI hook) | Job `state: cancelled` — `test:cf-failure-matrix` (`cancelDuringHold`) |
| **Missing manifest fields** | `POST /jobs` with `kind: "row-export"` but empty body | `400` before accept |
| **Async export/compact routing** | `POST /row-export` or `/page-compact` with `async: true` and no `coordinatorJob` | `202` on coordinator; **`GET …/status?jobId=`** reads coordinator by default (`useCoordinatorJobStatus`); opt out with `?coordinator=0` or `WCOL_LEGACY_GRAPH_JOB_STATUS=1` |
| **Partial upload fail** | `failUploadAfterParts` on export/compact task | Worker aborts multipart; coordinator job `failed` with **`exportPartial`** `{ failed, published }` for reuse — `test:cf-failure-matrix` |
| **WCS guard on shard wire** | GraphHost WorkerShard egress after `EmitPartial` END | Reducer `PLAN_BYTE_PIPE` validate+copy accepts stream (`test:cf-wcs-bench-graph` with `WCS_VALIDATE_AND_COPY_PLAN`) |

## Related

- [DISTRIBUTED_STREAMING_GRAPH_EXECUTION.md](./DISTRIBUTED_STREAMING_GRAPH_EXECUTION.md)
- `scripts/cf-shard-lifecycle.ts`, `scripts/cf-failure-matrix.ts`
