# Console

The coordinator Worker serves a console at `/`. It submits a query, polls `GET /jobs/{id}/live` every few hundred milliseconds, and draws the job as it runs. The page is one HTML file. `scripts/build-workers.ts` bundles it from `cloudflare/console`, with the supported ClickBench queries built in.

If the Worker has a `WCOL_API_TOKEN`, enter it in the token field. The page keeps it in this browser's local storage and sends it as a bearer token.

## Controls

- **ClickBench query**: every query the planner supports, or your own SQL in the editor.
- **Reducers on**: `auto` lets the planner choose. `node` and `container` force a choice.
- **Container size**, **Scans (up to)**, **Reducer count**: the `reducerInstance`, `maxNodes` and `hashReducers` fields of the [job API](job-api.md).
- **Source key**: the Parquet object in the bucket.

## Panels

| Panel | Shows |
|---|---|
| Status | State, elapsed time, the planner's reducer choice and its reason, and findings such as stragglers, spill and skew |
| Timeline | One lane per task, coloured by phase. **Replay** plays a finished job back. Drag the slider to any moment, and every panel shows that moment |
| Shuffle matrix | Bytes from each scan to each reducer, or when each edge finished |
| Where it ran | The Cloudflare data centres that ran the coordinator, scans and reducers |
| Data flow | Bytes read from R2, sent between tasks, spilled, and returned |
| Result | The rows |
| Totals | Tasks, attempts, bytes and spill |
| Task | Click a lane or a matrix cell to see that task's timings, bytes and errors |
| Estimated cost | Durable Object requests and duration, R2 operations, and container time, at Cloudflare's list prices. Beside it, what Athena would charge for the same bytes scanned ($5 per TB, at least 10 MB) |
| Runs | This browser's earlier runs, to compare times and costs |

The view, the cost model and the findings are pure functions in `cloudflare/src/coordinator/live.ts` and `live-findings.ts`. The page bundles the same code, so the live view and replay cannot disagree.
