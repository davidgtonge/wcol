# Q31: grouped aggregation by WatchID and ClientIP, plan walkthrough

This is the full 100M-row `hits.parquet` (14.8 GB), planned for 32 scan slots with the coordinator's default settings. It is the plan the hosted job runs.

```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
FROM hits
WHERE SearchPhrase <> ''
GROUP BY WatchID, ClientIP
ORDER BY c DESC
LIMIT 10
```

## Logical plan (DataFusion)

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.WatchID, hits.ClientIP, count(*) AS c, sum(hits.IsRefresh), avg(hits.ResolutionWidth)
    Aggregate: groupBy=[[hits.WatchID, hits.ClientIP]], aggr=[[count(*), sum(CAST(hits.IsRefresh AS Int64)), avg(CAST(hits.ResolutionWidth AS Float64))]]
      TableScan: hits projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth], full_filters=[hits.SearchPhrase != Utf8("")]
```

## WCOL logical plan

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[0, 7], count(*), sum(15), avg(20))
      Filter(39 ne text:"")
        Scan(hits, [0, 7, 15, 20, 39])
```

Column indexes: 0 `WatchID`, 7 `ClientIP`, 15 `IsRefresh`, 20 `ResolutionWidth`, 39 `SearchPhrase`.

## WCOL physical plan, annotated

The physical plan has 132 operators. Scans take 128 of them: 32 identical pipelines, one per node, each with 4 operators. Node 0's ops are shown in full. Node *n* uses ops `4n`–`4n+3`. Operators 128–131 are the exchange, finalise, sort and limit stages that follow.

```text
── SCAN, nodes 0–31 (x32) ──────────────────────────────────────
op 0  r2_fetch           ~49 range reads per node, most of 2 MiB each.
                         Reads only 5 of 105 columns, across ~7 row groups
op 1  decode             those 5 columns, rows per row group
                         (for example 450,560 / 612,174 / 344,064 …)
op 2  filter             SearchPhrase ≠ ''
op 3  project            WatchID, ClientIP, IsRefresh, ResolutionWidth

── EXCHANGE (plan label: coordinator) ──────────────────────────
op 128 hash_exchange     keys [WatchID, ClientIP], 64 partitions
                         inputs: all 32 project ops (op 3, 7, … 127)

── REDUCE + FINALISE, per partition (x64) ─────────────────────
op 129 final_aggregate   group by (WatchID, ClientIP): count, sum, avg

── FINALIZER (plan label: coordinator) ─────────────────────────
op 130 sort              c DESC NULLS LAST
op 131 limit             top 10
```

**Bytes moved:** across all 32 scans there are 1,531 range requests, with 1.47 GB fetched for 1.47 GB logical bytes. That is about 10% of the file, because only 5 columns are read.

## Job dispatch: from coordinator to tasks

The coordinator plans the job and sends each slot its program. Tasks then run in parallel and stream results to the next stage. Nothing is merged on the coordinator.

```text
Client ──POST /jobs──▶ Coordinator (plans the query)
                            │
          runScanTask ×32   │   runFinalizer ×1
   ┌────────────────────────┼─────────────────────┐
   ▼                        ▼                     ▼
 Scanner 0 … 31         (one per node)        Finalizer
 7 row groups, 5 cols                          sort · limit
   │
   │  rows routed by hash(WatchID, ClientIP)
   │  one stream per scanner–reducer pair
   ▼
 Reducer 0 … 63   (one per hash partition)
 final aggregate for its own keys
   │
   └── partial groups (WCS stream) ──▶ Finalizer ──10 rows──▶ Coordinator ──JSON──▶ Client
```

| Task | Count | Gets from the coordinator | Sends |
| --- | --- | --- | --- |
| Coordinator | 1 | The SQL and the plan | Programs to every task; JSON to the client |
| Scanner | 32 | Its program: 7 row groups, 5 columns, filter | Each row to the reducer that owns its key |
| Reducer | 64 | Its program: final aggregate for one partition | Its groups to the finalizer |
| Finalizer | 1 | Its program: sort, limit 10 | The top 10 rows to the coordinator |

Each scanner opens one stream to each reducer, so there are 32 × 64 = 2,048 streams. A reducer finishes once every scanner has sent it its last rows, so its groups are complete. The finalizer receives only groups, never scan rows. Those streams don't pass through the coordinator.

## What each stage does

| Stage | Where | Work |
| --- | --- | --- |
| Scan | 32 nodes | Read 5 columns, decode, filter, project. No aggregation yet |
| Exchange | Hash, 64 partitions | Route each row to the partition that owns its (WatchID, ClientIP) |
| Finalise | 64 reducers | Aggregate each partition's groups to final counts, sums and averages |
| Sort and limit | Finalizer | Keep the 10 groups with the highest count |

## Why this plan has an exchange

The 500K-row file has few enough groups to merge partial results on one coordinator. On the full file there are about 100M distinct (WatchID, ClientIP) pairs, far more than one merge can hold. The planner therefore partitions the rows by group key instead of pre-aggregating them on the scans. Each finalise partition owns its own keys, so no single node holds all 100M groups.

At run time, the scans stream their rows straight to the reducers. The coordinator does not relay the shuffle bytes; the `coordinator` label in the plan text describes where the operator is placed, not where the rows pass through.
