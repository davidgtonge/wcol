# Q7: counting by AdvEngineID, plan walkthrough

```sql
SELECT AdvEngineID, COUNT(*)
FROM hits
WHERE AdvEngineID <> 0
GROUP BY AdvEngineID
ORDER BY COUNT(*) DESC
```

## Logical plan (DataFusion)

```text
Sort: count(*) DESC NULLS LAST
  Aggregate: groupBy=[[hits.AdvEngineID]], aggr=[[count(*)]]
    TableScan: hits projection=[AdvEngineID], full_filters=[hits.AdvEngineID != Int32(0)]
```

## WCOL logical plan

```text
Sort(agg:0 desc nulls_last)
  Aggregate(group=[40], count(*))
    Filter(40 ne i64:0)
      Scan(hits, [40])
```

Column 40 is `AdvEngineID`.

## WCOL physical plan, annotated

The file has 5 row groups, split across 2 nodes. Each node runs the same scan pipeline on its own row groups. The `fetch` and `read` lines are grouped below. The raw plan has one of each per row group.

```text
── SCAN, node 0 (row groups 0–1) ───────────────────────────────
op 0  r2_fetch           2 range reads, column 40 (fetch 0–1)
op 1  decode             2 row groups, 122,880 rows each
op 2  filter             AdvEngineID ≠ 0
op 3  partial reduce     group by AdvEngineID, count(*)

── SCAN, node 1 (row groups 2–4) ───────────────────────────────
op 4  r2_fetch           3 range reads, column 40 (fetch 2–4)
op 5  decode             122,880 + 122,880 + 8,480 rows
op 6  filter             AdvEngineID ≠ 0
op 7  partial reduce     group by AdvEngineID, count(*)

── FINALIZER (plan label: coordinator) ─────────────────────────
op 8  gather             receive both partial results
op 9  finalise           sum the partial counts per AdvEngineID
op 10 sort               count(*) DESC NULLS LAST
```

**Bytes moved:** node 0 reads 12,771 bytes in 2 requests. Node 1 reads 12,117 bytes in 3 requests. Total: 24,888 bytes, with no over-read.

## Job dispatch: from coordinator to tasks

The coordinator plans the job and sends each slot its program. Tasks then run in parallel and stream results to the next stage. Nothing is merged on the coordinator.

```text
Client ──POST /jobs──▶ Coordinator (plans the query)
                            │
             runScanTask    │    runFinalizer
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   Scanner 0           Scanner 1           Finalizer
   node 0              node 1              (own slot)
   rg 0–1, col 40      rg 2–4, col 40      merge · sort · limit
        │                   │                   ▲
        └── partial counts (WCS stream) ────────┘
                                                │
                              10 rows ──▶ Coordinator ──JSON──▶ Client
```

| Task | Count | Gets from the coordinator | Sends |
| --- | --- | --- | --- |
| Coordinator | 1 | The SQL and the plan | Programs to every task; JSON to the client |
| Scanner | 2 | Its program: row groups, column 40, the partial reduce | Partial counts to the finalizer |
| Finalizer | 1 | Its program: merge, sort, limit | The top 10 rows to the coordinator |

Each scanner streams one partial per `AdvEngineID` value. The finalizer merges the two streams.

## What each stage does

| Stage | Where | Work |
| --- | --- | --- |
| Scan | Each node | Read only the needed column, decode, filter, and reduce to one row per group |
| Gather | Finalizer | Collect the partial results from every scanner |
| Finalise | Finalizer | Combine partials into final counts |
| Sort | Finalizer | Order the final rows |

Only one row per `AdvEngineID` value leaves each node, not the rows themselves.
