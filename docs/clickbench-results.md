# ClickBench results on Cloudflare

Run 2026-11-22, before container reducers: every reducer here is a node Durable Object. Full `hits.parquet` (100M rows, 14.8 GB) in R2, fixed pool of 32 scans and 64 reducers, `STAGE=c CLICKBENCH_TSV=1 ORACLE=cached npm run clickbench:cloudflare`. Since then, `auto` sends Q32-sized aggregates to container reducers: Q32 takes 6.8 s on 64 `basic` containers, against 20.1 s here ([containers](containers.md)).

- **50 of 53 passed**; 1 needed a retry (Q8).
- Sum of wall times for passing queries: **271 s**.
- Q18, Q35 and Q42 are not in the ladder: the planner does not support them yet.
- *Verified* means the rows match DuckDB's cached answer. S7 and S10 are Parquet exports and are not compared; queries without a cached answer are not compared either.

Column meanings: **Wall** is submit to answer as the ladder sees it. **Scans done**, **Reducers done** and **Finalized** are offsets from the job's start in the coordinator's span timeline for the final attempt. **Read** is Parquet bytes read from R2; **Spill** is bytes reducers wrote to R2 spill runs.

| Query | Result | Wall (s) | Attempts | Nodes | Scans done (s) | Reducers done (s) | Finalized (s) | Read (GB) | Spill (GB) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A1 | pass | 2.2 | 1 | 33 |  |  |  |  |  |
| A2 | pass | 2.9 | 1 | 96 | 1.5 | 2.0 | 2.2 | 0.33 |  |
| A3 | pass | 3.9 | 1 | 96 | 2.1 | 2.6 | 3.1 | 0.37 |  |
| A4 | pass | 2.6 | 1 | 33 |  |  |  |  |  |
| D1 | pass | 2.0 | 1 | 33 |  |  |  |  |  |
| D2 | pass | 2.7 | 1 | 33 |  |  |  |  |  |
| D3 | pass | 2.1 | 1 | 33 |  |  |  |  |  |
| D4 | pass | 2.7 | 1 | 33 |  |  |  |  |  |
| D5 | pass | 8.6 | 1 | 96 | 7.6 | 7.8 | 8.3 | 0.12 |  |
| Q0 | pass | 1.6 | 1 |  |  |  |  |  |  |
| Q1 | pass, verified | 2.0 | 1 | 33 |  |  |  |  |  |
| Q2 | pass | 2.1 | 1 | 33 |  |  |  |  |  |
| Q3 | pass | 2.9 | 1 | 33 |  |  |  |  |  |
| Q4 | pass, verified | 7.3 | 1 | 96 | 4.9 | 6.4 | 6.9 | 0.27 | 0.74 |
| Q5 | pass | 4.6 | 1 | 96 | 3.3 | 3.8 | 4.2 | 0.37 |  |
| Q6 | pass | 1.7 | 1 | 33 |  |  |  |  |  |
| Q7 | pass | 2.0 | 1 | 33 |  |  |  |  |  |
| Q8 | pass | 11.7 | 2 | 96 | 4.4 | 6.3 | 6.9 | 0.33 | 1.09 |
| Q9 | pass | 9.9 | 1 | 96 | 7.3 | 8.7 | 9.5 | 0.37 | 1.64 |
| Q10 | pass | 3.7 | 1 | 96 | 2.6 | 2.9 | 3.2 | 0.27 |  |
| Q11 | pass | 3.4 | 1 | 96 | 2.1 | 2.4 | 2.9 | 0.28 |  |
| Q12 | pass | 5.1 | 1 | 96 | 2.8 | 4.1 | 4.7 | 0.37 | 0.57 |
| Q13 | pass | 8.3 | 1 | 96 | 3.6 | 7.2 | 7.9 | 0.64 | 1.05 |
| Q14 | pass | 7.7 | 1 | 96 | 2.7 | 6.7 | 7.3 | 0.39 | 0.71 |
| Q15 | pass | 9.0 | 1 | 96 | 6.7 | 8.1 | 8.6 | 0.27 | 0.78 |
| Q16 | pass | 7.9 | 1 | 96 | 4.5 | 7.1 | 7.6 | 0.64 | 1.68 |
| Q19 | pass | 2.2 | 1 | 33 |  |  |  |  |  |
| Q20 | pass | 5.3 | 1 | 33 |  |  |  |  |  |
| Q24 | pass | 9.3 | 1 | 33 |  |  |  |  |  |
| Q25 | pass | 3.3 | 1 | 33 |  |  |  |  |  |
| Q26 | pass | 2.4 | 1 | 33 |  |  |  |  |  |
| Q27 | pass | 3.0 | 1 | 33 |  |  |  |  |  |
| Q29 | pass | 1.9 | 1 | 33 |  |  |  |  |  |
| Q30 | pass | 7.4 | 1 | 96 | 6.3 | 6.5 | 7.0 | 0.63 |  |
| Q31 | pass, verified | 8.0 | 1 | 96 | 5.3 | 7.1 | 7.6 | 1.47 | 0.96 |
| Q32 | pass | 20.1 | 1 | 96 | 12.2 | 18.6 | 19.2 | 1.10 | 7.30 |
| Q33 | pass | 14.2 | 1 | 96 | 10.0 | 12.6 | 13.1 | 2.65 | 4.30 |
| Q36 | **fail** |  | 3 |  | 3.1 |  |  | 2.48 |  |
| Q37 | **fail** |  | 3 |  | 3.2 |  |  | 2.37 |  |
| Q38 | **fail** |  | 3 |  | 2.9 | 3.0 |  | 2.61 |  |
| Q40 | pass | 4.0 | 1 | 96 | 3.0 | 3.3 | 3.6 | 0.81 |  |
| Q41 | pass | 3.9 | 1 | 96 | 2.2 | 3.1 | 3.3 | 0.57 |  |
| S1 | pass | 4.6 | 1 | 33 |  |  |  |  |  |
| S2 | pass | 10.2 | 1 | 96 | 7.8 | 9.2 | 9.8 | 2.65 | 0.77 |
| S3 | pass | 3.0 | 1 | 96 | 1.1 | 1.9 | 2.2 | 0.00 |  |
| S4 | pass | 3.6 | 1 | 96 | 1.9 | 2.5 | 3.1 | 0.37 |  |
| S5 | pass | 2.6 | 1 | 33 |  |  |  |  |  |
| S6 | pass | 2.7 | 1 | 33 |  |  |  |  |  |
| S7 | pass, export 0.50 GB | 6.7 | 1 | 96 | 2.9 | 4.2 | 5.7 | 0.38 | 0.59 |
| S8 | pass | 4.4 | 1 | 33 |  |  |  |  |  |
| S9 | pass | 8.4 | 1 | 33 |  |  |  |  |  |
| S10 | pass, export 1.37 GB | 11.8 | 1 | 96 | 6.7 | 9.4 | 10.4 | 2.43 | 1.79 |
| S11 | pass | 6.9 | 1 | 33 |  |  |  |  |  |

## Failures

- **Q36**: Error: job k-3c62c365197d4b6db7ce41096e5c3b4d failed: failed after 3 attempts: Network connection lost.
- **Q37**: Error: job k-ca8f674da782753a07634d0acf4b6b98 failed: failed after 3 attempts: stream pool failed
- **Q38**: Error: Q38 500 {"jobId":"k-509a378c133fb8e4bb86650cfab7cf3d","error":"stream pool failed"}

Cause, found after the run: in every failing job, Cloudflare had placed two reducer Durable Objects in one isolate, and the same pairs recur across jobs (slots 46 and 86, 67 and 84, 33 and 44). The two reducers then share one 128 MB isolate, and it is reset when they exceed it together. Jobs with one reducer per isolate passed. The same queries passed when rerun in a period without this sharing.

## Slowest ten

| Query | Wall (s) | SQL |
|---|---:|---|
| Q32 | 20.1 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` |
| Q33 | 14.2 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10` |
| S10 | 11.8 | `SELECT Title, COUNT(*) FROM hits WHERE Title <> '' GROUP BY Title` |
| Q8 | 11.7 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` |
| S2 | 10.2 | `SELECT COUNT(DISTINCT URL) FROM hits` |
| Q9 | 9.9 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC L` |
| Q24 | 9.3 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` |
| Q15 | 9.0 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` |
| D5 | 8.6 | `SELECT RegionID, OS, UserAgent, IsMobile, COUNT(*) AS c FROM hits GROUP BY RegionID, OS, UserAgent, IsMobile ORDER BY c DESC LIMIT 10` |
| S9 | 8.4 | `SELECT COUNT(*) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''` |
