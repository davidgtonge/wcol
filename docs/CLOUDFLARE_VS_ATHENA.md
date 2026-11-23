# ClickBench: Cloudflare (WCOL) vs Amazon Athena

This compares our single ClickBench run on Cloudflare Workers and Durable Objects (see [CLOUDFLARE_LADDER_TIMINGS.md](CLOUDFLARE_LADDER_TIMINGS.md)) with Amazon Athena's published ClickBench results.

**Sources**

- Athena, single Parquet file, 2026-11-16 (primary): [athena/results/20261116/serverless.json](https://raw.githubusercontent.com/ClickHouse/ClickBench/main/athena/results/20261116/serverless.json)
- Athena, partitioned (100 files), 2026-11-16: [athena-partitioned/results/20261116/serverless.json](https://raw.githubusercontent.com/ClickHouse/ClickBench/main/athena-partitioned/results/20261116/serverless.json)
- Athena, single file, 2022-07-01: [athena/results/20220701/serverless.json](https://raw.githubusercontent.com/ClickHouse/ClickBench/main/athena/results/20220701/serverless.json)
- Athena, partitioned, 2022-07-01: [athena-partitioned/results/20220701/serverless.json](https://raw.githubusercontent.com/ClickHouse/ClickBench/main/athena-partitioned/results/20220701/serverless.json)
- Query text: [athena/queries.sql](https://github.com/ClickHouse/ClickBench/blob/main/athena/queries.sql) in ClickHouse/ClickBench

All four Athena files are `"machine": "serverless"`. The 2026 files record "Athena engine version 3". The JSON `"result"` arrays are `[run1, run2, run3]` per query, in order Q0..Q42. As ClickBench defines them, Athena cold = run1 and Athena hot = min(run2, run3).

**Our run:** 2026-11-22, build `7e2966b` plus uncommitted edge fixes. Full `hits.parquet` (100M rows, 14.8 GB) in R2, fixed pool of 32 scans and 64 reducers. Ours = the "Wall (s)" column of the ladder timings.

**Caveats**

- Our time is one run per query. There is no warm/cold distinction. Q8 needed a retry, and its time includes the failed attempt.
- Our wall time is submit to answer through an HTTP API, including polling. Athena's time is `TotalExecutionTimeInMillis` from ClickBench's harness, which excludes client polling and start-up.
- The layouts differ. We read one 14.8 GB Parquet file from R2 on Cloudflare. Athena reads the ClickBench S3 dataset, either as one ~14 GB file or as 100 files.
- Hardware and cost models are not comparable: Athena is a managed, per-byte-scanned service, and our figures come from Workers and Durable Objects.

**Query numbering.** ClickBench's `athena/queries.sql` numbers 43 queries, 0 to 42. I compared all 43 against our `scripts/clickbench-queries.tsv` by SQL text. Q0 to Q16, Q19, Q20, Q29 to Q33, Q35, Q36 to Q38 and Q40 to Q41 match. Q18, Q40, Q41 and Q42 differ only in how the SQL is written (`toDateTime` wrapper, or `LIMIT`/`OFFSET` order), not in what it computes. Two naming differences matter:

- Our `Q24`, `Q25`, `Q26` and `Q27` are ClickBench Q23, Q24, Q25 and Q26. This table uses ClickBench numbers, so our Q24 time is shown on row Q23, and so on.
- Our TSV has no query for ClickBench Q17, Q21, Q22, Q27, Q28, Q34 or Q39, so those rows are "not run". Q18, Q35 and Q42 are also "not run", as we do not support them.

Failed in our run: Q36, Q37, Q38 (see the Failures section of the ladder doc).

## Per-query comparison

Ratios use Athena hot. Under 1.0× means we are faster. Athena values are shown to two decimals.

| Query | Ours (s) | Athena cold (s) | Athena hot (s) | Ours ÷ Athena hot |
|---|---:|---:|---:|---:|
| Q0 | 1.6 | 1.80 | 1.16 | 1.4× |
| Q1 | 2.0 | 1.55 | 2.22 | 0.9× |
| Q2 | 2.1 | 2.63 | 1.64 | 1.3× |
| Q3 | 2.9 | 1.54 | 1.37 | 2.1× |
| Q4 | 7.3 | 1.90 | 1.98 | 3.7× |
| Q5 | 4.6 | 2.31 | 2.01 | 2.3× |
| Q6 | 1.7 | 2.48 | 1.57 | 1.1× |
| Q7 | 2.0 | 2.81 | 1.40 | 1.4× |
| Q8 | 11.7 | 2.40 | 1.89 | 6.2× |
| Q9 | 9.9 | 3.33 | 3.47 | 2.9× |
| Q10 | 3.7 | 1.85 | 1.96 | 1.9× |
| Q11 | 3.4 | 2.57 | 1.75 | 1.9× |
| Q12 | 5.1 | 2.76 | 1.91 | 2.7× |
| Q13 | 8.3 | 3.67 | 2.68 | 3.1× |
| Q14 | 7.7 | 2.67 | 2.10 | 3.7× |
| Q15 | 9.0 | 1.97 | 1.74 | 5.2× |
| Q16 | 7.9 | 4.49 | 2.44 | 3.2× |
| Q17 | not run | 2.29 | 2.57 | — |
| Q18 | not run | 2.96 | 3.28 | — |
| Q19 | 2.2 | 1.88 | 1.67 | 1.3× |
| Q20 | 5.3 | 2.46 | 1.95 | 2.7× |
| Q21 | not run | 2.35 | 2.76 | — |
| Q22 | not run | 2.53 | 3.30 | — |
| Q23 | 9.3 | 3.93 | 3.83 | 2.4× |
| Q24 | 3.3 | 2.42 | 2.00 | 1.6× |
| Q25 | 2.4 | 1.92 | 1.91 | 1.3× |
| Q26 | 3.0 | 2.24 | 1.45 | 2.1× |
| Q27 | not run | 2.57 | 2.77 | — |
| Q28 | not run | 4.51 | 4.12 | — |
| Q29 | 1.9 | 4.71 | 3.99 | 0.5× |
| Q30 | 7.4 | 3.79 | 2.05 | 3.6× |
| Q31 | 8.0 | 2.99 | 3.00 | 2.7× |
| Q32 | 20.1 | 3.04 | 2.96 | 6.8× |
| Q33 | 14.2 | 2.76 | 2.68 | 5.3× |
| Q34 | not run | 3.75 | 2.89 | — |
| Q35 | not run | 3.27 | 2.47 | — |
| Q36 | failed | 1.89 | 1.80 | — |
| Q37 | failed | 1.81 | 1.59 | — |
| Q38 | failed | 1.53 | 1.55 | — |
| Q39 | not run | 2.23 | 2.71 | — |
| Q40 | 4.0 | 1.54 | 1.98 | 2.0× |
| Q41 | 3.9 | 1.43 | 1.55 | 2.5× |
| Q42 | not run | 1.49 | 1.55 | — |

"Not run" means we have no timing for that ClickBench query (see the numbering notes above). "Failed" means the run failed.

## Totals

The comparison set is the 30 queries we completed (Q0 to Q16, Q19, Q20, Q23 to Q26, Q29 to Q33, Q40, Q41) that also have Athena results. The same 30 queries are used for every row below. Our sum is 175.9 s for these 30 queries (our full run passed 50 queries, 271 s in total).

| Athena result file | Queries | Ours sum (s) | Athena cold sum (s) | Athena hot sum (s) | Geomean of Ours ÷ Athena hot |
|---|---:|---:|---:|---:|---:|
| Single file, 2026-11-16 (primary) | 30 | 175.9 | 77.83 | 64.32 | 2.3× |
| Partitioned, 2026-11-16 | 30 | 175.9 | 62.02 | 54.90 | 2.7× |
| Single file, 2022-07-01 | 30 | 175.9 | 90.65 | 82.19 | 1.8× |
| Partitioned, 2022-07-01 | 30 | 175.9 | 114.96 | 90.42 | 1.6× |

## Observations

- We are faster than Athena's hot time on 2 of the 30 queries: Q29 (0.5×) and Q1 (0.9×).
- Between 1.1× and 1.6× on Q0, Q2, Q6, Q7, Q19, Q24 and Q25.
- Our largest ratios are Q32 (6.8×), Q8 (6.2×), Q33 (5.3×) and Q15 (5.2×). Each groups by a high-cardinality key or counts distinct values per group: Q32 by `WatchID, ClientIP`, Q8 counts distinct `UserID` per `RegionID`, Q33 groups by `URL`, and Q15 groups by `UserID`.
- Athena's cold and hot times are close to each other on these queries (see the 2026 totals). In the 2026 single-file results, the cold sum is 77.8 s and the hot sum is 64.3 s.
- Q36, Q37 and Q38 failed for us. Athena ran them in 1.6 to 1.9 s hot.

## Cost comparison (estimates)

These are estimates, not bills. Prices were checked on 2026-11-22 against the pages listed under Price sources. Free-tier allowances are ignored, as requested.

**Method and assumptions.** For each of the 30 completed queries, our cost is three terms.
- Durable Object duration = active nodes × wall time × 0.125 GB × $12.50 per million GB-s. This is an upper bound, because not every node runs for the whole wall time. Active nodes are the "Nodes" column (33 or 96). Q0 has no node count, so it uses 33. The coordinator is not counted separately; it adds roughly 1%.
- R2 Class B GETs = bytes read ÷ 2 MiB, at $0.36 per million. Bytes read is the "Read" column, in decimal GB.
- R2 Class A PUTs for spill = spill bytes ÷ 5 MiB, at $4.50 per million. Egress is free.
- Not estimated: DO requests, Workers requests and CPU time. The ladder doesn't record request counts. At $0.15 per million DO requests, 1,000 requests per query adds about $0.00015, which matters for the cheapest queries.

Athena cost = bytes scanned ÷ 1,000 GB per TB × $5, with a 10 MB minimum. We use our "bytes read" as the scanned-bytes proxy. This is an assumption: Athena scans only the referenced columns, and our reads may differ from that. Because the scanned bytes are the same for every Athena result file, the Athena cost is the same for all four files.

**Missing data.** The ladder records no bytes read for the 13 queries that ran on 33 nodes (Q0, Q1, Q2, Q3, Q6, Q7, Q19, Q20, Q23, Q24, Q25, Q26, Q29). For these we assume a full scan of the 14.8 GB file (marked *). This is an upper bound, and it drives most of the Athena total.

**Arithmetic for one query (Q32, ClickBench Q32 = our Q32).** Nodes 96, wall 20.1 s, read 1.10 GB, spill 7.30 GB.
- DO: 96 × 20.1 × 0.125 = 241.2 GB-s, × $12.50 / 10^6 = $0.00302.
- R2 GETs: 1.10 GB ÷ 2 MiB ≈ 525, × $0.36 / 10^6 = $0.00019.
- Spill PUTs: 7.30 GB ÷ 5 MiB ≈ 1,393, × $4.50 / 10^6 = $0.00627.
- Ours total ≈ $0.00947. Athena: 1.10 GB ÷ 1,000 × $5 = $0.00550. Ratio ≈ 1.7.

| Query | Ours est. ($) | Athena est. ($) | Ours ÷ Athena |
|---|---:|---:|---:|
| Q0* | 0.00262 | 0.07400 | 0.04 |
| Q1* | 0.00264 | 0.07400 | 0.04 |
| Q2* | 0.00265 | 0.07400 | 0.04 |
| Q3* | 0.00269 | 0.07400 | 0.04 |
| Q4 | 0.00178 | 0.00135 | 1.3 |
| Q5 | 0.00075 | 0.00185 | 0.4 |
| Q6* | 0.00263 | 0.07400 | 0.04 |
| Q7* | 0.00264 | 0.07400 | 0.04 |
| Q8 | 0.00275 | 0.00165 | 1.7 |
| Q9 | 0.00296 | 0.00185 | 1.6 |
| Q10 | 0.00060 | 0.00135 | 0.4 |
| Q11 | 0.00056 | 0.00140 | 0.4 |
| Q12 | 0.00132 | 0.00185 | 0.7 |
| Q13 | 0.00226 | 0.00320 | 0.7 |
| Q14 | 0.00183 | 0.00195 | 0.9 |
| Q15 | 0.00207 | 0.00135 | 1.5 |
| Q16 | 0.00274 | 0.00320 | 0.9 |
| Q19* | 0.00265 | 0.07400 | 0.04 |
| Q20* | 0.00281 | 0.07400 | 0.04 |
| Q23* (our Q24) | 0.00302 | 0.07400 | 0.04 |
| Q24* (our Q25) | 0.00271 | 0.07400 | 0.04 |
| Q25* (our Q26) | 0.00267 | 0.07400 | 0.04 |
| Q26* (our Q27) | 0.00270 | 0.07400 | 0.04 |
| Q29* | 0.00264 | 0.07400 | 0.04 |
| Q30 | 0.00122 | 0.00315 | 0.4 |
| Q31 | 0.00228 | 0.00735 | 0.3 |
| Q32 | 0.00947 | 0.00550 | 1.7 |
| Q33 | 0.00628 | 0.01325 | 0.5 |
| Q40 | 0.00074 | 0.00405 | 0.2 |
| Q41 | 0.00068 | 0.00285 | 0.2 |

\* No bytes read recorded; full 14.8 GB file assumed.

**Totals (30 queries both systems completed).**
- All 30: ours ≈ $0.0754 (DO $0.0225, GETs $0.0350, PUTs $0.0179). Athena ≈ $1.019. Ratio ≈ 0.07.
- 17 queries with recorded bytes read (no *): ours ≈ $0.0403, Athena ≈ $0.0572. Ratio ≈ 0.7.

**What dominates.** On our side, for the 17 queries with recorded reads, DO duration is about half the cost and spill PUTs about 45%; R2 reads are about 5%. Q32 and Q33 are the largest, because their reducers spill 7.3 GB and 4.3 GB. On Athena's side, cost is set entirely by bytes scanned at $5/TB. At these scan sizes the per-query minimum is not reached except for the 10 MB floor on the smallest scans, so the cost tracks bytes read.

**Main assumption.** The full-file assumption for the 13 queries with no recorded reads drives the all-30 totals. Without it, the ratio falls to about 0.7 (the recorded-only figure). Recording bytes read for the 33-node queries would settle this.

**Price sources (checked 2026-11-22).**
- Athena: [AWS Athena pricing](https://aws.amazon.com/athena/pricing/) gives a $5/TB example. The 10 MB minimum appears there only for federated queries. The [AWS Big Data Analytics Options whitepaper](https://docs.aws.amazon.com/whitepapers/latest/big-data-analytics-options/amazon-athena.html) states $5/TB scanned with rounding to the nearest MB and a 10 MB minimum per query. We used the whitepaper figure, which is older, so confirm the minimum before relying on it.
- Durable Objects: [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/). Duration $12.50 per million GB-s after 400,000 included. Requests $0.15 per million after 1 million included. Billed on 128 MB per active object. The page states 0.128 GB-s per second; we used the 0.125 GB from the brief, which is 2.4% lower.
- R2: [R2 pricing](https://developers.cloudflare.com/r2/pricing/). Class A $4.50 per million, Class B $0.36 per million, egress free.
- Workers: [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/). Requests $0.30 per million after 10 million included. CPU $0.02 per million ms after 30 million included. Not used in the estimates, since no request or CPU counts were recorded.
