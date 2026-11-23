# Athena real run: ClickBench, single-file hits

- Date: 2026-11-22
- Region: eu-central-1 (same region as the public bucket `clickhouse-public-datasets`)
- Engine: Athena engine version 3, workgroup `primary` (no workgroup changes made)
- Data: `s3://clickhouse-public-datasets/hits_compatible/athena/` (one 13.8 GiB `hits.parquet`)
- Queries: ClickBench `athena/queries.sql` (43 queries), each run once, sequentially, result reuse disabled
- Source: https://github.com/ClickHouse/ClickBench/tree/main/athena
- Raw `get-query-execution` JSON per query: `data/athena-run/qNN.json`

## Cost model
Athena SQL pricing is $5 per TB scanned, rounded up to the nearest megabyte, with a 10 MB minimum per query
(https://aws.amazon.com/athena/pricing/). Cost per query = max(bytes, 10 MB) rounded up to whole MB x $5/TB.
Decimal units are assumed (1 MB = 10^6 B, 1 TB = 10^12 B); with binary units the figure is within about 10%.
Q0 (`COUNT(*)`) scanned 0 bytes, so it is billed at the 10 MB minimum.

## Created in AWS (account 358473271133, eu-central-1)
1. S3 bucket `wcol-athena-results-358473271133-eu-central-1` (all public access blocked, no lifecycle rule). Query results go under `results/`.
2. Glue database `clickbench`.
3. Glue/Athena external table `clickbench.hits` (DDL below). It points at the public bucket, so no data was copied.
No existing eu-central-1 results location existed (the `primary` workgroup there has none), so a new bucket was made. Nothing else was created or modified.

## Table definition
ClickBench `athena/create.sql` with `{LOCATION}` = `s3://clickhouse-public-datasets/hits_compatible/athena/`
(105 columns, ParquetHiveSerDe, `CREATE EXTERNAL TABLE IF NOT EXISTS hits`). Full text:
https://github.com/ClickHouse/ClickBench/blob/main/athena/create.sql

## Results
| Q | SQL (first 80 chars) | State | Engine ms | Total ms | Queue ms | Bytes scanned | Cost |
|---|---|---|---|---|---|---|---|
| Q0 | `SELECT COUNT(*) FROM hits;` | SUCCEEDED | 1192 | 1357 | 58 | 0 | $0.000050 |
| Q1 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` | SUCCEEDED | 2286 | 2445 | 96 | 948546 | $0.000050 |
| Q2 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` | SUCCEEDED | 2269 | 2438 | 90 | 42013223 | $0.000215 |
| Q3 | `SELECT AVG(UserID) FROM hits;` | SUCCEEDED | 2004 | 2316 | 60 | 270116234 | $0.001355 |
| Q4 | `SELECT COUNT(DISTINCT UserID) FROM hits;` | SUCCEEDED | 1510 | 1702 | 91 | 270116234 | $0.001355 |
| Q5 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` | SUCCEEDED | 1856 | 2041 | 100 | 373073047 | $0.001870 |
| Q6 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` | SUCCEEDED | 3332 | 3512 | 57 | 80046 | $0.000050 |
| Q7 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngine` | SUCCEEDED | 2444 | 2615 | 90 | 948546 | $0.000050 |
| Q8 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER B` | SUCCEEDED | 2280 | 2449 | 101 | 328595492 | $0.001645 |
| Q9 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DI` | SUCCEEDED | 2519 | 2720 | 92 | 370608715 | $0.001855 |
| Q10 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhone` | SUCCEEDED | 1228 | 1429 | 92 | 275036481 | $0.001380 |
| Q11 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHER` | SUCCEEDED | 1568 | 1710 | 67 | 281232491 | $0.001410 |
| Q12 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY S` | SUCCEEDED | 2112 | 2253 | 65 | 395143226 | $0.001980 |
| Q13 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <>` | SUCCEEDED | 3764 | 3970 | 94 | 665259460 | $0.003330 |
| Q14 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase ` | SUCCEEDED | 2603 | 2744 | 74 | 413864771 | $0.002070 |
| Q15 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 1` | SUCCEEDED | 2188 | 2401 | 91 | 270116234 | $0.001355 |
| Q16 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase OR` | SUCCEEDED | 4036 | 4189 | 84 | 643189281 | $0.003220 |
| Q17 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LI` | SUCCEEDED | 3575 | 3914 | 101 | 643189281 | $0.003220 |
| Q18 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM ` | SUCCEEDED | 3176 | 3346 | 93 | 1046317546 | $0.005235 |
| Q19 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` | SUCCEEDED | 3028 | 3327 | 101 | 175258240 | $0.000880 |
| Q20 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` | SUCCEEDED | 3051 | 3252 | 103 | 2652579786 | $0.013265 |
| Q21 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%'` | SUCCEEDED | 3034 | 3175 | 67 | 3047723012 | $0.015240 |
| Q22 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)` | SUCCEEDED | 4593 | 4920 | 93 | 5172526607 | $0.025865 |
| Q23 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` | SUCCEEDED | 4924 | 5153 | 91 | 14443612459 | $0.072220 |
| Q24 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT ` | SUCCEEDED | 2024 | 2201 | 92 | 798271491 | $0.003995 |
| Q25 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIM` | SUCCEEDED | 1596 | 1777 | 96 | 395143226 | $0.001980 |
| Q26 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, Searc` | SUCCEEDED | 2327 | 2509 | 104 | 798271491 | $0.003995 |
| Q27 | `SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> ''` | SUCCEEDED | 4359 | 4548 | 92 | 2654742384 | $0.013275 |
| Q28 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AV` | SUCCEEDED | 3869 | 4023 | 90 | 2248595050 | $0.011245 |
| Q29 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2),` | SUCCEEDED | 8429 | 8626 | 95 | 41070721 | $0.000210 |
| Q30 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWi` | SUCCEEDED | 3500 | 3650 | 89 | 647475281 | $0.003240 |
| Q31 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR` | SUCCEEDED | 2998 | 3152 | 96 | 1491864909 | $0.007460 |
| Q32 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR` | SUCCEEDED | 6014 | 6186 | 97 | 1096721683 | $0.005485 |
| Q33 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` | SUCCEEDED | 3670 | 3860 | 98 | 2652579786 | $0.013265 |
| Q34 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` | SUCCEEDED | 2851 | 3072 | 85 | 2652579786 | $0.013265 |
| Q35 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hi` | SUCCEEDED | 3051 | 3206 | 93 | 187501764 | $0.000940 |
| Q36 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >` | SUCCEEDED | 3271 | 3421 | 95 | 46491935 | $0.000235 |
| Q37 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate` | SUCCEEDED | 1357 | 1505 | 59 | 17910064 | $0.000090 |
| Q38 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >` | SUCCEEDED | 1270 | 1438 | 84 | 46486756 | $0.000235 |
| Q39 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = ` | SUCCEEDED | 2280 | 2454 | 89 | 91499157 | $0.000460 |
| Q40 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 ` | SUCCEEDED | 1139 | 1305 | 78 | 15652340 | $0.000080 |
| Q41 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WH` | SUCCEEDED | 1217 | 1392 | 89 | 9848651 | $0.000050 |
| Q42 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHE` | SUCCEEDED | 1197 | 1379 | 57 | 4779537 | $0.000050 |

## Totals
- Sum engine time: 120991 ms (121.0 s)
- Sum total execution time: 129082 ms (129.1 s)
- Total bytes scanned: 47679034970 (47.68 GB)
- Total cost: $0.2387
- Failed queries: none

## Cleanup
```
aws s3 rb s3://wcol-athena-results-358473271133-eu-central-1 --force --region eu-central-1
aws glue delete-database --name clickbench --region eu-central-1   # also removes the hits table
```
