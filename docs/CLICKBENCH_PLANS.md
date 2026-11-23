# ClickBench plans

Each supported query from `scripts/clickbench-queries.tsv` is shown three times:

1. **DataFusion** — the optimized logical plan.
2. **WCOL logical** — the storage-free plan (`lower_logical`). No byte offsets and no node ids.
3. **WCOL physical** — `lower_to_physical` over the cached footer of `data/hits_subset_500k.parquet`.

Regenerate with `cargo run -p wcol-sql --features native --bin dump-clickbench-plans -- --out docs/CLICKBENCH_PLANS.md` from `rust/`.

The file has 5 row groups and 105 columns. Physical planning uses two nodes (`0` and `1`), a max gap of 128 KiB, and a max request of 2 MiB. Dictionary pages that the footer folds into the column-chunk span are marked on the logical read. The planner does not read column bytes.

43 queries produce all three plans. 9 queries stop earlier; the reason is at the bottom.

Numbers in a WCOL plan are footer column indexes.

| Index | Column |
| --- | --- |
| 0 | WatchID |
| 2 | Title |
| 5 | EventDate |
| 6 | CounterID |
| 7 | ClientIP |
| 8 | RegionID |
| 9 | UserID |
| 11 | OS |
| 12 | UserAgent |
| 13 | URL |
| 15 | IsRefresh |
| 20 | ResolutionWidth |
| 32 | IsMobile |
| 33 | MobilePhone |
| 34 | MobilePhoneModel |
| 37 | TraficSourceID |
| 38 | SearchEngineID |
| 39 | SearchPhrase |
| 40 | AdvEngineID |
| 42 | WindowClientWidth |
| 43 | WindowClientHeight |
| 52 | IsLink |
| 53 | IsDownload |
| 61 | DontCountHits |
| 103 | URLHash |

## Q0

```sql
SELECT COUNT(*) FROM hits
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Scan(hits, [])
```

### WCOL physical

```text
root 0
op 0 node=coordinator metadata_count rows=500000 inputs=[]
metric requests=0 logical=0 fetched=0 over_read=0 over_read_ppm=0
```

## Q1

```sql
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.AdvEngineID != Int32(0)]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(40 ne i64:0)
    Scan(hits, [40])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11807076 length=6374 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26425735 length=6397 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=41101917 length=6312 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55582619 length=5307 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59209079 length=498 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=40 offset=11807076 length=6374 dictionary=1 codec=1 fetch=0 pieces=[f0@0+6374]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=40 offset=26425735 length=6397 dictionary=1 codec=1 fetch=1 pieces=[f1@0+6397]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=40 offset=41101917 length=6312 dictionary=1 codec=1 fetch=2 pieces=[f2@0+6312]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=40 offset=55582619 length=5307 dictionary=1 codec=1 fetch=3 pieces=[f3@0+5307]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=40 offset=59209079 length=498 dictionary=1 codec=1 fetch=4 pieces=[f4@0+498]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[40] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[40] reads=[1]] inputs=[0]
op 2 node=0 filter 40 ne i64:0 inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[40] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[40] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[40] reads=[4]] inputs=[4]
op 6 node=1 filter 40 ne i64:0 inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=12771 fetched=12771 requests=2
metric node=1 logical=12117 fetched=12117 requests=3
metric requests=5 logical=24888 fetched=24888 over_read=0 over_read_ppm=0
```

## Q2

```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[sum(CAST(hits.AdvEngineID AS Int64)), count(*), avg(CAST(hits.ResolutionWidth AS Float64))]]
  TableScan: hits projection=[ResolutionWidth, AdvEngineID]
```

### WCOL logical

```text
Aggregate(group=[], sum(40), count(*), avg(20))
  Scan(hits, [20, 40])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11361203 length=22873 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11807076 length=6374 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=26008527 length=22033 reads=[2]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26425735 length=6397 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=40676712 length=22069 reads=[4]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=41101917 length=6312 reads=[5]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=55152912 length=21604 reads=[6]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55582619 length=5307 reads=[7]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=59175644 length=33933 reads=[8, 9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=0 pieces=[f0@0+22873]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=40 offset=11807076 length=6374 dictionary=1 codec=1 fetch=1 pieces=[f1@0+6374]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=2 pieces=[f2@0+22033]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=40 offset=26425735 length=6397 dictionary=1 codec=1 fetch=3 pieces=[f3@0+6397]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=4 pieces=[f4@0+22069]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=40 offset=41101917 length=6312 dictionary=1 codec=1 fetch=5 pieces=[f5@0+6312]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=6 pieces=[f6@0+21604]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=40 offset=55582619 length=5307 dictionary=1 codec=1 fetch=7 pieces=[f7@0+5307]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=8 pieces=[f8@0+1843]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=40 offset=59209079 length=498 dictionary=1 codec=1 fetch=8 pieces=[f8@0+498]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[20, 40] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[20, 40] reads=[2, 3]] inputs=[0]
op 2 node=0 partial_aggregate group=[] sum(40), count(*), avg(20) inputs=[1]
op 3 node=1 r2_fetch [4, 5, 6, 7, 8] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[20, 40] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[20, 40] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[20, 40] reads=[8, 9]] inputs=[3]
op 5 node=1 partial_aggregate group=[] sum(40), count(*), avg(20) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] sum(40), count(*), avg(20) inputs=[6]
metric node=0 logical=57677 fetched=57677 requests=4
metric node=1 logical=57633 fetched=89225 requests=5
metric requests=9 logical=115310 fetched=146902 over_read=31592 over_read_ppm=215054
```

## Q3

```sql
SELECT AVG(UserID) FROM hits
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[avg(CAST(hits.UserID AS Float64))]]
  TableScan: hits projection=[UserID]
```

### WCOL logical

```text
Aggregate(group=[], avg(9))
  Scan(hits, [9])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] avg(9) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] avg(9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] avg(9) inputs=[6]
metric node=0 logical=248997 fetched=248997 requests=2
metric node=1 logical=251415 fetched=251415 requests=3
metric requests=5 logical=500412 fetched=500412 over_read=0 over_read_ppm=0
```

## Q4

```sql
SELECT COUNT(DISTINCT UserID) FROM hits
```

### DataFusion

```text
Projection: count(alias1) AS count(DISTINCT hits.UserID)
  Aggregate: groupBy=[[]], aggr=[[count(alias1)]]
    Aggregate: groupBy=[[hits.UserID AS alias1]], aggr=[[]]
      TableScan: hits projection=[UserID]
```

### WCOL logical

```text
Aggregate(group=[], count(distinct 9))
  Scan(hits, [9])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] count(distinct 9) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] count(distinct 9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] count(distinct 9) inputs=[6]
metric node=0 logical=248997 fetched=248997 requests=2
metric node=1 logical=251415 fetched=251415 requests=3
metric requests=5 logical=500412 fetched=500412 over_read=0 over_read_ppm=0
```

## Q5

```sql
SELECT COUNT(DISTINCT SearchPhrase) FROM hits
```

### DataFusion

```text
Projection: count(alias1) AS count(DISTINCT hits.SearchPhrase)
  Aggregate: groupBy=[[]], aggr=[[count(alias1)]]
    Aggregate: groupBy=[[hits.SearchPhrase AS alias1]], aggr=[[]]
      TableScan: hits projection=[SearchPhrase]
```

### WCOL logical

```text
Aggregate(group=[], count(distinct 39))
  Scan(hits, [39])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[39] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[39] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] count(distinct 39) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[39] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[39] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[39] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] count(distinct 39) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] count(distinct 39) inputs=[6]
metric node=0 logical=360161 fetched=360161 requests=2
metric node=1 logical=385709 fetched=385709 requests=3
metric requests=5 logical=745870 fetched=745870 over_read=0 over_read_ppm=0
```

## Q10

```sql
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10
```

### DataFusion

```text
Sort: u DESC NULLS LAST, fetch=10
  Projection: hits.MobilePhoneModel, count(alias1) AS u
    Aggregate: groupBy=[[hits.MobilePhoneModel]], aggr=[[count(alias1)]]
      Aggregate: groupBy=[[hits.MobilePhoneModel, hits.UserID AS alias1]], aggr=[[]]
        TableScan: hits projection=[UserID, MobilePhoneModel], full_filters=[hits.MobilePhoneModel != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[34], count(distinct 9))
      Filter(34 ne text:"")
        Scan(hits, [9, 34])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11514812 length=1362 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[2]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26152069 length=1106 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[4]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40821507 length=1293 reads=[5]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[6]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55292227 length=1557 reads=[7]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[8]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59187227 length=146 reads=[9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=34 offset=11514812 length=1362 dictionary=1 codec=1 fetch=1 pieces=[f1@0+1362]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122222]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=34 offset=26152069 length=1106 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1106]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=4 pieces=[f4@0+122870]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=34 offset=40821507 length=1293 dictionary=1 codec=1 fetch=5 pieces=[f5@0+1293]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=6 pieces=[f6@0+121199]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=34 offset=55292227 length=1557 dictionary=1 codec=1 fetch=7 pieces=[f7@0+1557]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=8 pieces=[f8@0+7346]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=34 offset=59187227 length=146 dictionary=1 codec=1 fetch=9 pieces=[f9@0+146]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9, 34] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9, 34] reads=[2, 3]] inputs=[0]
op 2 node=0 filter 34 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[34] count(distinct 9) inputs=[2]
op 4 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9, 34] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9, 34] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9, 34] reads=[8, 9]] inputs=[4]
op 6 node=1 filter 34 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[34] count(distinct 9) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[34] count(distinct 9) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=251465 fetched=251465 requests=4
metric node=1 logical=254411 fetched=254411 requests=6
metric requests=10 logical=505876 fetched=505876 over_read=0 over_read_ppm=0
```

## Q11

```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10
```

### DataFusion

```text
Sort: u DESC NULLS LAST, fetch=10
  Projection: hits.MobilePhone, hits.MobilePhoneModel, count(alias1) AS u
    Aggregate: groupBy=[[hits.MobilePhone, hits.MobilePhoneModel]], aggr=[[count(alias1)]]
      Aggregate: groupBy=[[hits.MobilePhone, hits.MobilePhoneModel, hits.UserID AS alias1]], aggr=[[]]
        TableScan: hits projection=[UserID, MobilePhone, MobilePhoneModel], full_filters=[hits.MobilePhoneModel != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[33, 34], count(distinct 9))
      Filter(34 ne text:"")
        Scan(hits, [9, 33, 34])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11512806 length=3368 reads=[1, 2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26150337 length=2838 reads=[4, 5]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[6]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40819649 length=3151 reads=[7, 8]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[9]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55290282 length=3502 reads=[10, 11]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[12]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59187005 length=368 reads=[13, 14]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=33 offset=11512806 length=2006 dictionary=1 codec=1 fetch=1 pieces=[f1@0+2006]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=34 offset=11514812 length=1362 dictionary=1 codec=1 fetch=1 pieces=[f1@0+1362]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122222]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=33 offset=26150337 length=1732 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1732]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=34 offset=26152069 length=1106 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1106]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=4 pieces=[f4@0+122870]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=33 offset=40819649 length=1858 dictionary=1 codec=1 fetch=5 pieces=[f5@0+1858]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=34 offset=40821507 length=1293 dictionary=1 codec=1 fetch=5 pieces=[f5@0+1293]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=6 pieces=[f6@0+121199]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=33 offset=55290282 length=1945 dictionary=1 codec=1 fetch=7 pieces=[f7@0+1945]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=34 offset=55292227 length=1557 dictionary=1 codec=1 fetch=7 pieces=[f7@0+1557]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=8 pieces=[f8@0+7346]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=33 offset=59187005 length=222 dictionary=1 codec=1 fetch=9 pieces=[f9@0+222]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=34 offset=59187227 length=146 dictionary=1 codec=1 fetch=9 pieces=[f9@0+146]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9, 33, 34] reads=[0, 1, 2]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9, 33, 34] reads=[3, 4, 5]] inputs=[0]
op 2 node=0 filter 34 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[33, 34] count(distinct 9) inputs=[2]
op 4 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9, 33, 34] reads=[6, 7, 8]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9, 33, 34] reads=[9, 10, 11]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9, 33, 34] reads=[12, 13, 14]] inputs=[4]
op 6 node=1 filter 34 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[33, 34] count(distinct 9) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[33, 34] count(distinct 9) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=255203 fetched=255203 requests=4
metric node=1 logical=258436 fetched=258436 requests=6
metric requests=10 logical=513639 fetched=513639 over_read=0 over_read_ppm=0
```

## Q12

```sql
SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.SearchPhrase, count(*) AS c
    Aggregate: groupBy=[[hits.SearchPhrase]], aggr=[[count(*)]]
      TableScan: hits projection=[SearchPhrase], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[39], count(*))
      Filter(39 ne text:"")
        Scan(hits, [39])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[39] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[39] reads=[1]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[39] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[39] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[39] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[39] reads=[4]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[39] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[39] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=360161 fetched=360161 requests=2
metric node=1 logical=385709 fetched=385709 requests=3
metric requests=5 logical=745870 fetched=745870 over_read=0 over_read_ppm=0
```

## Q13

```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10
```

### DataFusion

```text
Sort: u DESC NULLS LAST, fetch=10
  Projection: hits.SearchPhrase, count(alias1) AS u
    Aggregate: groupBy=[[hits.SearchPhrase]], aggr=[[count(alias1)]]
      Aggregate: groupBy=[[hits.SearchPhrase, hits.UserID AS alias1]], aggr=[[]]
        TableScan: hits projection=[UserID, SearchPhrase], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[39], count(distinct 9))
      Filter(39 ne text:"")
        Scan(hits, [9, 39])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[2]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[4]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[5]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[6]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[7]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[8]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=1 pieces=[f1@0+187940]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122222]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=3 pieces=[f3@0+172221]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=4 pieces=[f4@0+122870]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=5 pieces=[f5@0+179670]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=6 pieces=[f6@0+121199]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=7 pieces=[f7@0+191875]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=8 pieces=[f8@0+7346]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=9 pieces=[f9@0+14164]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9, 39] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9, 39] reads=[2, 3]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[39] count(distinct 9) inputs=[2]
op 4 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9, 39] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9, 39] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9, 39] reads=[8, 9]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[39] count(distinct 9) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[39] count(distinct 9) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=609158 fetched=609158 requests=4
metric node=1 logical=637124 fetched=637124 requests=6
metric requests=10 logical=1246282 fetched=1246282 over_read=0 over_read_ppm=0
```

## Q14

```sql
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.SearchEngineID, hits.SearchPhrase, count(*) AS c
    Aggregate: groupBy=[[hits.SearchEngineID, hits.SearchPhrase]], aggr=[[count(*)]]
      TableScan: hits projection=[SearchEngineID, SearchPhrase], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[38, 39], count(*))
      Filter(39 ne text:"")
        Scan(hits, [38, 39])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11598610 length=208466 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26232726 length=193009 reads=[2, 3]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40901820 length=200097 reads=[4, 5]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55370906 length=211713 reads=[6, 7]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59193573 length=15506 reads=[8, 9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=38 offset=11598610 length=20526 dictionary=1 codec=1 fetch=0 pieces=[f0@0+20526]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=38 offset=26232726 length=20788 dictionary=1 codec=1 fetch=1 pieces=[f1@0+20788]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=38 offset=40901820 length=20427 dictionary=1 codec=1 fetch=2 pieces=[f2@0+20427]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=38 offset=55370906 length=19838 dictionary=1 codec=1 fetch=3 pieces=[f3@0+19838]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=38 offset=59193573 length=1342 dictionary=1 codec=1 fetch=4 pieces=[f4@0+1342]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[38, 39] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[38, 39] reads=[2, 3]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[38, 39] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[38, 39] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[38, 39] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[38, 39] reads=[8, 9]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[38, 39] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[38, 39] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=401475 fetched=401475 requests=2
metric node=1 logical=427316 fetched=427316 requests=3
metric requests=5 logical=828791 fetched=828791 over_read=0 over_read_ppm=0
```

## Q16

```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
```

### DataFusion

```text
Sort: count(*) DESC NULLS LAST, fetch=10
  Aggregate: groupBy=[[hits.UserID, hits.SearchPhrase]], aggr=[[count(*)]]
    TableScan: hits projection=[UserID, SearchPhrase]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[9, 39], count(*))
      Scan(hits, [9, 39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[2]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[4]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[5]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[6]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[7]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[8]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=1 pieces=[f1@0+187940]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122222]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=3 pieces=[f3@0+172221]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=4 pieces=[f4@0+122870]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=5 pieces=[f5@0+179670]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=6 pieces=[f6@0+121199]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=7 pieces=[f7@0+191875]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=8 pieces=[f8@0+7346]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=9 pieces=[f9@0+14164]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9, 39] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9, 39] reads=[2, 3]] inputs=[0]
op 2 node=0 partial_aggregate group=[9, 39] count(*) inputs=[1]
op 3 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9, 39] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9, 39] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9, 39] reads=[8, 9]] inputs=[3]
op 5 node=1 partial_aggregate group=[9, 39] count(*) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[9, 39] count(*) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=609158 fetched=609158 requests=4
metric node=1 logical=637124 fetched=637124 requests=6
metric requests=10 logical=1246282 fetched=1246282 over_read=0 over_read_ppm=0
```

## Q33

```sql
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.URL, count(*) AS c
    Aggregate: groupBy=[[hits.URL]], aggr=[[count(*)]]
      TableScan: hits projection=[URL]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[13], count(*))
      Scan(hits, [13])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58973967 length=93079 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=0 pieces=[f0@0+1858606]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1834388]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=2 pieces=[f2@0+1838352]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1826481]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=4 pieces=[f4@0+93079]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[13] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[13] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[13] count(*) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[13] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[13] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[13] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[13] count(*) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[13] count(*) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=3692994 fetched=3692994 requests=2
metric node=1 logical=3757912 fetched=3757912 requests=3
metric requests=5 logical=7450906 fetched=7450906 over_read=0 over_read_ppm=0
```

## Q36

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10
```

### DataFusion

```text
Sort: PageViews DESC NULLS LAST, fetch=10
  Projection: hits.URL, count(*) AS PageViews
    Aggregate: groupBy=[[hits.URL]], aggr=[[count(*)]]
      TableScan: hits projection=[URL], full_filters=[hits.CounterID = Int32(62), hits.EventDate >= Date32("2013-07-01"), hits.EventDate <= Date32("2013-07-31"), hits.DontCountHits = Int32(0), hits.IsRefresh = Int32(0), hits.URL != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[13], count(*))
      Filter(6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 13 ne text:"")
        Scan(hits, [5, 6, 13, 15, 61])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=118 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11267706 length=21745 reads=[3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=13029417 length=1682 reads=[4]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=21649986 length=106 reads=[5, 6]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[7]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=25920866 length=21929 reads=[8]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=27630663 length=1811 reads=[9]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=36330670 length=106 reads=[10, 11]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[12]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=40591060 length=21763 reads=[13]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=42296717 length=1930 reads=[14]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=50943729 length=106 reads=[15, 16]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[17]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=55063182 length=21700 reads=[18]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=56779083 length=2005 reads=[19]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=58956431 length=343820 reads=[20, 21, 22, 23, 24]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=6 offset=6951430 length=65 dictionary=1 codec=1 fetch=0 pieces=[f0@0+65]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1858606]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=2 pieces=[f2@0+21745]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=61 offset=13029417 length=1682 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1682]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=6 offset=21650039 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=5 pieces=[f5@0+1834388]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=6 pieces=[f6@0+21929]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=61 offset=27630663 length=1811 dictionary=1 codec=1 fetch=7 pieces=[f7@0+1811]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=8 pieces=[f8@0+53]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=6 offset=36330723 length=53 dictionary=1 codec=1 fetch=8 pieces=[f8@0+53]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=9 pieces=[f9@0+1838352]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21763]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=61 offset=42296717 length=1930 dictionary=1 codec=1 fetch=11 pieces=[f11@0+1930]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=12 pieces=[f12@0+53]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=6 offset=50943782 length=53 dictionary=1 codec=1 fetch=12 pieces=[f12@0+53]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=13 pieces=[f13@0+1826481]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=14 pieces=[f14@0+21700]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=61 offset=56779083 length=2005 dictionary=1 codec=1 fetch=15 pieces=[f15@0+2005]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=6 offset=58956484 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=16 pieces=[f16@0+93079]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=16 pieces=[f16@0+1748]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=61 offset=59300116 length=135 dictionary=1 codec=1 fetch=16 pieces=[f16@0+135]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5, 6, 13, 15, 61] reads=[0, 1, 2, 3, 4]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5, 6, 13, 15, 61] reads=[5, 6, 7, 8, 9]] inputs=[0]
op 2 node=0 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 13 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[13] count(*) inputs=[2]
op 4 node=1 r2_fetch [8, 9, 10, 11, 12, 13, 14, 15, 16] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[5, 6, 13, 15, 61] reads=[10, 11, 12, 13, 14]; hits_subset_500k.parquet rg=3 rows=122880 columns=[5, 6, 13, 15, 61] reads=[15, 16, 17, 18, 19]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5, 6, 13, 15, 61] reads=[20, 21, 22, 23, 24]] inputs=[4]
op 6 node=1 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 13 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[13] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[13] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=3740385 fetched=3740385 requests=8
metric node=1 logical=3807511 fetched=4056263 requests=9
metric requests=17 logical=7547896 fetched=7796648 over_read=248752 over_read_ppm=31904
```

## Q37

```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10
```

### DataFusion

```text
Sort: PageViews DESC NULLS LAST, fetch=10
  Projection: hits.Title, count(*) AS PageViews
    Aggregate: groupBy=[[hits.Title]], aggr=[[count(*)]]
      TableScan: hits projection=[Title], full_filters=[hits.CounterID = Int32(62), hits.EventDate >= Date32("2013-07-01"), hits.EventDate <= Date32("2013-07-31"), hits.DontCountHits = Int32(0), hits.IsRefresh = Int32(0), hits.Title != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[2], count(*))
      Filter(6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 2 ne text:"")
        Scan(hits, [2, 5, 6, 15, 61])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=990475 length=2097152 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=3087627 length=2097152 reads=[0]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=5184779 length=1372537 reads=[0]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=6951377 length=118 reads=[1, 2]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=11267706 length=21745 reads=[3]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=13029417 length=1682 reads=[4]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=15912214 length=2097152 reads=[5]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=18009366 length=2097152 reads=[5]
fetch 8 node=0 object="hits_subset_500k.parquet" offset=20106518 length=1150215 reads=[5]
fetch 9 node=0 object="hits_subset_500k.parquet" offset=21649986 length=106 reads=[6, 7]
fetch 10 node=0 object="hits_subset_500k.parquet" offset=25920866 length=21929 reads=[8]
fetch 11 node=0 object="hits_subset_500k.parquet" offset=27630663 length=1811 reads=[9]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=30502434 length=2097152 reads=[10]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=32599586 length=2097152 reads=[10]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=34696738 length=1240654 reads=[10]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=36330670 length=106 reads=[11, 12]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=40591060 length=21763 reads=[13]
fetch 17 node=1 object="hits_subset_500k.parquet" offset=42296717 length=1930 reads=[14]
fetch 18 node=1 object="hits_subset_500k.parquet" offset=45162159 length=2097152 reads=[15]
fetch 19 node=1 object="hits_subset_500k.parquet" offset=47259311 length=2097152 reads=[15]
fetch 20 node=1 object="hits_subset_500k.parquet" offset=49356463 length=1194948 reads=[15]
fetch 21 node=1 object="hits_subset_500k.parquet" offset=50943729 length=106 reads=[16, 17]
fetch 22 node=1 object="hits_subset_500k.parquet" offset=55063182 length=21700 reads=[18]
fetch 23 node=1 object="hits_subset_500k.parquet" offset=56779083 length=2005 reads=[19]
fetch 24 node=1 object="hits_subset_500k.parquet" offset=58698148 length=258389 reads=[20, 21, 22]
fetch 25 node=1 object="hits_subset_500k.parquet" offset=59169469 length=130782 reads=[23, 24]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=2 offset=990475 length=5566841 dictionary=0 codec=1 fetch=0 pieces=[f0@0+2097152,f1@2097152+2097152,f2@4194304+1372537]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=3 pieces=[f3@0+53]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=6 offset=6951430 length=65 dictionary=1 codec=1 fetch=3 pieces=[f3@0+65]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=4 pieces=[f4@0+21745]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=61 offset=13029417 length=1682 dictionary=1 codec=1 fetch=5 pieces=[f5@0+1682]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=2 offset=15912214 length=5344519 dictionary=0 codec=1 fetch=6 pieces=[f6@0+2097152,f7@2097152+2097152,f8@4194304+1150215]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=9 pieces=[f9@0+53]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=6 offset=21650039 length=53 dictionary=1 codec=1 fetch=9 pieces=[f9@0+53]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21929]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=61 offset=27630663 length=1811 dictionary=1 codec=1 fetch=11 pieces=[f11@0+1811]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=2 offset=30502434 length=5434958 dictionary=0 codec=1 fetch=12 pieces=[f12@0+2097152,f13@2097152+2097152,f14@4194304+1240654]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=6 offset=36330723 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=16 pieces=[f16@0+21763]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=61 offset=42296717 length=1930 dictionary=1 codec=1 fetch=17 pieces=[f17@0+1930]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=2 offset=45162159 length=5389252 dictionary=0 codec=1 fetch=18 pieces=[f18@0+2097152,f19@2097152+2097152,f20@4194304+1194948]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=21 pieces=[f21@0+53]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=6 offset=50943782 length=53 dictionary=1 codec=1 fetch=21 pieces=[f21@0+53]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=22 pieces=[f22@0+21700]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=61 offset=56779083 length=2005 dictionary=1 codec=1 fetch=23 pieces=[f23@0+2005]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=2 offset=58698148 length=224923 dictionary=1 codec=1 fetch=24 pieces=[f24@0+224923]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=24 pieces=[f24@0+53]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=6 offset=58956484 length=53 dictionary=1 codec=1 fetch=24 pieces=[f24@0+53]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=25 pieces=[f25@0+1748]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=61 offset=59300116 length=135 dictionary=1 codec=1 fetch=25 pieces=[f25@0+135]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[2, 5, 6, 15, 61] reads=[0, 1, 2, 3, 4]; hits_subset_500k.parquet rg=1 rows=122880 columns=[2, 5, 6, 15, 61] reads=[5, 6, 7, 8, 9]] inputs=[0]
op 2 node=0 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 2 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[2] count(*) inputs=[2]
op 4 node=1 r2_fetch [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[2, 5, 6, 15, 61] reads=[10, 11, 12, 13, 14]; hits_subset_500k.parquet rg=3 rows=122880 columns=[2, 5, 6, 15, 61] reads=[15, 16, 17, 18, 19]; hits_subset_500k.parquet rg=4 rows=8480 columns=[2, 5, 6, 15, 61] reads=[20, 21, 22, 23, 24]] inputs=[4]
op 6 node=1 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 61 eq i64:0 and 15 eq i64:0 and 2 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[2] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[2] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=10958751 fetched=10958751 requests=12
metric node=1 logical=11098732 fetched=11260991 requests=14
metric requests=26 logical=22057483 fetched=22219742 over_read=162259 over_read_ppm=7302
```

## Q38

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10
```

### DataFusion

```text
Limit: skip=1000, fetch=10
  Sort: PageViews DESC NULLS LAST, fetch=1010
    Projection: hits.URL, count(*) AS PageViews
      Aggregate: groupBy=[[hits.URL]], aggr=[[count(*)]]
        TableScan: hits projection=[URL], full_filters=[hits.CounterID = Int32(62), hits.EventDate >= Date32("2013-07-01"), hits.EventDate <= Date32("2013-07-31"), hits.IsRefresh = Int32(0), hits.IsLink != Int32(0), hits.IsDownload = Int32(0)]
```

### WCOL logical

```text
Limit(skip=1000, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[13], count(*))
      Filter(6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 52 ne i64:0 and 53 eq i64:0)
        Scan(hits, [5, 6, 13, 15, 52, 53])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=118 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11267706 length=21745 reads=[3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=12379845 length=106 reads=[4, 5]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=21649986 length=106 reads=[6, 7]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[8]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=25920866 length=21929 reads=[9]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=26995191 length=106 reads=[10, 11]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=36330670 length=106 reads=[12, 13]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[14]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=40591060 length=21763 reads=[15]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=41663278 length=106 reads=[16, 17]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=50943729 length=106 reads=[18, 19]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[20]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=55063182 length=21700 reads=[21]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=56138489 length=106 reads=[22, 23]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=58956431 length=298580 reads=[24, 25, 26, 27, 28, 29]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=6 offset=6951430 length=65 dictionary=1 codec=1 fetch=0 pieces=[f0@0+65]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1858606]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=2 pieces=[f2@0+21745]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=52 offset=12379845 length=53 dictionary=1 codec=1 fetch=3 pieces=[f3@0+53]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=53 offset=12379898 length=53 dictionary=1 codec=1 fetch=3 pieces=[f3@0+53]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=6 offset=21650039 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=5 pieces=[f5@0+1834388]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=6 pieces=[f6@0+21929]
read 10 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=52 offset=26995191 length=53 dictionary=1 codec=1 fetch=7 pieces=[f7@0+53]
read 11 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=53 offset=26995244 length=53 dictionary=1 codec=1 fetch=7 pieces=[f7@0+53]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=8 pieces=[f8@0+53]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=6 offset=36330723 length=53 dictionary=1 codec=1 fetch=8 pieces=[f8@0+53]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=9 pieces=[f9@0+1838352]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21763]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=52 offset=41663278 length=53 dictionary=1 codec=1 fetch=11 pieces=[f11@0+53]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=53 offset=41663331 length=53 dictionary=1 codec=1 fetch=11 pieces=[f11@0+53]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=12 pieces=[f12@0+53]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=6 offset=50943782 length=53 dictionary=1 codec=1 fetch=12 pieces=[f12@0+53]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=13 pieces=[f13@0+1826481]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=14 pieces=[f14@0+21700]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=52 offset=56138489 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=53 offset=56138542 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
read 25 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=6 offset=58956484 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
read 26 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=16 pieces=[f16@0+93079]
read 27 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=16 pieces=[f16@0+1748]
read 28 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=52 offset=59254905 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
read 29 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=53 offset=59254958 length=53 dictionary=1 codec=1 fetch=16 pieces=[f16@0+53]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5, 6, 13, 15, 52, 53] reads=[0, 1, 2, 3, 4, 5]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5, 6, 13, 15, 52, 53] reads=[6, 7, 8, 9, 10, 11]] inputs=[0]
op 2 node=0 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 52 ne i64:0 and 53 eq i64:0 inputs=[1]
op 3 node=0 partial_aggregate group=[13] count(*) inputs=[2]
op 4 node=1 r2_fetch [8, 9, 10, 11, 12, 13, 14, 15, 16] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[5, 6, 13, 15, 52, 53] reads=[12, 13, 14, 15, 16, 17]; hits_subset_500k.parquet rg=3 rows=122880 columns=[5, 6, 13, 15, 52, 53] reads=[18, 19, 20, 21, 22, 23]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5, 6, 13, 15, 52, 53] reads=[24, 25, 26, 27, 28, 29]] inputs=[4]
op 6 node=1 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 52 ne i64:0 and 53 eq i64:0 inputs=[5]
op 7 node=1 partial_aggregate group=[13] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[13] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=1000 fetch=10 inputs=[10]
metric node=0 logical=3737104 fetched=3737104 requests=8
metric node=1 logical=3803759 fetched=4007300 requests=9
metric requests=17 logical=7540863 fetched=7744404 over_read=203541 over_read_ppm=26282
```

## Q6

```sql
SELECT MIN(EventDate), MAX(EventDate) FROM hits
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[min(hits.EventDate), max(hits.EventDate)]]
  TableScan: hits projection=[EventDate]
```

### WCOL logical

```text
Aggregate(group=[], min(5), max(5))
  Scan(hits, [5])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=53 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21649986 length=53 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=36330670 length=53 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=50943729 length=53 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58956431 length=53 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=1 pieces=[f1@0+53]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=2 pieces=[f2@0+53]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=3 pieces=[f3@0+53]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
op 0 node=0 r2_fetch [0, 1, 2] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5] reads=[1]; hits_subset_500k.parquet rg=2 rows=122880 columns=[5] reads=[2]] inputs=[0]
op 2 node=0 partial_aggregate group=[] min(5), max(5) inputs=[1]
op 3 node=1 r2_fetch [3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=3 rows=122880 columns=[5] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] min(5), max(5) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] min(5), max(5) inputs=[6]
metric node=0 logical=159 fetched=159 requests=3
metric node=1 logical=106 fetched=106 requests=2
metric requests=5 logical=265 fetched=265 over_read=0 over_read_ppm=0
```

## Q7

```sql
SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC
```

### DataFusion

```text
Sort: count(*) DESC NULLS LAST
  Aggregate: groupBy=[[hits.AdvEngineID]], aggr=[[count(*)]]
    TableScan: hits projection=[AdvEngineID], full_filters=[hits.AdvEngineID != Int32(0)]
```

### WCOL logical

```text
Sort(agg:0 desc nulls_last)
  Aggregate(group=[40], count(*))
    Filter(40 ne i64:0)
      Scan(hits, [40])
```

### WCOL physical

```text
root 10
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11807076 length=6374 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26425735 length=6397 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=41101917 length=6312 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55582619 length=5307 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59209079 length=498 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=40 offset=11807076 length=6374 dictionary=1 codec=1 fetch=0 pieces=[f0@0+6374]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=40 offset=26425735 length=6397 dictionary=1 codec=1 fetch=1 pieces=[f1@0+6397]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=40 offset=41101917 length=6312 dictionary=1 codec=1 fetch=2 pieces=[f2@0+6312]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=40 offset=55582619 length=5307 dictionary=1 codec=1 fetch=3 pieces=[f3@0+5307]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=40 offset=59209079 length=498 dictionary=1 codec=1 fetch=4 pieces=[f4@0+498]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[40] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[40] reads=[1]] inputs=[0]
op 2 node=0 filter 40 ne i64:0 inputs=[1]
op 3 node=0 partial_aggregate group=[40] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[40] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[40] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[40] reads=[4]] inputs=[4]
op 6 node=1 filter 40 ne i64:0 inputs=[5]
op 7 node=1 partial_aggregate group=[40] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[40] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
metric node=0 logical=12771 fetched=12771 requests=2
metric node=1 logical=12117 fetched=12117 requests=3
metric requests=5 logical=24888 fetched=24888 over_read=0 over_read_ppm=0
```

## Q8

```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10
```

### DataFusion

```text
Sort: u DESC NULLS LAST, fetch=10
  Projection: hits.RegionID, count(alias1) AS u
    Aggregate: groupBy=[[hits.RegionID]], aggr=[[count(alias1)]]
      Aggregate: groupBy=[[hits.RegionID, hits.UserID AS alias1]], aggr=[[]]
        TableScan: hits projection=[RegionID, UserID]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[8], count(distinct 9))
      Scan(hits, [8, 9])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7033445 length=158405 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21729133 length=151744 reads=[2, 3]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36410017 length=152434 reads=[4, 5]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51022153 length=150152 reads=[6, 7]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58961865 length=9621 reads=[8, 9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=1 pieces=[f1@0+29522]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=2 pieces=[f2@0+29564]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=3 pieces=[f3@0+28953]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=4 pieces=[f4@0+2275]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[8, 9] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[8, 9] reads=[2, 3]] inputs=[0]
op 2 node=0 partial_aggregate group=[8] count(distinct 9) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[8, 9] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[8, 9] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[8, 9] reads=[8, 9]] inputs=[3]
op 5 node=1 partial_aggregate group=[8] count(distinct 9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[8] count(distinct 9) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=310149 fetched=310149 requests=2
metric node=1 logical=312207 fetched=312207 requests=3
metric requests=5 logical=622356 fetched=622356 over_read=0 over_read_ppm=0
```

## Q9

```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.RegionID, sum(hits.AdvEngineID), count(*) AS c, avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)
    Aggregate: groupBy=[[hits.RegionID]], aggr=[[sum(CAST(hits.AdvEngineID AS Int64)), count(*), avg(CAST(hits.ResolutionWidth AS Float64)), count(DISTINCT hits.UserID)]]
      TableScan: hits projection=[RegionID, UserID, ResolutionWidth, AdvEngineID]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:1 desc nulls_last)
    Aggregate(group=[8], sum(40), count(*), avg(20), count(distinct 9))
      Scan(hits, [8, 9, 20, 40])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7033445 length=158405 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11361203 length=22873 reads=[2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11807076 length=6374 reads=[3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=21729133 length=151744 reads=[4, 5]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=26008527 length=22033 reads=[6]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=26425735 length=6397 reads=[7]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=36410017 length=152434 reads=[8, 9]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=40676712 length=22069 reads=[10]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=41101917 length=6312 reads=[11]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=51022153 length=150152 reads=[12, 13]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=55152912 length=21604 reads=[14]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=55582619 length=5307 reads=[15]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=58961865 length=9621 reads=[16, 17]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=59175644 length=33933 reads=[18, 19]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=1 pieces=[f1@0+22873]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=40 offset=11807076 length=6374 dictionary=1 codec=1 fetch=2 pieces=[f2@0+6374]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=3 pieces=[f3@0+29522]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=3 pieces=[f3@0+122222]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=4 pieces=[f4@0+22033]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=40 offset=26425735 length=6397 dictionary=1 codec=1 fetch=5 pieces=[f5@0+6397]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=6 pieces=[f6@0+29564]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=6 pieces=[f6@0+122870]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=7 pieces=[f7@0+22069]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=40 offset=41101917 length=6312 dictionary=1 codec=1 fetch=8 pieces=[f8@0+6312]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=9 pieces=[f9@0+28953]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=9 pieces=[f9@0+121199]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21604]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=40 offset=55582619 length=5307 dictionary=1 codec=1 fetch=11 pieces=[f11@0+5307]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=12 pieces=[f12@0+2275]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=12 pieces=[f12@0+7346]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1843]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=40 offset=59209079 length=498 dictionary=1 codec=1 fetch=13 pieces=[f13@0+498]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[8, 9, 20, 40] reads=[0, 1, 2, 3]; hits_subset_500k.parquet rg=1 rows=122880 columns=[8, 9, 20, 40] reads=[4, 5, 6, 7]] inputs=[0]
op 2 node=0 partial_aggregate group=[8] sum(40), count(*), avg(20), count(distinct 9) inputs=[1]
op 3 node=1 r2_fetch [6, 7, 8, 9, 10, 11, 12, 13] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[8, 9, 20, 40] reads=[8, 9, 10, 11]; hits_subset_500k.parquet rg=3 rows=122880 columns=[8, 9, 20, 40] reads=[12, 13, 14, 15]; hits_subset_500k.parquet rg=4 rows=8480 columns=[8, 9, 20, 40] reads=[16, 17, 18, 19]] inputs=[3]
op 5 node=1 partial_aggregate group=[8] sum(40), count(*), avg(20), count(distinct 9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[8] sum(40), count(*), avg(20), count(distinct 9) inputs=[6]
op 8 node=coordinator sort agg:1 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=367826 fetched=367826 requests=6
metric node=1 logical=369840 fetched=401432 requests=8
metric requests=14 logical=737666 fetched=769258 over_read=31592 over_read_ppm=41068
```

## Q15

```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
```

### DataFusion

```text
Sort: count(*) DESC NULLS LAST, fetch=10
  Aggregate: groupBy=[[hits.UserID]], aggr=[[count(*)]]
    TableScan: hits projection=[UserID]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[9], count(*))
      Scan(hits, [9])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[9] count(*) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[9] count(*) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[9] count(*) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=248997 fetched=248997 requests=2
metric node=1 logical=251415 fetched=251415 requests=3
metric requests=5 logical=500412 fetched=500412 over_read=0 over_read_ppm=0
```

## Q19

```sql
SELECT UserID FROM hits WHERE UserID = 435090932899640449
```

### DataFusion

```text
TableScan: hits projection=[UserID], full_filters=[hits.UserID = Int64(435090932899640449)]
```

### WCOL logical

```text
Filter(9 eq i64:435090932899640449)
  Scan(hits, [9])
```

### WCOL physical

```text
root 6
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9] reads=[1]] inputs=[0]
op 2 node=0 filter 9 eq i64:435090932899640449 inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9] reads=[4]] inputs=[3]
op 5 node=1 filter 9 eq i64:435090932899640449 inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
metric node=0 logical=248997 fetched=248997 requests=2
metric node=1 logical=251415 fetched=251415 requests=3
metric requests=5 logical=500412 fetched=500412 over_read=0 over_read_ppm=0
```

## Q20

```sql
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.URL LIKE Utf8("%google%")]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(13 like text:"%google%")
    Scan(hits, [13])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58973967 length=93079 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=0 pieces=[f0@0+1858606]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1834388]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=2 pieces=[f2@0+1838352]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1826481]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=4 pieces=[f4@0+93079]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[13] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[13] reads=[1]] inputs=[0]
op 2 node=0 filter 13 like text:"%google%" inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[13] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[13] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[13] reads=[4]] inputs=[4]
op 6 node=1 filter 13 like text:"%google%" inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=3692994 fetched=3692994 requests=2
metric node=1 logical=3757912 fetched=3757912 requests=3
metric requests=5 logical=7450906 fetched=7450906 over_read=0 over_read_ppm=0
```

## Q30

```sql
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.SearchEngineID, hits.ClientIP, count(*) AS c, sum(hits.IsRefresh), avg(hits.ResolutionWidth)
    Aggregate: groupBy=[[hits.SearchEngineID, hits.ClientIP]], aggr=[[count(*), sum(CAST(hits.IsRefresh AS Int64)), avg(CAST(hits.ResolutionWidth AS Float64))]]
      TableScan: hits projection=[ClientIP, IsRefresh, ResolutionWidth, SearchEngineID], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[38, 7], count(*), sum(15), avg(20))
      Filter(39 ne text:"")
        Scan(hits, [7, 15, 20, 38, 39])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951495 length=81950 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11267706 length=116370 reads=[1, 2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11598610 length=208466 reads=[3, 4]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=21650092 length=79041 reads=[5]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=25920866 length=109694 reads=[6, 7]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=26232726 length=193009 reads=[8, 9]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=36330776 length=79241 reads=[10]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=40591060 length=107721 reads=[11, 12]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=40901820 length=200097 reads=[13, 14]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=50943835 length=78318 reads=[15]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=55063182 length=111334 reads=[16, 17]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=55370906 length=211713 reads=[18, 19]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=58956537 length=5328 reads=[20]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=59169469 length=39610 reads=[21, 22, 23, 24]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=7 offset=6951495 length=81950 dictionary=0 codec=1 fetch=0 pieces=[f0@0+81950]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=1 pieces=[f1@0+21745]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=1 pieces=[f1@0+22873]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=38 offset=11598610 length=20526 dictionary=1 codec=1 fetch=2 pieces=[f2@0+20526]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=2 pieces=[f2@0+187940]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=7 offset=21650092 length=79041 dictionary=0 codec=1 fetch=3 pieces=[f3@0+79041]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=4 pieces=[f4@0+21929]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=4 pieces=[f4@0+22033]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=38 offset=26232726 length=20788 dictionary=1 codec=1 fetch=5 pieces=[f5@0+20788]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=5 pieces=[f5@0+172221]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=7 offset=36330776 length=79241 dictionary=0 codec=1 fetch=6 pieces=[f6@0+79241]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=7 pieces=[f7@0+21763]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=7 pieces=[f7@0+22069]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=38 offset=40901820 length=20427 dictionary=1 codec=1 fetch=8 pieces=[f8@0+20427]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=8 pieces=[f8@0+179670]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=7 offset=50943835 length=78318 dictionary=0 codec=1 fetch=9 pieces=[f9@0+78318]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21700]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21604]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=38 offset=55370906 length=19838 dictionary=1 codec=1 fetch=11 pieces=[f11@0+19838]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=11 pieces=[f11@0+191875]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=7 offset=58956537 length=5328 dictionary=1 codec=1 fetch=12 pieces=[f12@0+5328]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1748]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1843]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=38 offset=59193573 length=1342 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1342]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=13 pieces=[f13@0+14164]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[7, 15, 20, 38, 39] reads=[0, 1, 2, 3, 4]; hits_subset_500k.parquet rg=1 rows=122880 columns=[7, 15, 20, 38, 39] reads=[5, 6, 7, 8, 9]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[38, 7] count(*), sum(15), avg(20) inputs=[2]
op 4 node=1 r2_fetch [6, 7, 8, 9, 10, 11, 12, 13] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[7, 15, 20, 38, 39] reads=[10, 11, 12, 13, 14]; hits_subset_500k.parquet rg=3 rows=122880 columns=[7, 15, 20, 38, 39] reads=[15, 16, 17, 18, 19]; hits_subset_500k.parquet rg=4 rows=8480 columns=[7, 15, 20, 38, 39] reads=[20, 21, 22, 23, 24]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[38, 7] count(*), sum(15), avg(20) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[38, 7] count(*), sum(15), avg(20) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=651046 fetched=788530 requests=6
metric node=1 logical=680930 fetched=833362 requests=8
metric requests=14 logical=1331976 fetched=1621892 over_read=289916 over_read_ppm=178751
```

## Q31

```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.WatchID, hits.ClientIP, count(*) AS c, sum(hits.IsRefresh), avg(hits.ResolutionWidth)
    Aggregate: groupBy=[[hits.WatchID, hits.ClientIP]], aggr=[[count(*), sum(CAST(hits.IsRefresh AS Int64)), avg(CAST(hits.ResolutionWidth AS Float64))]]
      TableScan: hits projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[0, 7], count(*), sum(15), avg(20))
      Filter(39 ne text:"")
        Scan(hits, [0, 7, 15, 20, 39])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=4 length=983120 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=6951495 length=81950 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11267706 length=116370 reads=[2, 3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[4]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=14922558 length=983120 reads=[5]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=21650092 length=79041 reads=[6]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=25920866 length=109694 reads=[7, 8]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[9]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=29512240 length=983120 reads=[10]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=36330776 length=79241 reads=[11]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=40591060 length=107721 reads=[12, 13]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[14]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=44171598 length=983120 reads=[15]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=50943835 length=78318 reads=[16]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=55063182 length=111334 reads=[17, 18]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[19]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=58629656 length=67880 reads=[20]
fetch 17 node=1 object="hits_subset_500k.parquet" offset=58956537 length=5328 reads=[21]
fetch 18 node=1 object="hits_subset_500k.parquet" offset=59169469 length=39610 reads=[22, 23, 24]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=0 offset=4 length=983120 dictionary=0 codec=1 fetch=0 pieces=[f0@0+983120]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=7 offset=6951495 length=81950 dictionary=0 codec=1 fetch=1 pieces=[f1@0+81950]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=2 pieces=[f2@0+21745]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=2 pieces=[f2@0+22873]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=3 pieces=[f3@0+187940]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=0 offset=14922558 length=983120 dictionary=0 codec=1 fetch=4 pieces=[f4@0+983120]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=7 offset=21650092 length=79041 dictionary=0 codec=1 fetch=5 pieces=[f5@0+79041]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=6 pieces=[f6@0+21929]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=6 pieces=[f6@0+22033]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=7 pieces=[f7@0+172221]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=0 offset=29512240 length=983120 dictionary=0 codec=1 fetch=8 pieces=[f8@0+983120]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=7 offset=36330776 length=79241 dictionary=0 codec=1 fetch=9 pieces=[f9@0+79241]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21763]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=10 pieces=[f10@0+22069]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=11 pieces=[f11@0+179670]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=0 offset=44171598 length=983120 dictionary=0 codec=1 fetch=12 pieces=[f12@0+983120]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=7 offset=50943835 length=78318 dictionary=0 codec=1 fetch=13 pieces=[f13@0+78318]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=14 pieces=[f14@0+21700]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=14 pieces=[f14@0+21604]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=15 pieces=[f15@0+191875]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=0 offset=58629656 length=67880 dictionary=0 codec=1 fetch=16 pieces=[f16@0+67880]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=7 offset=58956537 length=5328 dictionary=1 codec=1 fetch=17 pieces=[f17@0+5328]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=18 pieces=[f18@0+1748]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=18 pieces=[f18@0+1843]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=18 pieces=[f18@0+14164]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[0, 7, 15, 20, 39] reads=[0, 1, 2, 3, 4]; hits_subset_500k.parquet rg=1 rows=122880 columns=[0, 7, 15, 20, 39] reads=[5, 6, 7, 8, 9]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[2]
op 4 node=1 r2_fetch [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[0, 7, 15, 20, 39] reads=[10, 11, 12, 13, 14]; hits_subset_500k.parquet rg=3 rows=122880 columns=[0, 7, 15, 20, 39] reads=[15, 16, 17, 18, 19]; hits_subset_500k.parquet rg=4 rows=8480 columns=[0, 7, 15, 20, 39] reads=[20, 21, 22, 23, 24]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=0 fetch=10 inputs=[10]
metric node=0 logical=2575972 fetched=2713456 requests=8
metric node=1 logical=2673443 fetched=2827217 requests=11
metric requests=19 logical=5249415 fetched=5540673 over_read=291258 over_read_ppm=52567
```

## Q32

```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.WatchID, hits.ClientIP, count(*) AS c, sum(hits.IsRefresh), avg(hits.ResolutionWidth)
    Aggregate: groupBy=[[hits.WatchID, hits.ClientIP]], aggr=[[count(*), sum(CAST(hits.IsRefresh AS Int64)), avg(CAST(hits.ResolutionWidth AS Float64))]]
      TableScan: hits projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[0, 7], count(*), sum(15), avg(20))
      Scan(hits, [0, 7, 15, 20])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=4 length=983120 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=6951495 length=81950 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11267706 length=116370 reads=[2, 3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=14922558 length=983120 reads=[4]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=21650092 length=79041 reads=[5]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=25920866 length=109694 reads=[6, 7]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=29512240 length=983120 reads=[8]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=36330776 length=79241 reads=[9]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=40591060 length=107721 reads=[10, 11]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=44171598 length=983120 reads=[12]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=50943835 length=78318 reads=[13]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=55063182 length=111334 reads=[14, 15]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=58629656 length=67880 reads=[16]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=58956537 length=5328 reads=[17]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=59169469 length=8018 reads=[18, 19]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=0 offset=4 length=983120 dictionary=0 codec=1 fetch=0 pieces=[f0@0+983120]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=7 offset=6951495 length=81950 dictionary=0 codec=1 fetch=1 pieces=[f1@0+81950]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=2 pieces=[f2@0+21745]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=2 pieces=[f2@0+22873]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=0 offset=14922558 length=983120 dictionary=0 codec=1 fetch=3 pieces=[f3@0+983120]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=7 offset=21650092 length=79041 dictionary=0 codec=1 fetch=4 pieces=[f4@0+79041]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=5 pieces=[f5@0+21929]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=5 pieces=[f5@0+22033]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=0 offset=29512240 length=983120 dictionary=0 codec=1 fetch=6 pieces=[f6@0+983120]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=7 offset=36330776 length=79241 dictionary=0 codec=1 fetch=7 pieces=[f7@0+79241]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=8 pieces=[f8@0+21763]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=8 pieces=[f8@0+22069]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=0 offset=44171598 length=983120 dictionary=0 codec=1 fetch=9 pieces=[f9@0+983120]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=7 offset=50943835 length=78318 dictionary=0 codec=1 fetch=10 pieces=[f10@0+78318]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=11 pieces=[f11@0+21700]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=11 pieces=[f11@0+21604]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=0 offset=58629656 length=67880 dictionary=0 codec=1 fetch=12 pieces=[f12@0+67880]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=7 offset=58956537 length=5328 dictionary=1 codec=1 fetch=13 pieces=[f13@0+5328]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=14 pieces=[f14@0+1748]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=14 pieces=[f14@0+1843]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[0, 7, 15, 20] reads=[0, 1, 2, 3]; hits_subset_500k.parquet rg=1 rows=122880 columns=[0, 7, 15, 20] reads=[4, 5, 6, 7]] inputs=[0]
op 2 node=0 partial_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[1]
op 3 node=1 r2_fetch [6, 7, 8, 9, 10, 11, 12, 13, 14] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[0, 7, 15, 20] reads=[8, 9, 10, 11]; hits_subset_500k.parquet rg=3 rows=122880 columns=[0, 7, 15, 20] reads=[12, 13, 14, 15]; hits_subset_500k.parquet rg=4 rows=8480 columns=[0, 7, 15, 20] reads=[16, 17, 18, 19]] inputs=[3]
op 5 node=1 partial_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[0, 7] count(*), sum(15), avg(20) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=2215811 fetched=2353295 requests=6
metric node=1 logical=2287734 fetched=2424080 requests=9
metric requests=15 logical=4503545 fetched=4777375 over_read=273830 over_read_ppm=57318
```

## Q41

```sql
SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
```

### DataFusion

```text
Limit: skip=10000, fetch=10
  Sort: PageViews DESC NULLS LAST, fetch=10010
    Projection: hits.WindowClientWidth, hits.WindowClientHeight, count(*) AS PageViews
      Aggregate: groupBy=[[hits.WindowClientWidth, hits.WindowClientHeight]], aggr=[[count(*)]]
        TableScan: hits projection=[WindowClientWidth, WindowClientHeight], full_filters=[hits.CounterID = Int32(62), hits.EventDate >= Date32("2013-07-01"), hits.EventDate <= Date32("2013-07-31"), hits.IsRefresh = Int32(0), hits.DontCountHits = Int32(0), hits.URLHash = Int64(2868770270353813622)]
```

### WCOL logical

```text
Limit(skip=10000, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[42, 43], count(*))
      Filter(6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 61 eq i64:0 and 103 eq i64:2868770270353813622)
        Scan(hits, [5, 6, 15, 42, 43, 61, 103])
```

### WCOL physical

```text
root 11
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=118 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11267706 length=21745 reads=[2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11831997 length=114412 reads=[3, 4]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=13029417 length=1682 reads=[5]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=14446307 length=476198 reads=[6]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=21649986 length=106 reads=[7, 8]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=25920866 length=21929 reads=[9]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=26450546 length=113077 reads=[10, 11]
fetch 8 node=0 object="hits_subset_500k.parquet" offset=27630663 length=1811 reads=[12]
fetch 9 node=0 object="hits_subset_500k.parquet" offset=29041162 length=471025 reads=[13]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=36330670 length=106 reads=[14, 15]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=40591060 length=21763 reads=[16]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=41126710 length=106024 reads=[17, 18]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=42296717 length=1930 reads=[19]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=43697709 length=473836 reads=[20]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=50943729 length=106 reads=[21, 22]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=55063182 length=21700 reads=[23]
fetch 17 node=1 object="hits_subset_500k.parquet" offset=55606321 length=103907 reads=[24, 25]
fetch 18 node=1 object="hits_subset_500k.parquet" offset=56779083 length=2005 reads=[26]
fetch 19 node=1 object="hits_subset_500k.parquet" offset=58158878 length=470725 reads=[27]
fetch 20 node=1 object="hits_subset_500k.parquet" offset=58956431 length=106 reads=[28, 29]
fetch 21 node=1 object="hits_subset_500k.parquet" offset=59169469 length=271603 reads=[30, 31, 32, 33, 34]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=6 offset=6951430 length=65 dictionary=1 codec=1 fetch=0 pieces=[f0@0+65]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=1 pieces=[f1@0+21745]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=42 offset=11831997 length=63813 dictionary=1 codec=1 fetch=2 pieces=[f2@0+63813]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=43 offset=11895810 length=50599 dictionary=1 codec=1 fetch=2 pieces=[f2@0+50599]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=61 offset=13029417 length=1682 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1682]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=103 offset=14446307 length=476198 dictionary=0 codec=1 fetch=4 pieces=[f4@0+476198]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=5 pieces=[f5@0+53]
read 8 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=6 offset=21650039 length=53 dictionary=1 codec=1 fetch=5 pieces=[f5@0+53]
read 9 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=6 pieces=[f6@0+21929]
read 10 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=42 offset=26450546 length=64142 dictionary=1 codec=1 fetch=7 pieces=[f7@0+64142]
read 11 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=43 offset=26514688 length=48935 dictionary=1 codec=1 fetch=7 pieces=[f7@0+48935]
read 12 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=61 offset=27630663 length=1811 dictionary=1 codec=1 fetch=8 pieces=[f8@0+1811]
read 13 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=103 offset=29041162 length=471025 dictionary=0 codec=1 fetch=9 pieces=[f9@0+471025]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=10 pieces=[f10@0+53]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=6 offset=36330723 length=53 dictionary=1 codec=1 fetch=10 pieces=[f10@0+53]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=11 pieces=[f11@0+21763]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=42 offset=41126710 length=56371 dictionary=1 codec=1 fetch=12 pieces=[f12@0+56371]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=43 offset=41183081 length=49653 dictionary=1 codec=1 fetch=12 pieces=[f12@0+49653]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=61 offset=42296717 length=1930 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1930]
read 20 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=103 offset=43697709 length=473836 dictionary=0 codec=1 fetch=14 pieces=[f14@0+473836]
read 21 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 22 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=6 offset=50943782 length=53 dictionary=1 codec=1 fetch=15 pieces=[f15@0+53]
read 23 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=16 pieces=[f16@0+21700]
read 24 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=42 offset=55606321 length=55563 dictionary=1 codec=1 fetch=17 pieces=[f17@0+55563]
read 25 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=43 offset=55661884 length=48344 dictionary=1 codec=1 fetch=17 pieces=[f17@0+48344]
read 26 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=61 offset=56779083 length=2005 dictionary=1 codec=1 fetch=18 pieces=[f18@0+2005]
read 27 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=103 offset=58158878 length=470725 dictionary=0 codec=1 fetch=19 pieces=[f19@0+470725]
read 28 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=20 pieces=[f20@0+53]
read 29 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=6 offset=58956484 length=53 dictionary=1 codec=1 fetch=20 pieces=[f20@0+53]
read 30 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=21 pieces=[f21@0+1748]
read 31 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=42 offset=59211227 length=3496 dictionary=1 codec=1 fetch=21 pieces=[f21@0+3496]
read 32 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=43 offset=59214723 length=4525 dictionary=1 codec=1 fetch=21 pieces=[f21@0+4525]
read 33 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=61 offset=59300116 length=135 dictionary=1 codec=1 fetch=21 pieces=[f21@0+135]
read 34 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=103 offset=59409545 length=31527 dictionary=1 codec=1 fetch=21 pieces=[f21@0+31527]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5, 6, 15, 42, 43, 61, 103] reads=[0, 1, 2, 3, 4, 5, 6]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5, 6, 15, 42, 43, 61, 103] reads=[7, 8, 9, 10, 11, 12, 13]] inputs=[0]
op 2 node=0 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 61 eq i64:0 and 103 eq i64:2868770270353813622 inputs=[1]
op 3 node=0 partial_aggregate group=[42, 43] count(*) inputs=[2]
op 4 node=1 r2_fetch [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[5, 6, 15, 42, 43, 61, 103] reads=[14, 15, 16, 17, 18, 19, 20]; hits_subset_500k.parquet rg=3 rows=122880 columns=[5, 6, 15, 42, 43, 61, 103] reads=[21, 22, 23, 24, 25, 26, 27]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5, 6, 15, 42, 43, 61, 103] reads=[28, 29, 30, 31, 32, 33, 34]] inputs=[4]
op 6 node=1 filter 6 eq i64:62 and 5 ge i64:15887 and 5 le i64:15917 and 15 eq i64:0 and 61 eq i64:0 and 103 eq i64:2868770270353813622 inputs=[5]
op 7 node=1 partial_aggregate group=[42, 43] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[42, 43] count(*) inputs=[8]
op 10 node=coordinator sort agg:0 desc nulls_last inputs=[9]
op 11 node=coordinator limit skip=10000 fetch=10 inputs=[10]
metric node=0 logical=1222103 fetched=1222103 requests=10
metric node=1 logical=1243639 fetched=1473811 requests=12
metric requests=22 logical=2465742 fetched=2695914 over_read=230172 over_read_ppm=85378
```

## D1

```sql
SELECT EventDate, COUNT(*), SUM(IsRefresh) FROM hits WHERE EventDate >= '2013-07-15' AND EventDate <= '2013-07-20' GROUP BY EventDate
```

### DataFusion

```text
Aggregate: groupBy=[[hits.EventDate]], aggr=[[count(*), sum(CAST(hits.IsRefresh AS Int64))]]
  TableScan: hits projection=[EventDate, IsRefresh], full_filters=[hits.EventDate >= Date32("2013-07-15"), hits.EventDate <= Date32("2013-07-20")]
```

### WCOL logical

```text
Aggregate(group=[5], count(*), sum(15))
  Filter(5 ge i64:15901 and 5 le i64:15906)
    Scan(hits, [5, 15])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=53 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11267706 length=21745 reads=[1]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21649986 length=53 reads=[2]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=25920866 length=21929 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36330670 length=53 reads=[4]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40591060 length=21763 reads=[5]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=50943729 length=53 reads=[6]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55063182 length=21700 reads=[7]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58956431 length=53 reads=[8]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59169469 length=1748 reads=[9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=15 offset=11267706 length=21745 dictionary=1 codec=1 fetch=1 pieces=[f1@0+21745]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=2 pieces=[f2@0+53]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=15 offset=25920866 length=21929 dictionary=1 codec=1 fetch=3 pieces=[f3@0+21929]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=15 offset=40591060 length=21763 dictionary=1 codec=1 fetch=5 pieces=[f5@0+21763]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=6 pieces=[f6@0+53]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=15 offset=55063182 length=21700 dictionary=1 codec=1 fetch=7 pieces=[f7@0+21700]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=8 pieces=[f8@0+53]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=15 offset=59169469 length=1748 dictionary=1 codec=1 fetch=9 pieces=[f9@0+1748]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5, 15] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5, 15] reads=[2, 3]] inputs=[0]
op 2 node=0 filter 5 ge i64:15901 and 5 le i64:15906 inputs=[1]
op 3 node=0 partial_aggregate group=[5] count(*), sum(15) inputs=[2]
op 4 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[5, 15] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[5, 15] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5, 15] reads=[8, 9]] inputs=[4]
op 6 node=1 filter 5 ge i64:15901 and 5 le i64:15906 inputs=[5]
op 7 node=1 partial_aggregate group=[5] count(*), sum(15) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[5] count(*), sum(15) inputs=[8]
metric node=0 logical=43780 fetched=43780 requests=4
metric node=1 logical=45370 fetched=45370 requests=6
metric requests=10 logical=89150 fetched=89150 over_read=0 over_read_ppm=0
```

## D3

```sql
SELECT TraficSourceID, COUNT(*) FROM hits WHERE 2 < TraficSourceID GROUP BY TraficSourceID
```

### DataFusion

```text
Aggregate: groupBy=[[hits.TraficSourceID]], aggr=[[count(*)]]
  TableScan: hits projection=[TraficSourceID], full_filters=[hits.TraficSourceID > Int32(2)]
```

### WCOL logical

```text
Aggregate(group=[37], count(*))
  Filter(37 gt i64:2)
    Scan(hits, [37])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11574725 length=23885 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26209176 length=23550 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40878481 length=23339 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55348070 length=22836 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59191813 length=1760 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=37 offset=11574725 length=23885 dictionary=1 codec=1 fetch=0 pieces=[f0@0+23885]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=37 offset=26209176 length=23550 dictionary=1 codec=1 fetch=1 pieces=[f1@0+23550]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=37 offset=40878481 length=23339 dictionary=1 codec=1 fetch=2 pieces=[f2@0+23339]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=37 offset=55348070 length=22836 dictionary=1 codec=1 fetch=3 pieces=[f3@0+22836]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=37 offset=59191813 length=1760 dictionary=1 codec=1 fetch=4 pieces=[f4@0+1760]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[37] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[37] reads=[1]] inputs=[0]
op 2 node=0 filter 37 gt i64:2 inputs=[1]
op 3 node=0 partial_aggregate group=[37] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[37] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[37] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[37] reads=[4]] inputs=[4]
op 6 node=1 filter 37 gt i64:2 inputs=[5]
op 7 node=1 partial_aggregate group=[37] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[37] count(*) inputs=[8]
metric node=0 logical=47435 fetched=47435 requests=2
metric node=1 logical=47935 fetched=47935 requests=3
metric requests=5 logical=95370 fetched=95370 over_read=0 over_read_ppm=0
```

## D5

```sql
SELECT RegionID, OS, UserAgent, IsMobile, COUNT(*) AS c FROM hits GROUP BY RegionID, OS, UserAgent, IsMobile ORDER BY c DESC LIMIT 10
```

### DataFusion

```text
Sort: c DESC NULLS LAST, fetch=10
  Projection: hits.RegionID, hits.OS, hits.UserAgent, hits.IsMobile, count(*) AS c
    Aggregate: groupBy=[[hits.RegionID, hits.OS, hits.UserAgent, hits.IsMobile]], aggr=[[count(*)]]
      TableScan: hits projection=[RegionID, OS, UserAgent, IsMobile]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[8, 11, 12, 32], count(*))
      Scan(hits, [8, 11, 12, 32])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7033445 length=194457 reads=[0, 1, 2]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11510406 length=2400 reads=[3]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=21729133 length=185159 reads=[4, 5, 6]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=26148483 length=1854 reads=[7]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=36410017 length=185644 reads=[8, 9, 10]
fetch 5 node=1 object="hits_subset_500k.parquet" offset=40817536 length=2113 reads=[11]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=51022153 length=183587 reads=[12, 13, 14]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=55288028 length=2254 reads=[15]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=58961865 length=12102 reads=[16, 17, 18]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=59186786 length=219 reads=[19]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=11 offset=7191903 length=16561 dictionary=1 codec=1 fetch=0 pieces=[f0@0+16561]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=12 offset=7208464 length=19438 dictionary=1 codec=1 fetch=0 pieces=[f0@0+19438]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=32 offset=11510406 length=2400 dictionary=1 codec=1 fetch=1 pieces=[f1@0+2400]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=2 pieces=[f2@0+29522]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=11 offset=21880930 length=15145 dictionary=1 codec=1 fetch=2 pieces=[f2@0+15145]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=12 offset=21896075 length=18217 dictionary=1 codec=1 fetch=2 pieces=[f2@0+18217]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=32 offset=26148483 length=1854 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1854]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=4 pieces=[f4@0+29564]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=11 offset=36562504 length=14975 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14975]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=12 offset=36577479 length=18182 dictionary=1 codec=1 fetch=4 pieces=[f4@0+18182]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=32 offset=40817536 length=2113 dictionary=1 codec=1 fetch=5 pieces=[f5@0+2113]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=6 pieces=[f6@0+28953]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=11 offset=51172358 length=14588 dictionary=1 codec=1 fetch=6 pieces=[f6@0+14588]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=12 offset=51186946 length=18794 dictionary=1 codec=1 fetch=6 pieces=[f6@0+18794]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=32 offset=55288028 length=2254 dictionary=1 codec=1 fetch=7 pieces=[f7@0+2254]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=8 pieces=[f8@0+2275]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=11 offset=58971539 length=1193 dictionary=1 codec=1 fetch=8 pieces=[f8@0+1193]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=12 offset=58972732 length=1235 dictionary=1 codec=1 fetch=8 pieces=[f8@0+1235]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=32 offset=59186786 length=219 dictionary=1 codec=1 fetch=9 pieces=[f9@0+219]
op 0 node=0 r2_fetch [0, 1, 2, 3] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[8, 11, 12, 32] reads=[0, 1, 2, 3]; hits_subset_500k.parquet rg=1 rows=122880 columns=[8, 11, 12, 32] reads=[4, 5, 6, 7]] inputs=[0]
op 2 node=0 partial_aggregate group=[8, 11, 12, 32] count(*) inputs=[1]
op 3 node=1 r2_fetch [4, 5, 6, 7, 8, 9] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[8, 11, 12, 32] reads=[8, 9, 10, 11]; hits_subset_500k.parquet rg=3 rows=122880 columns=[8, 11, 12, 32] reads=[12, 13, 14, 15]; hits_subset_500k.parquet rg=4 rows=8480 columns=[8, 11, 12, 32] reads=[16, 17, 18, 19]] inputs=[3]
op 5 node=1 partial_aggregate group=[8, 11, 12, 32] count(*) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[8, 11, 12, 32] count(*) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=134767 fetched=383870 requests=4
metric node=1 logical=134345 fetched=385919 requests=6
metric requests=10 logical=269112 fetched=769789 over_read=500677 over_read_ppm=650408
```

## A1

```sql
SELECT approx_count_distinct(UserID) FROM hits
```

### DataFusion

```text
Projection: approx_distinct(hits.UserID) AS approx_count_distinct(hits.UserID)
  Aggregate: groupBy=[[]], aggr=[[approx_distinct(hits.UserID)]]
    TableScan: hits projection=[UserID]
```

### WCOL logical

```text
Aggregate(group=[], approx_distinct(9))
  Scan(hits, [9])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7065075 length=126775 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21758655 length=122222 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36439581 length=122870 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51051106 length=121199 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58964140 length=7346 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[9] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[9] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] approx_distinct(9) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[9] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[9] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[9] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] approx_distinct(9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] approx_distinct(9) inputs=[6]
metric node=0 logical=248997 fetched=248997 requests=2
metric node=1 logical=251415 fetched=251415 requests=3
metric requests=5 logical=500412 fetched=500412 over_read=0 over_read_ppm=0
```

## A2

```sql
SELECT RegionID, approx_count_distinct(UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10
```

### DataFusion

```text
Sort: u DESC NULLS LAST, fetch=10
  Projection: hits.RegionID, approx_distinct(hits.UserID) AS approx_count_distinct(hits.UserID) AS u
    Aggregate: groupBy=[[hits.RegionID]], aggr=[[approx_distinct(hits.UserID)]]
      TableScan: hits projection=[RegionID, UserID]
```

### WCOL logical

```text
Limit(skip=0, fetch=10)
  Sort(agg:0 desc nulls_last)
    Aggregate(group=[8], approx_distinct(9))
      Scan(hits, [8, 9])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7033445 length=158405 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21729133 length=151744 reads=[2, 3]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36410017 length=152434 reads=[4, 5]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51022153 length=150152 reads=[6, 7]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58961865 length=9621 reads=[8, 9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=1 pieces=[f1@0+29522]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=1 pieces=[f1@0+122222]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=2 pieces=[f2@0+29564]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=2 pieces=[f2@0+122870]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=3 pieces=[f3@0+28953]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=3 pieces=[f3@0+121199]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=4 pieces=[f4@0+2275]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=4 pieces=[f4@0+7346]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[8, 9] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[8, 9] reads=[2, 3]] inputs=[0]
op 2 node=0 partial_aggregate group=[8] approx_distinct(9) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[8, 9] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[8, 9] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[8, 9] reads=[8, 9]] inputs=[3]
op 5 node=1 partial_aggregate group=[8] approx_distinct(9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[8] approx_distinct(9) inputs=[6]
op 8 node=coordinator sort agg:0 desc nulls_last inputs=[7]
op 9 node=coordinator limit skip=0 fetch=10 inputs=[8]
metric node=0 logical=310149 fetched=310149 requests=2
metric node=1 logical=312207 fetched=312207 requests=3
metric requests=5 logical=622356 fetched=622356 over_read=0 over_read_ppm=0
```

## A3

```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), approx_count_distinct(UserID) FROM hits GROUP BY RegionID
```

### DataFusion

```text
Projection: hits.RegionID, sum(hits.AdvEngineID), count(*) AS c, avg(hits.ResolutionWidth), approx_distinct(hits.UserID) AS approx_count_distinct(hits.UserID)
  Aggregate: groupBy=[[hits.RegionID]], aggr=[[sum(CAST(hits.AdvEngineID AS Int64)), count(*), avg(CAST(hits.ResolutionWidth AS Float64)), approx_distinct(hits.UserID)]]
    TableScan: hits projection=[RegionID, UserID, ResolutionWidth, AdvEngineID]
```

### WCOL logical

```text
Aggregate(group=[8], sum(40), count(*), avg(20), approx_distinct(9))
  Scan(hits, [8, 9, 20, 40])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7033445 length=158405 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=11361203 length=22873 reads=[2]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=11807076 length=6374 reads=[3]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=21729133 length=151744 reads=[4, 5]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=26008527 length=22033 reads=[6]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=26425735 length=6397 reads=[7]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=36410017 length=152434 reads=[8, 9]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=40676712 length=22069 reads=[10]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=41101917 length=6312 reads=[11]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=51022153 length=150152 reads=[12, 13]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=55152912 length=21604 reads=[14]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=55582619 length=5307 reads=[15]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=58961865 length=9621 reads=[16, 17]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=59175644 length=33933 reads=[18, 19]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=9 offset=7065075 length=126775 dictionary=0 codec=1 fetch=0 pieces=[f0@0+126775]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=20 offset=11361203 length=22873 dictionary=1 codec=1 fetch=1 pieces=[f1@0+22873]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=40 offset=11807076 length=6374 dictionary=1 codec=1 fetch=2 pieces=[f2@0+6374]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=3 pieces=[f3@0+29522]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=9 offset=21758655 length=122222 dictionary=0 codec=1 fetch=3 pieces=[f3@0+122222]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=20 offset=26008527 length=22033 dictionary=1 codec=1 fetch=4 pieces=[f4@0+22033]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=40 offset=26425735 length=6397 dictionary=1 codec=1 fetch=5 pieces=[f5@0+6397]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=6 pieces=[f6@0+29564]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=9 offset=36439581 length=122870 dictionary=0 codec=1 fetch=6 pieces=[f6@0+122870]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=20 offset=40676712 length=22069 dictionary=1 codec=1 fetch=7 pieces=[f7@0+22069]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=40 offset=41101917 length=6312 dictionary=1 codec=1 fetch=8 pieces=[f8@0+6312]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=9 pieces=[f9@0+28953]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=9 offset=51051106 length=121199 dictionary=0 codec=1 fetch=9 pieces=[f9@0+121199]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=20 offset=55152912 length=21604 dictionary=1 codec=1 fetch=10 pieces=[f10@0+21604]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=40 offset=55582619 length=5307 dictionary=1 codec=1 fetch=11 pieces=[f11@0+5307]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=12 pieces=[f12@0+2275]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=9 offset=58964140 length=7346 dictionary=1 codec=1 fetch=12 pieces=[f12@0+7346]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=20 offset=59175644 length=1843 dictionary=1 codec=1 fetch=13 pieces=[f13@0+1843]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=40 offset=59209079 length=498 dictionary=1 codec=1 fetch=13 pieces=[f13@0+498]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[8, 9, 20, 40] reads=[0, 1, 2, 3]; hits_subset_500k.parquet rg=1 rows=122880 columns=[8, 9, 20, 40] reads=[4, 5, 6, 7]] inputs=[0]
op 2 node=0 partial_aggregate group=[8] sum(40), count(*), avg(20), approx_distinct(9) inputs=[1]
op 3 node=1 r2_fetch [6, 7, 8, 9, 10, 11, 12, 13] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[8, 9, 20, 40] reads=[8, 9, 10, 11]; hits_subset_500k.parquet rg=3 rows=122880 columns=[8, 9, 20, 40] reads=[12, 13, 14, 15]; hits_subset_500k.parquet rg=4 rows=8480 columns=[8, 9, 20, 40] reads=[16, 17, 18, 19]] inputs=[3]
op 5 node=1 partial_aggregate group=[8] sum(40), count(*), avg(20), approx_distinct(9) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[8] sum(40), count(*), avg(20), approx_distinct(9) inputs=[6]
metric node=0 logical=367826 fetched=367826 requests=6
metric node=1 logical=369840 fetched=401432 requests=8
metric requests=14 logical=737666 fetched=769258 over_read=31592 over_read_ppm=41068
```

## A4

```sql
SELECT OS, approx_count_distinct(ClientIP), approx_count_distinct(RegionID) FROM hits WHERE EventDate > '2013-07-02' GROUP BY OS
```

### DataFusion

```text
Projection: hits.OS, approx_distinct(hits.ClientIP) AS approx_count_distinct(hits.ClientIP), approx_distinct(hits.RegionID) AS approx_count_distinct(hits.RegionID)
  Aggregate: groupBy=[[hits.OS]], aggr=[[approx_distinct(hits.ClientIP), approx_distinct(hits.RegionID)]]
    TableScan: hits projection=[ClientIP, RegionID, OS], full_filters=[hits.EventDate > Date32("2013-07-02")]
```

### WCOL logical

```text
Aggregate(group=[11], approx_distinct(7), approx_distinct(8))
  Filter(5 gt i64:15888)
    Scan(hits, [5, 7, 8, 11])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=6951377 length=257087 reads=[0, 1, 2, 3]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21649986 length=246089 reads=[4, 5, 6, 7]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36330670 length=246809 reads=[8, 9, 10, 11]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=50943729 length=243217 reads=[12, 13, 14, 15]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58956431 length=16301 reads=[16, 17, 18, 19]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=5 offset=6951377 length=53 dictionary=1 codec=1 fetch=0 pieces=[f0@0+53]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=7 offset=6951495 length=81950 dictionary=0 codec=1 fetch=0 pieces=[f0@0+81950]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=8 offset=7033445 length=31630 dictionary=1 codec=1 fetch=0 pieces=[f0@0+31630]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=11 offset=7191903 length=16561 dictionary=1 codec=1 fetch=0 pieces=[f0@0+16561]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=5 offset=21649986 length=53 dictionary=1 codec=1 fetch=1 pieces=[f1@0+53]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=7 offset=21650092 length=79041 dictionary=0 codec=1 fetch=1 pieces=[f1@0+79041]
read 6 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=8 offset=21729133 length=29522 dictionary=1 codec=1 fetch=1 pieces=[f1@0+29522]
read 7 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=11 offset=21880930 length=15145 dictionary=1 codec=1 fetch=1 pieces=[f1@0+15145]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=5 offset=36330670 length=53 dictionary=1 codec=1 fetch=2 pieces=[f2@0+53]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=7 offset=36330776 length=79241 dictionary=0 codec=1 fetch=2 pieces=[f2@0+79241]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=8 offset=36410017 length=29564 dictionary=1 codec=1 fetch=2 pieces=[f2@0+29564]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=11 offset=36562504 length=14975 dictionary=1 codec=1 fetch=2 pieces=[f2@0+14975]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=5 offset=50943729 length=53 dictionary=1 codec=1 fetch=3 pieces=[f3@0+53]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=7 offset=50943835 length=78318 dictionary=0 codec=1 fetch=3 pieces=[f3@0+78318]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=8 offset=51022153 length=28953 dictionary=1 codec=1 fetch=3 pieces=[f3@0+28953]
read 15 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=11 offset=51172358 length=14588 dictionary=1 codec=1 fetch=3 pieces=[f3@0+14588]
read 16 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=5 offset=58956431 length=53 dictionary=1 codec=1 fetch=4 pieces=[f4@0+53]
read 17 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=7 offset=58956537 length=5328 dictionary=1 codec=1 fetch=4 pieces=[f4@0+5328]
read 18 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=8 offset=58961865 length=2275 dictionary=1 codec=1 fetch=4 pieces=[f4@0+2275]
read 19 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=11 offset=58971539 length=1193 dictionary=1 codec=1 fetch=4 pieces=[f4@0+1193]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[5, 7, 8, 11] reads=[0, 1, 2, 3]; hits_subset_500k.parquet rg=1 rows=122880 columns=[5, 7, 8, 11] reads=[4, 5, 6, 7]] inputs=[0]
op 2 node=0 filter 5 gt i64:15888 inputs=[1]
op 3 node=0 partial_aggregate group=[11] approx_distinct(7), approx_distinct(8) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[5, 7, 8, 11] reads=[8, 9, 10, 11]; hits_subset_500k.parquet rg=3 rows=122880 columns=[5, 7, 8, 11] reads=[12, 13, 14, 15]; hits_subset_500k.parquet rg=4 rows=8480 columns=[5, 7, 8, 11] reads=[16, 17, 18, 19]] inputs=[4]
op 6 node=1 filter 5 gt i64:15888 inputs=[5]
op 7 node=1 partial_aggregate group=[11] approx_distinct(7), approx_distinct(8) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[11] approx_distinct(7), approx_distinct(8) inputs=[8]
metric node=0 logical=253955 fetched=503176 requests=2
metric node=1 logical=254594 fetched=506327 requests=3
metric requests=5 logical=508549 fetched=1009503 over_read=500954 over_read_ppm=496238
```

## S1

```sql
SELECT COUNT(URL), COUNT(*) FROM hits
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(hits.URL), count(*)]]
  TableScan: hits projection=[URL]
```

### WCOL logical

```text
Aggregate(group=[], count(13), count(*))
  Scan(hits, [13])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58973967 length=93079 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=0 pieces=[f0@0+1858606]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1834388]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=2 pieces=[f2@0+1838352]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1826481]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=4 pieces=[f4@0+93079]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[13] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[13] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] count(13), count(*) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[13] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[13] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[13] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] count(13), count(*) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] count(13), count(*) inputs=[6]
metric node=0 logical=3692994 fetched=3692994 requests=2
metric node=1 logical=3757912 fetched=3757912 requests=3
metric requests=5 logical=7450906 fetched=7450906 over_read=0 over_read_ppm=0
```

## S2

```sql
SELECT COUNT(DISTINCT URL) FROM hits
```

### DataFusion

```text
Projection: count(alias1) AS count(DISTINCT hits.URL)
  Aggregate: groupBy=[[]], aggr=[[count(alias1)]]
    Aggregate: groupBy=[[hits.URL AS alias1]], aggr=[[]]
      TableScan: hits projection=[URL]
```

### WCOL logical

```text
Aggregate(group=[], count(distinct 13))
  Scan(hits, [13])
```

### WCOL physical

```text
root 7
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58973967 length=93079 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=0 pieces=[f0@0+1858606]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1834388]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=2 pieces=[f2@0+1838352]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1826481]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=4 pieces=[f4@0+93079]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[13] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[13] reads=[1]] inputs=[0]
op 2 node=0 partial_aggregate group=[] count(distinct 13) inputs=[1]
op 3 node=1 r2_fetch [2, 3, 4] inputs=[]
op 4 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[13] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[13] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[13] reads=[4]] inputs=[3]
op 5 node=1 partial_aggregate group=[] count(distinct 13) inputs=[4]
op 6 node=coordinator exchange inputs=[2, 5]
op 7 node=coordinator final_aggregate group=[] count(distinct 13) inputs=[6]
metric node=0 logical=3692994 fetched=3692994 requests=2
metric node=1 logical=3757912 fetched=3757912 requests=3
metric requests=5 logical=7450906 fetched=7450906 over_read=0 over_read_ppm=0
```

## S4

```sql
SELECT SearchPhrase, COUNT(*) FROM hits WHERE SearchPhrase LIKE 'ведь%' GROUP BY SearchPhrase
```

### DataFusion

```text
Aggregate: groupBy=[[hits.SearchPhrase]], aggr=[[count(*)]]
  TableScan: hits projection=[SearchPhrase], full_filters=[hits.SearchPhrase LIKE Utf8("ведь%")]
```

### WCOL logical

```text
Aggregate(group=[39], count(*))
  Filter(39 like text:"ведь%")
    Scan(hits, [39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[39] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[39] reads=[1]] inputs=[0]
op 2 node=0 filter 39 like text:"ведь%" inputs=[1]
op 3 node=0 partial_aggregate group=[39] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[39] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[39] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[39] reads=[4]] inputs=[4]
op 6 node=1 filter 39 like text:"ведь%" inputs=[5]
op 7 node=1 partial_aggregate group=[39] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[39] count(*) inputs=[8]
metric node=0 logical=360161 fetched=360161 requests=2
metric node=1 logical=385709 fetched=385709 requests=3
metric requests=5 logical=745870 fetched=745870 over_read=0 over_read_ppm=0
```

## S5

```sql
SELECT COUNT(*) FROM hits WHERE SearchPhrase LIKE '_едь%'
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.SearchPhrase LIKE Utf8("_едь%")]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(39 like text:"_едь%")
    Scan(hits, [39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[39] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[39] reads=[1]] inputs=[0]
op 2 node=0 filter 39 like text:"_едь%" inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[39] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[39] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[39] reads=[4]] inputs=[4]
op 6 node=1 filter 39 like text:"_едь%" inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=360161 fetched=360161 requests=2
metric node=1 logical=385709 fetched=385709 requests=3
metric requests=5 logical=745870 fetched=745870 over_read=0 over_read_ppm=0
```

## S6

```sql
SELECT COUNT(*) FROM hits WHERE SearchPhrase = 'ведь'
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.SearchPhrase = Utf8("ведь")]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(39 eq text:"ведь")
    Scan(hits, [39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59194915 length=14164 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[39] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[39] reads=[1]] inputs=[0]
op 2 node=0 filter 39 eq text:"ведь" inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[39] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[39] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[39] reads=[4]] inputs=[4]
op 6 node=1 filter 39 eq text:"ведь" inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=360161 fetched=360161 requests=2
metric node=1 logical=385709 fetched=385709 requests=3
metric requests=5 logical=745870 fetched=745870 over_read=0 over_read_ppm=0
```

## S7

```sql
SELECT SearchPhrase, MobilePhoneModel, COUNT(*) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase, MobilePhoneModel
```

### DataFusion

```text
Aggregate: groupBy=[[hits.SearchPhrase, hits.MobilePhoneModel]], aggr=[[count(*)]]
  TableScan: hits projection=[MobilePhoneModel, SearchPhrase], full_filters=[hits.SearchPhrase != Utf8("")]
```

### WCOL logical

```text
Aggregate(group=[39, 34], count(*))
  Filter(39 ne text:"")
    Scan(hits, [34, 39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=11514812 length=292264 reads=[0, 1]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=26152069 length=273666 reads=[2, 3]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=40821507 length=280410 reads=[4, 5]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=55292227 length=290392 reads=[6, 7]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=59187227 length=21852 reads=[8, 9]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=34 offset=11514812 length=1362 dictionary=1 codec=1 fetch=0 pieces=[f0@0+1362]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=0 pieces=[f0@0+187940]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=34 offset=26152069 length=1106 dictionary=1 codec=1 fetch=1 pieces=[f1@0+1106]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=1 pieces=[f1@0+172221]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=34 offset=40821507 length=1293 dictionary=1 codec=1 fetch=2 pieces=[f2@0+1293]
read 5 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=2 pieces=[f2@0+179670]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=34 offset=55292227 length=1557 dictionary=1 codec=1 fetch=3 pieces=[f3@0+1557]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=3 pieces=[f3@0+191875]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=34 offset=59187227 length=146 dictionary=1 codec=1 fetch=4 pieces=[f4@0+146]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=4 pieces=[f4@0+14164]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[34, 39] reads=[0, 1]; hits_subset_500k.parquet rg=1 rows=122880 columns=[34, 39] reads=[2, 3]] inputs=[0]
op 2 node=0 filter 39 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[39, 34] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[34, 39] reads=[4, 5]; hits_subset_500k.parquet rg=3 rows=122880 columns=[34, 39] reads=[6, 7]; hits_subset_500k.parquet rg=4 rows=8480 columns=[34, 39] reads=[8, 9]] inputs=[4]
op 6 node=1 filter 39 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[39, 34] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[39, 34] count(*) inputs=[8]
metric node=0 logical=362629 fetched=565930 requests=2
metric node=1 logical=388705 fetched=592654 requests=3
metric requests=5 logical=751334 fetched=1158584 over_read=407250 over_read_ppm=351506
```

## S9

```sql
SELECT COUNT(*) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.SearchPhrase != Utf8(""), hits.Title LIKE Utf8("%Google%"), hits.URL NOT LIKE Utf8("%.google.%")]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(39 ne text:"" and 2 like text:"%Google%" and 13 not like text:"%.google.%")
    Scan(hits, [2, 13, 39])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=990475 length=2097152 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=3087627 length=2097152 reads=[0]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=5184779 length=1372537 reads=[0]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[1]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=11619136 length=187940 reads=[2]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=15912214 length=2097152 reads=[3]
fetch 6 node=0 object="hits_subset_500k.parquet" offset=18009366 length=2097152 reads=[3]
fetch 7 node=0 object="hits_subset_500k.parquet" offset=20106518 length=1150215 reads=[3]
fetch 8 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[4]
fetch 9 node=0 object="hits_subset_500k.parquet" offset=26253514 length=172221 reads=[5]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=30502434 length=2097152 reads=[6]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=32599586 length=2097152 reads=[6]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=34696738 length=1240654 reads=[6]
fetch 13 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[7]
fetch 14 node=1 object="hits_subset_500k.parquet" offset=40922247 length=179670 reads=[8]
fetch 15 node=1 object="hits_subset_500k.parquet" offset=45162159 length=2097152 reads=[9]
fetch 16 node=1 object="hits_subset_500k.parquet" offset=47259311 length=2097152 reads=[9]
fetch 17 node=1 object="hits_subset_500k.parquet" offset=49356463 length=1194948 reads=[9]
fetch 18 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[10]
fetch 19 node=1 object="hits_subset_500k.parquet" offset=55390744 length=191875 reads=[11]
fetch 20 node=1 object="hits_subset_500k.parquet" offset=58698148 length=510931 reads=[12, 13, 14]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=2 offset=990475 length=5566841 dictionary=0 codec=1 fetch=0 pieces=[f0@0+2097152,f1@2097152+2097152,f2@4194304+1372537]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1858606]
read 2 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=39 offset=11619136 length=187940 dictionary=1 codec=1 fetch=4 pieces=[f4@0+187940]
read 3 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=2 offset=15912214 length=5344519 dictionary=0 codec=1 fetch=5 pieces=[f5@0+2097152,f6@2097152+2097152,f7@4194304+1150215]
read 4 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=8 pieces=[f8@0+1834388]
read 5 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=39 offset=26253514 length=172221 dictionary=1 codec=1 fetch=9 pieces=[f9@0+172221]
read 6 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=2 offset=30502434 length=5434958 dictionary=0 codec=1 fetch=10 pieces=[f10@0+2097152,f11@2097152+2097152,f12@4194304+1240654]
read 7 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=13 pieces=[f13@0+1838352]
read 8 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=39 offset=40922247 length=179670 dictionary=1 codec=1 fetch=14 pieces=[f14@0+179670]
read 9 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=2 offset=45162159 length=5389252 dictionary=0 codec=1 fetch=15 pieces=[f15@0+2097152,f16@2097152+2097152,f17@4194304+1194948]
read 10 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=18 pieces=[f18@0+1826481]
read 11 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=39 offset=55390744 length=191875 dictionary=1 codec=1 fetch=19 pieces=[f19@0+191875]
read 12 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=2 offset=58698148 length=224923 dictionary=1 codec=1 fetch=20 pieces=[f20@0+224923]
read 13 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=20 pieces=[f20@0+93079]
read 14 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=39 offset=59194915 length=14164 dictionary=1 codec=1 fetch=20 pieces=[f20@0+14164]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[2, 13, 39] reads=[0, 1, 2]; hits_subset_500k.parquet rg=1 rows=122880 columns=[2, 13, 39] reads=[3, 4, 5]] inputs=[0]
op 2 node=0 filter 39 ne text:"" and 2 like text:"%Google%" and 13 not like text:"%.google.%" inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[2, 13, 39] reads=[6, 7, 8]; hits_subset_500k.parquet rg=3 rows=122880 columns=[2, 13, 39] reads=[9, 10, 11]; hits_subset_500k.parquet rg=4 rows=8480 columns=[2, 13, 39] reads=[12, 13, 14]] inputs=[4]
op 6 node=1 filter 39 ne text:"" and 2 like text:"%Google%" and 13 not like text:"%.google.%" inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=14964515 fetched=14964515 requests=10
metric node=1 logical=15192754 fetched=15371519 requests=11
metric requests=21 logical=30157269 fetched=30336034 over_read=178765 over_read_ppm=5892
```

## S10

```sql
SELECT Title, COUNT(*) FROM hits WHERE Title <> '' GROUP BY Title
```

### DataFusion

```text
Aggregate: groupBy=[[hits.Title]], aggr=[[count(*)]]
  TableScan: hits projection=[Title], full_filters=[hits.Title != Utf8("")]
```

### WCOL logical

```text
Aggregate(group=[2], count(*))
  Filter(2 ne text:"")
    Scan(hits, [2])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=990475 length=2097152 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=3087627 length=2097152 reads=[0]
fetch 2 node=0 object="hits_subset_500k.parquet" offset=5184779 length=1372537 reads=[0]
fetch 3 node=0 object="hits_subset_500k.parquet" offset=15912214 length=2097152 reads=[1]
fetch 4 node=0 object="hits_subset_500k.parquet" offset=18009366 length=2097152 reads=[1]
fetch 5 node=0 object="hits_subset_500k.parquet" offset=20106518 length=1150215 reads=[1]
fetch 6 node=1 object="hits_subset_500k.parquet" offset=30502434 length=2097152 reads=[2]
fetch 7 node=1 object="hits_subset_500k.parquet" offset=32599586 length=2097152 reads=[2]
fetch 8 node=1 object="hits_subset_500k.parquet" offset=34696738 length=1240654 reads=[2]
fetch 9 node=1 object="hits_subset_500k.parquet" offset=45162159 length=2097152 reads=[3]
fetch 10 node=1 object="hits_subset_500k.parquet" offset=47259311 length=2097152 reads=[3]
fetch 11 node=1 object="hits_subset_500k.parquet" offset=49356463 length=1194948 reads=[3]
fetch 12 node=1 object="hits_subset_500k.parquet" offset=58698148 length=224923 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=2 offset=990475 length=5566841 dictionary=0 codec=1 fetch=0 pieces=[f0@0+2097152,f1@2097152+2097152,f2@4194304+1372537]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=2 offset=15912214 length=5344519 dictionary=0 codec=1 fetch=3 pieces=[f3@0+2097152,f4@2097152+2097152,f5@4194304+1150215]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=2 offset=30502434 length=5434958 dictionary=0 codec=1 fetch=6 pieces=[f6@0+2097152,f7@2097152+2097152,f8@4194304+1240654]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=2 offset=45162159 length=5389252 dictionary=0 codec=1 fetch=9 pieces=[f9@0+2097152,f10@2097152+2097152,f11@4194304+1194948]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=2 offset=58698148 length=224923 dictionary=1 codec=1 fetch=12 pieces=[f12@0+224923]
op 0 node=0 r2_fetch [0, 1, 2, 3, 4, 5] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[2] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[2] reads=[1]] inputs=[0]
op 2 node=0 filter 2 ne text:"" inputs=[1]
op 3 node=0 partial_aggregate group=[2] count(*) inputs=[2]
op 4 node=1 r2_fetch [6, 7, 8, 9, 10, 11, 12] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[2] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[2] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[2] reads=[4]] inputs=[4]
op 6 node=1 filter 2 ne text:"" inputs=[5]
op 7 node=1 partial_aggregate group=[2] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[2] count(*) inputs=[8]
metric node=0 logical=10911360 fetched=10911360 requests=6
metric node=1 logical=11049133 fetched=11049133 requests=7
metric requests=13 logical=21960493 fetched=21960493 over_read=0 over_read_ppm=0
```

## S11

```sql
SELECT COUNT(*) FROM hits WHERE URL LIKE '%a%a%a%a%a%a%q%'
```

### DataFusion

```text
Aggregate: groupBy=[[]], aggr=[[count(*)]]
  TableScan: hits projection=[], full_filters=[hits.URL LIKE Utf8("%a%a%a%a%a%a%q%")]
```

### WCOL logical

```text
Aggregate(group=[], count(*))
  Filter(13 like text:"%a%a%a%a%a%a%q%")
    Scan(hits, [13])
```

### WCOL physical

```text
root 9
fetch 0 node=0 object="hits_subset_500k.parquet" offset=7227902 length=1858606 reads=[0]
fetch 1 node=0 object="hits_subset_500k.parquet" offset=21914292 length=1834388 reads=[1]
fetch 2 node=1 object="hits_subset_500k.parquet" offset=36595661 length=1838352 reads=[2]
fetch 3 node=1 object="hits_subset_500k.parquet" offset=51205740 length=1826481 reads=[3]
fetch 4 node=1 object="hits_subset_500k.parquet" offset=58973967 length=93079 reads=[4]
read 0 scan=0 node=0 object="hits_subset_500k.parquet" row_group=0 column=13 offset=7227902 length=1858606 dictionary=0 codec=1 fetch=0 pieces=[f0@0+1858606]
read 1 scan=0 node=0 object="hits_subset_500k.parquet" row_group=1 column=13 offset=21914292 length=1834388 dictionary=0 codec=1 fetch=1 pieces=[f1@0+1834388]
read 2 scan=0 node=1 object="hits_subset_500k.parquet" row_group=2 column=13 offset=36595661 length=1838352 dictionary=0 codec=1 fetch=2 pieces=[f2@0+1838352]
read 3 scan=0 node=1 object="hits_subset_500k.parquet" row_group=3 column=13 offset=51205740 length=1826481 dictionary=0 codec=1 fetch=3 pieces=[f3@0+1826481]
read 4 scan=0 node=1 object="hits_subset_500k.parquet" row_group=4 column=13 offset=58973967 length=93079 dictionary=1 codec=1 fetch=4 pieces=[f4@0+93079]
op 0 node=0 r2_fetch [0, 1] inputs=[]
op 1 node=0 decode [hits_subset_500k.parquet rg=0 rows=122880 columns=[13] reads=[0]; hits_subset_500k.parquet rg=1 rows=122880 columns=[13] reads=[1]] inputs=[0]
op 2 node=0 filter 13 like text:"%a%a%a%a%a%a%q%" inputs=[1]
op 3 node=0 partial_aggregate group=[] count(*) inputs=[2]
op 4 node=1 r2_fetch [2, 3, 4] inputs=[]
op 5 node=1 decode [hits_subset_500k.parquet rg=2 rows=122880 columns=[13] reads=[2]; hits_subset_500k.parquet rg=3 rows=122880 columns=[13] reads=[3]; hits_subset_500k.parquet rg=4 rows=8480 columns=[13] reads=[4]] inputs=[4]
op 6 node=1 filter 13 like text:"%a%a%a%a%a%a%q%" inputs=[5]
op 7 node=1 partial_aggregate group=[] count(*) inputs=[6]
op 8 node=coordinator exchange inputs=[3, 7]
op 9 node=coordinator final_aggregate group=[] count(*) inputs=[8]
metric node=0 logical=3692994 fetched=3692994 requests=2
metric node=1 logical=3757912 fetched=3757912 requests=3
metric requests=5 logical=7450906 fetched=7450906 over_read=0 over_read_ppm=0
```

## Not lowered

| Query | Stopped at | Reason |
| --- | --- | --- |
| Q18 | DataFusion | plan: Error during planning: Invalid function 'todatetime'. Did you mean 'count'? |
| Q29 | WCOL logical | unsupported expression: projection is not a column list |
| Q35 | WCOL logical | unsupported expression: projection is not a column or aggregate list |
| Q40 | WCOL logical | unsupported expression: compare needs a column and a literal |
| Q42 | DataFusion | plan: Error during planning: Invalid function 'date_trunc'. Did you mean 'count'? |
| D2 | WCOL logical | unsupported expression: compare needs a column and a literal |
| D4 | WCOL logical | unsupported expression: binary Plus |
| S3 | WCOL logical | unsupported expression: compare needs a column and a literal |
| S8 | WCOL logical | unsupported expression: Case(Case { expr: Some(IsNotNull(Column(Column { relation: Some(Bare { table: "hits" }), name: "URL" }))), when_then_expr: [(Literal(Boolean(true), None), Literal(Boolean(true), None))], else_expr: None }) |
