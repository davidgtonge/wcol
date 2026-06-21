# Demo datasets

Five `.wcol` fixtures are **committed in git** (~50 MB total) so the explorer demo works from a fresh clone and in CI.

| File | Kind | Rows | Queries |
|------|------|------|---------|
| `hits_subset_500k.wcol` | hits (ClickBench) | 500k | filters, group-by, SELECT |
| `trends_crate_downloads_30d.wcol` | trends rollup | ~271k | fastest-growing rankings |
| `trends_serde_version_downloads.wcol` | trends rollup | ~315 | serde version adoption |
| `crates_categories.wcol` | categories | ~237k | category rankings, browse |
| `crate_maintainers.wcol` | maintainers | ~307k | owner search, portfolios |

`npm run prepare:datasets` copies these from `data/` when you have larger local encodes; otherwise the committed copies are kept.

## Optional full datasets (not in git)

Larger tables (`crates_versions`, `crates_dependencies`, `version_downloads_daily`) can be generated under `data/` for local development:

```bash
./scripts/prepare-crates-parquet.sh   # duckdb → data/*.parquet
./scripts/encode-datasets.sh        # parquet → .wcol
npm run prepare:datasets -w @wcol/explorer
```

Set `WCOL_CLI_ROOT` if wcol-cli lives outside `rust/`.
