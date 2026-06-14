# Demo datasets

Bundled `.wcol` files are staged here by `npm run prepare:datasets` (also runs before `build:demo`).

| File | Kind | Rows | Queries |
|------|------|------|---------|
| `crates_versions.wcol` | crates | ~2.4M | rankings, search, profiles |
| `crates_dependencies.wcol` | dependencies | ~27M | dependency graph, reverse-deps |
| `crates_categories.wcol` | categories | ~237k | category rankings, browse |
| `crate_maintainers.wcol` | maintainers | ~307k | owner search, portfolios |
| `hits_subset_500k.wcol` | hits (ClickBench) | 500k | filters, group-by, SELECT |

## Build from CSV dump

```bash
./scripts/prepare-crates-parquet.sh   # duckdb → data/*.parquet
./scripts/encode-datasets.sh        # parquet → .wcol (uses ../wcol/rust wcol-cli)
npm run prepare:datasets
```

Set `WCOL_CLI_ROOT` if wcol-cli lives outside `../wcol/rust`.

Future: `version_downloads_daily.parquet` for download trends (encode when needed).
