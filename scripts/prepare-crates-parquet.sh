#!/usr/bin/env bash
# Build denormalized parquet tables from the crates.io CSV dump for wcol encoding.
# Requires: duckdb CLI on PATH.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DUMP="${WCOL_CRATES_DUMP:-$ROOT/data/crates-dump/2026-06-03-020029/data}"
OUT="$ROOT/data"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI not found — install from https://duckdb.org/docs/installation/" >&2
  exit 1
fi

if [[ ! -f "$DUMP/versions.csv" ]]; then
  echo "Missing crates dump at $DUMP" >&2
  exit 1
fi

mkdir -p "$OUT"

echo "==> crates_versions.parquet (versions + crate names)"
duckdb -c "
COPY (
  SELECT
    v.id AS version_id,
    v.crate_id,
    c.name AS crate_name,
    regexp_replace(v.num, '^\\s+|\\s+\$', '') AS version,
    v.downloads,
    v.crate_size,
    v.created_at AS published_date,
    v.yanked,
    v.edition,
    v.license,
    v.has_lib,
    v.rust_version,
    c.downloads AS crate_total_downloads,
    v.num_no_build AS dep_count
  FROM read_csv('$DUMP/versions.csv', header=true) v
  JOIN read_csv('$DUMP/crates.csv', header=true) c ON c.id = v.crate_id
) TO '$OUT/crates_versions.parquet' (FORMAT PARQUET);
"

echo "==> crates_dependencies.parquet (denormalized edges)"
duckdb -c "
COPY (
  SELECT
    d.id AS dependency_id,
    d.version_id,
    v.crate_id AS parent_crate_id,
    pc.name AS parent_crate_name,
    dc.id AS dep_crate_id,
    COALESCE(NULLIF(d.explicit_name, ''), d.req) AS dep_crate_name,
    d.kind,
    d.optional
  FROM read_csv('$DUMP/dependencies.csv', header=true) d
  JOIN read_csv('$DUMP/versions.csv', header=true) v ON v.id = d.version_id
  JOIN read_csv('$DUMP/crates.csv', header=true) pc ON pc.id = v.crate_id
  LEFT JOIN read_csv('$DUMP/crates.csv', header=true) dc ON dc.name = COALESCE(NULLIF(d.explicit_name, ''), split_part(d.req, ' ', 1))
) TO '$OUT/crates_dependencies.parquet' (FORMAT PARQUET);
"

echo "==> crates_categories.parquet (crate × category, for future encode)"
duckdb -c "
COPY (
  SELECT
    c.id AS crate_id,
    c.name AS crate_name,
    cat.slug AS category_slug,
    cat.category AS category_name,
    COALESCE(cd.downloads, 0) AS crate_downloads
  FROM read_csv('$DUMP/crates.csv', header=true) c
  JOIN read_csv('$DUMP/crates_categories.csv', header=true) cc ON cc.crate_id = c.id
  JOIN read_csv('$DUMP/categories.csv', header=true) cat ON cat.id = cc.category_id
  LEFT JOIN read_csv('$DUMP/crate_downloads.csv', header=true) cd ON cd.crate_id = c.id
) TO '$OUT/crates_categories.parquet' (FORMAT PARQUET);
"

echo "==> crate_maintainers.parquet (owners × users, for future encode)"
duckdb -c "
COPY (
  SELECT
    c.id AS crate_id,
    c.name AS crate_name,
    u.gh_login AS owner_login,
    u.name AS owner_name,
    co.owner_kind,
    COALESCE(cd.downloads, 0) AS crate_downloads
  FROM read_csv('$DUMP/crate_owners.csv', header=true) co
  JOIN read_csv('$DUMP/crates.csv', header=true) c ON c.id = co.crate_id
  JOIN read_csv('$DUMP/users.csv', header=true) u ON u.id = co.owner_id
  LEFT JOIN read_csv('$DUMP/crate_downloads.csv', header=true) cd ON cd.crate_id = c.id
) TO '$OUT/crate_maintainers.parquet' (FORMAT PARQUET);
"

echo "==> version_downloads_daily.parquet (daily download time series)"
if [[ ! -f "$OUT/crates_versions.parquet" ]]; then
  echo "  requires $OUT/crates_versions.parquet — run the versions step first" >&2
  exit 1
fi
duckdb -c "
COPY (
  SELECT
    vd.version_id,
    cv.crate_id,
    cv.crate_name,
    cv.version,
    vd.date,
    vd.downloads
  FROM read_csv('$DUMP/version_downloads.csv', header=true) vd
  JOIN read_parquet('$OUT/crates_versions.parquet') cv ON cv.version_id = vd.version_id
  ORDER BY vd.date, vd.version_id
) TO '$OUT/version_downloads_daily.parquet' (FORMAT PARQUET);
"

echo "==> trends rollups (pre-aggregated for fast ranking presets)"
duckdb -c "
COPY (
  SELECT crate_name, CAST(SUM(downloads) AS BIGINT) AS downloads
  FROM read_parquet('$OUT/version_downloads_daily.parquet')
  WHERE date >= DATE '2026-05-04'
  GROUP BY crate_name
  ORDER BY downloads DESC
) TO '$OUT/trends_crate_downloads_30d.parquet' (FORMAT PARQUET);

COPY (
  SELECT version, CAST(SUM(downloads) AS BIGINT) AS downloads
  FROM read_parquet('$OUT/version_downloads_daily.parquet')
  WHERE crate_name = 'serde'
  GROUP BY version
  ORDER BY downloads DESC
) TO '$OUT/trends_serde_version_downloads.parquet' (FORMAT PARQUET);
"

echo ""
echo "Parquet files written to $OUT/"
echo "Encode to .wcol when wcol-cli is available:"
echo "  cargo run --manifest-path rust/wcol-cli/Cargo.toml -- $OUT/<name>.parquet -o $OUT/<name>.wcol"
echo "Then: npm run prepare:datasets"
