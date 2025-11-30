#!/usr/bin/env bash
# Encode trends rollup parquet only (after prepare-trends-rollups.mjs).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI_ROOT="${WCOL_CLI_ROOT:-$(cd "$ROOT/../wcol/rust" 2>/dev/null && pwd || cd "$ROOT/rust" 2>/dev/null && pwd || true)}"
OUT="$ROOT/data"

if [[ ! -f "$CLI_ROOT/Cargo.toml" ]]; then
  echo "wcol-cli not found. Set WCOL_CLI_ROOT to the rust/ dir containing wcol-cli." >&2
  exit 1
fi

encode() {
  local parquet="$1"
  local wcol="$2"
  if [[ ! -f "$parquet" ]]; then
    echo "skip  $wcol (missing $parquet — run npm run prepare:rollups)" >&2
    return 0
  fi
  echo "==> $wcol"
  (cd "$CLI_ROOT" && cargo run --release -p wcol-cli -- "$parquet" -o "$wcol")
}

encode "$OUT/trends_crate_downloads_30d.parquet" "$OUT/trends_crate_downloads_30d.wcol"
encode "$OUT/trends_serde_version_downloads.parquet" "$OUT/trends_serde_version_downloads.wcol"

echo "Done. Stage with: npm run prepare:datasets"
