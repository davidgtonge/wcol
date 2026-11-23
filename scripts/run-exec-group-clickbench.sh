#!/usr/bin/env bash
# ClickBench Q8/Q3/Q1 through Rust ExecutionGroup.
#
# Local full file:
#   HITS_PARQUET=/path/to/hits.parquet ./scripts/run-exec-group-clickbench.sh
#
# Full hits over HTTP (no local download) — use the TS harness instead:
#   npx tsx scripts/clickbench.ts --query q8
#   npx tsx scripts/clickbench.ts --query q3 --max-row-groups 10
# Default URL: https://datasets.clickhouse.com/hits_compatible/hits.parquet
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HITS="${HITS_PARQUET:-}"
if [[ -z "$HITS" || ! -f "$HITS" ]]; then
  for candidate in \
    "$ROOT/data/hits_subset_500k.parquet" \
    "$ROOT/data/hits_subset_2m.parquet" \
    "$ROOT/data/hits_subset.parquet"; do
    if [[ -f "$candidate" ]]; then
      HITS="$candidate"
      echo "using local subset: $HITS" >&2
      break
    fi
  done
fi
if [[ -z "$HITS" || ! -f "$HITS" ]]; then
  echo "Set HITS_PARQUET to a local hits parquet, or run:" >&2
  echo "  npx tsx scripts/clickbench.ts --query q8 [--max-row-groups N]" >&2
  exit 1
fi
export HITS_PARQUET="$HITS"
cd "$ROOT/rust"
cargo test -p wcol --test clickbench_exec_group -- --nocapture
