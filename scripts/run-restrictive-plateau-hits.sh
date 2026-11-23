#!/usr/bin/env bash
# I-09 P0: 226-RG Q3 restrictive plateau.
# Full ClickBench hits is the HTTP object (range reads). Set HITS_PARQUET to run the native harness instead.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ -n "${HITS_PARQUET:-}" && -f "$HITS_PARQUET" ]]; then
  cd "$ROOT/rust"
  echo "== restrictive plateau (native file): $HITS_PARQUET =="
  cargo test -p wcol --features memory-plateau --test memory_plateau \
    q3_memory_plateau_restrictive_budget -- --ignored --nocapture
  echo "== I-09 restrictive gate: PASS (native) =="
  exit 0
fi

cd "$ROOT"
echo "== restrictive plateau (HTTP full hits) =="
HITS_HTTP_RESTRICTIVE=1 npx tsx --test tests/hits-http-restrictive-plateau.test.ts
echo "== I-09 restrictive gate: PASS (HTTP) =="
