#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST="$ROOT/rust"

echo "== native memory_plateau (flat fixture + alloc probes when feature enabled) =="
cargo test -p wcol --manifest-path "$RUST/Cargo.toml" \
  --features memory-plateau \
  --test memory_plateau \
  -- --nocapture

if [[ -n "${HITS_PARQUET:-}" && -f "${HITS_PARQUET}" ]]; then
  echo "== Q3 baseline harness (226 row groups) =="
  HITS_PARQUET="$HITS_PARQUET" WCOL_I05_ASSERT_PLATEAU=1 cargo test -p wcol --manifest-path "$RUST/Cargo.toml" \
    --features memory-plateau \
    --test memory_plateau q3_memory_plateau_baseline_harness \
    -- --ignored --nocapture
else
  echo "skip Q3 harness (set HITS_PARQUET to a hits.parquet path)"
fi

echo "== wasm32 release build (linear memory probes run inside Wasm on DO) =="
rustup target add wasm32-unknown-unknown 2>/dev/null || true
cargo build -p wcol --manifest-path "$RUST/Cargo.toml" \
  --target wasm32-unknown-unknown --release

echo "done"
