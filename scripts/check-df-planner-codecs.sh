#!/usr/bin/env bash
# Fail if planner wasm pulls parquet/IPC compression (should be SQL→logical only).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)/rust"
cd "$ROOT"
TARGET=wasm32-unknown-unknown
PKG=df-planner-wasm
bad=0
for codec in zstd zstd-sys lz4_flex arrow-csv arrow-json; do
  if cargo tree -p "$PKG" --target "$TARGET" -i "$codec" &>/dev/null; then
    echo "FAIL: $codec is in the $PKG dependency tree"
    cargo tree -p "$PKG" --target "$TARGET" -i "$codec" | head -5
    bad=1
  fi
done
if [[ "$bad" -eq 0 ]]; then
  echo "OK: no zstd/lz4/csv/json format crates in $PKG tree"
fi
exit "$bad"
