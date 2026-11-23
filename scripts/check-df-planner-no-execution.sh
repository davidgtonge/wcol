#!/usr/bin/env bash
# Fail if planner wasm pulls execution / object_store (physical planner stack).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)/rust"
cd "$ROOT"
TARGET=wasm32-unknown-unknown
PKG=df-planner-wasm
bad=0
for dep in object_store datafusion-execution datafusion-functions datafusion-functions-aggregate datafusion-functions-nested; do
  if cargo tree -p "$PKG" --target "$TARGET" -i "$dep" &>/dev/null; then
    echo "FAIL: $dep is in the $PKG tree"
    cargo tree -p "$PKG" --target "$TARGET" -i "$dep" | head -5
    bad=1
  fi
done
if [[ "$bad" -eq 0 ]]; then
  echo "OK: no execution/object_store/functions stack in $PKG tree"
fi
exit "$bad"
