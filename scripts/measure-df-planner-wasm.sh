#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)/rust"
TARGET=wasm32-unknown-unknown
PROFILE="${1:-wasm-size}"
WASM="$ROOT/target/$TARGET/$PROFILE/df_planner_wasm.wasm"

cd "$ROOT"
cargo build -p df-planner-wasm --target "$TARGET" --profile "$PROFILE"

bytes=$(wc -c < "$WASM" | tr -d ' ')
gzip_bytes=$(gzip -c "$WASM" | wc -c | tr -d ' ')
echo "profile: $PROFILE"
echo "path:    $WASM"
mib() { awk -v b="$1" 'BEGIN { printf "%.2f", b / 1024 / 1024 }'; }
echo "raw:     $bytes bytes ($(mib "$bytes") MiB)"
echo "gzip:    $gzip_bytes bytes ($(mib "$gzip_bytes") MiB)"
