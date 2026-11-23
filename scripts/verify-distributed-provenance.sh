#!/usr/bin/env bash
# Clean source → wasm32 build, then full distributed HTTP acceptance (226 RG Q3/Q8).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/rust"
echo "== cargo build wcol wasm32 release =="
cargo build -p wcol --target wasm32-unknown-unknown --release
WASM="$ROOT/rust/target/wasm32-unknown-unknown/release/wcol.wasm"
test -f "$WASM" || { echo "missing $WASM"; exit 1; }
echo "== wasm artifact =="
ls -la "$WASM"
shasum -a 256 "$WASM"
cd "$ROOT"
echo "== distributed acceptance =="
HITS_HTTP_DISTRIBUTED=1 npx tsx --test tests/hits-http-distributed-acceptance.test.ts
