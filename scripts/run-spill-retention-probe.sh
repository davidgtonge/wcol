#!/usr/bin/env bash
# Native spill retention probe with DHAT (requires nightly + dhat feature wiring).
# Example:
#   cd rust && cargo +nightly run -p wcol --features alloc-track-native --test spill_groups -- spill_memory_plateau_under_combined_budget -- --nocapture
#
# For a full host-path reproduction, use the Wasm lifecycle test instead:
#   npm run build:node-wasm && npx tsx --test tests/reducer-spill-lifecycle.test.ts

set -euo pipefail
cd "$(dirname "$0")/../rust"
cargo test -p wcol --test spill_groups spill_memory_plateau_under_combined_budget -- --nocapture
