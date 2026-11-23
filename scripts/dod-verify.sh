#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/rust"

cargo test -p wcol \
  --test clickbench_exec_group \
  --test typed_colocated_graph \
  --test vertical_slice \
  --test serialization_boundaries \
  --test opcode10_backpressure \
  --test distributed_semantics \
  --test ingest_boundaries \
  --test pipeline_json \
  --test i15_single_impl \
  --test dod_acceptance \
  --test legacy_paths_removed \
  --test memory_plateau

cargo test -p wcol --features memory-plateau --test memory_plateau

cd "$ROOT"
npm run build:wasm
npm test
npm run test:hits-http-full-q1
npm run test:hits-http-exec -- --query all --max-row-groups 3
HITS_HTTP_CLICKBENCH=1 npm run test:hits-http-clickbench

echo "DoD verify: OK"
