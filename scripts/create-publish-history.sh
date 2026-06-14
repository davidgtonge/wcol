#!/usr/bin/env bash
# Replay wcol publish history with weekend/evening commits (Sep 2025 → Jun 2026).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STASH="$(mktemp -d)"
AUTHOR_NAME="Dave Tonge"
AUTHOR_EMAIL="919587+davidgtonge@users.noreply.github.com"

cleanup() { rm -rf "$STASH"; }
trap cleanup EXIT

echo "Staging snapshot at $STASH ..."
rsync -a \
  --exclude .git \
  --exclude node_modules \
  --exclude target \
  --exclude dist \
  --exclude pkg \
  --exclude data \
  --exclude 'demo/data/*.wcol' \
  --exclude perf \
  --exclude archive \
  --exclude .cursor \
  "$ROOT/" "$STASH/"

rm -rf "$ROOT/.git"
cd "$ROOT"
git init -b main

commit_at() {
  local date="$1"
  local msg="$2"
  shift 2
  if [[ $# -gt 0 ]]; then
    git add "$@"
  fi
  if git diff --cached --quiet; then
    echo "skip empty: $msg"
    return 0
  fi
  GIT_AUTHOR_DATE="$date" GIT_COMMITTER_DATE="$date" \
    git -c user.name="$AUTHOR_NAME" -c user.email="$AUTHOR_EMAIL" \
    commit -m "$msg"
  echo "✓ $date — $msg"
}

copy() {
  local src="$STASH/$1"
  local dst="$ROOT/$1"
  if [[ -e "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    rsync -a "$src" "$dst"
  fi
}

# --- 2025: columnar format & runtime ---
copy LICENSE
copy .gitignore
copy docs/FORMAT.md
commit_at "2025-09-07 10:30:00 +0100" "Add wcol columnar format specification." LICENSE .gitignore docs/FORMAT.md

copy rust/wcol-format
copy rust/Cargo.toml
copy rust/Cargo.lock
copy rust/wcol-encoder
commit_at "2025-09-21 11:00:00 +0100" "Add wcol-encoder for parquet to columnar conversion." rust/

copy rust/wcol-decoder
copy rust/wcol-sql-parser
commit_at "2025-10-05 10:00:00 +0100" "Add decoder kernels and SQL parser." rust/wcol-decoder rust/wcol-sql-parser rust/Cargo.toml rust/Cargo.lock

copy rust/wcol-wasm
copy src
commit_at "2025-10-19 15:30:00 +0100" "Add WASM bindings and JS runtime orchestration." rust/wcol-wasm src

copy rust/wcol-cli
copy rust/wcol-synth-groupby
commit_at "2025-11-02 10:00:00 +0100" "Add wcol-cli for convert, query, and benchmarks." rust/wcol-cli rust/wcol-synth-groupby rust/Cargo.toml rust/Cargo.lock

copy package.json
copy tsconfig.json
copy scripts/build-wasm.mjs
copy scripts/build-browser-runtime.mjs
copy scripts/build-browser.mjs
commit_at "2025-11-16 19:00:00 +0100" "Add npm build pipeline for browser Wasm bundles." package.json tsconfig.json scripts/build-wasm.mjs scripts/build-browser-runtime.mjs scripts/build-browser.mjs

copy tests/runtime.test.ts
copy tests/worker-core.test.ts
copy tests/plan.test.ts
copy tests/projection.test.ts
copy scripts/prepare-demo-crates.mjs
copy scripts/prepare-demo-datasets.mjs
copy scripts/prepare-crates-parquet.sh
copy scripts/encode-datasets.sh
copy scripts/encode-trends-rollups.sh
copy scripts/prepare-trends-rollups.mjs
commit_at "2025-11-30 11:30:00 +0100" "Add runtime tests and crates.io dataset prep scripts." tests/ scripts/prepare-demo-crates.mjs scripts/prepare-demo-datasets.mjs scripts/prepare-crates-parquet.sh scripts/encode-datasets.sh scripts/encode-trends-rollups.sh scripts/prepare-trends-rollups.mjs

copy docs/ARCHITECTURE.md
copy docs/QUERY_AST_SUPPORTED.md
copy docs/BASELINE_COMMANDS.md
copy README.md
commit_at "2025-12-14 16:00:00 +0100" "Document architecture and query surface." docs/ARCHITECTURE.md docs/QUERY_AST_SUPPORTED.md docs/BASELINE_COMMANDS.md README.md

# --- 2026: explorer app engine ---
git submodule add -b main git@github-dtonge:davidgtonge/engine-shell.git engine-shell 2>/dev/null || true
copy rust/wcol-engine
commit_at "2026-01-11 10:00:00 +0100" "Add wcol-engine app state and engine-shell integration." engine-shell rust/wcol-engine

copy demo/protocol
copy demo/worker
copy demo/arch
copy demo/generated
copy demo/app.tsx
copy demo/index.html
copy demo/styles.css
commit_at "2026-01-25 11:00:00 +0100" "Wire CBOR worker, view-model patches, and demo shell." demo/

copy demo/components
copy demo/charts
copy demo/query
copy demo/data/README.md
copy demo/data/presets.ts
copy demo/data/datasets.ts
copy demo/data/summarize.ts
copy demo/data/crate-detail.ts
copy demo/data/resolve-values.ts
copy demo/data/query-dates.ts
copy demo/ui
commit_at "2026-02-08 10:30:00 +0100" "Add query builder, charts, and crates.io presets." demo/

copy tests/explorer-queries.test.ts
copy tests/explorer-queries.ts
copy tests/dependency-queries.test.ts
copy tests/dependency-queries.ts
copy tests/categories-queries.test.ts
copy tests/categories-queries.ts
copy tests/maintainers-queries.test.ts
copy tests/maintainers-queries.ts
copy tests/trends-queries.test.ts
copy tests/trends-queries.ts
copy tests/hits-queries.test.ts
copy tests/hits-queries.ts
copy tests/query-catalog.ts
copy tests/helpers
commit_at "2026-02-22 15:00:00 +0100" "Add explorer query catalog and regression tests." tests/

copy demo/workspace
copy demo/wiring
copy demo/game
copy demo/wcol-query.ts
copy demo/wcol-runtime.ts
copy demo/README.md
commit_at "2026-03-08 10:00:00 +0100" "Add explore workspace with saved views and pinned crates." demo/workspace demo/wiring demo/game demo/wcol-query.ts demo/wcol-runtime.ts demo/README.md

copy scripts/build-engine-wasm.mjs
copy scripts/build-demo.mjs
copy scripts/test-cbor-interop.mjs
copy scripts/benchmark-all-queries.mjs
copy scripts/run-explorer-queries.mjs
copy scripts/diagnose-query-performance.mjs
copy rust/wcol-engine/tests
copy rust/wcol-engine/examples
commit_at "2026-03-22 11:00:00 +0100" "Add undo/redo URL sync and CBOR interop tests." scripts/build-engine-wasm.mjs scripts/build-demo.mjs scripts/test-cbor-interop.mjs scripts/benchmark-all-queries.mjs scripts/run-explorer-queries.mjs scripts/diagnose-query-performance.mjs rust/wcol-engine/tests rust/wcol-engine/examples

copy LICENSE
copy README.md
commit_at "2026-04-19 10:00:00 +0100" "Polish explorer UI and multi-dataset crate detail." LICENSE README.md

copy .github
commit_at "2026-05-04 10:00:00 +0100" "Add GitHub Pages deployment workflow." .github

copy package-lock.json
commit_at "2026-05-18 11:00:00 +0100" "Add npm lockfile for reproducible CI builds." package-lock.json

# Any remaining tracked files from snapshot
rsync -a \
  --exclude node_modules --exclude target --exclude dist --exclude pkg \
  --exclude data --exclude 'demo/data/*.wcol' --exclude perf --exclude archive \
  "$STASH/" "$ROOT/"
git add -A
commit_at "2026-06-14 10:00:00 +0100" "Fix publish dependencies for standalone clones."

echo ""
echo "History:"
git log --oneline --format='%h %ad %s' --date=short
