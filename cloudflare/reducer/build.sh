#!/bin/bash
# Build the native reducer (`rust/wcol-reducer`) for Cloudflare Containers, which run
# linux/amd64. It cross-compiles in a native arm64 Rust image, so nothing runs under
# emulation. The binary is static (musl, linked by Rust's bundled lld), so the image needs
# no base OS: the Dockerfile copies it into `scratch`.
#
# The build mounts this repository only; its dependencies come from crates.io.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
target=x86_64-unknown-linux-musl
docker run --rm \
  -v "$repo:/src" \
  -v wcol-reducer-cargo:/usr/local/cargo/registry \
  -w /src/rust \
  rust:1.94.0-slim-bookworm \
  bash -c "rustup target add $target >/dev/null &&
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
      cargo build --release -p wcol-reducer --target $target --target-dir target-linux"
mkdir -p "$here/bin"
cp "$repo/rust/target-linux/$target/release/wcol-reducer" "$here/bin/"
echo "built $here/bin/wcol-reducer"
