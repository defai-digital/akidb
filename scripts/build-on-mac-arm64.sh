#!/bin/bash
# Build and validate AkiDB on macOS Apple Silicon (M2 or later).

set -euo pipefail

echo "=== Building AkiDB on macOS Apple Silicon ==="

if [ "$(uname -s)" != "Darwin" ]; then
    echo "Error: this script is intended for macOS." >&2
    exit 1
fi

if [ "$(uname -m)" != "arm64" ]; then
    echo "Error: this script requires Apple Silicon arm64 hardware." >&2
    exit 1
fi

if ! command -v rustc >/dev/null 2>&1; then
    echo "Error: rustc not found. Install Rust with rustup first." >&2
    exit 1
fi

echo "Rust: $(rustc --version)"
echo "Target: $(rustc -vV | awk -F': ' '/host/ {print $2}')"

if [ "${AKIDB_ENABLE_GPU:-}" = "1" ]; then
    echo "Error: NVIDIA CUDA GPU mode is not supported on macOS." >&2
    echo "Use the default CPU/portable build on Apple Silicon." >&2
    exit 1
fi

cargo check --workspace --features cpu
cargo test --workspace --features cpu
cargo build --release -p akidb-server --features cpu

echo ""
echo "=== macOS Apple Silicon build complete ==="
echo "Binary: target/release/akidb-server"
