#!/usr/bin/env bash
# Docker-free local install/build for AkiDB (GAP-024).
# Builds the akidb CLI and installs it to PREFIX/bin (default: ~/.local/bin).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PREFIX="${PREFIX:-$HOME/.local}"
BIN_DIR="$PREFIX/bin"

echo "=== AkiDB local install ==="
echo "Project: $PROJECT_ROOT"
echo "Install: $BIN_DIR/akidb"

cd "$PROJECT_ROOT"
cargo build -p akidb-cli --release
mkdir -p "$BIN_DIR"
install -m 755 "$PROJECT_ROOT/target/release/akidb" "$BIN_DIR/akidb"

echo "Installed: $BIN_DIR/akidb"
echo "Ensure $BIN_DIR is on PATH, then:"
echo "  akidb server --standalone --config config/default.toml"
echo "Or use: $SCRIPT_DIR/akidb-start.sh"
