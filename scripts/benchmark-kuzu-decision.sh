#!/usr/bin/env bash
# Run a native-vs-Kuzu graph benchmark and validate the decision artifact.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NODES="${NODES:-1000}"
EDGES="${EDGES:-5000}"
QUERIES_PER_KIND="${QUERIES_PER_KIND:-250}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_ROOT/docs/reports}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT="${OUTPUT:-$OUTPUT_DIR/kuzu-decision-${STAMP}.json}"
WORK_DIR="${WORK_DIR:-${TMPDIR:-/tmp}/akidb-kuzu-bench-${STAMP}-$$}"
KEEP_WORK_DIR="${KEEP_WORK_DIR:-0}"

cleanup() {
    if [ "$KEEP_WORK_DIR" != "1" ]; then
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

export KUZU_SHARED="${KUZU_SHARED:-1}"
export KUZU_LIBRARY_DIR="${KUZU_LIBRARY_DIR:-/opt/homebrew/lib}"
export KUZU_INCLUDE_DIR="${KUZU_INCLUDE_DIR:-/opt/homebrew/include}"

cd "$PROJECT_ROOT"
mkdir -p "$OUTPUT_DIR"

echo "=== Running native-vs-Kuzu graph benchmark ==="
cargo run -p akidb-graph --features kuzu --bin kuzu-graph-bench -- \
    --nodes "$NODES" \
    --edges "$EDGES" \
    --queries-per-kind "$QUERIES_PER_KIND" \
    --work-dir "$WORK_DIR" \
    --output "$OUTPUT"

echo "=== Validating Kuzu decision artifact ==="
python3 "$PROJECT_ROOT/scripts/validate-kuzu-decision.py" "$OUTPUT"

echo "Kuzu decision artifact: $OUTPUT"
