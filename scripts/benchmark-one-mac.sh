#!/usr/bin/env bash
# Run a reproducible one-Mac AkiDB synthetic benchmark against a clean
# standalone server and write a machine-readable JSON artifact.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_TEMPLATE="$PROJECT_ROOT/config/standalone.toml"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-benchmark.XXXXXX")"
CONFIG="$TMP_DIR/standalone.toml"
GRPC_PORT="${GRPC_PORT:-50051}"
DIMENSIONS="${DIMENSIONS:-768}"
VECTORS="${VECTORS:-100000}"
QUERIES="${QUERIES:-1000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
TOP_K="${TOP_K:-10}"
NPROBE="${NPROBE:-64}"
CONCURRENCY="${CONCURRENCY:-1}"
SLO_MS="${SLO_MS:-50}"
SEED="${SEED:-42}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_ROOT/benchmark-results}"
ONE_MAC_REFERENCE="${ONE_MAC_REFERENCE:-0}"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

wait_for_tcp() {
    local host=$1
    local port=$2
    local max_wait=${3:-30}
    local waited=0
    while ! nc -z "$host" "$port" >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$max_wait" ]; then
            echo "ERROR: server did not open $host:$port within ${max_wait}s"
            exit 1
        fi
        if [ -n "$SERVER_PID" ] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "ERROR: akidb-server exited during startup"
            exit 1
        fi
    done
}

cd "$PROJECT_ROOT"
mkdir -p "$OUTPUT_DIR"

case "$BUILD_PROFILE" in
    debug)
        CARGO_PROFILE_ARGS=()
        BIN_DIR="$PROJECT_ROOT/target/debug"
        ;;
    release)
        CARGO_PROFILE_ARGS=(--release)
        BIN_DIR="$PROJECT_ROOT/target/release"
        ;;
    *)
        echo "ERROR: BUILD_PROFILE must be debug or release, got '$BUILD_PROFILE'"
        exit 1
        ;;
esac

python3 - "$CONFIG_TEMPLATE" "$CONFIG" "$TMP_DIR" "$DIMENSIONS" <<'PY'
import pathlib
import re
import sys

template = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
tmp_dir = pathlib.Path(sys.argv[3])
dimensions = sys.argv[4]

content = template.read_text()
content = content.replace('rocksdb_path = "./data/rocksdb"', f'rocksdb_path = "{tmp_dir / "rocksdb"}"')
content = content.replace('wal_path = "./data/wal"', f'wal_path = "{tmp_dir / "wal"}"')
content = content.replace("dimensions = 2560", f"dimensions = {dimensions}")
content = re.sub(r"(\[embedding\]\s*)enabled = true", r"\1enabled = false", content, count=1)
output.write_text(content)
PY

echo "=== Building benchmark binaries ($BUILD_PROFILE) ==="
cargo build "${CARGO_PROFILE_ARGS[@]}" -p akidb-server -p akidb-benchmark

echo "=== Starting clean standalone akidb-server ==="
"$BIN_DIR/akidb-server" \
    --config "$CONFIG" \
    --standalone \
    --listen "127.0.0.1:$GRPC_PORT" \
    --log-level warn &
SERVER_PID=$!
wait_for_tcp 127.0.0.1 "$GRPC_PORT" 30

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT="$OUTPUT_DIR/one-mac-${DIMENSIONS}d-${VECTORS}v-c${CONCURRENCY}-${STAMP}.json"
ID_PREFIX="one-mac-${STAMP}-$$"

echo "=== Running one-Mac benchmark ==="
"$BIN_DIR/akidb-bench" \
    --server "http://127.0.0.1:$GRPC_PORT" \
    --dimension "$DIMENSIONS" \
    --num-vectors "$VECTORS" \
    --batch-size "$BATCH_SIZE" \
    --num-queries "$QUERIES" \
    --top-k "$TOP_K" \
    --nprobe "$NPROBE" \
    --concurrency "$CONCURRENCY" \
    --slo-ms "$SLO_MS" \
    --seed "$SEED" \
    --id-prefix "$ID_PREFIX" \
    --output-json "$OUTPUT"

echo "=== Validating benchmark artifact ==="
if [ "$ONE_MAC_REFERENCE" = "1" ]; then
    python3 "$PROJECT_ROOT/scripts/validate-one-mac-benchmark.py" "$OUTPUT" --reference
else
    python3 "$PROJECT_ROOT/scripts/validate-one-mac-benchmark.py" "$OUTPUT" \
        --expected-dimensions "$DIMENSIONS" \
        --expected-vectors "$VECTORS" \
        --expected-queries "$QUERIES" \
        --expected-top-k "$TOP_K" \
        --expected-nprobe "$NPROBE" \
        --expected-concurrency "$CONCURRENCY" \
        --expected-slo-ms "$SLO_MS" \
        --max-p95-ms "$SLO_MS" \
        --min-slo-compliance 95
fi

echo "Benchmark artifact: $OUTPUT"
