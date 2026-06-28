#!/usr/bin/env bash
# Run a reproducible synthetic benchmark against an existing four-Mac cell
# endpoint and write the cell benchmark artifact consumed by
# build-four-mac-cell-artifact.py --cell-artifact.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SERVER="${SERVER:-}"
DIMENSIONS="${DIMENSIONS:-768}"
VECTORS="${VECTORS:-1000000}"
QUERIES="${QUERIES:-5000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
TOP_K="${TOP_K:-10}"
NPROBE="${NPROBE:-64}"
CONCURRENCY="${CONCURRENCY:-1}"
SLO_MS="${SLO_MS:-50}"
MAX_P95_MS="${MAX_P95_MS:-$SLO_MS}"
MAX_P99_MS="${MAX_P99_MS:-100}"
MIN_SLO_COMPLIANCE="${MIN_SLO_COMPLIANCE:-95}"
SEED="${SEED:-42}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_ROOT/docs/reports}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
SKIP_BUILD="${SKIP_BUILD:-0}"

if [ -z "$SERVER" ]; then
    echo "ERROR: SERVER is required, for example SERVER=http://mac-1.local:50051"
    exit 1
fi

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

if [ "$SKIP_BUILD" != "1" ]; then
    echo "=== Building benchmark binary ($BUILD_PROFILE) ==="
    cargo build "${CARGO_PROFILE_ARGS[@]}" -p akidb-benchmark
fi

BENCH_BIN="${AKIDB_BENCH_BIN:-$BIN_DIR/akidb-bench}"
if [ ! -x "$BENCH_BIN" ]; then
    echo "ERROR: benchmark binary is not executable: $BENCH_BIN"
    exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT="${OUTPUT:-$OUTPUT_DIR/four-mac-benchmark-${DIMENSIONS}d-${VECTORS}v-c${CONCURRENCY}-${STAMP}.json}"
ID_PREFIX="four-mac-${STAMP}-$$"

echo "=== Running four-Mac cell benchmark ==="
"$BENCH_BIN" \
    --server "$SERVER" \
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

echo "=== Validating cell benchmark artifact ==="
python3 "$PROJECT_ROOT/scripts/validate-one-mac-benchmark.py" "$OUTPUT" \
    --expected-dimensions "$DIMENSIONS" \
    --expected-vectors "$VECTORS" \
    --expected-queries "$QUERIES" \
    --expected-top-k "$TOP_K" \
    --expected-nprobe "$NPROBE" \
    --expected-concurrency "$CONCURRENCY" \
    --expected-slo-ms "$SLO_MS" \
    --max-p95-ms "$MAX_P95_MS" \
    --max-p99-ms "$MAX_P99_MS" \
    --min-slo-compliance "$MIN_SLO_COMPLIANCE" \
    --require-apple-silicon

echo "Cell benchmark artifact: $OUTPUT"
