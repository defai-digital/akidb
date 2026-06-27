#!/usr/bin/env bash
# Run the README one-Mac reference benchmark shape and validate with the
# reference gate. This is intentionally separate from the smoke runner so a
# release artifact cannot accidentally pass with smaller workload parameters.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export DIMENSIONS="${DIMENSIONS:-768}"
export VECTORS="${VECTORS:-1000000}"
export QUERIES="${QUERIES:-5000}"
export BATCH_SIZE="${BATCH_SIZE:-1000}"
export TOP_K="${TOP_K:-10}"
export NPROBE="${NPROBE:-64}"
export CONCURRENCY="${CONCURRENCY:-1}"
export SLO_MS="${SLO_MS:-50}"
export BUILD_PROFILE="${BUILD_PROFILE:-release}"
export ONE_MAC_REFERENCE=1

"$SCRIPT_DIR/benchmark-one-mac.sh"
