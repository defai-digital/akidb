#!/usr/bin/env bash
# Run one isolated authoritative-Memory systems qualification on Linux AMD64.
# The caller supplies fresh work and output directories; this script never
# deletes either directory and never copies credentials into the evidence set.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSIONS=""
QUERIES="1000"
WARMUP_QUERIES="20"
COMMIT_CONCURRENCY="8"
QUERY_CONCURRENCY="8"
PORT="50051"
METRICS_PORT="19090"
RUN_ID=""
HOST_LABEL=""
WORK_DIR=""
OUTPUT_DIR=""

usage() {
  printf '%s\n' \
    "usage: $0 --versions N --run-id ID --host-label LABEL --work-dir /absolute/path --output-dir /absolute/path [options]" \
    "" \
    "options:" \
    "  --queries N                 measured recall count (default: 1000)" \
    "  --warmup-queries N          unmeasured recall count (default: 20)" \
    "  --commit-concurrency N      simultaneous Remember RPCs (default: 8)" \
    "  --query-concurrency N       simultaneous Recall RPCs (default: 8)" \
    "  --port N                    loopback gRPC port (default: 50051)" \
    "  --metrics-port N            loopback Prometheus port (default: 19090)"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --versions) VERSIONS="${2:-}"; shift 2 ;;
    --queries) QUERIES="${2:-}"; shift 2 ;;
    --warmup-queries) WARMUP_QUERIES="${2:-}"; shift 2 ;;
    --commit-concurrency) COMMIT_CONCURRENCY="${2:-}"; shift 2 ;;
    --query-concurrency) QUERY_CONCURRENCY="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    --metrics-port) METRICS_PORT="${2:-}"; shift 2 ;;
    --run-id) RUN_ID="${2:-}"; shift 2 ;;
    --host-label) HOST_LABEL="${2:-}"; shift 2 ;;
    --work-dir) WORK_DIR="${2:-}"; shift 2 ;;
    --output-dir) OUTPUT_DIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

for value in "$VERSIONS" "$QUERIES" "$WARMUP_QUERIES" \
  "$COMMIT_CONCURRENCY" "$QUERY_CONCURRENCY" "$PORT" "$METRICS_PORT"; do
  if [[ ! "$value" =~ ^(0|[1-9][0-9]*)$ || "${#value}" -gt 10 ]]; then
    printf 'numeric qualification arguments must be canonical bounded decimal values\n' >&2
    exit 2
  fi
done
if [[ "$VERSIONS" -lt 1 || "$QUERIES" -lt 1 \
  || "$COMMIT_CONCURRENCY" -lt 1 || "$QUERY_CONCURRENCY" -lt 1 ]]; then
  printf 'versions, queries, and concurrency must be greater than zero\n' >&2
  exit 2
fi
if [[ "$VERSIONS" -gt 10000000 || "$QUERIES" -gt 1000000 \
  || "$WARMUP_QUERIES" -gt 1000000 || "$COMMIT_CONCURRENCY" -gt 4096 \
  || "$QUERY_CONCURRENCY" -gt 4096 ]]; then
  printf 'qualification workload exceeds the benchmark safety bounds\n' >&2
  exit 2
fi
if [[ "$PORT" -lt 1 || "$PORT" -gt 65535 \
  || "$METRICS_PORT" -lt 1 || "$METRICS_PORT" -gt 65535 \
  || "$PORT" -eq "$METRICS_PORT" ]]; then
  printf 'gRPC and metrics ports must be distinct values from 1 through 65535\n' >&2
  exit 2
fi
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  printf 'run ID must be 1-128 portable identifier characters\n' >&2
  exit 2
fi
if [[ ! "$HOST_LABEL" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  printf 'host label must be 1-128 portable identifier characters\n' >&2
  exit 2
fi
for path in "$WORK_DIR" "$OUTPUT_DIR"; do
  if [[ "$path" != /* || ! "$path" =~ ^[A-Za-z0-9._/-]+$ \
    || "$path" == *"//"* || "$path" == */ || "$path" == *"/./"* \
    || "$path" == *"/../"* || "$path" == */. || "$path" == */.. ]]; then
    printf 'work and output directories must be canonical absolute paths\n' >&2
    exit 2
  fi
done
case "$WORK_DIR/" in
  "$PROJECT_ROOT/"* | "$OUTPUT_DIR/"*)
    printf 'work directory must not overlap the source or output directory\n' >&2
    exit 2
    ;;
esac
case "$OUTPUT_DIR/" in
  "$PROJECT_ROOT/"* | "$WORK_DIR/"*)
    printf 'output directory must not overlap the source or work directory\n' >&2
    exit 2
    ;;
esac
case "$PROJECT_ROOT/" in
  "$WORK_DIR/"* | "$OUTPUT_DIR/"*)
    printf 'source directory must not be nested in work or output directory\n' >&2
    exit 2
    ;;
esac
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  if [[ "${AKIDB_ALLOW_NON_AMD64:-0}" != "1" ]]; then
    printf 'this qualification requires Linux x86_64\n' >&2
    exit 2
  fi
fi
if [[ -n "$(git -C "$PROJECT_ROOT" status --porcelain)" \
  && "${AKIDB_ALLOW_DIRTY:-0}" != "1" ]]; then
  printf 'qualification requires a clean Git worktree\n' >&2
  exit 2
fi
if [[ -e "$WORK_DIR" || -e "$OUTPUT_DIR" ]]; then
  printf 'work and output directories must not already exist\n' >&2
  exit 2
fi
for port in "$PORT" "$METRICS_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    printf 'qualification loopback port is already in use: %s\n' "$port" >&2
    exit 2
  fi
done

cd "$PROJECT_ROOT"
mkdir -p "$WORK_DIR" "$OUTPUT_DIR"
chmod 700 "$WORK_DIR"
TOKEN_FILE="$WORK_DIR/principal.token"
LEGACY_TOKEN_FILE="$WORK_DIR/legacy.token"
CONFIG_FILE="$WORK_DIR/akidb-memory-qualification.toml"
SERVER_LOG="$OUTPUT_DIR/server.log"
REPORT_FILE="$OUTPUT_DIR/report.json"
METRICS_FILE="$OUTPUT_DIR/metrics.prom"
CHECKSUM_FILE="$OUTPUT_DIR/SHA256SUMS"

create_token_file() {
  local token_file="$1"
  umask 077
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32 >"$token_file"
  else
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$token_file"
    printf '\n' >>"$token_file"
  fi
  chmod 600 "$token_file"
}

create_token_file "$TOKEN_FILE"
create_token_file "$LEGACY_TOKEN_FILE"

sed \
  -e 's|memory-preview|memory-benchmark|g' \
  -e 's|namespaces = \["memory/\*\*"\]|namespaces = ["benchmark/**"]|' \
  -e 's|purposes = \["agent-memory", "incident-replay"\]|purposes = ["memory-benchmark"]|' \
  -e "s|grpc_port = 50051|grpc_port = $PORT|" \
  -e "s|metrics_port = 9090|metrics_port = $METRICS_PORT|" \
  -e 's|metrics_enabled = false|metrics_enabled = true|' \
  -e "s|./data/memory-benchmark/legacy.token|$LEGACY_TOKEN_FILE|" \
  -e "s|./data/memory-benchmark/principal.token|$TOKEN_FILE|" \
  -e "s|./data/memory-benchmark/memory-rocksdb|$WORK_DIR/memory-rocksdb|" \
  -e "s|./data/memory-benchmark/vector-rocksdb|$WORK_DIR/vector-rocksdb|" \
  -e "s|./data/memory-benchmark/vector-wal|$WORK_DIR/vector-wal|" \
  -e "s|./data/memory-benchmark/metadata.sqlite|$WORK_DIR/metadata.sqlite|" \
  "$PROJECT_ROOT/config/memory-preview.toml" >"$CONFIG_FILE"
chmod 600 "$CONFIG_FILE"

BUILD_PROFILE="${AKIDB_BUILD_PROFILE:-release}"
if [[ "$BUILD_PROFILE" != "release" && "$BUILD_PROFILE" != "debug" ]]; then
  printf 'AKIDB_BUILD_PROFILE must be release or debug\n' >&2
  exit 2
fi
if [[ "${AKIDB_SKIP_BUILD:-0}" != "1" ]]; then
  (
    cd "$PROJECT_ROOT"
    export CC="${CC:-gcc}"
    export CXX="${CXX:-g++}"
    if [[ "$(uname -s)" == "Linux" \
      && " ${CXXFLAGS:-} " != *" -include cstdint "* ]]; then
      export CXXFLAGS="${CXXFLAGS:+${CXXFLAGS} }-include cstdint"
    fi
    if [[ "$BUILD_PROFILE" == "release" ]]; then
      CARGO_INCREMENTAL=0 cargo build --locked --release \
        --bin akidb-server \
        --bin akidb-memory-bench
    else
      CARGO_INCREMENTAL=0 cargo build --locked \
        --bin akidb-server \
        --bin akidb-memory-bench
    fi
  )
fi

SERVER_BIN="$PROJECT_ROOT/target/$BUILD_PROFILE/akidb-server"
BENCHMARK_BIN="$PROJECT_ROOT/target/$BUILD_PROFILE/akidb-memory-bench"
if [[ ! -x "$SERVER_BIN" || ! -x "$BENCHMARK_BIN" ]]; then
  printf '%s server and memory benchmark binaries are required\n' "$BUILD_PROFILE" >&2
  exit 1
fi

"$SERVER_BIN" \
  --standalone \
  --config "$CONFIG_FILE" \
  --listen "127.0.0.1:$PORT" \
  --log-level info >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

stop_server() {
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID"
    wait "$SERVER_PID" || true
  fi
}
trap stop_server EXIT INT TERM

ready=0
for _ in $(seq 1 120); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    printf 'AkiDB server exited before becoming ready\n' >&2
    exit 1
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    exec 3>&-
    exec 3<&-
    sleep 0.1
    if kill -0 "$SERVER_PID" 2>/dev/null; then
      ready=1
      break
    fi
  fi
  sleep 1
done
if [[ "$ready" != "1" ]]; then
  printf 'AkiDB server did not become ready within 120 seconds\n' >&2
  exit 1
fi

"$BENCHMARK_BIN" \
  --server "http://127.0.0.1:$PORT" \
  --workspace memory-benchmark \
  --namespace "benchmark/$RUN_ID" \
  --purpose memory-benchmark \
  --run-id "$RUN_ID" \
  --host-label "$HOST_LABEL" \
  --token-file "$TOKEN_FILE" \
  --versions "$VERSIONS" \
  --commit-concurrency "$COMMIT_CONCURRENCY" \
  --queries "$QUERIES" \
  --warmup-queries "$WARMUP_QUERIES" \
  --query-concurrency "$QUERY_CONCURRENCY" \
  --data-dir "$WORK_DIR/memory-rocksdb" \
  --server-pid "$SERVER_PID" \
  --output-json "$REPORT_FILE" >"$WORK_DIR/benchmark.stdout"

if command -v curl >/dev/null 2>&1; then
  curl --fail --silent --show-error \
    "http://127.0.0.1:$METRICS_PORT/metrics" >"$METRICS_FILE"
else
  printf 'curl is required to capture Prometheus evidence\n' >&2
  exit 1
fi

stop_server
trap - EXIT INT TERM

for token_file in "$TOKEN_FILE" "$LEGACY_TOKEN_FILE"; do
  token="$(<"$token_file")"
  if grep -Fq "$token" "$REPORT_FILE" "$METRICS_FILE" "$SERVER_LOG"; then
    printf 'credential leakage check failed\n' >&2
    exit 1
  fi
done
if grep -Fq "benchmark/$RUN_ID" "$METRICS_FILE"; then
  printf 'content-free metrics check failed\n' >&2
  exit 1
fi
for metric in \
  akidb_memory_commit_total \
  akidb_memory_projection_applied_sequence \
  akidb_memory_projection_lag_sequences \
  akidb_memory_recall_latency_seconds \
  akidb_memory_recall_snapshot_total \
  akidb_memory_authorization_decision_total; do
  if ! grep -Fq "$metric" "$METRICS_FILE"; then
    printf 'required live metric is missing: %s\n' "$metric" >&2
    exit 1
  fi
done

if command -v sha256sum >/dev/null 2>&1; then
  (
    cd "$OUTPUT_DIR"
    sha256sum report.json metrics.prom server.log >SHA256SUMS
  )
else
  (
    cd "$OUTPUT_DIR"
    shasum -a 256 report.json metrics.prom server.log >SHA256SUMS
  )
fi
chmod 444 "$REPORT_FILE" "$METRICS_FILE" "$SERVER_LOG" "$CHECKSUM_FILE"
printf 'qualification evidence: %s\n' "$OUTPUT_DIR"
