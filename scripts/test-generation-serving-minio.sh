#!/usr/bin/env bash
# Real MinIO + gRPC + restart/rollback gate for the Phase 2 preview.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${AKIDB_QA_PYTHON:-$ROOT/sdks/python/.venv/bin/python}"
MINIO_IMAGE="${AKIDB_MINIO_IMAGE:-minio/minio:RELEASE.2025-09-07T16-13-09Z}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/akidb-generation-qa-target}"
SERVER_BIN="${AKIDB_SERVER_BIN:-$CARGO_TARGET_DIR/debug/akidb-server}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-generation-qa.XXXXXX")"
MINIO_CONTAINER="akidb-generation-minio-$$"
MINIO_ACCESS_KEY="generationqa"
MINIO_SECRET_KEY="$(openssl rand -hex 24)"
AKIDB_AUTH_TOKEN="data-$(openssl rand -hex 24)"
AKIDB_GENERATION_CONTROL_TOKEN="control-$(openssl rand -hex 24)"
export AKIDB_AUTH_TOKEN
export AKIDB_GENERATION_CONTROL_TOKEN
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  docker rm --force "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

for command in docker curl openssl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "ERROR: required command is missing: $command" >&2
    exit 1
  fi
done
if [[ ! -x "$PYTHON" ]]; then
  echo "ERROR: Python SDK environment is missing: $PYTHON" >&2
  echo 'Run: (cd sdks/python && python -m venv .venv && .venv/bin/pip install -e ".[dev]")' >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is unavailable" >&2
  exit 1
fi

if [[ -z "${AKIDB_SERVER_BIN:-}" ]]; then
  (
    cd "$ROOT"
    CARGO_TARGET_DIR="$CARGO_TARGET_DIR" \
      cargo build -p akidb-server --features generation-s3
  )
fi

docker run --detach --pull=missing \
  --name "$MINIO_CONTAINER" \
  --publish 127.0.0.1::9000 \
  --env "MINIO_ROOT_USER=$MINIO_ACCESS_KEY" \
  --env "MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY" \
  "$MINIO_IMAGE" server /data >/dev/null

MINIO_PORT="$(docker port "$MINIO_CONTAINER" 9000/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$MINIO_PORT" ]]; then
  echo "ERROR: failed to resolve the MinIO test port" >&2
  exit 1
fi
MINIO_ENDPOINT="http://127.0.0.1:$MINIO_PORT"
for _ in $(seq 1 60); do
  if curl --fail --silent "$MINIO_ENDPOINT/minio/health/ready" >/dev/null; then
    break
  fi
  sleep 0.5
done
curl --fail --silent "$MINIO_ENDPOINT/minio/health/ready" >/dev/null

"$PYTHON" "$ROOT/scripts/qa_generation_serving.py" prepare \
  --output "$TMP_DIR/artifacts" \
  --minio-endpoint "127.0.0.1:$MINIO_PORT" \
  --minio-access-key "$MINIO_ACCESS_KEY" \
  --minio-secret-key "$MINIO_SECRET_KEY"

s3_curl() {
  curl --fail --silent --show-error \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY" \
    "$@"
}

s3_curl --request PUT "$MINIO_ENDPOINT/knowledge"
for suffix in a b; do
  bundle="$TMP_DIR/artifacts/bundle-$suffix.ndjson"
  digest="$(openssl dgst -sha256 "$bundle" | awk '{print $NF}')"
  s3_curl --upload-file "$bundle" \
    "$MINIO_ENDPOINT/knowledge/generations/$digest/bundle-$suffix.ndjson"
done

GRPC_PORT="$(
  "$PYTHON" -c \
    'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
)"
ADDRESS="127.0.0.1:$GRPC_PORT"
CONFIG="$TMP_DIR/artifacts/akidb.toml"
SNAPSHOT="$TMP_DIR/generation-a-snapshot.json"

start_server() {
  local log_file="$1"
  "$SERVER_BIN" \
    --config "$CONFIG" \
    --listen "$ADDRESS" \
    --log-level info >"$log_file" 2>&1 &
  SERVER_PID="$!"
}

stop_server() {
  kill "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

start_server "$TMP_DIR/server-before-restart.log"
"$PYTHON" "$ROOT/scripts/qa_generation_serving.py" exercise \
  --phase initial \
  --address "$ADDRESS" \
  --artifacts "$TMP_DIR/artifacts" \
  --snapshot "$SNAPSHOT"
stop_server

start_server "$TMP_DIR/server-after-restart.log"
"$PYTHON" "$ROOT/scripts/qa_generation_serving.py" exercise \
  --phase after-restart \
  --address "$ADDRESS" \
  --artifacts "$TMP_DIR/artifacts" \
  --snapshot "$SNAPSHOT"
stop_server

echo "PASS: immutable MinIO publication, atomic cutover, restart recovery, and rollback"
