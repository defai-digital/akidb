#!/usr/bin/env bash
# Real SeaweedFS + gRPC + restart/rollback gate for the Phase 2 preview.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${AKIDB_QA_PYTHON:-$ROOT/sdks/python/.venv/bin/python}"
SEAWEEDFS_IMAGE="${AKIDB_SEAWEEDFS_IMAGE:-chrislusf/seaweedfs:4.47}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/akidb-generation-qa-target}"
SERVER_BIN="${AKIDB_SERVER_BIN:-$CARGO_TARGET_DIR/debug/akidb-server}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-generation-qa.XXXXXX")"
SEAWEEDFS_CONTAINER="akidb-generation-seaweedfs-$$"
SEAWEEDFS_ACCESS_KEY="generationqa"
SEAWEEDFS_SECRET_KEY="$(openssl rand -hex 24)"
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
  docker rm --force "$SEAWEEDFS_CONTAINER" >/dev/null 2>&1 || true
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

# SeaweedFS allows anonymous access to every operation when no credentials file
# is supplied, so the gateway is always started with -s3.config. The config is
# rendered inside the container rather than bind-mounted from the host: only the
# container filesystem is guaranteed to be shared with the daemon (Docker
# Desktop on macOS does not share $TMPDIR by default).
S3_CONFIG_JSON="$(
  printf '{"identities":[{"name":"generation-qa","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List","Tagging"]}]}' \
    "$SEAWEEDFS_ACCESS_KEY" "$SEAWEEDFS_SECRET_KEY"
)"

docker run --detach --pull=missing \
  --name "$SEAWEEDFS_CONTAINER" \
  --user 1000:1000 \
  --publish 127.0.0.1::8333 \
  --env "S3_CONFIG_JSON=$S3_CONFIG_JSON" \
  --entrypoint /bin/sh \
  "$SEAWEEDFS_IMAGE" \
  -c 'umask 027; printf "%s" "$S3_CONFIG_JSON" > /tmp/s3.json; exec weed server -filer -s3 -dir=/data -ip.bind=0.0.0.0 -s3.port=8333 -volume.max=0 -master.volumeSizeLimitMB=256 -s3.config=/tmp/s3.json' >/dev/null

SEAWEEDFS_PORT="$(docker port "$SEAWEEDFS_CONTAINER" 8333/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$SEAWEEDFS_PORT" ]]; then
  echo "ERROR: failed to resolve the SeaweedFS test port" >&2
  exit 1
fi
SEAWEEDFS_ENDPOINT="http://127.0.0.1:$SEAWEEDFS_PORT"
for _ in $(seq 1 60); do
  if curl --fail --silent "$SEAWEEDFS_ENDPOINT/healthz" >/dev/null; then
    break
  fi
  sleep 0.5
done
curl --fail --silent "$SEAWEEDFS_ENDPOINT/healthz" >/dev/null

"$PYTHON" "$ROOT/scripts/qa_generation_serving.py" prepare \
  --output "$TMP_DIR/artifacts" \
  --seaweedfs-endpoint "127.0.0.1:$SEAWEEDFS_PORT" \
  --seaweedfs-access-key "$SEAWEEDFS_ACCESS_KEY" \
  --seaweedfs-secret-key "$SEAWEEDFS_SECRET_KEY"

s3_curl() {
  curl --fail --silent --show-error \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$SEAWEEDFS_ACCESS_KEY:$SEAWEEDFS_SECRET_KEY" \
    "$@"
}

s3_curl --request PUT "$SEAWEEDFS_ENDPOINT/knowledge"
for suffix in a b; do
  bundle="$TMP_DIR/artifacts/bundle-$suffix.ndjson"
  digest="$(openssl dgst -sha256 "$bundle" | awk '{print $NF}')"
  s3_curl --upload-file "$bundle" \
    "$SEAWEEDFS_ENDPOINT/knowledge/generations/$digest/bundle-$suffix.ndjson"
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

echo "PASS: immutable SeaweedFS publication, atomic cutover, restart recovery, and rollback"
