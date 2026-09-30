#!/usr/bin/env bash
# Small functional gate for digest-addressed local generation bundles.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${AKIDB_QA_PYTHON:-$ROOT/sdks/python/.venv/bin/python}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
USER_SUPPLIED_BIN="${AKIDB_BIN:-}"
AKIDB_BIN="${AKIDB_BIN:-$CARGO_TARGET_DIR/debug/akidb}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-local-mirror-qa.XXXXXX")"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "${AKIDB_QA_KEEP:-0}" == "1" ]]; then
    printf 'Evidence directory: %s\n' "$TMP_DIR"
  else
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

if [[ ! -x "$PYTHON" ]]; then
  echo "ERROR: Python environment with grpcio is missing: $PYTHON" >&2
  exit 1
fi
if [[ -z "$USER_SUPPLIED_BIN" && ! -x "$AKIDB_BIN" ]]; then
  (
    cd "$ROOT"
    cargo build -p akidb-cli --features generation-s3
  )
fi
if [[ ! -x "$AKIDB_BIN" ]]; then
  echo "ERROR: AkiDB binary is missing: $AKIDB_BIN" >&2
  exit 1
fi

AKIDB_AUTH_TOKEN="data-$(openssl rand -hex 24)"
AKIDB_GENERATION_CONTROL_TOKEN="control-$(openssl rand -hex 24)"
export AKIDB_AUTH_TOKEN AKIDB_GENERATION_CONTROL_TOKEN

"$PYTHON" "$ROOT/scripts/qa_generation_serving.py" prepare \
  --output "$TMP_DIR/artifacts" \
  --bundle-source local_mirror

for suffix in a b; do
  bundle="$TMP_DIR/artifacts/bundle-$suffix.ndjson"
  manifest="$TMP_DIR/artifacts/manifest-$suffix.json"
  read -r digest size_bytes < <(
    "$PYTHON" -c \
      'import json, sys; ref=json.load(open(sys.argv[1]))["bundle"]; print(ref["sha256"], ref["size_bytes"])' \
      "$manifest"
  )
  "$AKIDB_BIN" bundle import \
    --file "$bundle" \
    --mirror "$TMP_DIR/artifacts/mirror" \
    --sha256 "$digest" \
    --size-bytes "$size_bytes" >"$TMP_DIR/import-$suffix.json"
done

GRPC_PORT="$(
  "$PYTHON" -c \
    'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
)"
ADDRESS="127.0.0.1:$GRPC_PORT"
CONFIG="$TMP_DIR/artifacts/akidb.toml"
SNAPSHOT="$TMP_DIR/generation-a-snapshot.json"

start_server() {
  "$AKIDB_BIN" server \
    --config "$CONFIG" \
    --listen "$ADDRESS" \
    --log-level info >"$1" 2>&1 &
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

"$PYTHON" - "$AKIDB_BIN" "$TMP_DIR" <<'PY'
import hashlib
import json
import platform
import sys
from pathlib import Path

binary = Path(sys.argv[1])
directory = Path(sys.argv[2])
evidence = {
    "check": "local_mirror_generation_functional",
    "result": "passed",
    "hostname": platform.node(),
    "os": platform.system(),
    "architecture": platform.machine(),
    "binary_sha256": "",
    "fixture_vectors_per_generation": 1,
    "checks": ["import", "stage", "atomic_cutover", "restart", "rollback", "write_rejection"],
}
hasher = hashlib.sha256()
with binary.open("rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        hasher.update(chunk)
evidence["binary_sha256"] = hasher.hexdigest()
(directory / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
print(json.dumps(evidence, sort_keys=True))
PY
