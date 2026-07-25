#!/usr/bin/env bash
# Generate deterministic, broadly compatible Python gRPC bindings.
set -euo pipefail

SDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${AKIDB_PROTO_PYTHON:-python3}"
MODE="${1:-generate}"
if [[ "$MODE" != "generate" && "$MODE" != "--check" ]]; then
  echo "usage: $0 [generate|--check]" >&2
  exit 2
fi

"$PYTHON" - <<'PY'
from importlib.metadata import version

expected = {
    "grpcio-tools": "1.68.1",
    "protobuf": "5.29.6",
}
actual = {package: version(package) for package in expected}
if actual != expected:
    raise SystemExit(
        "wrong protobuf codegen toolchain: "
        f"expected {expected}, observed {actual}; "
        "install codegen-requirements.txt in an isolated virtual environment"
    )
PY

OUTPUT_DIR="$SDK_DIR/akidb"
TMP_DIR=""
cleanup() {
  if [[ -n "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT
if [[ "$MODE" == "--check" ]]; then
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-python-proto.XXXXXX")"
  OUTPUT_DIR="$TMP_DIR"
fi

"$PYTHON" -m grpc_tools.protoc \
  -I "$SDK_DIR/proto" \
  --python_out="$OUTPUT_DIR" \
  --grpc_python_out="$OUTPUT_DIR" \
  "$SDK_DIR/proto/akidb.proto"

"$PYTHON" - "$OUTPUT_DIR/akidb_pb2_grpc.py" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
expected = "import akidb_pb2 as akidb__pb2"
if expected not in text:
    raise SystemExit(f"generated import shape changed in {path}")
path.write_text(text.replace(expected, "from . import akidb_pb2 as akidb__pb2", 1))
PY

if [[ "$MODE" == "--check" ]]; then
  for generated in akidb_pb2.py akidb_pb2_grpc.py; do
    if ! cmp -s "$OUTPUT_DIR/$generated" "$SDK_DIR/akidb/$generated"; then
      diff -u "$SDK_DIR/akidb/$generated" "$OUTPUT_DIR/$generated" || true
      echo "ERROR: committed Python binding is stale: $generated" >&2
      exit 1
    fi
  done
  echo "Python protobuf bindings match the pinned codegen toolchain"
fi
