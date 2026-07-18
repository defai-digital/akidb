#!/usr/bin/env bash
# Docker-free happy path: start standalone server on loopback and smoke insert/search.
# Usage:
#   ./scripts/akidb-start.sh              # start server (foreground)
#   ./scripts/akidb-start.sh --smoke      # start, smoke via cargo tests unit path, stop
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --smoke) SMOKE=true ;;
  esac
done

cd "$PROJECT_ROOT"
DATA_DIR="${AKIDB_DATA_DIR:-$PROJECT_ROOT/data/local-start}"
mkdir -p "$DATA_DIR"

CONFIG="$DATA_DIR/start.toml"
cat >"$CONFIG" <<EOF
[server]
host = "127.0.0.1"
port = 8080
grpc_port = 50051
tls_enabled = false

[auth]
mode = "loopback_optional"
token_file = "$DATA_DIR/auth.token"

[auth.acl]
default_workspace = "default"
enforce_workspace = true

[index]
index_type = "HNSW"
hnsw_m = 16
hnsw_ef_construction = 64
hnsw_ef_search = 32
vector_precision = "f32"
metric = "cosine"

[index.filter]
mode = "adaptive"
postfilter_overfetch_factor = 5
adaptive_pre_selectivity = 0.20

[index.rebuild]
tombstone_ratio_trigger = 0.10
max_duration_seconds = 300
preferred_hours = [2, 3, 4]

[index.tombstone]
max_count = 100000

[storage]
rocksdb_path = "$DATA_DIR/rocksdb"
wal_enabled = true
wal_path = "$DATA_DIR/wal"

[storage.minio]
endpoint = "localhost:9000"
bucket = "akidb-snapshots"
access_key = "akidb-admin"
secret_key = "akidb-secret-key"
use_ssl = false

[sql]
enabled = false
backend = "sqlite"
sqlite_path = "$DATA_DIR/meta.sqlite"

[observability]
tracing_enabled = false
metrics_enabled = false
metrics_port = 9090
log_level = "info"
log_format = "pretty"

[slo.reference]
dimensions = 8
vectors_per_shard = 10000
top_k = 10
nprobe = 32
batch_size = 1
target_p95_ms = 50

[slo.backpressure]
soft_breach_ms = 50
hard_breach_ms = 75
degraded_mode_enabled = true

[embedding]
enabled = false
url = "http://127.0.0.1:8081/v1/embeddings"
model = "none"
dimensions = 8
timeout_ms = 10000
max_batch_size = 32
EOF

echo "Config: $CONFIG"
echo "Starting akidb server on 127.0.0.1:50051 (standalone)..."

if $SMOKE; then
  # Live Docker-free path: boot real server, insert/search with workspace ACL.
  export AKIDB_SMOKE_SCRATCH="${AKIDB_SMOKE_SCRATCH:-$PROJECT_ROOT/qa-results}"
  PY="$PROJECT_ROOT/.venv-smoke/bin/python"
  if [ ! -x "$PY" ]; then
    python3 -m venv "$PROJECT_ROOT/.venv-smoke"
    "$PROJECT_ROOT/.venv-smoke/bin/pip" install -q 'grpcio>=1.60'
    PY="$PROJECT_ROOT/.venv-smoke/bin/python"
  fi
  "$PY" "$SCRIPT_DIR/entry_smoke_live.py"
  exit $?
fi

exec cargo run -p akidb-cli -- server --standalone --config "$CONFIG" --listen 127.0.0.1:50051
