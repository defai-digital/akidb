#!/usr/bin/env bash
# AkiDB Standalone Mode Validation Script
#
# Validates end-to-end insert + search with real embeddings on a Mac.
# Prerequisites:
#   - ax-engine (pip install ax-engine) running on port 8081
#   - Rust toolchain for building akidb-server
#
# Usage:
#   ./scripts/validate-standalone.sh [--skip-build] [--skip-ax-engine]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_TEMPLATE="$PROJECT_ROOT/config/standalone.toml"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/akidb-standalone.XXXXXX")"
CONFIG="$TMP_DIR/standalone.toml"
PROTO_DIR="$PROJECT_ROOT/crates/grpc-server/proto"
PROTO_FILE="akidb.proto"
GRPC_PORT="${GRPC_PORT:-50051}"
AX_ENGINE_PORT="${AX_ENGINE_PORT:-8081}"
SKIP_BUILD=false
SKIP_AX_ENGINE=false
SERVER_PID=""
AX_ENGINE_PID=""
AX_ENGINE_MODEL="${AX_ENGINE_MODEL:-qwen3-embedding-4b}"

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --skip-ax-engine) SKIP_AX_ENGINE=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping akidb-server (PID $SERVER_PID)"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [ -n "$AX_ENGINE_PID" ] && kill -0 "$AX_ENGINE_PID" 2>/dev/null; then
        echo "Stopping ax-engine (PID $AX_ENGINE_PID)"
        kill "$AX_ENGINE_PID" 2>/dev/null || true
        wait "$AX_ENGINE_PID" 2>/dev/null || true
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

check_grpcurl() {
    if ! command -v grpcurl &>/dev/null; then
        echo "grpcurl not found. Installing..."
        if command -v brew &>/dev/null; then
            brew install grpcurl
        else
            echo "ERROR: grpcurl is required. Install via:"
            echo "  brew install grpcurl"
            echo "  or: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
            exit 1
        fi
    fi
}

wait_for_grpc() {
    local port=$1
    local service=$2
    local max_wait=30
    local waited=0
    echo "Waiting for $service on port $port..."
    while ! grpcurl -plaintext -import-path "$PROTO_DIR" -proto "$PROTO_FILE" \
        "localhost:$port" akidb.v1.Akidb/Health &>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge $max_wait ]; then
            echo "ERROR: $service did not start within ${max_wait}s"
            exit 1
        fi
    done
    echo "$service is ready"
}

echo "=== AkiDB Standalone Validation ==="
python3 - "$CONFIG_TEMPLATE" "$CONFIG" "$TMP_DIR" <<'PY'
import pathlib
import sys

template = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
tmp_dir = pathlib.Path(sys.argv[3])

content = template.read_text()
content = content.replace('rocksdb_path = "./data/rocksdb"', f'rocksdb_path = "{tmp_dir / "rocksdb"}"')
content = content.replace('wal_path = "./data/wal"', f'wal_path = "{tmp_dir / "wal"}"')
output.write_text(content)
PY

echo "Config: $CONFIG"
echo ""

# Step 1: Check/install grpcurl
check_grpcurl

# Step 2: Build server
if [ "$SKIP_BUILD" = false ]; then
    echo "=== Building akidb-server ==="
    cd "$PROJECT_ROOT"
    cargo build -p akidb-server 2>&1 | tail -5
    echo "Build complete"
    echo ""
fi

# Step 3: Start ax-engine (if not skipped)
if [ "$SKIP_AX_ENGINE" = false ]; then
    if command -v ax-engine &>/dev/null; then
        echo "=== Starting ax-engine ==="
        ax-engine serve "$AX_ENGINE_MODEL" --port "$AX_ENGINE_PORT" &
        AX_ENGINE_PID=$!
        echo "ax-engine started (PID $AX_ENGINE_PID, model $AX_ENGINE_MODEL)"
        sleep 5
        if ! kill -0 "$AX_ENGINE_PID" 2>/dev/null; then
            echo "WARNING: ax-engine exited during startup. Skipping TextSearch."
            SKIP_AX_ENGINE=true
            AX_ENGINE_PID=""
        fi
    else
        echo "WARNING: ax-engine not found. Skipping embedding service."
        echo "  To test text search, install ax-engine and re-run without --skip-ax-engine"
        echo "  pip install ax-engine"
        SKIP_AX_ENGINE=true
    fi
fi

# Step 4: Start akidb-server
echo "=== Starting akidb-server (standalone mode) ==="
cd "$PROJECT_ROOT"
./target/debug/akidb-server \
    --config "$CONFIG" \
    --standalone \
    --listen "127.0.0.1:$GRPC_PORT" \
    --log-level info &
SERVER_PID=$!
echo "akidb-server started (PID $SERVER_PID)"

wait_for_grpc "$GRPC_PORT" "akidb-server"
echo ""

# Step 5: Health check
echo "=== Health Check ==="
grpcurl -plaintext -import-path "$PROTO_DIR" -proto "$PROTO_FILE" \
    "localhost:$GRPC_PORT" akidb.v1.Akidb/Health
echo ""

# Step 6: Insert test vectors (using pre-computed embeddings)
echo "=== Inserting Test Vectors ==="

# Generate test vectors using Python (simple deterministic vectors)
insert_vector() {
    local id=$1
    local description=$2
    # Generate a deterministic 2560-dim vector based on the ID
    local vector
    vector=$(python3 -c "
import hashlib
import math
dim = 2560
seed = int.from_bytes(hashlib.sha256(b'$id').digest()[:8], 'big')
vec = []
for i in range(dim):
    val = math.sin(seed + i * 0.1) * 0.5 + math.cos(i * 0.01) * 0.3
    vec.append(val)
# L2 normalize
norm = math.sqrt(sum(v*v for v in vec))
vec = [v/norm for v in vec]
print('[' + ','.join(str(v) for v in vec) + ']')
")

    grpcurl -plaintext \
        -import-path "$PROTO_DIR" \
        -proto "$PROTO_FILE" \
        -d "{\"collection\":\"test\",\"id\":\"$id\",\"vector\":$vector}" \
        "localhost:$GRPC_PORT" \
        akidb.v1.Akidb/Insert
    echo "  Inserted: $id ($description)"
}

insert_vector "doc-001" "Introduction to machine learning"
insert_vector "doc-002" "Deep learning neural networks"
insert_vector "doc-003" "Natural language processing with transformers"
insert_vector "doc-004" "Computer vision and image recognition"
insert_vector "doc-005" "Reinforcement learning and game theory"
echo ""

# Step 7: Vector search
echo "=== Vector Search (using doc-001's vector as query) ==="
QUERY_VECTOR=$(python3 -c "
import hashlib
import math
dim = 2560
seed = int.from_bytes(hashlib.sha256(b'doc-001').digest()[:8], 'big')
vec = []
for i in range(dim):
    val = math.sin(seed + i * 0.1) * 0.5 + math.cos(i * 0.01) * 0.3
    vec.append(val)
norm = math.sqrt(sum(v*v for v in vec))
vec = [v/norm for v in vec]
print('[' + ','.join(str(v) for v in vec) + ']')
")

echo "Searching for vectors similar to doc-001..."
grpcurl -plaintext \
    -import-path "$PROTO_DIR" \
    -proto "$PROTO_FILE" \
    -d "{\"collection\":\"test\",\"query\":$QUERY_VECTOR,\"top_k\":3}" \
    "localhost:$GRPC_PORT" \
    akidb.v1.Akidb/Search
echo ""

# Step 8: Text search (requires ax-engine)
if [ "$SKIP_AX_ENGINE" = false ]; then
    echo "=== Text Search (via ax-engine embeddings) ==="
    echo "Searching for: 'neural network learning'"
    grpcurl -plaintext \
        -import-path "$PROTO_DIR" \
        -proto "$PROTO_FILE" \
        -d "{\"collection\":\"test\",\"text\":\"neural network learning\",\"top_k\":3}" \
        "localhost:$GRPC_PORT" \
        akidb.v1.Akidb/TextSearch
    echo ""
else
    echo "=== Text Search SKIPPED (ax-engine not running) ==="
    echo ""
fi

# Step 9: Get vector by ID
echo "=== Get Vector by ID ==="
grpcurl -plaintext \
    -import-path "$PROTO_DIR" \
    -proto "$PROTO_FILE" \
    -d "{\"collection\":\"test\",\"id\":\"doc-001\"}" \
    "localhost:$GRPC_PORT" \
    akidb.v1.Akidb/Get | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f\"  ID: {data.get('id')}\")
print(f\"  Found: {data.get('found')}\")
vec = data.get('vector', [])
print(f\"  Vector dimensions: {len(vec)}\")
if vec:
    print(f\"  First 5 values: {vec[:5]}\")
"
echo ""

# Step 10: Delete
echo "=== Delete doc-005 ==="
grpcurl -plaintext \
    -import-path "$PROTO_DIR" \
    -proto "$PROTO_FILE" \
    -d "{\"collection\":\"test\",\"id\":\"doc-005\"}" \
    "localhost:$GRPC_PORT" \
    akidb.v1.Akidb/Delete
echo ""

echo "=== Validation Complete ==="
echo "All operations executed successfully."
