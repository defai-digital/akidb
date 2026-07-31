#!/bin/bash
# AkiDB End-to-End Test Script
#
# This script verifies the Docker Compose document-ingress path through NATS,
# MinIO, the parser, and the upload gateway. Full embedding-to-AkiDB ingestion
# is covered separately because the native embedding service runs on the host.
#
# Usage: ./e2e-test.sh [--timeout seconds]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(dirname "$SCRIPT_DIR")"
E2E_COMPOSE_PROJECT_NAME="${E2E_COMPOSE_PROJECT_NAME:-akidb-e2e-$$}"
export COMPOSE_PROJECT_NAME="$E2E_COMPOSE_PROJECT_NAME"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TIMEOUT=300
HEALTH_POLL_INTERVAL="${HEALTH_POLL_INTERVAL:-2}"
HEALTH_REQUEST_TIMEOUT="${HEALTH_REQUEST_TIMEOUT:-2}"
DOC_PARSER_WAIT_SECONDS="${DOC_PARSER_WAIT_SECONDS:-30}"
TEST_DIR=""
DEADLINE=0

find_available_port_base() {
    python3 - << 'PY'
import socket

for base in range(20000, 60000, 10):
    sockets = []
    try:
        for port in range(base, base + 6):
            sock = socket.socket()
            sock.bind(("127.0.0.1", port))
            sockets.append(sock)
    except OSError:
        pass
    else:
        print(base)
        break
    finally:
        for sock in sockets:
            sock.close()
else:
    raise SystemExit("no free six-port range found")
PY
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --timeout)
                if [ "$#" -lt 2 ]; then
                    log_error "--timeout requires a value"
                    return 1
                fi
                TIMEOUT="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                return 1
                ;;
        esac
    done

    if [[ ! "$TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
        log_error "--timeout must be a positive integer (got '${TIMEOUT}')"
        return 1
    fi
    if [[ ! "$DOC_PARSER_WAIT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
        log_error "DOC_PARSER_WAIT_SECONDS must be a positive integer"
        return 1
    fi
}

wait_for_http() {
    local url="$1"
    local wait_deadline="${2:-$DEADLINE}"
    local service="${3:-}"

    while [ "$SECONDS" -lt "$wait_deadline" ]; do
        if [ -n "$service" ] \
            && ! docker compose ps --status running -q "$service" \
                | grep -q .; then
            return 1
        fi
        if curl --fail --silent --show-error \
            --max-time "$HEALTH_REQUEST_TIMEOUT" \
            "$url" > /dev/null 2>&1; then
            return 0
        fi
        sleep "$HEALTH_POLL_INTERVAL"
    done
    return 1
}

wait_for_nats_cluster() {
    local route_url="$1"
    local route_count

    while [ "$SECONDS" -lt "$DEADLINE" ]; do
        if route_count="$(curl --fail --silent --show-error \
            --max-time "$HEALTH_REQUEST_TIMEOUT" "$route_url" \
            | jq -er '.num_routes')" \
            && [ "$route_count" -ge 2 ]; then
            return 0
        fi
        sleep "$HEALTH_POLL_INTERVAL"
    done
    return 1
}

cleanup() {
    local exit_status=$?
    log_info "Cleaning up test environment..."
    if [ -n "$TEST_DIR" ] && [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR"
        TEST_DIR=""
    fi
    cd "$COMPOSE_DIR"
    if [ "$exit_status" -ne 0 ]; then
        docker compose ps -a 2>/dev/null || true
        docker compose logs --tail 60 --no-color 2>/dev/null || true
    fi
    docker compose down -v --remove-orphans 2>/dev/null || true
    return "$exit_status"
}

main() {
    parse_args "$@"
    DEADLINE=$((SECONDS + TIMEOUT))
    trap cleanup EXIT

    for command in base64 curl docker grep jq python3 tr; do
        if ! command -v "$command" > /dev/null 2>&1; then
            log_error "Required command is unavailable: $command"
            exit 1
        fi
    done

    E2E_PORT_BASE="${E2E_PORT_BASE:-$(find_available_port_base)}"
    if [[ ! "$E2E_PORT_BASE" =~ ^[1-9][0-9]*$ ]] \
        || [ "$E2E_PORT_BASE" -gt 65529 ]; then
        log_error "E2E_PORT_BASE must leave six valid TCP ports"
        exit 1
    fi
    export NATS_CLIENT_PORT="$E2E_PORT_BASE"
    export NATS_MONITOR_PORT=$((E2E_PORT_BASE + 1))
    export MINIO_API_PORT=$((E2E_PORT_BASE + 2))
    export MINIO_CONSOLE_PORT=$((E2E_PORT_BASE + 3))
    export DOC_PARSER_PORT=$((E2E_PORT_BASE + 4))
    export UPLOAD_GATEWAY_PORT=$((E2E_PORT_BASE + 5))

# Start test
echo ""
echo "=========================================="
echo "  AkiDB E2E Test Suite"
echo "=========================================="
echo ""

# Step 1: Start infrastructure services
log_info "Starting infrastructure services (NATS, MinIO)..."
cd "$COMPOSE_DIR"

docker compose up -d nats-1 nats-2 nats-3 minio

# Wait for services to be healthy
log_info "Waiting for services to be healthy..."

# Step 2: Check NATS cluster
log_info "Checking NATS cluster status..."
NATS_HEALTHY=false
if wait_for_http \
    "http://localhost:${NATS_MONITOR_PORT}/healthz" \
    "$DEADLINE" \
    "nats-1" \
    && wait_for_nats_cluster \
        "http://localhost:${NATS_MONITOR_PORT}/routez"; then
    NATS_HEALTHY=true
fi

if [ "$NATS_HEALTHY" = true ]; then
    log_success "NATS cluster is healthy"
else
    log_error "NATS cluster failed to start"
    exit 1
fi

# Step 3: Check MinIO
log_info "Checking MinIO status..."
MINIO_HEALTHY=false
if wait_for_http \
    "http://localhost:${MINIO_API_PORT}/minio/health/live" \
    "$DEADLINE" \
    "minio"; then
    MINIO_HEALTHY=true
fi

if [ "$MINIO_HEALTHY" = true ]; then
    log_success "MinIO is healthy"
else
    log_error "MinIO failed to start"
    exit 1
fi

# Step 4: Start the HTTP ingress services
log_info "Starting document parser and upload gateway..."
docker compose up -d doc-parser upload-gateway

log_info "Checking doc-parser service..."
DOC_PARSER_HEALTHY=false
DOC_PARSER_DEADLINE=$((SECONDS + DOC_PARSER_WAIT_SECONDS))
if [ "$DOC_PARSER_DEADLINE" -gt "$DEADLINE" ]; then
    DOC_PARSER_DEADLINE="$DEADLINE"
fi
if wait_for_http \
    "http://localhost:${DOC_PARSER_PORT}/health" \
    "$DOC_PARSER_DEADLINE" \
    "doc-parser"; then
    DOC_PARSER_HEALTHY=true
fi

if [ "$DOC_PARSER_HEALTHY" = true ]; then
    log_success "Doc-parser service is healthy"
else
    log_error "Doc-parser service failed to start"
    exit 1
fi

log_info "Checking upload-gateway service..."
GATEWAY_HEALTHY=false
if wait_for_http \
    "http://localhost:${UPLOAD_GATEWAY_PORT}/health" \
    "$DEADLINE" \
    "upload-gateway"; then
    GATEWAY_HEALTHY=true
fi

if [ "$GATEWAY_HEALTHY" = true ]; then
    log_success "Upload-gateway service is healthy"
else
    log_error "Upload-gateway failed its dependency health check"
    exit 1
fi

# Step 5: Create test documents
log_info "Creating test documents..."
TEST_DIR="$(mktemp -d)"

# Create JSON test document
cat > "$TEST_DIR/test.json" << 'EOF'
{
    "title": "E2E Test Document",
    "content": "This is a test document for end-to-end testing of the AkiDB ingestion pipeline.",
    "metadata": {
        "author": "E2E Test Suite",
        "created": "2026-01-21",
        "version": "1.0"
    },
    "sections": [
        {"heading": "Introduction", "text": "This section introduces the test."},
        {"heading": "Body", "text": "This is the main content of the test document."},
        {"heading": "Conclusion", "text": "This concludes the test document."}
    ]
}
EOF

# Create CSV test document
cat > "$TEST_DIR/test.csv" << 'EOF'
id,name,description,value
1,Item One,First test item for E2E testing,100
2,Item Two,Second test item with more content,200
3,Item Three,Third test item for validation,300
4,Item Four,Fourth test item for completeness,400
5,Item Five,Fifth and final test item,500
EOF

# Create HTML test document
cat > "$TEST_DIR/test.html" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>E2E Test Page</title>
</head>
<body>
    <h1>End-to-End Test Document</h1>
    <p>This is a test HTML document for the AkiDB ingestion pipeline.</p>
    <ul>
        <li>First list item</li>
        <li>Second list item</li>
        <li>Third list item</li>
    </ul>
    <script>console.log('This should be excluded');</script>
</body>
</html>
EOF

# Create plain text document
cat > "$TEST_DIR/test.txt" << 'EOF'
E2E Test Plain Text Document

This is a plain text document for testing the ingestion pipeline.
It contains multiple paragraphs of text that should be properly
chunked and embedded.

The pipeline should handle this document format correctly,
even though it's the simplest format available.

This is the final paragraph of the test document.
EOF

# Create a small EndNote library for the Python sidecar path.
cat > "$TEST_DIR/test.enl" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<xml>
  <records>
    <record>
      <ref-type>1</ref-type>
      <contributors><author>AkiDB QA</author></contributors>
      <title>End-to-End Parser Validation</title>
      <year>2026</year>
    </record>
  </records>
</xml>
EOF

log_success "Created 5 test documents"

# Step 6: Upload through the gateway and require NATS publication
log_info "Uploading test documents through the gateway..."
for file in "$TEST_DIR"/*; do
    filename="$(basename "$file")"
    upload_timeout=$((DEADLINE - SECONDS))
    if [ "$upload_timeout" -le 0 ]; then
        log_error "E2E timeout expired before uploading $filename"
        exit 1
    fi
    if ! upload_response="$(curl --fail --silent --show-error \
        --max-time "$upload_timeout" \
        --request POST "http://localhost:${UPLOAD_GATEWAY_PORT}/upload" \
        --form "file=@${file}")"; then
        log_error "Upload failed: $filename"
        exit 1
    fi
    if ! grep -Eq '"event_published"[[:space:]]*:[[:space:]]*true' \
        <<< "$upload_response"; then
        log_error "Upload was stored but its NATS event was not published: $filename"
        exit 1
    fi
    log_info "  Uploaded: $filename"
done
log_success "All test documents were stored and published to NATS"

# Step 7: Verify NATS streams
log_info "Verifying NATS JetStream configuration..."
if ! docker compose exec -T upload-gateway python - <<'PY'
import asyncio
import os

import nats


async def verify_stream() -> None:
    connection = await nats.connect(os.environ["UPLOAD_GATEWAY_NATS_URL"])
    try:
        info = await connection.jetstream().stream_info(
            os.environ["UPLOAD_GATEWAY_NATS_STREAM"]
        )
        subjects = set(info.config.subjects or [])
        assert "minio.uploads" in subjects
        assert "minio.uploads.>" in subjects
        assert info.config.num_replicas == int(
            os.environ["UPLOAD_GATEWAY_NATS_REPLICAS"]
        )
    finally:
        await connection.close()


asyncio.run(verify_stream())
PY
then
    log_error "NATS stream subjects or replication do not match configuration"
    exit 1
fi
log_success "NATS stream subjects and replication are configured"

# Step 8: Run API tests
log_info "Running API tests..."

# Test doc-parser /parse endpoint (if healthy)
if [ "$DOC_PARSER_HEALTHY" = true ]; then
    log_info "  Testing doc-parser /parse endpoint..."
    PARSE_CONTENT="$(base64 < "$TEST_DIR/test.enl" | tr -d '\r\n')"
    if ! PARSE_RESULT="$(curl --fail --silent --show-error \
        --max-time "$HEALTH_REQUEST_TIMEOUT" \
        --request POST "http://localhost:${DOC_PARSER_PORT}/parse" \
        --header "Content-Type: application/json" \
        --data "{\"content_base64\":\"${PARSE_CONTENT}\",\"filename\":\"test.enl\"}")"; then
        log_error "  Doc-parser /parse request failed"
        exit 1
    fi

    if grep -Eq '"format"[[:space:]]*:[[:space:]]*"enl"' <<< "$PARSE_RESULT" \
        && grep -q "End-to-End Parser Validation" <<< "$PARSE_RESULT"; then
        log_success "  Doc-parser /parse endpoint working"
    else
        log_error "  Doc-parser returned an invalid parse response"
        exit 1
    fi
fi

# Step 9: Cleanup test data
log_info "Cleaning up test data..."
rm -rf "$TEST_DIR"
TEST_DIR=""

# Step 10: Summary
echo ""
echo "=========================================="
echo "         E2E Test Summary"
echo "=========================================="
echo ""

TESTS_PASSED=0
TESTS_TOTAL=5

# Count passed tests
if [ "$NATS_HEALTHY" = true ]; then
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
if [ "$MINIO_HEALTHY" = true ]; then
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
if [ "$DOC_PARSER_HEALTHY" = true ]; then
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
if [ "$GATEWAY_HEALTHY" = true ]; then
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
# Add 1 for successful document upload
TESTS_PASSED=$((TESTS_PASSED + 1))

echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}/$TESTS_TOTAL"
echo ""

if [ "$TESTS_PASSED" -eq "$TESTS_TOTAL" ]; then
    log_success "E2E tests completed successfully!"
    echo ""
    echo "Services running:"
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    exit 0
else
    log_error "E2E tests failed!"
    exit 1
fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
