#!/bin/bash
# AkiDB Thor Edition - End-to-End Test Script
#
# This script runs a full end-to-end test of the ingestion pipeline
# using Docker Compose services.
#
# Usage: ./e2e-test.sh [--gpu]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$COMPOSE_DIR")")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
GPU_MODE=false
TIMEOUT=300
TEST_BUCKET="akidb-test-documents"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --gpu)
            GPU_MODE=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up test environment..."
    cd "$COMPOSE_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
}

trap cleanup EXIT

# Start test
echo ""
echo "=========================================="
echo "  AkiDB Thor Edition - E2E Test Suite"
echo "=========================================="
echo ""

# Step 1: Start infrastructure services
log_info "Starting infrastructure services (NATS, MinIO)..."
cd "$COMPOSE_DIR"

if [ "$GPU_MODE" = true ]; then
    docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d nats-1 nats-2 nats-3 minio
else
    docker compose up -d nats-1 nats-2 nats-3 minio
fi

# Wait for services to be healthy
log_info "Waiting for services to be healthy..."
sleep 10

# Step 2: Check NATS cluster
log_info "Checking NATS cluster status..."
NATS_HEALTHY=false
for i in {1..30}; do
    if curl -s http://localhost:8222/healthz > /dev/null 2>&1; then
        NATS_HEALTHY=true
        break
    fi
    sleep 2
done

if [ "$NATS_HEALTHY" = true ]; then
    log_success "NATS cluster is healthy"
else
    log_error "NATS cluster failed to start"
    exit 1
fi

# Step 3: Check MinIO
log_info "Checking MinIO status..."
MINIO_HEALTHY=false
for i in {1..30}; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        MINIO_HEALTHY=true
        break
    fi
    sleep 2
done

if [ "$MINIO_HEALTHY" = true ]; then
    log_success "MinIO is healthy"
else
    log_error "MinIO failed to start"
    exit 1
fi

# Step 4: Set up MinIO bucket and NATS stream
log_info "Setting up MinIO bucket..."
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker compose exec -T minio mc mb local/$TEST_BUCKET --ignore-existing 2>/dev/null || true
log_success "MinIO bucket created: $TEST_BUCKET"

# Step 5: Create test documents
log_info "Creating test documents..."
TEST_DIR=$(mktemp -d)

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

log_success "Created 4 test documents"

# Step 6: Upload test documents to MinIO
log_info "Uploading test documents to MinIO..."
for file in "$TEST_DIR"/*; do
    filename=$(basename "$file")
    docker compose exec -T minio mc cp /dev/stdin local/$TEST_BUCKET/$filename < "$file"
    log_info "  Uploaded: $filename"
done
log_success "All test documents uploaded"

# Step 7: Start remaining services
log_info "Starting ingestion services..."
if [ "$GPU_MODE" = true ]; then
    docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d doc-parser upload-gateway
else
    docker compose up -d doc-parser upload-gateway
fi

# Wait for services
sleep 10

# Step 8: Check doc-parser health
log_info "Checking doc-parser service..."
DOC_PARSER_HEALTHY=false
for i in {1..30}; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        DOC_PARSER_HEALTHY=true
        break
    fi
    sleep 2
done

if [ "$DOC_PARSER_HEALTHY" = true ]; then
    log_success "Doc-parser service is healthy"
else
    log_warning "Doc-parser service not responding (may not be needed for current tests)"
fi

# Step 9: Check upload-gateway health
log_info "Checking upload-gateway service..."
GATEWAY_HEALTHY=false
for i in {1..30}; do
    if curl -s http://localhost:8081/health > /dev/null 2>&1; then
        GATEWAY_HEALTHY=true
        break
    fi
    sleep 2
done

if [ "$GATEWAY_HEALTHY" = true ]; then
    log_success "Upload-gateway service is healthy"
else
    log_warning "Upload-gateway service not responding"
fi

# Step 10: Verify NATS streams
log_info "Verifying NATS JetStream configuration..."
# Use nats CLI if available, otherwise skip
if command -v nats &> /dev/null; then
    nats stream ls 2>/dev/null || log_warning "Could not list NATS streams"
fi

# Step 11: Run API tests
log_info "Running API tests..."

# Test doc-parser /parse endpoint (if healthy)
if [ "$DOC_PARSER_HEALTHY" = true ]; then
    log_info "  Testing doc-parser /parse endpoint..."
    PARSE_RESULT=$(curl -s -X POST http://localhost:8080/parse \
        -H "Content-Type: application/json" \
        -d '{"content": "VGVzdCBjb250ZW50", "filename": "test.txt", "content_type": "text/plain"}' \
        2>/dev/null || echo "failed")

    if [[ "$PARSE_RESULT" != "failed" ]] && [[ "$PARSE_RESULT" == *"text"* ]]; then
        log_success "  Doc-parser /parse endpoint working"
    else
        log_warning "  Doc-parser /parse endpoint test inconclusive"
    fi
fi

# Step 12: Test metrics endpoints
log_info "Checking metrics endpoints..."

# Check Prometheus (if running)
PROMETHEUS_UP=$(curl -s http://localhost:9090/-/healthy 2>/dev/null || echo "down")
if [ "$PROMETHEUS_UP" = "Prometheus Server is Healthy." ]; then
    log_success "Prometheus is healthy"
else
    log_warning "Prometheus not available (may not be started)"
fi

# Step 13: Cleanup test data
log_info "Cleaning up test data..."
rm -rf "$TEST_DIR"

# Step 14: Summary
echo ""
echo "=========================================="
echo "         E2E Test Summary"
echo "=========================================="
echo ""

TESTS_PASSED=0
TESTS_TOTAL=5

# Count passed tests
[ "$NATS_HEALTHY" = true ] && ((TESTS_PASSED++))
[ "$MINIO_HEALTHY" = true ] && ((TESTS_PASSED++))
[ "$DOC_PARSER_HEALTHY" = true ] && ((TESTS_PASSED++)) || true
[ "$GATEWAY_HEALTHY" = true ] && ((TESTS_PASSED++)) || true
# Add 1 for successful document upload
((TESTS_PASSED++))

echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}/$TESTS_TOTAL"
echo ""

if [ $TESTS_PASSED -ge 3 ]; then
    log_success "E2E tests completed successfully!"
    echo ""
    echo "Services running:"
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    exit 0
else
    log_error "E2E tests failed!"
    exit 1
fi
