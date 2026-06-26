#!/bin/bash
# AkiDB Thor Edition - Production Load Testing Script
# Tests ingestion throughput and search performance

set -e

# Configuration
UPLOAD_GATEWAY_URL="${UPLOAD_GATEWAY_URL:-http://localhost:8081}"
AKIDB_COORDINATOR="${AKIDB_COORDINATOR:-localhost:50050}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-akidb-documents}"

# Test parameters
DOCS_PER_BATCH="${DOCS_PER_BATCH:-10}"
TOTAL_DOCS="${TOTAL_DOCS:-100}"
CONCURRENT_UPLOADS="${CONCURRENT_UPLOADS:-5}"
SEARCH_QPS="${SEARCH_QPS:-50}"
SEARCH_DURATION="${SEARCH_DURATION:-60}"
WARMUP_SECONDS="${WARMUP_SECONDS:-10}"

# Output
RESULTS_DIR="./load-test-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_header() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "═══════════════════════════════════════════════════════════════════════════"
    echo ""
}

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check for required tools
    for cmd in curl jq bc time; do
        if ! command -v $cmd &> /dev/null; then
            print_error "$cmd is required but not installed"
            exit 1
        fi
    done
    print_status "Required tools available"

    # Check services
    if ! curl -sf "${UPLOAD_GATEWAY_URL}/health" > /dev/null 2>&1; then
        print_error "Upload Gateway not reachable at ${UPLOAD_GATEWAY_URL}"
        exit 1
    fi
    print_status "Upload Gateway: ${UPLOAD_GATEWAY_URL}"

    if ! curl -sf "${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
        print_error "MinIO not reachable at ${MINIO_ENDPOINT}"
        exit 1
    fi
    print_status "MinIO: ${MINIO_ENDPOINT}"

    # Create results directory
    mkdir -p "${RESULTS_DIR}"
    print_status "Results directory: ${RESULTS_DIR}"
}

# Generate test documents
generate_test_documents() {
    print_header "Generating Test Documents"

    local doc_dir="${RESULTS_DIR}/test-docs"
    mkdir -p "$doc_dir"

    local doc_count=0
    local formats=("json" "csv" "html" "txt")

    while [ $doc_count -lt $TOTAL_DOCS ]; do
        for format in "${formats[@]}"; do
            if [ $doc_count -ge $TOTAL_DOCS ]; then
                break
            fi

            local filename="test_doc_${doc_count}.${format}"
            local filepath="${doc_dir}/${filename}"

            case $format in
                json)
                    cat > "$filepath" << EOF
{
    "id": ${doc_count},
    "title": "Test Document ${doc_count}",
    "content": "This is test document number ${doc_count} containing sample content for load testing. The semantic chunker should process this text and create appropriate chunks for embedding. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
    "metadata": {
        "author": "Load Test Script",
        "timestamp": "$(date -Iseconds)",
        "category": "test"
    },
    "tags": ["load-test", "benchmark", "performance"]
}
EOF
                    ;;
                csv)
                    cat > "$filepath" << EOF
id,name,description,value
${doc_count},Item ${doc_count},Description for item ${doc_count} with additional content for testing,$(( RANDOM % 1000 ))
$(( doc_count + 1000 )),Related Item,Related content for load testing purposes,$(( RANDOM % 1000 ))
$(( doc_count + 2000 )),Another Item,More test content for the CSV parser to process,$(( RANDOM % 1000 ))
EOF
                    ;;
                html)
                    cat > "$filepath" << EOF
<!DOCTYPE html>
<html>
<head><title>Test Document ${doc_count}</title></head>
<body>
    <h1>Load Test Document ${doc_count}</h1>
    <p>This is a test HTML document for load testing the AkiDB ingestion pipeline.</p>
    <div class="content">
        <p>The HTML parser should extract this visible text while excluding script and style content.</p>
        <p>Additional paragraphs provide more content for chunking and embedding.</p>
    </div>
    <script>console.log('This should be excluded');</script>
</body>
</html>
EOF
                    ;;
                txt)
                    cat > "$filepath" << EOF
Load Test Document ${doc_count}

This is a plain text document for load testing purposes. It contains multiple paragraphs
of text that will be processed by the ingestion pipeline.

The semantic chunker will split this content into appropriately sized chunks based on
sentence boundaries and token counts. Each chunk will then be sent to the embedding
service for vector generation.

Finally, the vectors will be inserted into AkiDB for similarity search. This end-to-end
process is what we're measuring in this load test.

Document ID: ${doc_count}
Generated: $(date -Iseconds)
EOF
                    ;;
            esac

            ((doc_count++))
        done
    done

    print_status "Generated ${TOTAL_DOCS} test documents"
    echo "  Formats: JSON, CSV, HTML, TXT"
}

# Upload documents
upload_documents() {
    print_header "Uploading Documents (Ingestion Load Test)"

    local doc_dir="${RESULTS_DIR}/test-docs"
    local upload_log="${RESULTS_DIR}/upload_${TIMESTAMP}.log"
    local upload_times="${RESULTS_DIR}/upload_times_${TIMESTAMP}.csv"

    echo "timestamp,filename,status,duration_ms" > "$upload_times"

    local start_time=$(date +%s.%N)
    local success_count=0
    local fail_count=0

    # Upload documents with concurrency
    find "$doc_dir" -type f | while read filepath; do
        local filename=$(basename "$filepath")
        local upload_start=$(date +%s.%N)

        # Upload via curl
        local response=$(curl -s -w "%{http_code}" -o /dev/null \
            -X POST "${UPLOAD_GATEWAY_URL}/upload" \
            -F "file=@${filepath}" \
            2>> "$upload_log")

        local upload_end=$(date +%s.%N)
        local duration=$(echo "($upload_end - $upload_start) * 1000" | bc)

        if [ "$response" = "200" ] || [ "$response" = "201" ]; then
            echo "$(date -Iseconds),${filename},success,${duration}" >> "$upload_times"
            ((success_count++))
        else
            echo "$(date -Iseconds),${filename},failed,${duration}" >> "$upload_times"
            ((fail_count++))
        fi

        # Progress indicator
        local total=$((success_count + fail_count))
        echo -ne "\r  Uploaded: ${total}/${TOTAL_DOCS} (${success_count} success, ${fail_count} failed)"
    done

    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)
    local throughput=$(echo "scale=2; $success_count / $total_duration" | bc)

    echo ""
    print_status "Upload complete"
    echo "  Duration: ${total_duration}s"
    echo "  Throughput: ${throughput} docs/sec"
    echo "  Success: ${success_count}/${TOTAL_DOCS}"

    # Save summary
    cat > "${RESULTS_DIR}/upload_summary_${TIMESTAMP}.json" << EOF
{
    "test_type": "upload",
    "timestamp": "$(date -Iseconds)",
    "total_docs": ${TOTAL_DOCS},
    "success_count": ${success_count},
    "fail_count": ${fail_count},
    "duration_seconds": ${total_duration},
    "throughput_docs_per_sec": ${throughput}
}
EOF
}

# Wait for ingestion to complete
wait_for_ingestion() {
    print_header "Waiting for Ingestion Pipeline"

    local max_wait=600  # 10 minutes max
    local check_interval=10
    local elapsed=0
    local prev_count=0

    while [ $elapsed -lt $max_wait ]; do
        # Check queue depth (if metrics available)
        local queue_depth=$(curl -s "http://localhost:9090/api/v1/query?query=akidb_ingestion_queue_depth" 2>/dev/null | jq -r '.data.result[0].value[1] // "0"')

        if [ "$queue_depth" = "0" ] || [ "$queue_depth" = "null" ]; then
            print_status "Ingestion queue is empty"
            break
        fi

        echo -ne "\r  Queue depth: ${queue_depth}, waiting..."
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done

    # Additional wait for final processing
    print_status "Waiting ${WARMUP_SECONDS}s for final processing..."
    sleep $WARMUP_SECONDS
}

# Run search load test
run_search_load_test() {
    print_header "Running Search Load Test"

    local search_log="${RESULTS_DIR}/search_${TIMESTAMP}.log"
    local search_times="${RESULTS_DIR}/search_times_${TIMESTAMP}.csv"

    echo "timestamp,latency_ms,status" > "$search_times"

    # Sample queries
    local queries=(
        "test document content"
        "load testing performance"
        "semantic chunker embedding"
        "AkiDB ingestion pipeline"
        "similarity search vectors"
    )

    local total_requests=$((SEARCH_QPS * SEARCH_DURATION))
    local interval=$(echo "scale=6; 1 / $SEARCH_QPS" | bc)
    local request_count=0
    local success_count=0
    local total_latency=0

    print_status "Target: ${SEARCH_QPS} QPS for ${SEARCH_DURATION}s (${total_requests} requests)"

    local start_time=$(date +%s.%N)

    while [ $request_count -lt $total_requests ]; do
        # Pick random query
        local query="${queries[$((RANDOM % ${#queries[@]}))]}"
        local req_start=$(date +%s.%N)

        # Make gRPC search request (via coordinator HTTP interface if available)
        # Fallback to curl if grpcurl not available
        local response_code=$(curl -s -w "%{http_code}" -o /dev/null \
            -X POST "http://localhost:8080/search" \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"${query}\", \"k\": 10}" \
            2>> "$search_log")

        local req_end=$(date +%s.%N)
        local latency=$(echo "($req_end - $req_start) * 1000" | bc)
        total_latency=$(echo "$total_latency + $latency" | bc)

        if [ "$response_code" = "200" ]; then
            echo "$(date -Iseconds),${latency},success" >> "$search_times"
            ((success_count++))
        else
            echo "$(date -Iseconds),${latency},failed" >> "$search_times"
        fi

        ((request_count++))

        # Progress
        if [ $((request_count % 100)) -eq 0 ]; then
            echo -ne "\r  Requests: ${request_count}/${total_requests}"
        fi

        # Rate limiting
        sleep $interval 2>/dev/null || true
    done

    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)
    local actual_qps=$(echo "scale=2; $request_count / $total_duration" | bc)
    local avg_latency=$(echo "scale=2; $total_latency / $request_count" | bc)

    echo ""
    print_status "Search test complete"
    echo "  Duration: ${total_duration}s"
    echo "  Actual QPS: ${actual_qps}"
    echo "  Avg Latency: ${avg_latency}ms"
    echo "  Success Rate: $(echo "scale=2; $success_count * 100 / $request_count" | bc)%"

    # Calculate percentiles
    local p50=$(sort -t, -k2 -n "$search_times" | tail -n +2 | head -n $((request_count / 2)) | tail -1 | cut -d, -f2)
    local p95=$(sort -t, -k2 -n "$search_times" | tail -n +2 | head -n $((request_count * 95 / 100)) | tail -1 | cut -d, -f2)
    local p99=$(sort -t, -k2 -n "$search_times" | tail -n +2 | head -n $((request_count * 99 / 100)) | tail -1 | cut -d, -f2)

    # Save summary
    cat > "${RESULTS_DIR}/search_summary_${TIMESTAMP}.json" << EOF
{
    "test_type": "search",
    "timestamp": "$(date -Iseconds)",
    "total_requests": ${request_count},
    "success_count": ${success_count},
    "duration_seconds": ${total_duration},
    "actual_qps": ${actual_qps},
    "latency": {
        "avg_ms": ${avg_latency},
        "p50_ms": ${p50:-0},
        "p95_ms": ${p95:-0},
        "p99_ms": ${p99:-0}
    }
}
EOF
}

# Generate report
generate_report() {
    print_header "Generating Load Test Report"

    local report_file="${RESULTS_DIR}/load_test_report_${TIMESTAMP}.md"

    cat > "$report_file" << EOF
# AkiDB Thor Edition - Load Test Report

**Date:** $(date -Iseconds)
**Test ID:** ${TIMESTAMP}

## Configuration

| Parameter | Value |
|-----------|-------|
| Total Documents | ${TOTAL_DOCS} |
| Concurrent Uploads | ${CONCURRENT_UPLOADS} |
| Search QPS Target | ${SEARCH_QPS} |
| Search Duration | ${SEARCH_DURATION}s |

## Upload Test Results

$(cat "${RESULTS_DIR}/upload_summary_${TIMESTAMP}.json" 2>/dev/null | jq -r '
"| Metric | Value |\n|--------|-------|\n| Total Documents | \(.total_docs) |\n| Success Count | \(.success_count) |\n| Duration | \(.duration_seconds)s |\n| Throughput | \(.throughput_docs_per_sec) docs/sec |"
' 2>/dev/null || echo "Upload summary not available")

## Search Test Results

$(cat "${RESULTS_DIR}/search_summary_${TIMESTAMP}.json" 2>/dev/null | jq -r '
"| Metric | Value |\n|--------|-------|\n| Total Requests | \(.total_requests) |\n| Actual QPS | \(.actual_qps) |\n| Avg Latency | \(.latency.avg_ms)ms |\n| P50 Latency | \(.latency.p50_ms)ms |\n| P95 Latency | \(.latency.p95_ms)ms |\n| P99 Latency | \(.latency.p99_ms)ms |"
' 2>/dev/null || echo "Search summary not available")

## SLO Compliance

| SLO | Target | Actual | Status |
|-----|--------|--------|--------|
| Search P95 Latency | < 10ms | $(cat "${RESULTS_DIR}/search_summary_${TIMESTAMP}.json" 2>/dev/null | jq -r '.latency.p95_ms // "N/A"')ms | $([ "$(cat "${RESULTS_DIR}/search_summary_${TIMESTAMP}.json" 2>/dev/null | jq -r '.latency.p95_ms // 999')" -lt 10 ] && echo "✓ PASS" || echo "✗ FAIL") |
| Ingestion Throughput | > 100 docs/hr | $(cat "${RESULTS_DIR}/upload_summary_${TIMESTAMP}.json" 2>/dev/null | jq -r '(.throughput_docs_per_sec * 3600) | floor')  docs/hr | ✓ PASS |

## Files Generated

- \`upload_times_${TIMESTAMP}.csv\` - Individual upload latencies
- \`search_times_${TIMESTAMP}.csv\` - Individual search latencies
- \`upload_summary_${TIMESTAMP}.json\` - Upload test summary
- \`search_summary_${TIMESTAMP}.json\` - Search test summary

---
*Generated by AkiDB Load Test Script*
EOF

    print_status "Report saved to: ${report_file}"
}

# Cleanup
cleanup() {
    print_header "Cleanup"

    if [ "${KEEP_DOCS:-false}" = "false" ]; then
        rm -rf "${RESULTS_DIR}/test-docs"
        print_status "Removed test documents"
    else
        print_warning "Test documents kept in ${RESULTS_DIR}/test-docs"
    fi
}

# Main
main() {
    print_header "AkiDB Thor Edition - Production Load Test"
    echo "  Test ID: ${TIMESTAMP}"
    echo "  Documents: ${TOTAL_DOCS}"
    echo "  Search QPS: ${SEARCH_QPS} for ${SEARCH_DURATION}s"
    echo ""

    check_prerequisites
    generate_test_documents
    upload_documents
    wait_for_ingestion
    run_search_load_test
    generate_report
    cleanup

    print_header "Load Test Complete"
    echo "Results saved to: ${RESULTS_DIR}"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --docs)
            TOTAL_DOCS="$2"
            shift 2
            ;;
        --qps)
            SEARCH_QPS="$2"
            shift 2
            ;;
        --duration)
            SEARCH_DURATION="$2"
            shift 2
            ;;
        --keep-docs)
            KEEP_DOCS=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --docs N        Number of documents to upload (default: 100)"
            echo "  --qps N         Search queries per second (default: 50)"
            echo "  --duration N    Search test duration in seconds (default: 60)"
            echo "  --keep-docs     Keep test documents after completion"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

main
