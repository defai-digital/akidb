#!/bin/bash
# AkiDB Production Load Testing Script
# Tests ingestion throughput and search performance

set -euo pipefail

# Configuration
UPLOAD_GATEWAY_URL="${UPLOAD_GATEWAY_URL:-http://localhost:8081}"
AKIDB_SERVER="${AKIDB_SERVER:-localhost:50051}"
AKIDB_COLLECTION="${AKIDB_COLLECTION:-default}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

# Test parameters
TOTAL_DOCS="${TOTAL_DOCS:-100}"
CONCURRENT_UPLOADS="${CONCURRENT_UPLOADS:-5}"
SEARCH_QPS="${SEARCH_QPS:-50}"
SEARCH_DURATION="${SEARCH_DURATION:-60}"
SEARCH_TIMEOUT_SECONDS="${SEARCH_TIMEOUT_SECONDS:-10}"
SEARCH_MAX_IN_FLIGHT="${SEARCH_MAX_IN_FLIGHT:-100}"
UPLOAD_TIMEOUT_SECONDS="${UPLOAD_TIMEOUT_SECONDS:-60}"
HEALTH_REQUEST_TIMEOUT_SECONDS="${HEALTH_REQUEST_TIMEOUT_SECONDS:-5}"
WARMUP_SECONDS="${WARMUP_SECONDS:-10}"
INGESTION_WAIT_SECONDS="${INGESTION_WAIT_SECONDS:-600}"
INGESTION_POLL_SECONDS="${INGESTION_POLL_SECONDS:-10}"
SEARCH_P95_SLO_MS="${SEARCH_P95_SLO_MS:-50}"
SEARCH_SUCCESS_SLO_PCT="${SEARCH_SUCCESS_SLO_PCT:-99}"
UPLOAD_SUCCESS_SLO_PCT="${UPLOAD_SUCCESS_SLO_PCT:-99}"
INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR="${INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR:-100}"

# Output
RESULTS_DIR="${RESULTS_DIR:-./load-test-results}"
TIMESTAMP="${TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PROTO_DIR="${PROJECT_ROOT}/crates/proto/proto"
PROTO_FILE="akidb.proto"
INGESTION_BASELINE_PROCESSED=""
INGESTION_START_TIME=""
EXPECTED_INGESTED_DOCS=""

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

require_positive_integer() {
    local name="$1"
    local value="$2"

    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        print_error "${name} must be a positive integer (got '${value}')"
        return 1
    fi
}

require_percentage() {
    local name="$1"
    local value="$2"

    require_positive_integer "$name" "$value" || return 1
    if [ "$value" -gt 100 ]; then
        print_error "${name} must be at most 100 (got '${value}')"
        return 1
    fi
}

validate_configuration() {
    require_positive_integer "TOTAL_DOCS" "$TOTAL_DOCS" || return 1
    require_positive_integer "CONCURRENT_UPLOADS" "$CONCURRENT_UPLOADS" || return 1
    require_positive_integer "SEARCH_QPS" "$SEARCH_QPS" || return 1
    require_positive_integer "SEARCH_DURATION" "$SEARCH_DURATION" || return 1
    require_positive_integer "SEARCH_TIMEOUT_SECONDS" "$SEARCH_TIMEOUT_SECONDS" || return 1
    require_positive_integer "SEARCH_MAX_IN_FLIGHT" "$SEARCH_MAX_IN_FLIGHT" || return 1
    require_positive_integer "UPLOAD_TIMEOUT_SECONDS" "$UPLOAD_TIMEOUT_SECONDS" || return 1
    require_positive_integer \
        "HEALTH_REQUEST_TIMEOUT_SECONDS" \
        "$HEALTH_REQUEST_TIMEOUT_SECONDS" || return 1
    require_positive_integer "WARMUP_SECONDS" "$WARMUP_SECONDS" || return 1
    require_positive_integer "INGESTION_WAIT_SECONDS" "$INGESTION_WAIT_SECONDS" || return 1
    require_positive_integer "INGESTION_POLL_SECONDS" "$INGESTION_POLL_SECONDS" || return 1
    require_positive_integer "SEARCH_P95_SLO_MS" "$SEARCH_P95_SLO_MS" || return 1
    require_percentage "SEARCH_SUCCESS_SLO_PCT" "$SEARCH_SUCCESS_SLO_PCT" || return 1
    require_percentage "UPLOAD_SUCCESS_SLO_PCT" "$UPLOAD_SUCCESS_SLO_PCT" || return 1
    require_positive_integer \
        "INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR" \
        "$INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR" || return 1
}

prometheus_query_value() {
    local query="$1"

    curl --fail --silent --show-error \
        --max-time "$HEALTH_REQUEST_TIMEOUT_SECONDS" \
        --get \
        --data-urlencode "query=${query}" \
        "${PROMETHEUS_URL}/api/v1/query" \
        | jq -er '
            if .status != "success" then
                error("Prometheus query failed")
            elif (.data.result | length) == 0 then
                "0"
            else
                .data.result[0].value[1]
            end
        '
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check for required tools
    for cmd in awk curl grpcurl jq; do
        if ! command -v "$cmd" &> /dev/null; then
            print_error "$cmd is required but not installed"
            exit 1
        fi
    done
    print_status "Required tools available"

    # Check services
    if ! curl -sf --max-time "$HEALTH_REQUEST_TIMEOUT_SECONDS" \
        "${UPLOAD_GATEWAY_URL}/health" > /dev/null 2>&1; then
        print_error "Upload Gateway not reachable at ${UPLOAD_GATEWAY_URL}"
        exit 1
    fi
    print_status "Upload Gateway: ${UPLOAD_GATEWAY_URL}"

    if ! curl -sf --max-time "$HEALTH_REQUEST_TIMEOUT_SECONDS" \
        "${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
        print_error "MinIO not reachable at ${MINIO_ENDPOINT}"
        exit 1
    fi
    print_status "MinIO: ${MINIO_ENDPOINT}"

    if ! prometheus_query_value "vector(1)" > /dev/null; then
        print_error "Prometheus not reachable at ${PROMETHEUS_URL}"
        exit 1
    fi
    print_status "Prometheus: ${PROMETHEUS_URL}"

    # Create results directory
    mkdir -p "${RESULTS_DIR}"
    print_status "Results directory: ${RESULTS_DIR}"
}

# Generate test documents
generate_test_documents() {
    print_header "Generating Test Documents"

    local doc_dir="${RESULTS_DIR}/test-docs"
    mkdir -p "$doc_dir"
    find "$doc_dir" -maxdepth 1 -type f -name 'test_doc_*' -delete

    local doc_count=0
    local formats=("json" "csv" "html" "txt")

    while [ "$doc_count" -lt "$TOTAL_DOCS" ]; do
        for format in "${formats[@]}"; do
            if [ "$doc_count" -ge "$TOTAL_DOCS" ]; then
                break
            fi

            local filename="test_doc_${doc_count}.${format}"
            local filepath="${doc_dir}/${filename}"

            case "$format" in
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

            doc_count=$((doc_count + 1))
        done
    done

    print_status "Generated ${TOTAL_DOCS} test documents"
    echo "  Formats: JSON, CSV, HTML, TXT"
}

# Upload a single document and write one isolated result file. Keeping worker
# output separate avoids concurrent writes corrupting the aggregate CSV.
upload_one_document() {
    local filepath="$1"
    local result_file="$2"
    local upload_log="$3"
    local filename
    local upload_start
    local response
    local upload_end
    local duration
    local status="failed"

    filename="$(basename "$filepath")"
    upload_start="$(date +%s.%N)"

    if response="$(curl --silent --show-error --write-out "%{http_code}" --output /dev/null \
        --max-time "$UPLOAD_TIMEOUT_SECONDS" \
        --request POST "${UPLOAD_GATEWAY_URL}/upload" \
        --form "file=@${filepath}" \
        2>> "$upload_log")"; then
        if [ "$response" = "200" ] || [ "$response" = "201" ]; then
            status="success"
        fi
    fi

    upload_end="$(date +%s.%N)"
    duration="$(awk -v start="$upload_start" -v finish="$upload_end" \
        'BEGIN { printf "%.3f", (finish - start) * 1000 }')"
    printf '%s,%s,%s,%s\n' \
        "$(date -Iseconds)" "$filename" "$status" "$duration" > "$result_file"
}

prepare_ingestion_measurement() {
    if ! INGESTION_BASELINE_PROCESSED="$(
        prometheus_query_value "sum(akidb_ingestion_documents_processed_total)"
    )"; then
        print_error "Could not read the ingestion processed-document baseline"
        return 1
    fi
    INGESTION_START_TIME="$(date +%s.%N)"
}

# Upload documents
upload_documents() {
    print_header "Uploading Documents (Ingestion Load Test)"

    local doc_dir="${RESULTS_DIR}/test-docs"
    local upload_log="${RESULTS_DIR}/upload_${TIMESTAMP}.log"
    local upload_times="${RESULTS_DIR}/upload_times_${TIMESTAMP}.csv"
    local worker_dir="${RESULTS_DIR}/.upload-workers-${TIMESTAMP}"
    local start_time
    local launched=0
    local completed=0
    local active=0
    local filepath
    local result_file
    local pid
    local end_time
    local total_duration
    local throughput
    local success_count
    local fail_count
    local success_rate
    local -a batch_pids=()

    printf 'timestamp,filename,status,duration_ms\n' > "$upload_times"
    rm -rf "$worker_dir"
    mkdir -p "$worker_dir"
    start_time="$(date +%s.%N)"

    # Bash 3.2 has no wait -n, so launch bounded batches and wait for each batch.
    while IFS= read -r -d '' filepath; do
        launched=$((launched + 1))
        result_file="$(printf '%s/%08d.csv' "$worker_dir" "$launched")"
        upload_one_document "$filepath" "$result_file" "$upload_log" &
        batch_pids[active]=$!
        active=$((active + 1))

        if [ "$active" -ge "$CONCURRENT_UPLOADS" ]; then
            for pid in "${batch_pids[@]}"; do
                wait "$pid"
            done
            completed=$((completed + active))
            printf '\r  Uploaded: %d/%d' "$completed" "$TOTAL_DOCS"
            batch_pids=()
            active=0
        fi
    done < <(find "$doc_dir" -type f -print0)

    if [ "$active" -gt 0 ]; then
        for pid in "${batch_pids[@]}"; do
            wait "$pid"
        done
        completed=$((completed + active))
    fi
    printf '\r  Uploaded: %d/%d\n' "$completed" "$TOTAL_DOCS"

    if [ "$launched" -ne "$TOTAL_DOCS" ]; then
        print_error "Expected ${TOTAL_DOCS} documents but found ${launched} in ${doc_dir}"
        rm -rf "$worker_dir"
        return 1
    fi

    for result_file in "$worker_dir"/*.csv; do
        cat "$result_file" >> "$upload_times"
    done
    success_count="$(awk -F, '$3 == "success" { count++ } END { print count + 0 }' \
        "$upload_times")"
    fail_count=$((launched - success_count))
    success_rate="$(awk -v successes="$success_count" -v requests="$launched" \
        'BEGIN { printf "%.2f", (requests > 0 ? successes * 100 / requests : 0) }')"
    EXPECTED_INGESTED_DOCS="$success_count"

    end_time="$(date +%s.%N)"
    total_duration="$(awk -v start="$start_time" -v finish="$end_time" \
        'BEGIN { printf "%.3f", finish - start }')"
    throughput="$(awk -v successes="$success_count" -v duration="$total_duration" \
        'BEGIN { printf "%.2f", (duration > 0 ? successes / duration : 0) }')"
    rm -rf "$worker_dir"

    print_status "Upload complete"
    echo "  Duration: ${total_duration}s"
    echo "  Throughput: ${throughput} docs/sec"
    echo "  Success: ${success_count}/${TOTAL_DOCS}"
    echo "  Success Rate: ${success_rate}%"

    # Save summary
    cat > "${RESULTS_DIR}/upload_summary_${TIMESTAMP}.json" << EOF
{
    "test_type": "upload",
    "timestamp": "$(date -Iseconds)",
    "total_docs": ${TOTAL_DOCS},
    "success_count": ${success_count},
    "fail_count": ${fail_count},
    "success_rate_pct": ${success_rate},
    "duration_seconds": ${total_duration},
    "throughput_docs_per_sec": ${throughput}
}
EOF
}

# Wait for ingestion to complete
wait_for_ingestion() {
    print_header "Waiting for Ingestion Pipeline"

    local max_wait="$INGESTION_WAIT_SECONDS"
    local check_interval="$INGESTION_POLL_SECONDS"
    local elapsed=0
    local expected="${EXPECTED_INGESTED_DOCS:-0}"
    local baseline="${INGESTION_BASELINE_PROCESSED:-0}"

    while [ "$elapsed" -lt "$max_wait" ]; do
        local queue_depth
        local processed_total
        local processed_delta
        local queue_empty=false
        local processed_complete=false

        if ! queue_depth="$(
            prometheus_query_value "sum(akidb_ingestion_queue_depth)" 2>/dev/null
        )"; then
            queue_depth="unavailable"
        fi
        if ! processed_total="$(
            prometheus_query_value \
                "sum(akidb_ingestion_documents_processed_total)" \
                2>/dev/null
        )"; then
            processed_total="unavailable"
        fi
        processed_delta="$(awk \
            -v current="$processed_total" \
            -v baseline="$baseline" \
            'BEGIN {
                if (current == "unavailable") {
                    print "unavailable"
                } else {
                    delta = current - baseline
                    printf "%.0f", (delta > 0 ? delta : 0)
                }
            }')"

        if [ "$queue_depth" != "unavailable" ] \
            && awk -v value="$queue_depth" 'BEGIN { exit !(value == 0) }'; then
            queue_empty=true
        fi
        if [ "$processed_delta" != "unavailable" ] \
            && [ "$processed_delta" -ge "$expected" ]; then
            processed_complete=true
        fi

        if [ "$queue_empty" = true ] && [ "$processed_complete" = true ]; then
            local finished_at
            local ingestion_duration
            local ingestion_per_second
            local ingestion_per_hour

            finished_at="$(date +%s.%N)"
            ingestion_duration="$(awk \
                -v start="${INGESTION_START_TIME:-$finished_at}" \
                -v finish="$finished_at" \
                'BEGIN { printf "%.3f", finish - start }')"
            ingestion_per_second="$(awk \
                -v documents="$expected" \
                -v duration="$ingestion_duration" \
                'BEGIN {
                    printf "%.2f", (duration > 0 ? documents / duration : 0)
                }')"
            ingestion_per_hour="$(awk \
                -v documents="$expected" \
                -v duration="$ingestion_duration" \
                'BEGIN {
                    printf "%.2f", (duration > 0 ? documents * 3600 / duration : 0)
                }')"

            cat > "${RESULTS_DIR}/ingestion_summary_${TIMESTAMP}.json" << EOF
{
    "test_type": "ingestion",
    "timestamp": "$(date -Iseconds)",
    "expected_documents": ${expected},
    "processed_documents": ${processed_delta},
    "duration_seconds": ${ingestion_duration},
    "throughput_docs_per_sec": ${ingestion_per_second},
    "throughput_docs_per_hour": ${ingestion_per_hour}
}
EOF
            print_status \
                "Ingestion completed: ${processed_delta}/${expected} documents"
            print_status \
                "End-to-end throughput: ${ingestion_per_hour} docs/hour"
            print_status "Waiting ${WARMUP_SECONDS}s before search..."
            sleep "$WARMUP_SECONDS"
            return 0
        fi

        echo -ne \
            "\r  Queue depth: ${queue_depth}, processed: ${processed_delta}/${expected}, waiting..."
        sleep "$check_interval"
        elapsed=$((elapsed + check_interval))
    done

    echo ""
    print_error "Ingestion did not complete within ${max_wait}s"
    return 1
}

# Run one lexical TextSearch request directly against the shard gRPC service.
# The coordinator cannot embed text, and the old HTTP request targeted the
# document parser rather than AkiDB.
search_one_request() {
    local query="$1"
    local result_file="$2"
    local search_log="$3"
    local payload
    local req_start
    local req_end
    local latency
    local status="failed"

    payload="$(jq -cn \
        --arg collection "$AKIDB_COLLECTION" \
        --arg text "$query" \
        '{collection: $collection, text: $text, topK: 10, retrievalMode: "bm25"}')"
    req_start="$(date +%s.%N)"
    if grpcurl -plaintext \
        -max-time "$SEARCH_TIMEOUT_SECONDS" \
        -import-path "$PROTO_DIR" \
        -proto "$PROTO_FILE" \
        -d "$payload" \
        "$AKIDB_SERVER" \
        akidb.v1.Akidb/TextSearch \
        > /dev/null 2>> "$search_log"; then
        status="success"
    fi
    req_end="$(date +%s.%N)"
    latency="$(awk -v start="$req_start" -v finish="$req_end" \
        'BEGIN { printf "%.3f", (finish - start) * 1000 }')"
    printf '%s,%s,%s\n' "$(date -Iseconds)" "$latency" "$status" > "$result_file"
}

percentile_from_sorted_csv() {
    local csv_file="$1"
    local percentile="$2"

    awk -F, -v percentile="$percentile" '
        {
            values[NR] = $2
        }
        END {
            if (NR == 0) {
                print "0.000"
                exit
            }
            rank = int((NR * percentile + 99) / 100)
            if (rank < 1) {
                rank = 1
            }
            print values[rank]
        }
    ' "$csv_file"
}

# Run search load test
run_search_load_test() {
    print_header "Running Search Load Test"

    local search_log="${RESULTS_DIR}/search_${TIMESTAMP}.log"
    local search_times="${RESULTS_DIR}/search_times_${TIMESTAMP}.csv"
    local sorted_times="${RESULTS_DIR}/.search-sorted-${TIMESTAMP}.csv"
    local worker_dir="${RESULTS_DIR}/.search-workers-${TIMESTAMP}"
    local -a queries=(
        "test document content"
        "load testing performance"
        "semantic chunker embedding"
        "AkiDB ingestion pipeline"
        "similarity search vectors"
    )
    local total_requests=$((SEARCH_QPS * SEARCH_DURATION))
    local request_count=0
    local next_wait=0
    local active=0
    local query
    local result_file
    local target_delay
    local now
    local pid
    local end_time
    local total_duration
    local actual_qps
    local avg_latency
    local success_rate
    local success_count
    local total_latency
    local p50
    local p95
    local p99
    local -a search_pids=()

    printf 'timestamp,latency_ms,status\n' > "$search_times"
    rm -rf "$worker_dir"
    mkdir -p "$worker_dir"
    print_status "Target: ${SEARCH_QPS} QPS for ${SEARCH_DURATION}s (${total_requests} requests)"

    local start_time
    start_time="$(date +%s.%N)"

    while [ "$request_count" -lt "$total_requests" ]; do
        query="${queries[$((RANDOM % ${#queries[@]}))]}"
        request_count=$((request_count + 1))
        result_file="$(printf '%s/%08d.csv' "$worker_dir" "$request_count")"
        search_one_request "$query" "$result_file" "$search_log" &
        search_pids[request_count - 1]=$!
        active=$((active + 1))

        # Bound stalled requests while preserving the requested launch rate
        # whenever the service keeps up.
        if [ "$active" -ge "$SEARCH_MAX_IN_FLIGHT" ]; then
            wait "${search_pids[next_wait]}"
            next_wait=$((next_wait + 1))
            active=$((active - 1))
        fi

        if [ $((request_count % 100)) -eq 0 ]; then
            printf '\r  Requests launched: %d/%d' "$request_count" "$total_requests"
        fi

        now="$(date +%s.%N)"
        target_delay="$(awk \
            -v start="$start_time" \
            -v now="$now" \
            -v count="$request_count" \
            -v qps="$SEARCH_QPS" \
            'BEGIN {
                delay = (start + count / qps) - now
                printf "%.6f", (delay > 0 ? delay : 0)
            }')"
        if [ "$target_delay" != "0.000000" ]; then
            sleep "$target_delay"
        fi
    done

    while [ "$next_wait" -lt "$request_count" ]; do
        pid="${search_pids[next_wait]}"
        wait "$pid"
        next_wait=$((next_wait + 1))
    done
    printf '\r  Requests completed: %d/%d\n' "$request_count" "$total_requests"

    for result_file in "$worker_dir"/*.csv; do
        cat "$result_file" >> "$search_times"
    done
    rm -rf "$worker_dir"

    success_count="$(awk -F, '$3 == "success" { count++ } END { print count + 0 }' \
        "$search_times")"
    total_latency="$(awk -F, 'NR > 1 && $3 == "success" { total += $2 } END { printf "%.3f", total }' \
        "$search_times")"
    end_time="$(date +%s.%N)"
    total_duration="$(awk -v start="$start_time" -v finish="$end_time" \
        'BEGIN { printf "%.3f", finish - start }')"
    actual_qps="$(awk -v requests="$request_count" -v duration="$total_duration" \
        'BEGIN { printf "%.2f", (duration > 0 ? requests / duration : 0) }')"
    avg_latency="$(awk -v total="$total_latency" -v requests="$success_count" \
        'BEGIN { printf "%.2f", (requests > 0 ? total / requests : 0) }')"
    success_rate="$(awk -v successes="$success_count" -v requests="$request_count" \
        'BEGIN { printf "%.2f", (requests > 0 ? successes * 100 / requests : 0) }')"

    awk -F, 'NR > 1 && $3 == "success" { print }' "$search_times" \
        | sort -t, -k2,2n > "$sorted_times"
    p50="$(percentile_from_sorted_csv "$sorted_times" 50)"
    p95="$(percentile_from_sorted_csv "$sorted_times" 95)"
    p99="$(percentile_from_sorted_csv "$sorted_times" 99)"
    rm -f "$sorted_times"

    print_status "Search test complete"
    echo "  Duration: ${total_duration}s"
    echo "  Actual QPS: ${actual_qps}"
    echo "  Avg Latency: ${avg_latency}ms"
    echo "  Success Rate: ${success_rate}%"

    # Save summary
    cat > "${RESULTS_DIR}/search_summary_${TIMESTAMP}.json" << EOF
{
    "test_type": "search",
    "timestamp": "$(date -Iseconds)",
    "total_requests": ${request_count},
    "success_count": ${success_count},
    "success_rate_pct": ${success_rate},
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
    local upload_summary="${RESULTS_DIR}/upload_summary_${TIMESTAMP}.json"
    local ingestion_summary="${RESULTS_DIR}/ingestion_summary_${TIMESTAMP}.json"
    local search_summary="${RESULTS_DIR}/search_summary_${TIMESTAMP}.json"
    local upload_table
    local ingestion_table
    local search_table
    local search_p95
    local search_success_rate
    local upload_success_rate
    local ingestion_per_hour
    local search_latency_slo_status
    local search_success_slo_status
    local ingestion_throughput_slo_status
    local upload_success_slo_status
    local report_passed=true

    upload_table="$(jq -r '
        "| Metric | Value |\n|--------|-------|\n| Total Documents | \(.total_docs) |\n| Success Count | \(.success_count) |\n| Failure Count | \(.fail_count) |\n| Success Rate | \(.success_rate_pct)% |\n| Duration | \(.duration_seconds)s |\n| Throughput | \(.throughput_docs_per_sec) docs/sec |"
    ' "$upload_summary")"
    ingestion_table="$(jq -r '
        "| Metric | Value |\n|--------|-------|\n| Expected Documents | \(.expected_documents) |\n| Processed Documents | \(.processed_documents) |\n| End-to-End Duration | \(.duration_seconds)s |\n| Throughput | \(.throughput_docs_per_sec) docs/sec (\(.throughput_docs_per_hour) docs/hour) |"
    ' "$ingestion_summary")"
    search_table="$(jq -r '
        "| Metric | Value |\n|--------|-------|\n| Total Requests | \(.total_requests) |\n| Success Count | \(.success_count) |\n| Success Rate | \(.success_rate_pct)% |\n| Actual QPS | \(.actual_qps) |\n| Avg Successful Latency | \(.latency.avg_ms)ms |\n| P50 Successful Latency | \(.latency.p50_ms)ms |\n| P95 Successful Latency | \(.latency.p95_ms)ms |\n| P99 Successful Latency | \(.latency.p99_ms)ms |"
    ' "$search_summary")"
    search_p95="$(jq -r '.latency.p95_ms' "$search_summary")"
    search_success_rate="$(jq -r '.success_rate_pct' "$search_summary")"
    upload_success_rate="$(jq -r '.success_rate_pct' "$upload_summary")"
    ingestion_per_hour="$(jq -r '.throughput_docs_per_hour' "$ingestion_summary")"
    search_latency_slo_status="$(awk -v p95="$search_p95" -v target="$SEARCH_P95_SLO_MS" \
        'BEGIN { print (p95 < target ? "✓ PASS" : "✗ FAIL") }')"
    search_success_slo_status="$(awk \
        -v actual="$search_success_rate" \
        -v target="$SEARCH_SUCCESS_SLO_PCT" \
        'BEGIN { print (actual >= target ? "✓ PASS" : "✗ FAIL") }')"
    ingestion_throughput_slo_status="$(awk \
        -v rate="$ingestion_per_hour" \
        -v target="$INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR" \
        'BEGIN { print (rate > target ? "✓ PASS" : "✗ FAIL") }')"
    upload_success_slo_status="$(awk \
        -v actual="$upload_success_rate" \
        -v target="$UPLOAD_SUCCESS_SLO_PCT" \
        'BEGIN { print (actual >= target ? "✓ PASS" : "✗ FAIL") }')"

    for status in \
        "$search_latency_slo_status" \
        "$search_success_slo_status" \
        "$ingestion_throughput_slo_status" \
        "$upload_success_slo_status"; do
        if [[ "$status" == *FAIL ]]; then
            report_passed=false
        fi
    done

    cat > "$report_file" << EOF
# AkiDB Load Test Report

**Date:** $(date -Iseconds)
**Test ID:** ${TIMESTAMP}

## Configuration

| Parameter | Value |
|-----------|-------|
| Total Documents | ${TOTAL_DOCS} |
| Concurrent Uploads | ${CONCURRENT_UPLOADS} |
| Search QPS Target | ${SEARCH_QPS} |
| Search Duration | ${SEARCH_DURATION}s |
| Search Endpoint | ${AKIDB_SERVER} |
| Search Collection | ${AKIDB_COLLECTION} |

## Upload Test Results

${upload_table}

## Ingestion Test Results

${ingestion_table}

## Search Test Results

${search_table}

## SLO Compliance

| SLO | Target | Actual | Status |
|-----|--------|--------|--------|
| Search P95 Successful Latency | < ${SEARCH_P95_SLO_MS}ms | ${search_p95}ms | ${search_latency_slo_status} |
| Search Success Rate | ≥ ${SEARCH_SUCCESS_SLO_PCT}% | ${search_success_rate}% | ${search_success_slo_status} |
| Ingestion Throughput | > ${INGESTION_THROUGHPUT_SLO_DOCS_PER_HOUR} docs/hr | ${ingestion_per_hour} docs/hr | ${ingestion_throughput_slo_status} |
| Upload Success Rate | ≥ ${UPLOAD_SUCCESS_SLO_PCT}% | ${upload_success_rate}% | ${upload_success_slo_status} |

## Files Generated

- \`upload_times_${TIMESTAMP}.csv\` - Individual upload latencies
- \`search_times_${TIMESTAMP}.csv\` - Individual search latencies
- \`upload_summary_${TIMESTAMP}.json\` - Upload test summary
- \`ingestion_summary_${TIMESTAMP}.json\` - End-to-end ingestion summary
- \`search_summary_${TIMESTAMP}.json\` - Search test summary

---
*Generated by AkiDB Load Test Script*
EOF

    print_status "Report saved to: ${report_file}"
    if [ "$report_passed" != true ]; then
        print_error "One or more load-test SLO gates failed"
        return 1
    fi
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
    validate_configuration

    print_header "AkiDB Production Load Test"
    echo "  Test ID: ${TIMESTAMP}"
    echo "  Documents: ${TOTAL_DOCS}"
    echo "  Search QPS: ${SEARCH_QPS} for ${SEARCH_DURATION}s"
    echo ""

    check_prerequisites
    generate_test_documents
    prepare_ingestion_measurement
    upload_documents
    wait_for_ingestion
    run_search_load_test
    local report_status=0
    generate_report || report_status=$?
    cleanup
    if [ "$report_status" -ne 0 ]; then
        return "$report_status"
    fi

    print_header "Load Test Complete"
    echo "Results saved to: ${RESULTS_DIR}"
}

require_option_value() {
    local option="$1"
    local count="$2"

    if [ "$count" -lt 2 ]; then
        print_error "${option} requires a value"
        return 1
    fi
}

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --docs N           Number of documents to upload (default: 100)"
    echo "  --concurrency N    Concurrent document uploads (default: 5)"
    echo "  --qps N            Search queries per second (default: 50)"
    echo "  --duration N       Search test duration in seconds (default: 60)"
    echo "  --server HOST:PORT AkiDB shard gRPC endpoint (default: localhost:50051)"
    echo "  --collection NAME  AkiDB collection to search (default: default)"
    echo "  --keep-docs        Keep test documents after completion"
    echo "  --help             Show this help message"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --docs)
                require_option_value "$1" "$#" || return 1
                TOTAL_DOCS="$2"
                shift 2
                ;;
            --concurrency)
                require_option_value "$1" "$#" || return 1
                CONCURRENT_UPLOADS="$2"
                shift 2
                ;;
            --qps)
                require_option_value "$1" "$#" || return 1
                SEARCH_QPS="$2"
                shift 2
                ;;
            --duration)
                require_option_value "$1" "$#" || return 1
                SEARCH_DURATION="$2"
                shift 2
                ;;
            --server)
                require_option_value "$1" "$#" || return 1
                AKIDB_SERVER="$2"
                shift 2
                ;;
            --collection)
                require_option_value "$1" "$#" || return 1
                AKIDB_COLLECTION="$2"
                shift 2
                ;;
            --keep-docs)
                KEEP_DOCS=true
                shift
                ;;
            --help)
                show_help
                return 2
                ;;
            *)
                print_error "Unknown option: $1"
                return 1
                ;;
        esac
    done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    if parse_args "$@"; then
        main
    else
        parse_status=$?
        if [ "$parse_status" -ne 2 ]; then
            exit "$parse_status"
        fi
        exit 0
    fi
fi
