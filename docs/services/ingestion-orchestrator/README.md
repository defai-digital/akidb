# AkiDB Ingestion Orchestrator

The Ingestion Orchestrator is a hybrid Rust/Python document processing pipeline for AkiDB on macOS Apple Silicon. It processes documents uploaded to MinIO, extracts text, generates embeddings, and stores vectors in AkiDB.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                     NATS JetStream (3-node)                       │
│                    Upload Event Consumer                          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Ingestion Orchestrator (Rust)                   │
│                                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │
│  │ Idempotency │  │   Memory    │  │Backpressure │               │
│  │   Checker   │  │ Coordinator │  │ Controller  │               │
│  └─────────────┘  └─────────────┘  └─────────────┘               │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    MinIO Storage Client                      │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                             │                                     │
│         ┌───────────────────┴───────────────────┐                │
│         ▼                                       ▼                │
│  ┌─────────────────┐                   ┌─────────────────┐       │
│  │  Rust Parsers   │                   │  Python Parser  │       │
│  │ (60-70% docs)   │                   │  (30-40% docs)  │       │
│  └────────┬────────┘                   └───────┬─────────┘       │
│           │        ┌─────────────────┐         │                 │
│           │        │ Circuit Breaker │─────────┘                 │
│           └────────┴──────┬──────────┘                           │
│                           ▼                                       │
│            ┌─────────────────────────────┐                       │
│            │    Semantic Chunker         │                       │
│            │   (tiktoken cl100k_base)    │                       │
│            └──────────────┬──────────────┘                       │
│                           ▼                                       │
│            ┌─────────────────────────────┐                       │
│            │    Embedding Client         │                       │
│            │  (Qwen3-Embedding-8B)       │                       │
│            └──────────────┬──────────────┘                       │
│                           ▼                                       │
│            ┌─────────────────────────────┐                       │
│            │    AkiDB gRPC Client        │                       │
│            └─────────────────────────────┘                       │
└──────────────────────────────────────────────────────────────────┘
```

## Features

### Document Formats

| Format | Parser | Notes |
|--------|--------|-------|
| JSON | Rust | Extracts all string values |
| CSV | Rust | Converts rows to text |
| HTML | Rust | Extracts visible text, excludes scripts/styles |
| XML | Rust | Extracts text content and CDATA |
| XLSX | Rust | Reads all sheets |
| TXT | Rust | Pass-through |
| PDF | Python | Uses pdfplumber |
| DOCX | Python | Uses python-docx |

### Resilience Patterns

- **Circuit Breaker**: Protects Python parser calls
  - States: CLOSED → OPEN → HALF-OPEN
  - Default threshold: 3 failures
  - Reset timeout: 30 seconds

- **Backpressure**: Monitors AkiDB insert latency
  - Threshold: 500ms
  - Pause duration: 5 seconds
  - Also monitors queue depth (high water: 10,000)

- **Memory Coordinator**: local memory monitoring
  - Pause threshold: 70%
  - Resume threshold: 60%

### Processing Pipeline

1. **Idempotency Check**: SHA-256 content hash
2. **Document Fetch**: MinIO/S3 storage
3. **Format Detection**: Extension-based routing
4. **Parsing**: Rust-native or Python sidecar
5. **Chunking**: Sentence-boundary aware, tiktoken token counting
6. **Embedding**: Qwen3-Embedding-8B via vLLM
7. **Insertion**: AkiDB gRPC batch insert

## Configuration

### Environment Variables

```bash
# NATS Configuration
NATS_URL=nats://localhost:4222
NATS_STREAM=akidb-uploads
NATS_CONSUMER=ingestion-orchestrator
NATS_DLQ_STREAM=akidb-dlq

# Storage Configuration
STORAGE_ENDPOINT=http://localhost:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin
STORAGE_BUCKET=akidb-documents
STORAGE_REGION=us-east-1

# AkiDB Configuration
AKIDB_ENDPOINT=http://localhost:50051
AKIDB_TIMEOUT_MS=30000
AKIDB_MAX_RETRIES=3

# Embedding Service
EMBEDDING_URL=http://localhost:8000

# Python Parser Service
DOC_PARSER_URL=http://localhost:8080

# Circuit Breaker
CIRCUIT_BREAKER_THRESHOLD=3
CIRCUIT_BREAKER_RESET_SECS=30
CIRCUIT_BREAKER_HALF_OPEN_CALLS=1

# Backpressure
BACKPRESSURE_LATENCY_THRESHOLD_MS=500
BACKPRESSURE_QUEUE_DEPTH=10000
BACKPRESSURE_PAUSE_SECS=5

# Memory
MEMORY_PAUSE_THRESHOLD_PCT=70
MEMORY_RESUME_THRESHOLD_PCT=60
MEMORY_POLL_INTERVAL_MS=1000

# Chunker
CHUNKER_TARGET_TOKENS=512
CHUNKER_MIN_OVERLAP=20
CHUNKER_MAX_OVERLAP=50

# Batcher
BATCHER_MIN_BATCH=16
BATCHER_MAX_BATCH=64
BATCHER_TIMEOUT_MS=100
```

## Running

### Development

```bash
# Build
cargo build -p akidb-ingestion

# Run with logging
RUST_LOG=info cargo run -p akidb-ingestion

# Run tests
cargo test -p akidb-ingestion

# Run benchmarks
cargo bench -p akidb-ingestion
```

### Docker Compose

```bash
cd deploy/compose

# Start all services
docker compose up -d

# Run E2E tests
./scripts/e2e-test.sh

# View logs
docker compose logs -f ingestion
```

## Metrics

The orchestrator exposes Prometheus metrics on the configured port:

| Metric | Type | Description |
|--------|------|-------------|
| `akidb_ingestion_documents_processed_total` | Counter | Documents processed by format/parser |
| `akidb_ingestion_documents_failed_total` | Counter | Failed documents by format/stage |
| `akidb_ingestion_chunks_created_total` | Counter | Chunks created |
| `akidb_ingestion_embeddings_generated_total` | Counter | Embeddings generated |
| `akidb_ingestion_vectors_inserted_total` | Counter | Vectors inserted into AkiDB |
| `akidb_ingestion_parse_latency_seconds` | Histogram | Parse latency by format |
| `akidb_ingestion_embed_latency_seconds` | Histogram | Embedding latency |
| `akidb_ingestion_insert_latency_seconds` | Histogram | AkiDB insert latency |
| `akidb_ingestion_circuit_breaker_state` | Gauge | Circuit breaker state (0/1/2) |
| `akidb_ingestion_backpressure_active` | Gauge | Backpressure active (0/1) |
| `akidb_ingestion_memory_usage_percent` | Gauge | Memory usage percentage |
| `akidb_ingestion_queue_depth` | Gauge | Current queue depth |
| `akidb_ingestion_batch_size` | Gauge | Current batch size |

## Grafana Dashboard

Import the dashboard from `deploy/compose/monitoring/dashboards/ingestion-pipeline.json`

The dashboard includes:
- Overview: Documents/min, Vectors/min, Circuit Breaker state, Backpressure status
- Throughput: Documents by format, Pipeline throughput
- Latency: Parse, Embed, Insert latency percentiles
- Resources: Memory usage, Queue depth, Batch size
- Errors: Failed documents by format and stage

## Troubleshooting

### Circuit Breaker Open

**Symptom**: PDF/DOCX processing failing, circuit breaker state = 1

**Resolution**:
1. Check Python parser service: `curl http://localhost:8080/health`
2. Check logs: `docker compose logs doc-parser`
3. Restart parser: `docker compose restart doc-parser`
4. Circuit will auto-reset after timeout

### Backpressure Active

**Symptom**: Processing paused, backpressure_active = 1

**Resolution**:
1. Check AkiDB health: `curl http://localhost:50051/health`
2. Check insert latency in Grafana
3. Reduce ingestion rate or batch size

### Memory Pressure

**Symptom**: Processing paused due to memory

**Resolution**:
1. Check local process memory with Activity Monitor or `top`
2. Reduce batch size: `BATCHER_MAX_BATCH=32`
3. Pause uploads until memory pressure clears

### Documents Stuck in DLQ

**Symptom**: Documents in dead letter queue

**Resolution**:
1. Check DLQ stream: `nats stream view akidb-dlq`
2. Investigate failure reason in logs
3. Fix issue and replay: `nats consumer ack akidb-dlq`
