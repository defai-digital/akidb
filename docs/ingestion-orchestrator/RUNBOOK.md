# AkiDB Ingestion Orchestrator - Operations Runbook

## Quick Reference

| Component | Port | Health Check |
|-----------|------|--------------|
| NATS (primary) | 4222 | `curl http://localhost:8222/healthz` |
| MinIO | 9000 | `curl http://localhost:9000/minio/health/live` |
| Doc Parser | 8080 | `curl http://localhost:8080/health` |
| Upload Gateway | 8081 | `curl http://localhost:8081/health` |
| Embedding (vLLM) | 8000 | `curl http://localhost:8000/health` |
| AkiDB | 50051 | gRPC health check |
| Prometheus | 9090 | `curl http://localhost:9090/-/healthy` |
| Grafana | 3000 | `curl http://localhost:3000/api/health` |

## Starting the Stack

### Development (CPU Mode)

```bash
cd deploy/compose

# Start infrastructure
docker compose up -d nats-1 nats-2 nats-3 minio

# Wait for health
sleep 10

# Start services
docker compose up -d doc-parser upload-gateway ingestion

# Start monitoring
docker compose up -d prometheus grafana
```

### Production (GPU Mode - Thor)

```bash
cd deploy/compose

# Ensure GPU is configured
./scripts/setup-thor-gpu.sh

# Start with GPU compose
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

## Stopping the Stack

```bash
# Graceful shutdown
docker compose down

# Full cleanup including volumes
docker compose down -v --remove-orphans
```

## Health Checks

### Automated Health Check Script

```bash
#!/bin/bash
# health-check.sh

check_service() {
    local name=$1
    local url=$2
    if curl -sf "$url" > /dev/null 2>&1; then
        echo "✅ $name: healthy"
        return 0
    else
        echo "❌ $name: unhealthy"
        return 1
    fi
}

check_service "NATS" "http://localhost:8222/healthz"
check_service "MinIO" "http://localhost:9000/minio/health/live"
check_service "Doc Parser" "http://localhost:8080/health"
check_service "Embedding" "http://localhost:8000/health"
check_service "Prometheus" "http://localhost:9090/-/healthy"
```

## Common Operations

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f ingestion

# Last N lines
docker compose logs --tail 100 ingestion

# Filter by time
docker compose logs --since 1h ingestion
```

### Checking NATS Streams

```bash
# List streams
nats stream ls

# View stream info
nats stream info akidb-uploads

# View consumer info
nats consumer info akidb-uploads ingestion-orchestrator

# View pending messages
nats consumer info akidb-uploads ingestion-orchestrator --json | jq '.num_pending'
```

### Checking MinIO

```bash
# List buckets
docker compose exec minio mc ls local/

# List objects in bucket
docker compose exec minio mc ls local/akidb-documents/

# Check bucket notifications
docker compose exec minio mc admin info local/
```

### Scaling Services

```bash
# Scale ingestion workers (if using replicas)
docker compose up -d --scale ingestion=3

# Note: Ensure NATS consumer is configured for shared subscriptions
```

## Incident Response

### High Latency Alert

**Symptoms**:
- Grafana alert for high insert latency
- Backpressure active
- Documents queuing up

**Investigation**:
```bash
# Check AkiDB status
docker compose logs akidb-server

# Check FAISS index size
docker compose exec akidb-server ls -la /var/lib/akidb/

# Check GPU memory (Thor)
tegrastats --interval 1000

# Check insert latency trend
curl -s 'http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,rate(akidb_ingestion_insert_latency_seconds_bucket[5m]))'
```

**Resolution**:
1. If AkiDB overloaded: Scale or optimize index
2. If GPU memory full: Restart embedding service
3. If network issues: Check container networking

### Circuit Breaker Open

**Symptoms**:
- PDF/DOCX documents failing
- Circuit breaker state = 1 (Open)
- Python parser errors in logs

**Investigation**:
```bash
# Check parser health
curl http://localhost:8080/health

# Check parser logs
docker compose logs doc-parser

# Check parser memory
docker stats akidb-doc-parser

# Test parser endpoint
curl -X POST http://localhost:8080/parse \
  -H "Content-Type: application/json" \
  -d '{"content": "dGVzdA==", "filename": "test.txt"}'
```

**Resolution**:
1. Restart parser: `docker compose restart doc-parser`
2. Increase timeout: `DOC_PARSER_TIMEOUT=60`
3. Check dependencies (pdfplumber, python-docx)

### Memory Pressure (Thor)

**Symptoms**:
- Ingestion paused
- Memory usage > 70%
- tegrastats showing high RAM usage

**Investigation**:
```bash
# Check tegrastats
tegrastats --interval 1000

# Check container memory
docker stats

# Check GPU memory allocation
nvidia-smi

# Check ingestion memory metric
curl -s 'http://localhost:9090/api/v1/query?query=akidb_ingestion_memory_usage_percent'
```

**Resolution**:
1. Reduce batch size: `BATCHER_MAX_BATCH=32`
2. Reduce vLLM memory: `--gpu-memory-utilization 0.6`
3. Restart services to free memory
4. Kill other GPU processes

### Documents in DLQ

**Symptoms**:
- Documents not appearing in search
- DLQ stream has messages

**Investigation**:
```bash
# Check DLQ count
nats stream info akidb-dlq

# View DLQ messages
nats consumer next akidb-dlq dlq-reader --no-ack

# Check original error
docker compose logs ingestion | grep "DLQ"
```

**Resolution**:
1. Fix underlying issue (parser, storage, etc.)
2. Replay messages:
   ```bash
   # Create replay consumer
   nats consumer add akidb-dlq replay --ack explicit

   # Process and replay
   # (implement replay logic in your code)
   ```

### NATS Cluster Issues

**Symptoms**:
- Messages not being processed
- NATS connection errors in logs
- JetStream unavailable

**Investigation**:
```bash
# Check cluster status
curl http://localhost:8222/varz | jq '.jetstream'

# Check all nodes
for port in 8222 8223 8224; do
  echo "Node on $port:"
  curl -s "http://localhost:$port/healthz"
done

# Check routes
curl http://localhost:8222/routez
```

**Resolution**:
1. Restart unhealthy node: `docker compose restart nats-2`
2. Check disk space for JetStream storage
3. Verify network connectivity between nodes

## Backup and Recovery

### Backup MinIO Data

```bash
# Export bucket to local directory
docker compose exec minio mc mirror local/akidb-documents /backup/

# Or use aws cli
aws --endpoint-url http://localhost:9000 s3 sync s3://akidb-documents ./backup/
```

### Backup NATS Streams

```bash
# Backup stream configuration
nats stream info akidb-uploads --json > stream-config.json

# Note: Message backup requires NATS Enterprise or custom tooling
```

### Backup State Database

```bash
# Copy SQLite database
docker compose exec ingestion cp /var/lib/akidb/ingestion.db /backup/

# Or from host
docker cp akidb-ingestion:/var/lib/akidb/ingestion.db ./backup/
```

### Recovery Procedures

**Full Stack Recovery**:
```bash
# 1. Start infrastructure
docker compose up -d nats-1 nats-2 nats-3 minio

# 2. Restore MinIO data
docker compose exec minio mc mirror /backup/ local/akidb-documents

# 3. Start remaining services
docker compose up -d

# 4. Verify health
./scripts/health-check.sh
```

## Performance Tuning

### For High Throughput

```bash
# Increase batch size
BATCHER_MAX_BATCH=128

# Reduce chunking overlap
CHUNKER_MIN_OVERLAP=10
CHUNKER_MAX_OVERLAP=30

# Increase queue depth tolerance
BACKPRESSURE_QUEUE_DEPTH=50000
```

### For Low Latency

```bash
# Reduce batch timeout
BATCHER_TIMEOUT_MS=50

# Smaller batches
BATCHER_MAX_BATCH=32

# Stricter latency threshold
BACKPRESSURE_LATENCY_THRESHOLD_MS=200
```

### For Memory-Constrained (Thor)

```bash
# Smaller batches
BATCHER_MAX_BATCH=16

# Lower memory threshold
MEMORY_PAUSE_THRESHOLD_PCT=60
MEMORY_RESUME_THRESHOLD_PCT=50

# Reduce vLLM memory
# In docker-compose.gpu.yml:
# --gpu-memory-utilization 0.5
```

## Monitoring Alerts

### Recommended Alert Rules

```yaml
# prometheus-alerts.yml
groups:
  - name: akidb-ingestion
    rules:
      - alert: HighInsertLatency
        expr: histogram_quantile(0.95, rate(akidb_ingestion_insert_latency_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High AkiDB insert latency"

      - alert: CircuitBreakerOpen
        expr: akidb_ingestion_circuit_breaker_state == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Circuit breaker is open"

      - alert: BackpressureActive
        expr: akidb_ingestion_backpressure_active == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Backpressure is active"

      - alert: HighMemoryUsage
        expr: akidb_ingestion_memory_usage_percent > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Memory usage above 80%"

      - alert: DocumentProcessingFailed
        expr: increase(akidb_ingestion_documents_failed_total[5m]) > 10
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Multiple document processing failures"
```

## Contact and Escalation

- **Level 1**: Check dashboards, restart services
- **Level 2**: Review logs, investigate specific failures
- **Level 3**: Code-level debugging, performance profiling

For urgent issues, check:
1. Grafana dashboard: http://localhost:3000
2. Prometheus alerts: http://localhost:9090/alerts
3. Container logs: `docker compose logs -f`
