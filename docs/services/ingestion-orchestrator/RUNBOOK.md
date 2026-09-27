# AkiDB Ingestion Orchestrator Runbook

## Supported Environment

The ingestion pipeline supports the portable runtime on macOS 26 Apple Silicon
(Mac Studio preferred; Mini/MacBook also fine for development) and Ubuntu
24.04+ AMD64. Published Compose images are AMD64. Linux ARM64, NVIDIA Thor,
and CUDA/GPU-accelerated index steps are unsupported.

This runbook covers the ingestion work queue, not immutable-generation
activation or replica recovery. NATS upload events are separate from the
PostgreSQL-authoritative knowledge-serving control path.

## Start The Stack

```bash
cd deploy/compose
docker compose up -d nats-1 nats-2 nats-3 seaweedfs
docker compose up -d doc-parser upload-gateway ingestion prometheus grafana
```

## Stop The Stack

```bash
docker compose down
docker compose down -v --remove-orphans
```

## Health Checks

```bash
curl http://localhost:8222/healthz
curl http://localhost:8333/healthz
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8000/health
docker compose logs --tail=100 ingestion
```

## Common Operations

```bash
nats stream ls
nats stream info akidb-uploads
nats consumer info akidb-uploads ingestion-orchestrator
docker compose exec seaweedfs sh -c 'echo "s3.bucket.list" | weed shell -master=localhost:9333'
docker compose logs -f ingestion
```

Uploads reach the orchestrator through the upload gateway's NATS publish
(`seaweedfs.uploads`). The SeaweedFS S3 gateway does not emit S3 bucket
notifications, so an object written directly into the bucket is picked up only
by the orchestrator's next scheduled bucket/manifest sync.

## Backpressure Active

Symptoms: ingestion pauses, queue depth grows, or insert latency rises.

Resolution:

- Check AkiDB health and insert latency.
- Reduce `BATCHER_MAX_BATCH`.
- Pause new uploads until queues drain.
- Review `docker compose logs ingestion`.

## Circuit Breaker Open

Symptoms: PDF/DOCX processing fails and parser calls are blocked.

Resolution:

- Check parser health: `curl http://localhost:8080/health`.
- Review parser logs: `docker compose logs doc-parser`.
- Restart parser: `docker compose restart doc-parser`.
- Increase parser timeout only after confirming slow documents are expected.

## Memory Pressure

Symptoms: high local memory usage or ingestion pauses.

Resolution:

- Check process memory with Activity Monitor or `top`.
- Reduce batch size.
- Restart services during a quiet period if memory is not released.

## DLQ Handling

```bash
nats stream info akidb-dlq
nats consumer next akidb-dlq dlq-reader --no-ack
docker compose logs ingestion | grep DLQ
```

Fix the root cause, then replay documents with an explicit operator action.
