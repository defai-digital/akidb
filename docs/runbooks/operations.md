# AkiDB Operations Runbook

## Supported Environment

AkiDB is supported on macOS Apple Silicon only. Use CPU/portable features for
all builds and tests. Thor, CUDA, NVIDIA GPU, Linux ARM, and Kubernetes
deployment procedures are not supported in active operations.

## Local Build And Validation

```bash
./scripts/build-on-mac-arm64.sh
cargo check --workspace --features cpu
cargo test --workspace --features cpu
cargo build --release -p akidb-server --features cpu
```

## Docker Compose Stack

Use Compose for local supporting services and integration testing:

```bash
cd deploy/compose
docker compose up -d nats-1 nats-2 nats-3 minio doc-parser upload-gateway
docker compose up -d akidb-server akidb-coordinator ingestion prometheus grafana
docker compose ps
```

Stop the stack:

```bash
docker compose down
docker compose down -v --remove-orphans
```

## Health Checks

```bash
curl http://localhost:8222/healthz
curl http://localhost:9000/minio/health/live
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:9090/-/healthy
docker compose logs --tail=100 akidb-server
docker compose logs --tail=100 akidb-coordinator
```

## Maintenance

Create a manual snapshot through the admin API when available, then verify the
snapshot in MinIO:

```bash
mc ls local/akidb-snapshots/
```

For local data recovery, stop traffic, restore the RocksDB/WAL/snapshot data,
restart services, and run a health check plus a known-query validation.

## Capacity And Performance

Track:

- Search P95/P99 latency.
- Vectors per shard.
- Tombstone ratio.
- WAL and snapshot disk growth.
- Ingestion queue depth and backpressure.

If latency rises, reduce ingestion concurrency, compact tombstones, or split hot
collections across the planned four-Mac cell topology.
