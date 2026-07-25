# AkiDB Operations Runbook

## Supported Environment

AkiDB is supported on macOS 26 Apple Silicon and Ubuntu 24.04+ on AMD64 and
ARM64. Use the CPU-portable path for all builds and tests. Thor, CUDA, NVIDIA
GPU, and Kubernetes production procedures are outside the active support
scope. The checked-in Ansible cluster profile is qualified on Ubuntu AMD64;
see `docs/platform/SUPPORT.md` for packaging-specific limits.

## Native Build And Validation

```bash
cargo check --workspace
cargo test --workspace
cargo build --release -p akidb-cli
akidb server --standalone --config config/default.toml
akidb coordinator --shards 127.0.0.1:50051
akidb tui \
  --coordinator 127.0.0.1:50050 \
  --management 127.0.0.1:50051
```

On macOS 26 Apple Silicon, `./scripts/build-on-mac-arm64.sh` runs the complete
native validation path.

The Operations Console is read/plan-only. The same server state is available as
JSON for automation:

```bash
akidb ops --management 127.0.0.1:50051 capabilities
akidb ops --management 127.0.0.1:50051 collections
akidb ops --management 127.0.0.1:50051 operations
akidb ops --management 127.0.0.1:50051 snapshots
akidb ops --management 127.0.0.1:50051 audit
```

Set `AKIDB_AUTH_TOKEN` or point `AKIDB_AUTH_TOKEN_FILE` at a regular mode-0600
file. The TUI and CLI report only the credential source category, never its
value or path. Import planning accepts only a server-issued immutable staging
reference and remains unavailable until trusted upload staging is connected.

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
curl http://localhost:8000/health
curl http://localhost:9090/-/healthy
docker compose logs --tail=100 akidb-server
docker compose logs --tail=100 akidb-coordinator
```

## Text Embeddings

`TextSearch` expects an OpenAI-compatible local embedding endpoint. With current
`ax-engine`, start AkiDB's sidecar with local Qwen embedding native artifacts
containing `model-manifest.json`:

```bash
python3 scripts/ax_engine_embedding_server.py \
  --model-dir /path/to/Qwen3-Embedding-4B \
  --model-id Qwen/Qwen3-Embedding-4B \
  --port 8000
```

`ax-engine serve <embedding-alias>` is not the supported embedding path. The
validator uses `AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-4B` to start the
sidecar on port 8000, and skips `TextSearch` when that variable is absent.
For `Qwen3-Embedding-0.6B`, also set `AX_ENGINE_MODEL=Qwen/Qwen3-Embedding-0.6B`
and `EMBEDDING_DIMENSIONS=1024`.

## Maintenance

Create a manual snapshot through the admin API when available, then verify the
snapshot in MinIO:

```bash
mc ls local/akidb-snapshots/
```

For local data recovery, stop traffic, restore the RocksDB and snapshot data,
restart services, and run a health check plus a known-query validation.

## Capacity And Performance

Track:

- Search P95/P99 latency.
- Vectors per shard.
- Tombstone ratio.
- RocksDB and snapshot disk growth.
- Ingestion queue depth and backpressure.

If latency rises, reduce ingestion concurrency, compact tombstones, or split
hot collections across additional qualified shards.
