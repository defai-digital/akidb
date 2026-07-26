# AkiDB Operations Runbook

## Supported Environment

Best-fit targets are a single **Mac Studio** or **AMD64 PC**, and enterprise
**Mac Studio** or **AMD64 cloud** clusters. Mac Mini / MacBook standalone are
also supported secondary form factors. Use the CPU-portable path for all
builds and tests. Linux ARM64, NVIDIA Thor, CUDA/GPU-accelerated index paths,
and Kubernetes production procedures are outside the active support scope.
See `docs/platform/SUPPORT.md` for packaging-specific limits.

## Runtime Profiles

| Profile | Write and recovery authority | Operational status |
| --- | --- | --- |
| Mutable standalone | Direct AkiDB writes; local RocksDB and snapshots | Primary supported profile |
| Immutable single node | MinIO bundle plus privileged local generation control | Opt-in atomic-publication preview |
| PostgreSQL-led full replicas | AX Fabric PostgreSQL control state plus immutable MinIO bundles | Supported Ubuntu AMD64 knowledge-serving profile |
| Multi-shard coordinator | Independent shard-local state | Qualification/capacity path, not replication |

Do not mix recovery procedures between these profiles. In particular, a
mutable snapshot is not a generation backup, and independent shards are not
interchangeable generation replicas. See the
[knowledge-serving architecture](../architecture/knowledge-serving.md).

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

## Immutable Generation Serving

Build and start the single-node publication preview with:

```bash
cargo build --release -p akidb-server --features generation-s3
./target/release/akidb-server --config config/default.toml
```

`generation_serving.enabled` must be true, the server must not use
`--standalone`, and the generation, control, and download paths must be
distinct. Use a read-only MinIO credential on the replica and keep the
generation-control bearer token separate from the read data-plane token.
This privileged gRPC control service is exposed only when
`generation_serving.replica_control.enabled` is false.

The PostgreSQL replica worker uses the `generation-postgres` feature:

```bash
cargo build --release -p akidb-server --features generation-postgres
```

Database credentials are resolved only from the environment variable named by
`generation_serving.replica_control.postgres_url_env`. Keep PostgreSQL TLS in
`require` mode outside loopback-only development. Each data volume is bound to
one stable `replica_id`; never point a second identity at an existing
generation root. In this profile `GenerationManagement` is not exposed;
PostgreSQL is the publication authority.

The worker rebuilds a complete local revision from the immutable base bundle
plus every ordered mutation through the required checkpoint. Upserts reference
bounded checksum-addressed payloads in MinIO; deletes have no payload. A
duplicate is idempotent, while a sequence gap, identity conflict, invalid
payload, or cross-replica digest/count mismatch blocks readiness and must not
be bypassed.

During a PostgreSQL or MinIO outage, do not delete or deactivate a known-good
local generation merely because publication cannot progress. Existing reads
are designed to remain local; new build, checkpoint, and activation work
pauses. A failed shadow build must not disturb the active pointer.

The full configuration, current limitations, and focused checks are in
[Immutable Generation Serving](../development/generation-serving-preview.md).

## Maintenance

For mutable standalone mode, create a manual snapshot through the admin API
when available, then verify the snapshot in MinIO:

```bash
mc ls local/akidb-snapshots/
```

For local data recovery, stop traffic, restore the RocksDB and snapshot data,
restart services, and run a health check plus a known-query validation.

For generation mode, do not restore or copy a live RocksDB/HNSW directory from
another replica. Isolate the failed volume, provision blank local generation
and control paths for the intended stable replica identity, and rebuild from
the authoritative manifest/bundle and checkpoint state. This replacement
workflow is automated by the knowledge-cell Ansible playbooks and documented
in [Knowledge Serving Operations](knowledge-serving.md).

## Capacity And Performance

Track:

- Search P95/P99 latency.
- Vectors per shard.
- Tombstone ratio.
- RocksDB and snapshot disk growth.
- Ingestion queue depth and backpressure.
- Active/staged generation identity and manifest digest when generation mode
  is enabled.
- Replica checkpoint, build state, heartbeat age, and ready-replica count for
  the PostgreSQL convergence profile.

If latency rises, reduce ingestion concurrency, compact tombstones, or split
hot collections across additional qualified shards.
