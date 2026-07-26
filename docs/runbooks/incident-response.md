# AkiDB Incident Response Runbook

## Scope

This runbook covers supported macOS 26 Apple Silicon and Ubuntu 24.04+ native
deployments (Mac Studio / Mini / MacBook and AMD64 PC or cloud), plus local
Compose stacks. Linux ARM64, NVIDIA Thor, CUDA/GPU index paths, and Kubernetes
production incidents are outside the active support scope.

The immutable single-node path is a preview. The PostgreSQL-led Ubuntu AMD64
cell provides generation-aware read failover; PostgreSQL and MinIO availability
remain external responsibilities. Keep the
[knowledge-serving architecture](../architecture/knowledge-serving.md) open
when responding to a generation-mode incident.

## Severity Levels

| Level | Description | Response Time |
| --- | --- | --- |
| SEV1 | Complete service outage or suspected data loss | 15 minutes |
| SEV2 | Major degradation, high error rate, or failed recovery | 30 minutes |
| SEV3 | Single component unhealthy or elevated latency | 2 hours |
| SEV4 | Non-critical warning or documentation issue | 24 hours |

## High Search Latency

Diagnosis:

```bash
docker compose logs --tail=100 akidb-server
docker compose logs --tail=100 akidb-coordinator
curl -s http://localhost:9090/metrics | grep akidb_search_latency
```

Resolution:

- Reduce ingestion rate or batch size.
- Check disk pressure and WAL growth.
- Trigger compaction if tombstone ratio is high.
- Restart a stuck local service only after logs are captured.

## Shard Or Coordinator Down

Diagnosis:

```bash
docker compose ps
docker compose logs --tail=200 akidb-server
docker compose logs --tail=200 akidb-coordinator
```

Resolution:

- Restart the unhealthy service: `docker compose restart akidb-server`.
- Verify data paths under `./data/rocksdb` and `./data/wal` (or the configured paths in `config/default.toml`).
- Restore from the latest valid snapshot if storage is corrupt.

## Ingestion Backpressure

Diagnosis:

```bash
docker compose logs --tail=200 ingestion
curl -s http://localhost:9090/metrics | grep backpressure
```

Resolution:

- Reduce `BATCHER_MAX_BATCH`.
- Pause document uploads until queues drain.
- Confirm parser and upload gateway health.

## Parser Failures

Diagnosis:

```bash
curl http://localhost:8080/health
docker compose logs --tail=200 doc-parser
```

Resolution:

- Restart `doc-parser`.
- Check PDF/DOCX dependencies and input file size.
- Move unrecoverable documents to the DLQ with a reason.

## Generation Build Or Activation Failure

Diagnosis:

```bash
docker compose logs --tail=200 akidb-server
```

Record the workspace, collection, generation ID, manifest SHA-256, bundle
SHA-256, local active/staged/previous pointers, applied checkpoint, and exact
build phase. Verify the immutable object version or checksum-addressed key and
available shadow-build disk headroom.

Resolution:

- Keep the last known-good active generation serving.
- Do not force activation, edit a generation in place, or weaken checksum,
  model, dimension, count, or compare-and-swap checks.
- Correct the publisher or source artifact and publish a new generation.
- Use rollback only to a retained, verified prior generation and only with the
  expected-active precondition.

## Replica Or Control-Plane Degradation

A PostgreSQL or MinIO outage should pause new convergence without invalidating
an already active local generation. Capture heartbeat age, replica identity,
failure domain, generation/digest/checkpoint, and build state before changing
anything.

- Do not treat process liveness as generation readiness.
- The gateway automatically excludes stale, wrong-generation, drained, or
  failed replicas; verify its eligibility and evidence-mismatch metrics.
- Never reuse an existing generation volume under a different `replica_id`.
- If local projection state is corrupt, isolate the volume and rebuild a blank
  replica from the canonical MinIO bundle and PostgreSQL control state; do not
  copy a live RocksDB or HNSW directory from a peer.

## Post-Incident

Create an incident ticket with timeline, customer impact, commands run, root
cause, and follow-up tasks. For generation incidents, include the manifest and
bundle digests, active/required checkpoint, affected replica IDs, and whether
any request could have reached a stale generation. Update this runbook when a
new recovery step proves useful.
