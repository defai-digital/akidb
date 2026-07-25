# AkiDB Incident Response Runbook

## Scope

This runbook covers supported macOS 26 Apple Silicon and Ubuntu 24.04+ native
deployments, plus local Compose stacks. GPU, CUDA, Thor, and Kubernetes
incidents are outside the active support scope.

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

## Post-Incident

Create an incident ticket with timeline, customer impact, commands run, root
cause, and follow-up tasks. Update this runbook when a new recovery step proves
useful.
