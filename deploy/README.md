# Deployment Assets

This directory contains deployment manifests, templates, and scripts.

- `compose/` - Docker Compose stacks and monitoring dashboards.
- `docker/` - Dockerfiles for AkiDB services.
- `grafana/` - standalone monitoring dashboards.
- `ansible/` - checksum-pinned Linux AMD64 cluster qualification, WireGuard
  overlay, rolling deployment, verification, and rollback for the independent
  multi-shard lab. It is not the PostgreSQL-led full-replica HA design.

Compiled binaries are build artifacts and are not tracked in git. Build them
under `target/` for development. Cluster deployments consume one immutable CI
archive; see [`ansible/README.md`](ansible/README.md).

The accepted knowledge-serving topology and the boundary between full replicas
and shards are documented in
[`docs/architecture/knowledge-serving.md`](../docs/architecture/knowledge-serving.md).
