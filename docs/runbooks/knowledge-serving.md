# Knowledge-Serving Cell Runbook

This runbook operates the PostgreSQL-authoritative, full-replica AkiDB
knowledge cell. It does not apply to the legacy independent-shard coordinator
profile.

## Supported topology

- Ubuntu 24.04 or newer on AMD64 with systemd.
- Three independent full AkiDB replicas, each on its own local data volume.
- Two stateless AX knowledge gateways.
- Managed HA PostgreSQL and durable S3/MinIO in production.
- The checked-in dependency role is a single-host qualification dependency,
  not a production HA database or object-store topology.
- Native gRPC TLS, gateway HTTPS, PostgreSQL TLS, bearer authentication, and
  private-network binding are mandatory.

The canonical recovery set is PostgreSQL plus MinIO/OpenWiki. Local
RocksDB/HNSW/BM25/graph directories are disposable projections and must never
be copied from a live peer.

## Deployment

Build the two immutable AMD64 artifacts once, record their SHA-256 digests,
provide credentials through environment or Ansible Vault, then run from
`deploy/ansible`:

```bash
ansible-galaxy collection install -r requirements.yml
ansible-playbook playbooks/knowledge-preflight.yml
ansible-playbook playbooks/knowledge-site.yml
ansible-playbook playbooks/knowledge-verify.yml
```

`knowledge-site.yml` is safely rerunnable. It deploys dependencies only when
`akidb_knowledge_manage_lab_dependencies=true`; production inventory points to
existing managed PostgreSQL and S3 services.

## Generation publication and evidence

Publication is complete only when:

1. the immutable logical bundle exists in MinIO and its byte length and SHA-256
   match the manifest;
2. PostgreSQL records the staged generation and outbox event transactionally;
3. the activation policy observes at least two ready replicas in two failure
   domains;
4. the active pointer changes by compare-and-swap;
5. both gateways report the same active generation and at least two eligible
   replicas;
6. golden semantic, exact, relation, and multi-hop queries return resolvable
   canonical citations.

Use `knowledge-control status` and keep the JSON as release evidence. Never
force a failed, gapped, wrong-schema, wrong-model, or wrong-digest replica into
the route set.

## Replica quorum

Alert: `AkiDBKnowledgeReplicaQuorumLost`.

1. Check both gateway `/readyz` endpoints.
2. Inspect `akidb_replica_route_ready`, heartbeat age, generation ID, digest,
   vector/edge counts, applied sequence, and drain state.
3. If one replica is healthy, active reads may continue, but freeze
   publication and maintenance because there is no failure margin.
4. Drain the unhealthy replica before repair.
5. Rebuild it from canonical state; do not copy another replica's data.
6. Re-admit only after exact convergence and gateway evidence checks.

## Gateway failover

Alert: `AkiDBKnowledgeRouteFailureBudgetBurn`.

The gateway retries only read-only retrieval. A failed attempt is placed in a
cooldown, and the next eligible replica is selected by inflight work and
latency. Do not add retry behavior to mutation or publication endpoints.

During a drill, stop one selected replica and issue continuous authenticated
search requests through both gateways. Record p50/p95/p99 interruption and
require routing recovery within 30 seconds p95. Restart the replica and
require exact convergence before it receives traffic.

## Evidence mismatch

Alert: `AkiDBKnowledgeEvidenceMismatch`.

This is fail-closed and severity critical. Immediately drain the reported
replica. Compare:

- authoritative active generation and target sequence;
- replica served generation, manifest SHA-256, and sequence;
- materialization digest and vector/edge counts;
- workspace and collection;
- binary and index-format versions.

Rebuild the replica on a blank local volume. Do not waive the evidence barrier.

## Control-plane outage

Alert: `AkiDBKnowledgeControlCacheStale`.

Existing active reads continue from the last verified gateway route snapshot
and local AkiDB generations. New publication, activation, membership change,
checkpoint progress, drain, and rollback are frozen. MinIO and PostgreSQL
outages must not remove the last known-good local generation. Restore the
authority, verify checkpoints, then resume publication.

NATS is deliberately disabled. PostgreSQL polling remains the replay and
activation path. Adopt JetStream only after measured polling load or
notification latency violates the SLO; NATS must then remain optional,
at-least-once, payload-light, and backed by PostgreSQL replay.

## Replica lag

Alert: `AkiDBKnowledgeReplicaLag`.

Check PostgreSQL connectivity, ordered mutation rows, payload availability,
and `akidb_mutation_gap_total`. A sequence gap blocks only that replica. Never
skip the missing sequence. Repair canonical mutation state or rebuild a
self-contained generation, then verify the target checkpoint.

## Generation build failure

Alert: `AkiDBKnowledgeBuildFailures`.

Read the structured replica log and checkpoint `last_error`. Corrupt bytes,
wrong dimension/model/schema, count divergence, and disk admission remain
fail-closed. The current active generation continues serving. Abandon or
repair the staged generation; never modify an immutable bundle in place.

## Disk pressure

Alert: `AkiDBKnowledgeDiskAdmissionRejected`.

AkiDB estimates bundle, vector, graph, and build amplification before creating
the shadow generation and requires the configured post-build reserve. Run:

```bash
scripts/knowledge-capacity-plan.sh 1000000 768 f32 32 320 3
```

The calculator is conservative planning evidence, not a replacement for the
qualification report. Reduce corpus/dimensions, use a qualified precision,
increase RAM/disk, or publish after safe generation GC. Never lower the
reserve simply to force a build.

## Generation retention and GC

Each replica periodically scans its local generation scope under an
in-process transition lock. Active, previous, staged, and authoritative
publication generations are always retained. Unknown directory shapes fail
closed. The minimum-age barrier applies before deletion, and each run is
recorded in PostgreSQL audit plus Prometheus metrics.

Canonical object GC is dry-run first:

```bash
ax-knowledge-gateway knowledge-control gc \
  --workspace qualification \
  --collection knowledge \
  --bucket knowledge-generations \
  --prefix ax-fabric/knowledge-generations
```

Review candidates, then repeat with both `--apply` and
`--confirm-delete-orphans`. The cleaner only removes old objects unreferenced
by any non-abandoned generation or mutation and only below the supplied
bounded prefix.

## Drain, replacement, and blank rebuild

Normal replacement is:

```bash
export AKIDB_REBUILD_TARGET=akidb-amd64-3
export AKIDB_CONFIRM_BLANK_REBUILD=rebuild:akidb-amd64-3
ansible-playbook playbooks/knowledge-blank-rebuild.yml
```

The playbook drains one replica, stops it, moves local projection directories
to a recoverable quarantine, recreates blank paths, waits for exact active
generation readiness, and re-admits it. A failed target remains drained and
the quarantine remains intact.

## Rolling upgrade and rollback

Stage artifacts everywhere, then run:

```bash
ansible-playbook playbooks/knowledge-rolling-upgrade.yml
```

Gateways and replicas roll one at a time. A replica is drained before the
binary/config switch and re-admitted only after generation readiness. A
failure stops the rollout with the target drained.

Rollback uses only already installed immutable release IDs and their versioned
configurations:

```bash
export AKIDB_ROLLBACK_RELEASE_ID=<installed-release>
export AX_GATEWAY_ROLLBACK_RELEASE_ID=<installed-release>
ansible-playbook playbooks/knowledge-rollback.yml
```

Index-format changes are rebuild boundaries. A new binary must continue
reporting supported knowledge/graph schemas and its exact index-format
version. Mixed-version routing is allowed only when all replicas return the
same generation digest/counts/evidence. An incompatible format is rolled out
by blank rebuild, never by opening and mutating an unknown on-disk index.

## Backup and disaster recovery

Back up canonical systems, not disposable AkiDB directories:

```bash
export AKIDB_KNOWLEDGE_BACKUP_ID=backup-YYYYMMDD-HHMMSS
export AKIDB_KNOWLEDGE_BACKUP_DIR=/absolute/protected/controller/path
ansible-playbook playbooks/knowledge-backup.yml
```

Production uses managed PostgreSQL point-in-time recovery and versioned,
replicated/object-locked S3. The lab playbook creates a checksum-evidenced
`pg_dump` plus MinIO object archive and records it in authority audit.

Verify without touching production state:

```bash
export AKIDB_KNOWLEDGE_BACKUP_SHA256=<recorded-sha256>
ansible-playbook playbooks/knowledge-restore-verify.yml
```

The drill restores PostgreSQL into a disposable database, checks control
tables and canonical objects, removes the disposable database, and records
audit evidence. Complete DR additionally provisions a fresh cell, restores
PostgreSQL/S3, performs a blank three-replica rebuild, and reruns golden
queries. Target RPO is the managed PostgreSQL/S3 policy; target routing RTO for
one replica is 30 seconds p95. Full-cell recovery RTO must be measured per
qualified corpus tier.

## Latency

Alert: `AkiDBKnowledgeRouteLatency`.

Separate gateway routing, replica search, graph expansion, rerank, and context
packing. Inspect p50/p95/p99, query class, graph hop/node/token budgets, cache
state, and replica EWMA. Do not hide latency by weakening generation or
citation barriers.
