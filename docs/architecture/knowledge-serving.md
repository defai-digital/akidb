# Agentic Knowledge-Serving Architecture

Status: implemented. Immutable single-node publication remains an opt-in
preview; PostgreSQL-led full-replica convergence, quorum activation, and the
generation-aware read gateway form the Ubuntu AMD64 knowledge-serving profile.

## Product boundary

AkiDB is the low-latency retrieval-serving engine inside AX Fabric's agentic
knowledge layer. It materializes vector, BM25, payload, and bounded-graph
indexes from versioned knowledge and returns source-grounded retrieval
evidence to agents and GenAI services.

This profile does not make AkiDB the canonical document store, a general graph
database, or a cloud-scale distributed vector database. The mutable standalone
profile remains available for local applications that write directly to one
AkiDB process.

## Ownership and authority

| Plane | Owner and technology | Authority |
| --- | --- | --- |
| Source and canonical artifacts | AX Fabric, OpenWiki, and MinIO | Documents, relationships, source versions, and immutable logical bundles |
| Publication control | AX Fabric on HA PostgreSQL | Generation lifecycle, active pointer, ordered sequence, replica checkpoints, and audit |
| Retrieval serving | Independent AkiDB replicas on local storage | Rebuildable RocksDB, HNSW, BM25, payload, and bounded-graph projections |
| Request routing | Stateless AX retrieval gateway | Generation/checkpoint barriers and selection among eligible replicas |
| Optional notification | NATS JetStream | Wake-up/acceleration only; never replay or activation authority |

Serving an already active local generation must not synchronously depend on
PostgreSQL, MinIO, OpenWiki, or NATS.

## Logical architecture

```text
 OpenWiki ─────┐
               ├─► AX Fabric ingestion and distillation
 MinIO sources ┘                 │
                                 │ immutable logical bundle + checksum
                                 ▼
                            MinIO artifacts
                                 │
                                 │ generation/outbox transaction
                                 ▼
                       HA PostgreSQL control plane
                       active pointer + checkpoints
                                 │
                      optional NATS notification
                        ┌────────┴────────┐
                        ▼                 ▼
                 AkiDB replica 1   AkiDB replica 2   [replica 3 recommended]
                 local RocksDB     local RocksDB
                 HNSW/BM25/graph   HNSW/BM25/graph
                        └────────┬────────┘
                                 ▼
                     AX retrieval gateway
                                 │
                                 ▼
                          Agents / GenAI
```

Every AkiDB replica owns a full logical copy for the configured
`(workspace_id, collection)` scope. Each copy is built independently from the
same logical contract. Live RocksDB, HNSW, or graph directories are never
copied from a running peer, shared through NFS, or mounted from MinIO.

## Why full replicas come before sharding

The first availability design separates replication from capacity scaling:

- two full replicas are the minimum engineering topology for one-node read
  availability;
- three are recommended when maintenance must not consume the only failure
  margin;
- each replica has its own stable identity, data volume, checkpoint, and
  failure domain;
- sharding is deferred until measured corpus, RAM, disk, build-time, or QPS
  limits require it.

The existing `akidb-coordinator` fans queries out to independent shards. It
does not turn those shards into replicas, enforce generation barriers, or
provide the AX agent-facing failover contract.

## Generation contract and lifecycle

A knowledge generation is immutable and scoped by workspace and collection.
Its manifest binds:

- generation and parent identities;
- embedding model and dimensions;
- graph schema version;
- deterministic NDJSON bundle format and compression;
- immutable S3/MinIO object URI, byte length, and SHA-256;
- base and immutable-bundle target sequence;
- expected vector and edge counts.

The bundle contains logical records, graph nodes, and graph edges in stable
order. It never contains engine-specific RocksDB or HNSW files, which keeps a
blank-node rebuild independent of host architecture and internal index layout.
PostgreSQL may later advance the generation's required sequence beyond the
bundle target. Each replica then rebuilds a complete immutable local revision
from the base bundle plus every ordered mutation through that checkpoint.

The target publication flow is:

1. AX Fabric writes and verifies an immutable bundle in MinIO.
2. AX Fabric records the generation and ordered publication event in one
   PostgreSQL transaction.
3. Each replica validates the manifest, downloads the bounded bundle, verifies
   its digest and size, and builds a shadow local generation.
4. Each replica verifies schema/model compatibility, counts, index readiness,
   and its required checkpoint before reporting ready.
5. The control plane changes the active pointer with compare-and-swap only
   after the configured ready-replica and failure-domain policy passes.
6. A replica switches its local active pointer only after observing that
   authoritative activation.
7. The gateway routes reads only to replicas serving the active generation at
   or above the requested checkpoint.

The prior verified generation is retained for rollback. Rollback changes a
pointer to that immutable generation; it does not edit the failed generation
in place.

## Consistency classes

| Data | Contract |
| --- | --- |
| Published documents, OpenWiki content, and distilled knowledge | Atomic generation publication; stable after activation |
| Curated updates between generations | Ordered, idempotent mutation sequence with an explicit checkpoint barrier |
| Agent session state and immediate working memory | Strongly consistent PostgreSQL path first, or an explicit wait/fallback barrier before querying AkiDB |

Generation publication is eventually materialized but atomically visible.
It must never expose a mixture of vector, lexical, payload, and graph data from
two generations.

## Current implementation boundary

| Capability | Current status |
| --- | --- |
| Versioned Rust knowledge contracts and JSON fixtures | Implemented |
| Crash-persistent local staged/active/previous state | Implemented |
| Deterministic logical bundle materialization | Implemented |
| Single-node S3/MinIO stage, verify, activate, and rollback | Implemented preview |
| Generation/checkpoint evidence on retrieval responses | Implemented in generation mode |
| Ordered mutation payload validation and deterministic post-bundle revisions | Implemented |
| PostgreSQL-led independent replica convergence | Implemented with exact checkpoint/digest/count gates |
| Generation-aware AX routing and read-only automatic failover | Implemented |
| Drain, replacement, rolling upgrade/rollback, GC, backup/restore evidence, metrics, and alerts | Implemented |
| Stateful Kubernetes profile or automatic sharding | Deferred until measured need |

The single-node preview uses the privileged `GenerationManagement` gRPC
service. The replica design instead treats PostgreSQL as authoritative and
keeps AkiDB's local publication control subordinate to the observed global
pointer. These are two stages of the same lifecycle, not two competing
authorities.

## Retrieval graph boundary

The graph is a generation-scoped retrieval projection:

- typed document, section, chunk, file, symbol, entity, person, and commit
  nodes;
- typed directed edges with source version, evidence chunk IDs, confidence,
  and extractor identity;
- default one-hop traversal, a hard maximum of three, and strict fan-out,
  result, token, workspace, and generation limits.

OpenWiki and AX Fabric remain authoritative for semantic relationships. AkiDB
does not add Cypher, arbitrary graph transactions, unbounded traversal, or an
independent canonical graph write API.

## Failure behavior

The design deliberately separates serving continuity from publication
progress:

- a failed shadow build leaves the active generation serving;
- a corrupt or wrong-model bundle never becomes ready;
- a sequence gap or digest/count divergence blocks only the affected replica;
- PostgreSQL or MinIO outage pauses new convergence/activation but must not
  tear down the last known-good local generation;
- deleting a replica volume requires a blank rebuild from canonical artifacts
  and control state;
- a data volume cannot be silently adopted by a different replica identity;
- the gateway excludes failed, stale, drained, or wrong-generation replicas
  and retries only read-only retrieval within the request deadline.

## Security invariants

- Single-node data-plane and `GenerationManagement` credentials are separate;
  the replica profile replaces that local control API with a distinct
  PostgreSQL service credential.
- PostgreSQL credentials come from the configured environment-variable name,
  not the checked-in TOML file.
- PostgreSQL TLS verification is the default; plaintext is limited to
  loopback-only development and tests.
- Replica MinIO credentials are read-only and bundle keys are immutable or
  checksum-addressed.
- Workspace and collection scope is validated at every contract and serving
  boundary.
- AkiDB data/control ports stay on a private network and use built-in gRPC TLS.
  The qualified VM profile additionally uses a WireGuard overlay.
- Activation, rollback, replica admission/drain, and destructive generation
  cleanup require audit evidence.

## Scale-evolution decision

Phase 7 remains deliberately deferred. The supported first cell is one logical
scope copied in full to three replicas. Sharding, a stateful Kubernetes
operator, and NATS/JetStream are not enabled by default and are not required
for correctness.

Reopen the architecture only when checked-in measurements show that a
supported corpus cannot fit the admitted RAM/disk envelope, rebuild time or
QPS violates its SLO, PostgreSQL polling is a bottleneck, or a stateful
Kubernetes deployment is a business requirement. A compile-time possibility
or an unmeasured preference is not a trigger.

## Platform and release claims

Native runtime support and knowledge-cell qualification are different claims.
AkiDB's portable standalone runtime supports the platforms listed in
[Platform Support](../platform/SUPPORT.md). The full PostgreSQL-led replica
cell, gateway failover, and production capacity envelope require their own
multi-replica correctness, failure, security, and performance evidence.

See [Immutable Generation Serving](../development/generation-serving-preview.md)
for the implemented preview and configuration surface, and the
[Operations Runbook](../runbooks/operations.md) for operator boundaries.
