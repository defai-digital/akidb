# AkiDB Technical Specification

**Version:** 2.0
**Date:** 2026-06-26
**Status:** Draft
**License:** Apache-2.0
**Related:** [AkiDB PRD](../product/PRD.md), [ADR-0001: Mac-First Cell Architecture](../adr/ADR-0001-mac-first-cell-architecture.md)

---

## 1. Scope

This document specifies the technical architecture for AkiDB v2.0:

- One Apple Silicon Mac as the baseline deployment.
- Four Apple Silicon Macs as a Thunderbolt-connected cell.
- Multiple cells as the horizontal scaling unit.

This is a technical specification, not a sprint plan. It defines component boundaries, data contracts, storage layout, cluster behavior, and verification requirements.

---

## 2. Design Principles

1. One Mac must be useful before distributed mode exists.
2. Distributed mode must prove value over one Mac.
3. Four Macs are one cell, not an unbounded mesh.
4. Hot cells are homogeneous by default.
5. Heterogeneous hardware is explicit and weighted.
6. Thunderbolt is a fast network transport, not shared memory.
7. Search APIs are topology-neutral.
8. Admin APIs expose topology, placement, and failure state.

---

## 3. System Topology

### 3.1 One-Mac Deployment

```text
Client
  |
  v
AkiDB Server
  |
  +-- Coordinator facade
  +-- Storage engine
  +-- Vector backend
  +-- WAL
  +-- Snapshot manager
```

The one-Mac deployment uses the same internal abstractions as a cell, but all shard placement resolves locally.

### 3.2 Four-Mac Cell

```text
                 Client
                   |
                   v
            Logical Cell Endpoint
                   |
                   v
             Coordinator API
                   |
      +------------+------------+
      |            |            |
      v            v            v
   Mac A <----> Mac B <----> Mac C
      ^            ^            ^
      |            |            |
      +---------- Mac D --------+

Data plane: shard search, replica reads, vector transfer.
Control plane: membership, placement, publication epochs, recovery state.
```

The exact Thunderbolt topology must be validated by benchmark. The software must not assume uniform links.

### 3.3 Multi-Cell Deployment

```text
Client
  |
  v
Global Router / Coordinator
  |
  +-- Cell A: 4 Macs
  +-- Cell B: 4 Macs
  +-- Cell C: 4 Macs
```

Multi-cell deployments route before fan-out. The default query path must avoid broadcasting every query to every cell.

---

## 4. Components

### 4.1 `akidb-server`

The server owns the public API, local runtime, and process lifecycle.

Responsibilities:

- Start and validate local node configuration.
- Expose search, ingestion, admin, and health APIs.
- Host the local shard worker.
- Coordinate clean shutdown and restart recovery.

### 4.2 Coordinator

The coordinator owns request planning.

Responsibilities:

- Resolve collection to cell and shard placement.
- Split search requests across shards.
- Merge shard-local topK results.
- Apply degraded-result rules.
- Surface topology and health through admin APIs.

The coordinator must not own durable vector data.

### 4.3 Storage Engine

The storage engine owns durable metadata and payload references.

Responsibilities:

- WAL.
- Document metadata.
- Vector metadata.
- Shard manifests.
- Tombstone state.
- Snapshot metadata.
- Publication epochs.

### 4.4 Vector Backend

The vector backend owns index-specific operations.

Required trait:

```rust
pub trait VectorBackend: Send + Sync {
    fn backend_id(&self) -> BackendId;
    fn create_index(&self, spec: IndexSpec) -> Result<IndexHandle>;
    fn add_batch(&self, index: &IndexHandle, batch: VectorBatch) -> Result<AddResult>;
    fn search(&self, index: &IndexHandle, query: SearchQuery) -> Result<SearchResultSet>;
    fn mark_deleted(&self, index: &IndexHandle, ids: &[InternalId]) -> Result<()>;
    fn snapshot(&self, index: &IndexHandle, target: &SnapshotPath) -> Result<SnapshotManifest>;
    fn load_snapshot(&self, manifest: &SnapshotManifest) -> Result<IndexHandle>;
    fn stats(&self, index: &IndexHandle) -> Result<BackendStats>;
}
```

Initial backend requirements:

- Portable on Apple Silicon macOS.
- Does not require CUDA.
- Supports tombstone exclusion.
- Supports deterministic snapshot and restore.

### 4.5 Cell Agent

Each Mac in a cell runs a cell agent inside `akidb-server`.

Responsibilities:

- Node capability detection.
- Heartbeats.
- Link metrics.
- Shard worker registration.
- Replica health reporting.
- Snapshot participation.

---

## 5. Control Plane

### 5.1 Membership

Node identity:

```text
node_id = stable UUIDv7 generated at first boot
machine_id = hash(hardware serial or operator-provided stable ID)
cell_id = operator-provided or generated at cell bootstrap
```

Node join flow:

1. Node starts with local config.
2. Node reports capability: CPU, memory, disk, Thunderbolt generation, macOS version.
3. Existing voters validate compatibility.
4. Placement engine decides whether the node can accept data.
5. Node is admitted as voter, learner, or data-only member.

### 5.2 Consensus

For a four-Mac cell, control-plane consensus uses three voters and one learner/data-only node.

Consensus state includes:

- Cell membership.
- Collection definitions.
- Shard placement.
- Replica placement.
- Active publication epoch.
- Recovery locks.

Consensus state does not include raw vector payloads.

### 5.3 Publication Epochs

Every searchable index version is published under an epoch:

```text
PublicationEpoch {
  epoch_id: u64,
  collection_id: CollectionId,
  shard_id: ShardId,
  primary_node: NodeId,
  replica_nodes: Vec<NodeId>,
  index_manifest_hash: Hash,
  published_at: Timestamp,
}
```

Search requests use the latest committed epoch unless a debug or recovery request pins an epoch.

---

## 6. Data Plane

### 6.1 Search Flow

```text
Client request
  -> coordinator validates collection
  -> coordinator resolves shard map
  -> coordinator sends shard-local search requests
  -> shard workers search local backend
  -> coordinator merges results
  -> coordinator returns result set and optional diagnostics
```

Result merge requirements:

- Stable ordering by score, then internal ID.
- Explicit degraded flag if required shards are missing.
- Per-shard timing when debug diagnostics are requested.

### 6.2 Ingestion Flow

```text
Ingest request
  -> validate collection and schema
  -> compute document identity
  -> append WAL
  -> assign shard
  -> send to primary and replicas
  -> add to vector backend
  -> publish visibility
  -> acknowledge based on durability policy
```

Durability policies:

| Policy | Behavior | Use |
| --- | --- | --- |
| `local_durable` | WAL durable on primary before ack | One-Mac default |
| `replica_durable` | WAL durable on primary and at least one replica before ack | Cell default |
| `published` | Ack only after vectors are visible in the active epoch | Stronger client workflow |

### 6.3 Delete Flow

Deletes are tombstone-first.

Steps:

1. Append delete intent to WAL.
2. Mark vector IDs tombstoned in metadata.
3. Notify backend to exclude IDs.
4. Publish delete visibility.
5. Remove payload during compaction.

Hard delete without tombstone is not allowed in the normal API.

### 6.4 Reindex Flow

Reindex uses shadow publish:

1. Build new index version in the background.
2. Keep old version serving queries.
3. Validate new index.
4. Commit publication epoch.
5. Tombstone old version after successful publish.
6. Compact old version later.

---

## 7. Data Model

### 7.1 Collection

```text
Collection {
  collection_id: UUID,
  name: String,
  vector_dimension: u32,
  distance: DistanceMetric,
  backend: BackendId,
  shard_count: u32,
  replication_factor: u8,
  created_at: Timestamp,
  updated_at: Timestamp,
}
```

### 7.2 Shard

```text
Shard {
  shard_id: UUID,
  collection_id: UUID,
  ordinal: u32,
  placement_policy: PlacementPolicy,
  primary_node: NodeId,
  replica_nodes: Vec<NodeId>,
}
```

### 7.3 Document Identity

```text
DocumentIdentifier {
  content_hash: [u8; 32],
  source_uri: Option<String>,
  category_uid: Option<String>,
  instance_id: UUIDv7,
  tags: Map<String, TagValue>,
}
```

### 7.4 Vector Metadata

```text
VectorMetadata {
  internal_id: InternalId,
  external_id: Option<String>,
  document_id: DocumentIdentifier,
  collection_id: UUID,
  shard_id: UUID,
  version: u64,
  tombstone: bool,
  inserted_at: Timestamp,
  visible_epoch: Option<u64>,
}
```

---

## 8. Storage Layout

Each node stores data under one configured root:

```text
akidb-data/
  node.toml
  wal/
    collection-{id}/
  metadata/
    rocksdb/
  indexes/
    collection-{id}/
      shard-{id}/
        epoch-{n}/
  snapshots/
    snapshot-{timestamp}/
  tmp/
```

Storage rules:

- WAL is append-only until checkpointed.
- Index files are immutable once published.
- Temporary build directories must be atomically renamed into place.
- Snapshot manifests must contain content hashes for all files.

---

## 9. Placement

### 9.1 Homogeneous Cell Policy

Default placement for a four-Mac homogeneous cell:

- `shard_count = 4` or multiple of 4.
- `replication_factor = 2` for production collections.
- Primary shards distributed evenly.
- Replicas placed on different nodes.

Example with 4 shards:

| Shard | Primary | Replica |
| --- | --- | --- |
| S0 | Mac A | Mac C |
| S1 | Mac B | Mac D |
| S2 | Mac C | Mac A |
| S3 | Mac D | Mac B |

### 9.2 Weighted Heterogeneous Policy

Node weight is derived from:

- Available RAM.
- Sustained disk throughput.
- CPU class.
- Link health.
- Operator override.

Placement must never assign a shard if projected memory usage exceeds policy headroom.

Default headroom:

- RAM: keep 20 percent free.
- Disk: keep 15 percent free.

---

## 10. Failure Handling

### 10.1 One-Mac Failure

If the only Mac fails:

- Service is unavailable.
- Restore from local or external snapshot.
- WAL replay restores acknowledged writes since last snapshot.

### 10.2 Cell Node Failure

If one Mac fails in an RF=2 cell:

- Coordinator routes reads to replicas.
- Writes to affected shards follow durability policy.
- Admin API reports degraded placement.
- Re-replication is recommended before another failure.

### 10.3 Link Failure

If one Thunderbolt link fails:

- Cell agent marks link degraded.
- Coordinator avoids the link where topology allows.
- If required shard traffic cannot route, affected shard is degraded.

### 10.4 Split Brain

Only the control-plane majority may publish new epochs. Data-plane workers without a valid epoch lease must not serve as authoritative primaries.

---

## 11. APIs

### 11.1 Public Data APIs

Required operations:

- `CreateCollection`
- `DescribeCollection`
- `UpsertVectors`
- `Search`
- `DeleteVectors`
- `Reindex`
- `CreateSnapshot`
- `RestoreSnapshot`

### 11.2 Admin APIs

Required operations:

- `GetNodeStatus`
- `GetCellStatus`
- `ListShards`
- `ListPlacements`
- `MoveShard`
- `DrainNode`
- `JoinCell`
- `LeaveCell`
- `GetBenchInfo`

### 11.3 Diagnostics

Search diagnostics may include:

```json
{
  "cell_id": "cell-a",
  "epoch": 42,
  "degraded": false,
  "shards": [
    {
      "shard_id": "s0",
      "node_id": "mac-a",
      "latency_ms": 3.4,
      "candidate_count": 10
    }
  ]
}
```

Diagnostics are disabled by default for normal client requests.

---

## 12. Configuration

Example one-Mac config:

```toml
[node]
mode = "single"
data_dir = "/var/lib/akidb"

[server]
bind = "127.0.0.1:8080"

[storage]
wal_sync = true
snapshot_dir = "/var/lib/akidb/snapshots"

[backend]
type = "portable"
```

Example cell node config:

```toml
[node]
mode = "cell"
cell_id = "cell-a"
node_role = "auto"
data_dir = "/var/lib/akidb"

[server]
bind = "0.0.0.0:8080"

[cell]
expected_data_nodes = 4
voters = 3
replication_factor_default = 2
require_homogeneous_hot_cell = true

[network]
preferred_transport = "thunderbolt"
fallback_transport = "ethernet"
```

---

## 13. Observability

Metrics:

- `akidb_search_latency_ms`
- `akidb_search_shard_latency_ms`
- `akidb_ingest_latency_ms`
- `akidb_wal_fsync_latency_ms`
- `akidb_cell_node_up`
- `akidb_cell_link_latency_ms`
- `akidb_cell_link_throughput_bytes`
- `akidb_shard_replica_lag`
- `akidb_snapshot_duration_seconds`

Logs:

- JSON structured by default.
- Include `node_id`, `cell_id`, `collection_id`, `shard_id`, and `epoch` where applicable.

Tracing:

- One trace per client request.
- Search spans per shard.
- Ingestion spans per WAL append and backend add.

---

## 14. Security

Defaults:

- Single node binds to localhost.
- Cell mode requires explicit network bind.
- Admin APIs require authentication outside localhost.
- Internode requests include node identity.

Required checks:

- Reject unknown nodes by default.
- Verify snapshot hashes before restore.
- Refuse to serve from uncommitted epochs.
- Do not log vector payloads by default.

---

## 15. Benchmark Specification

Benchmarks must publish:

- Hardware SKU.
- macOS version.
- AkiDB commit SHA.
- Backend type.
- Dataset size and dimension.
- topK.
- Filter selectivity.
- Query concurrency.
- P50, P95, P99 latency.
- QPS.
- Recall, where approximate search is used.
- Failure-mode results for cell benchmarks.

Minimum benchmark commands:

```text
akidb-bench single --dataset sift1m --dimension 128 --topk 10
akidb-bench single --dataset synthetic --vectors 1000000 --dimension 768 --topk 10
akidb-bench cell --cell cell-a --vectors 1000000 --dimension 768 --topk 10
akidb-bench cell-failover --cell cell-a --kill-node mac-c --topk 10
```

---

## 16. Compatibility and Migration

### 16.1 From Prior Hardware-Specific Docs

The previous hardware-specific docs are superseded. Concepts that may remain useful:

- Tombstones.
- Versioned reindexing.
- Snapshot manifests.
- Backpressure.
- Coordinator fan-out.

Concepts that are no longer primary:

- Hardware-specific accelerator backends.
- Non-macOS production defaults.
- No-replication shard design.
- Kubernetes or container-first operations.

### 16.2 On-Disk Format Versioning

Every persisted directory must include:

```text
format_version
akidb_version
backend_id
created_at
```

Unknown major format versions must fail closed.

---

## 17. Implementation Phases

### Phase A: One-Mac Core

- Portable backend trait.
- Local storage layout.
- WAL.
- Collection and vector APIs.
- Snapshot and restore.
- Single-node benchmark.

### Phase B: Cell Control Plane

- Node identity.
- Cell bootstrap.
- Three-voter metadata consensus.
- Capability detection.
- Placement metadata.

### Phase C: Cell Data Plane

- Shard fan-out.
- Replica placement.
- Replica reads.
- Degraded responses.
- Cell benchmark.

### Phase D: Multi-Cell Routing

- Global router.
- Collection-to-cell placement.
- Explicit cross-cell query.
- Cell addition workflow.

---

## 18. Open Technical Questions

| Question | Notes |
| --- | --- |
| Initial backend choice | Exact CPU is simplest; HNSW or FAISS CPU may be needed for target latency. |
| Thunderbolt topology | Full mesh is attractive but must be validated on reference hardware. |
| Consensus library | Evaluate existing Rust Raft implementations before building custom. |
| macOS service manager | Decide launchd plist generation vs manual process manager. |
| Snapshot destination | Local first; external object storage is useful but not a v2.0 one-Mac blocker. |
