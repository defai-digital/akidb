# Immutable Generation Serving Preview

Status: Phase 2 single-node preview. This is not an HA, replication, Linux, or
cluster support claim.

AkiDB can run an opt-in immutable data plane for AX Fabric publication. MinIO
remains the canonical bundle store; AkiDB materializes disposable local
RocksDB, HNSW, BM25, payload, and bounded-graph projections. Enabling this mode
replaces the legacy mutable gRPC data path.

## Safety model

- an active generation continues serving while another generation is fetched
  and built;
- the manifest and bundle have independent exact SHA-256 and byte-count checks;
- S3 access uses a configured endpoint and credentials, a bucket allowlist,
  streaming byte limits, and a private temporary directory;
- an object URI must carry `versionId` or use a key containing its SHA-256;
- activation and rollback require an explicit expected-active
  compare-and-swap condition;
- data-plane insert, update, delete, batch-insert, collection-create, and
  collection-drop RPCs are rejected;
- vector, BM25, hybrid, and bounded graph reads use one immutable runtime and
  return its generation, manifest digest, and applied checkpoint;
- workspace identity comes from the authenticated request, not the manifest
  alone.

The existing read/plan-only `ManagementService` remains non-mutating.
Publication uses the separate privileged `GenerationManagement` service.

## Configuration

The complete disabled-by-default example is in
[`config/default.toml`](../../config/default.toml). At minimum:

```toml
[generation_serving]
enabled = true
replica_id = "akidb-replica-01"
generation_root = "/var/lib/akidb/generations"
control_rocksdb_path = "/var/lib/akidb/generation-control"
download_path = "/var/lib/akidb/generation-downloads"
default_collection = "knowledge"
allowed_buckets = ["ax-knowledge"]
require_version_or_digest_key = true
```

Build the cloud server with `cargo build --release -p akidb-server --features
generation-s3`. A default Mac server build does not include the S3 control
surface.

The three local paths must be distinct and non-overlapping. Configure
`storage.minio` for the fixed S3/MinIO endpoint and credentials. Use TLS for
MinIO. Because this binary does not yet terminate gRPC TLS, expose its gRPC
listener only through the existing private/WireGuard deployment boundary.

## Publication flow

1. AX Fabric writes a deterministic logical NDJSON bundle to immutable S3.
2. `StageGeneration` sends the exact manifest JSON bytes and their digest.
3. AkiDB fetches, verifies, builds, seals, and prewarms the shadow generation.
4. `GetGenerationStatus` reports local active/previous/staged state.
5. `ActivateGeneration` atomically swaps reads after an explicit CAS.
6. `RollbackGeneration` swaps back to the retained prior generation after an
   explicit CAS.

Phase 2 currently requires `target_sequence == base_sequence`; mutation-tail
replay belongs to the PostgreSQL-led replica phase.

## Current boundaries

- one node; no replication, quorum, failover, or sharding;
- synchronous stage/build RPC;
- one retained rollback generation;
- S3/MinIO object fetch only;
- gRPC only—MCP startup is refused in generation mode to prevent a mutable-path
  bypass;
- Mac ARM64 remains the established platform path; Ubuntu 24.04+ AMD64 remains
  a qualification target until its release gates pass;
- NVIDIA Thor, CUDA, and Linux ARM64 are not supported by this preview.

## Local verification

```bash
cargo test -p akidb-grpc --lib
cargo test -p akidb-server --lib
cargo clippy -p akidb-grpc -p akidb-server --all-targets -- -D warnings
cargo check --workspace
```

An external MinIO test and kill/restart publication test are required before
the single-node preview can be promoted.
