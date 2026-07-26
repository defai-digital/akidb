# Immutable Generation Serving

Status: opt-in single-node publication preview plus a qualified Ubuntu AMD64
full-replica profile. The latter adds PostgreSQL authority and automatic
read-only failover through the AX gateway.

For the ownership and target-cell design, read
[Agentic Knowledge-Serving Architecture](../architecture/knowledge-serving.md)
first.

## What this mode changes

Generation serving replaces the mutable gRPC write path for the configured
server. AX Fabric publishes a deterministic, immutable knowledge bundle;
AkiDB materializes disposable local RocksDB, HNSW, BM25, payload, and
bounded-graph projections from that bundle.

The mode has two delivery stages:

| Stage | Authority | Current status |
| --- | --- | --- |
| Privileged single-node publication | `GenerationManagement` gRPC calls to one AkiDB node | Implemented preview |
| Full-replica convergence | AX Fabric's PostgreSQL generation/outbox state; each AkiDB worker independently converges | Implemented and qualified on the documented Ubuntu AMD64 cell |

The second stage does not supersede the first stage's materializer. It changes
who decides which generation should be built and activated.

## Safety model

- An active generation continues serving while another generation is fetched
  and built.
- The manifest and bundle have independent exact SHA-256 and byte-count
  checks.
- S3 access uses a configured endpoint and credentials, a bucket allowlist,
  streaming byte limits, and a private temporary directory.
- An object URI must carry `versionId` or use a key containing its SHA-256.
- Activation and rollback require an explicit expected-active
  compare-and-swap condition.
- Data-plane insert, update, delete, batch-insert, collection-create, and
  collection-drop RPCs are rejected.
- Vector, BM25, hybrid, and bounded-graph reads use one immutable runtime and
  return its generation, manifest digest, and applied checkpoint.
- Workspace identity comes from the authenticated request, not the manifest
  alone.
- In replica-control mode, a generation root is permanently claimed by one
  stable `replica_id`; a replacement identity must start with a blank volume
  and rebuild.

The existing read/plan-only `ManagementService` remains non-mutating.
Single-node publication uses the separate privileged
`GenerationManagement` service.

## Build profiles

The unified `akidb` CLI is built without cloud generation features by default.
Build the server binary explicitly for generation work:

```bash
# Single-node S3/MinIO publication surface
cargo build --release -p akidb-server --features generation-s3

# PostgreSQL replica worker; includes generation-s3
cargo build --release -p akidb-server --features generation-postgres
```

Start the resulting binary without `--standalone`, because immutable
publication requires S3/MinIO:

```bash
./target/release/akidb-server --config config/default.toml
```

## Base configuration

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
control_token_file = "/etc/akidb/generation-control.token"
allowed_buckets = ["ax-knowledge"]
require_version_or_digest_key = true
```

The three local paths must be distinct and non-overlapping. Configure
`storage.minio` for the fixed S3/MinIO endpoint and read-only replica
credentials. Use TLS for MinIO.

In single-node mode, `GenerationManagement` always requires its own bearer
token from `AKIDB_GENERATION_CONTROL_TOKEN` or `control_token_file`; startup
rejects reuse of the read data-plane token. When
`generation_serving.replica_control.enabled = true`, the server does not expose
`GenerationManagement`: PostgreSQL is authoritative and the local worker is
the only generation mutator.

Enable the built-in gRPC TLS certificate and key for remote listeners. The
qualified cell also binds only to its private WireGuard overlay.

## Single-node publication flow

1. AX Fabric writes a deterministic logical NDJSON bundle to immutable S3.
2. `StageGeneration` sends the exact manifest JSON bytes and their digest.
3. AkiDB fetches, verifies, builds, seals, and prewarms the shadow generation.
4. `GetGenerationStatus` reports local active, previous, and staged state.
5. `ActivateGeneration` atomically swaps reads after an explicit CAS.
6. `RollbackGeneration` swaps back to the retained prior generation after an
   explicit CAS.

```text
active generation A keeps serving
        │
        ├─ build B fails ─────────────► A still serves
        │
        └─ B verifies + local CAS ────► B serves atomically
                                       rollback returns to A
```

The privileged single-node flow accepts self-contained base bundles only:
`target_sequence` must equal `base_sequence`. PostgreSQL replica mode can
advance that immutable base to a later required checkpoint by building and
atomically installing a deterministic mutation-tail revision.

## PostgreSQL replica-control configuration

The full-replica profile adds:

```toml
[generation_serving.replica_control]
enabled = true
postgres_url_env = "AKIDB_KNOWLEDGE_POSTGRES_URL"
postgres_tls_mode = "require"
# postgres_ca_certificate_path = "/etc/akidb/certs/postgres-ca.pem"
endpoint = "akidb-r1.internal.example:50051"
failure_domain = "zone-a"
poll_interval_ms = 1000
heartbeat_interval_ms = 5000
index_format_version = "akidb-generation-v1"
supported_graph_schema_versions = ["ax.knowledge-graph.v1"]
```

Set the database URL only in the named environment variable:

```bash
export AKIDB_KNOWLEDGE_POSTGRES_URL='postgresql://...'
```

Do not put the URL or credentials in the TOML file. TLS verification is the
default. `postgres_tls_mode = "disable"` is restricted to a loopback
PostgreSQL endpoint for development and tests.

The replica worker is designed to:

1. verify the AX Fabric control-plane schema version;
2. register its stable identity, endpoint, failure domain, software/index
   compatibility, and heartbeat;
3. observe the configured workspace/collection publication directive;
4. independently fetch and materialize the authoritative base bundle;
5. page mutation contracts strictly in sequence from PostgreSQL and fetch
   checksum-addressed upsert payloads from MinIO;
6. validate each payload's scope, identity, record, graph nodes, graph edges,
   evidence, size, and digest; deletes carry no payload;
7. rebuild a complete shadow revision from the immutable base plus the ordered
   tail, normalize internal IDs, rebuild HNSW/BM25, verify graph invariants,
   and compute a logical materialization digest;
8. atomically install the revision and compare its checkpoint, vector/edge
   counts, and digest with control state and ready peers;
9. report catching-up, ready, serving, or failed checkpoint state;
10. change its local active pointer only after PostgreSQL commits the global
    active pointer;
11. keep the last known-good local generation readable while PostgreSQL is
    unavailable.

Control-plane migrations, activation policy, and audit are owned by AX Fabric.
NATS is not required by this path. If notification acceleration is added
later, PostgreSQL remains the replay and activation authority.

## Current boundaries

- The qualified full-replica profile uses three independent replicas and two
  stateless AX gateways. Two replicas are the minimum failover topology but
  are not the recommended production shape.
- Ordered mutation-tail revisions, duplicate/gap handling, blank rebuild, and
  generation-aware routing are implemented.
- The privileged single-node `GenerationManagement` flow does not accept a
  mutation tail; tail convergence belongs to PostgreSQL replica mode.
- The initial worker converges one configured workspace/collection scope per
  server process.
- A drained worker stops convergence work and the gateway excludes it.
- Active, previous, staged, and publication generations are retained;
  age-bounded local and object-store GC produces audit evidence.
- S3/MinIO is the only bundle fetch backend.
- MCP startup is refused in generation mode to prevent a mutable-path bypass.
- The full replica cell and its capacity envelope are not implied by native
  runtime support on a platform; see
  [Platform Support](../platform/SUPPORT.md).

## Verification

Single-node generation checks:

```bash
cargo test -p akidb-grpc --features generation-s3 --lib
cargo test -p akidb-server --features generation-s3 --lib
cargo clippy -p akidb-grpc -p akidb-server \
  --features generation-s3 --all-targets -- -D warnings
cargo check --workspace
./scripts/test-generation-serving-minio.sh
```

The MinIO gate publishes two checksum-addressed bundles, verifies concurrent
atomic cutover, restarts the real server process, checks generation evidence,
rolls back, and compares the original results and citation context exactly.

The PostgreSQL worker's minimum focused checks are:

```bash
cargo test -p akidb-storage --lib generation_layout
cargo test -p akidb-contracts --lib \
  mutation_payload_is_bound_to_exact_ordered_chunk_projection
cargo test -p akidb-storage --lib \
  sealed_revision_advances_active_checkpoint_and_markers_atomically
cargo test -p akidb-grpc --features generation-postgres --lib replica_worker
cargo test -p akidb-grpc --features generation-postgres --lib \
  mutation_tail_builds_sealed_revisions_and_keeps_base_immutable
cargo check -p akidb-server --features generation-postgres
```

Focused tests alone do not establish HA. The supported cell additionally runs
independently stored replicas, authoritative activation policy, gateway route
barriers, failure injection, blank-node rebuild, rolling upgrade/rollback, and
measured recovery evidence.
