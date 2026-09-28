# AkiDB

**Local-first retrieval for private AI.**

Cited, token-budgeted context for agents, from one Rust service you run
yourself.

## The problem

An agent working on private data needs one pack of context for the call it is
about to make. That pack has to name its sources, and it has to fit a token
budget. The source data stays on machines the operator controls. The retrieval
that builds the pack stays in the same service.

Building that pack usually means several lookups at once: nearby passages from
vectors, exact wording from keywords, related passages from a bounded graph,
and a metadata scope such as a workspace. The result still has to be ranked,
diversified, and cut to a token limit, with a score, a reason, and a citation
on each passage. Splitting that work across a vector index, a keyword engine,
a graph database, and a separate packer leaves the operator to join the
boundary, the credentials, and the failure domain.

## What AkiDB returns

AkiDB is the service that does this in one process. A request can combine:

- durable vector storage and CPU HNSW (cosine, inner product, or L2; `f32` or
  `f16`)
- in-process BM25
- reciprocal rank fusion, optional reranking, and MMR
- typed metadata and tag filters, plus an optional SQLite metadata index
  (PostgreSQL is feature-gated)
- bounded graph expansion over a native GraphRAG index, without a second
  graph database

The agent call is `TextSearch` with `pack` set. That request returns
`ContextPackV1`: each passage has text, a score, a reason, and a citation,
plus the token budget, the counter name (`conservative_v1`), and whether the
budget or a filter candidate window cut the result. Vector `Search` stays a
top-k call and does not build that pack. Clients use gRPC, the Python and
TypeScript SDKs, or the MCP `pack` tool. A terminal UI and JSON operations
commands are included for the operator.

Loopback is the default bind. Bearer tokens and workspace controls apply when
the server is reachable beyond the local machine.

## Two ways the data lives

**Writable standalone is the default.** Clients write vectors and records into
one AkiDB process. This is the primary supported profile. Vectors and metadata
stay in local RocksDB. On startup, a matching HNSW snapshot is loaded. If the
snapshot is missing or does not match the stored vectors and index settings,
the graph is rebuilt from those vectors. The lexical index is rebuilt in memory from the persisted
records.

**Published generations are optional and off by default.** AX Fabric publishes
an immutable, checksum-addressed knowledge generation. AkiDB builds a local
projection in a shadow directory, verifies it, and then switches the serving
pointer. The projection can be discarded and rebuilt. In this profile AkiDB is
not the system of record and not a consensus database. Canonical data stays in
AX Fabric object storage and PostgreSQL. `--standalone` does not enable this
profile.

> **Direction:** smaller, better-cited context for each agent step. Memory and
> temporal behavior stay experimental and are not part of the supported
> product. AkiDB does not plan, call tools, or run the agent.

## Why this matters

- **The data stays on the operator's machines.** The best-fit hosts are one
  Mac Studio or one AMD64 PC, and an on-prem Mac Studio cluster or an AMD64
  cloud cluster. Mac Mini and MacBook run the same Apple Silicon build for
  lighter loads.
- **One call returns what the agent can read.** The pack is cited passages
  inside a token budget, assembled by this service.
- **Publication and retrieval stay apart when a generation is served.** Fabric
  remains the publisher. AkiDB serves a verified local projection and can
  rebuild it. The writable profile is a different lifecycle: there, AkiDB is
  the process that holds the vectors and records the client wrote.

## Where it runs

AkiDB v2.0.0 uses the CPU-portable HNSW backend. Supported release targets are
macOS 26 on Apple Silicon and Ubuntu 24.04 or newer on AMD64.

| Audience | Best-fit target | Also supported |
| --- | --- | --- |
| Single user | **Mac Studio** or **AMD64 PC** standalone | Mac Mini / MacBook standalone |
| Enterprise | **Mac Studio cluster** or **AMD64 cloud cluster** | Mac cluster qualification is pending; the checked-in cell evidence is Ubuntu AMD64 |

| Operating system | Architecture | Support tier | Delivery path |
| --- | --- | --- | --- |
| macOS 26 | Apple Silicon (`arm64`, M2 or newer) | Primary on Mac Studio; also Mac Mini / MacBook | Release archive or source build |
| Ubuntu 24.04+ | AMD64 (`x86_64`) | Primary workstation and cloud | Release archive, source build, Docker, qualified Ansible artifacts |

Linux ARM64 (including NVIDIA Thor), macOS Intel, Ubuntu older than 24.04,
other Linux distributions, and CUDA/GPU-accelerated index paths are outside
the support matrix. A successful source build on an unsupported target is not
a product support claim. See [Platform Support](docs/platform/SUPPORT.md).

> **Project status:** writable standalone is the primary supported deployment.
> Immutable generation serving adds independently rebuilt full replicas, quorum
> activation, and generation-aware read failover. The Ubuntu AMD64
> three-replica knowledge cell is qualified for a bounded 100k × 768 envelope.
> Broader market ANN, graph, and competitor-parity claims remain an active
> release gate, not a completed verdict. The multi-shard coordinator is a
> separate capacity path. It fans out queries; it does not replicate shard
> data.

## How one request becomes a pack

### Retrieval core

```text
Applications and agents
  ├── gRPC
  ├── Python / TypeScript SDKs
  ├── MCP over stdio
  └── CLI / TUI / operations API
                 │
                 ▼
┌───────────────────────────────────────────────────────────┐
│                         AkiDB                             │
│  auth + workspaces + collections + management surface    │
│                           │                               │
│               deterministic query planner                │
│        ┌──────────┬──────────┬──────────┬──────────┐      │
│        │ HNSW     │ BM25     │ metadata │ graph    │      │
│        │ vectors  │ lexical  │ / SQL    │ expand   │      │
│        └──────────┴──────────┴──────────┴──────────┘      │
│                           │                               │
│             RRF → rerank → MMR → context pack            │
│                           │                               │
│     RocksDB + snapshot inventory + native graph state    │
└───────────────────────────────────────────────────────────┘
                 │ optional
                 ▼
       OpenAI-compatible embedding endpoint
```

Both lifecycles use this core. In the writable profile, vectors and metadata
stay in RocksDB. Startup loads a persisted HNSW snapshot when it matches the
stored vectors and index settings; otherwise it rebuilds the graph from those
vectors. The lexical index is rebuilt in memory from the persisted records. In
the generation profile, a manifest binds vector, lexical, payload, and graph
data to one immutable generation. AkiDB builds that generation in a shadow
directory and changes the local serving pointer only after verification. Graph
expansion uses the same retrieval boundary.

Text-to-vector conversion stays behind an OpenAI-compatible embedding
interface and can be disabled when clients provide vectors directly.

### Retrieval path

```text
query
  │
  ▼
planner ──► dense HNSW
  │       ├► BM25 lexical
  │       ├► metadata / SQL filters
  │       └► bounded graph expansion
  ▼
rank fusion ──► optional rerank and diversity ──► context pack + citations
```

The planner selects dense, lexical, hybrid, graph, or graph-hybrid retrieval
from explicit request controls and query signals. Metadata filters are applied
through the same path, and packed context remains tied to the returned source
chunks.

### Optional generation cell

This is the published-generation profile, not the default server. Canonical
data, publication, the local retrieval projection, and request routing stay
separate:

```text
AX Wiki / DocProc inputs + source objects
            │
            ▼
  AX Fabric ingestion/distillation
            │
            ├── immutable logical bundles ──► SeaweedFS
            └── generation + outbox ────────► HA PostgreSQL
                                                │
                              ┌─────────────────┼─────────────────┐
                              ▼                 ▼                 ▼
                         AkiDB replica 1    AkiDB replica 2    [replica 3]
                         local RocksDB,     local RocksDB,      recommended
                         HNSW/BM25/graph    HNSW/BM25/graph
                              └─────────────────┬─────────────────┘
                                                ▼
                                  AX retrieval gateway
                                                │
                                                ▼
                                         Agents / GenAI
```

SeaweedFS remains the canonical object store. PostgreSQL is the publication
and ordered checkpoint authority. Each AkiDB node keeps an independent,
rebuildable full copy on local storage. Replicas do not share live RocksDB or
index files. NATS may later accelerate notifications; it is not the
correctness authority. The experimental Memory preview is a separate
single-process path, and it is off by default.

The checked-in implementation covers publication, independent
materialization and checkpoints, quorum activation, bounded GraphRAG
evidence, and read-only gateway failover. The qualified envelope is the
Ubuntu AMD64 100k × 768 cell above. See the
[knowledge-serving architecture](docs/architecture/knowledge-serving.md) for
ownership, consistency, and release boundaries.

### Deployment shapes

| Shape | Components | Status and intended use |
| --- | --- | --- |
| Mutable standalone | One `akidb` server and local storage | Best-fit single-user path on Mac Studio or AMD64 PC; also Mac Mini / MacBook |
| Immutable single node | SeaweedFS plus one generation-enabled AkiDB server | Opt-in atomic-publication preview; no replication or failover |
| Full-replica cell | HA PostgreSQL, SeaweedFS, three independent AkiDB replicas, and two or more AX gateways | Enterprise design: Mac Studio cluster or AMD64 cloud cell. Ubuntu AMD64 envelope is the checked-in qualification; PostgreSQL and object-store HA remain external |
| Multi-shard | Coordinator plus two or more independent shard servers | Fan-out search and capacity experiments; not the HA replica design |
| Ingestion stack | Upload gateway, parsers, NATS, SeaweedFS, ingestion workers, embedding service, and AkiDB | Document-processing and integration workflows; its NATS stream is separate from knowledge-generation authority |

The coordinator merges results across shards and applies backpressure, but it
is not yet a replication layer. The current coordinator also does not forward
bearer/workspace metadata to shards. The Ansible cluster profile therefore
runs only on an isolated WireGuard service network and must not expose AkiDB
ports publicly.

## Quick start

The commands below build and run the writable standalone server. That is the
default profile. Generation serving, the experimental Memory preview, and MCP
are separate opt-in entry points later in this section.

### Prerequisites

- Rust stable via [rustup](https://rustup.rs/)
- Protocol Buffers compiler (`protoc`)
- C/C++ build tools, CMake, Clang, and `pkg-config`

On macOS 26:

```bash
xcode-select --install
brew install cmake protobuf
```

On Ubuntu 24.04 or newer on AMD64:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential clang cmake libclang-dev libssl-dev \
  pkg-config protobuf-compiler
```

### Build

```bash
git clone https://github.com/defai-digital/akidb.git
cd akidb

cargo build --release -p akidb-cli
```

Apple Silicon developers can run the full macOS validation path:

```bash
./scripts/build-on-mac-arm64.sh
```

### Run a standalone server

```bash
./target/release/akidb server \
  --standalone \
  --config config/standalone.toml
```

In another terminal:

```bash
./target/release/akidb health \
  --server 127.0.0.1:50051 \
  --require-ready
```

The default configuration binds to loopback and does not require a token for
loopback clients. To inspect available commands:

```bash
./target/release/akidb --help
./target/release/akidb server --help
```

### Connect a client

- [Python SDK](sdks/python/README.md)
- [TypeScript SDK](sdks/typescript/README.md)
- Canonical gRPC API: [`crates/proto/proto/akidb.proto`](crates/proto/proto/akidb.proto)

The SDKs cover vector CRUD, batch operations, collections, vector and text
search, cluster state, health, and agent-memory calls. `TextSearch` requires an
embedding endpoint; vector APIs do not.

### Authoritative Memory developer preview

Authoritative Memory is an **experimental developer preview** with one
authoritative workspace per process. The implementation exposes immutable
history, valid-time and system-time queries, exact retained replay, and
plan-then-execute deletion, but those surfaces are not yet a production or
system-of-record qualification. No HA or fleet claim is made. Its canonical
ledger is separate from the legacy metadata-backed `memory_write`/`memory_read`
helpers.

Start the no-cloud/no-embedding profile:

```bash
./scripts/akidb-memory-preview.sh
```

The script creates separate mode-0600 legacy and principal token files below
`data/memory-preview/` without printing either token. In another terminal,
install the Python SDK and run the real incident-replay ritual:

```bash
python3 -m venv sdks/python/.venv
sdks/python/.venv/bin/pip install -e sdks/python
sdks/python/.venv/bin/python scripts/agentic_memory_incident_replay.py
```

The demonstration commits an incorrect procedure, retains the recall that
would have led to a wrong action, commits a successor correction, verifies
later recall changed, and exactly replays the original snapshot.

For MCP-capable agents, the same profile exposes explicitly named
`memory_remember` and `memory_recall` tools:

```bash
./scripts/akidb-memory-preview.sh --mcp
```

The existing `memory_write` and `memory_read` MCP tools remain labeled
`LEGACY DOCUMENT MEMORY`.

### Run as an MCP server

```bash
./target/release/akidb mcp \
  --standalone \
  --config config/standalone.toml
```

MCP uses newline-delimited JSON-RPC on stdio and logs only to stderr.

## Text embeddings

AkiDB calls an OpenAI-compatible `/v1/embeddings` endpoint when
`embedding.enabled = true`. On macOS, the included sidecar can serve local
Qwen native artifacts through `ax-engine`:

```bash
python3 scripts/ax_engine_embedding_server.py \
  --model-dir /path/to/Qwen3-Embedding-4B \
  --model-id Qwen/Qwen3-Embedding-4B \
  --port 8081

AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-4B \
  ./scripts/validate-standalone.sh
```

Linux deployments may point the same interface at any compatible local
embedding service. Keep the configured embedding dimension aligned with the
collection/index dimension.

## Configuration and security

Start with [`config/standalone.toml`](config/standalone.toml) for a local
server or [`config/default.toml`](config/default.toml) for the complete option
reference.

| Section | Purpose |
| --- | --- |
| `server` | Bind address, gRPC port, and transport settings |
| `auth` / `auth.acl` | Loopback policy, legacy bearer token source, default workspace, and workspace enforcement |
| `auth.memory` / `auth.principals` | Disabled-by-default authoritative Memory workspace, versioned identities, credentials, scope ceilings, and capabilities |
| `memory` | Experimental authoritative ledger path, bounded recall limits, snapshots, and explicit retention declarations |
| `generation_serving` | Opt-in immutable generation paths, publication credential, S3 limits, and generation materialization |
| `generation_serving.replica_control` | Disabled-by-default PostgreSQL replica-worker settings for the Ubuntu AMD64 knowledge-serving profile |
| `index` | HNSW construction/search settings, metric, precision, filtering, and rebuild thresholds |
| `storage` | RocksDB and snapshot-related paths; WAL settings are reserved for the not-yet-wired server WAL path |
| `storage.seaweedfs` | S3-compatible object-store endpoint (SeaweedFS S3 gateway, port 8333 by default), bucket, credentials, and TLS for snapshots and generation bundles |
| `sql` | Optional SQLite or feature-gated PostgreSQL metadata index |
| `embedding` | Optional text embedding endpoint, model identity, dimensions, and timeouts |
| `observability` / `slo` | Logs, metrics, tracing, backpressure, and reference targets |

Security defaults and requirements:

- Keep `127.0.0.1` unless remote access is intentionally configured.
- A non-loopback server bind requires bearer authentication unless
  `auth.mode = "disabled"` is explicitly selected for an isolated network.
- Pass tokens through `AKIDB_AUTH_TOKEN` or a mode-`0600` file referenced by
  `AKIDB_AUTH_TOKEN_FILE`; never commit tokens or inventories.
- Authoritative Memory uses a separately registered principal credential and
  derives its workspace, namespace, purpose, sensitivity, entity, subject,
  session, task, agent, and capability ceilings from that server-side grant.
  Client fields can only narrow it.
- Single-node generation publication requires a distinct
  `AKIDB_GENERATION_CONTROL_TOKEN`; PostgreSQL replica mode removes that local
  control API and reads its database URL only from the configured environment
  variable, with verified TLS by default.
- The SeaweedFS S3 gateway serves every request anonymously when it is started
  without a credential configuration. Every documented deployment must start it
  with an S3 identity config (`-s3.config`); never point AkiDB at an
  unauthenticated object-store gateway.
- Built-in server TLS is supported. The knowledge-cell profile also uses an
  encrypted private overlay, HTTPS at the gateway and SeaweedFS, and verified
  PostgreSQL TLS.
- Real Ansible inventories, vault-password files, SSH keys, and local agent
  instructions are gitignored and rejected by the CI sensitive-file policy.

See the [operations runbook](docs/runbooks/operations.md), [incident-response
runbook](docs/runbooks/incident-response.md), and [security review
baseline](docs/security/SECURITY_REVIEW.md) before exposing a deployment
beyond one trusted host.

## Validation

Run the portable workspace checks on every supported platform:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo check --workspace --no-default-features
cargo test --workspace
python3 scripts/check-sensitive-files.py
```

SDK and proto-drift checks:

```bash
./sdks/check-proto-drift.sh
(cd sdks/python && pytest tests/ -v)
(cd sdks/typescript && npm ci && npm test)
```

Retrieval quality:

```bash
./scripts/qa_all.sh --build
python3 scripts/qa_vector_quality.py --build
```

When a local embedding model is available:

```bash
AX_ENGINE_MODEL_DIR=/path/to/model \
AX_ENGINE_MODEL=Qwen/Qwen3-Embedding-0.6B \
EMBEDDING_DIMENSIONS=1024 \
./scripts/qa_all.sh --build --require-text
```

## Performance evidence

The checked-in one-node reference artifact uses an Apple M3 Max with 128 GB of
memory and macOS 26.5.1. For 1,000,000 768-dimensional vectors and 5,000
`topK=10` queries, it recorded 586 queries/second with P95/P99 search latency
of 2.16/2.43 ms.

That result is a reproducible reference point, not a universal latency claim.
Dataset shape, dimensions, filters, HNSW settings, storage, concurrency, and
hardware all affect performance. See the [one-node benchmark
methodology](docs/quality/one-mac-benchmark.md) and [vector quality
gates](docs/quality/vector-quality.md).

The separate Authoritative Memory Linux AMD64 systems profile passed 15 fresh
release runs across four shared 8-vCPU/32-GB VMs, with five runs at each of
1k, 10k, and 100k versions. At 100k versions, the run medians were 444.49
synced visible commits/second with 21.358-ms P95 and 730.07 known-answer
recalls/second with 11.995-ms P95; all 555,000 commits and 15,000 measured
recalls completed with zero failures, incorrect recalls, or observed
projection lag. This is a synthetic single-process preview envelope, not a
production or semantic-quality claim. See the [Linux AMD64 Authoritative
Memory qualification](docs/quality/linux-amd64-authoritative-memory-qualification.md).

## Project layout

```text
akidb/
├── crates/
│   ├── common/                 shared configuration, errors, metrics
│   ├── proto/                  canonical protobuf and gRPC bindings
│   ├── embedding/              embedding abstraction and client
│   ├── contracts/              API and invariant contracts
│   ├── invariants/             property-based safety checks
│   ├── faiss-wrapper/          portable usearch HNSW index
│   ├── graph/                  native persisted graph index
│   ├── retrieval/              BM25, planning, fusion, rerank, context
│   ├── sql/                    optional metadata SQL adapters
│   ├── storage/                RocksDB, ID mapping, WAL, snapshots
│   ├── grpc-server/            data and management services
│   ├── coordinator/            multi-shard routing and result merge
│   ├── server/                 shard/server composition
│   ├── cli/                    unified `akidb` command
│   ├── tui/                    terminal operations console
│   ├── benchmark/              load and latency tooling
│   └── ingestion-orchestrator/ document ingestion pipeline
├── sdks/                       Python and TypeScript clients
├── services/                   document parser and upload gateway
├── config/                     example runtime configuration
├── deploy/                     Docker and Ansible assets
├── docs/                       support, quality, security, and runbooks
└── scripts/                    build, validation, QA, and packaging tools
```

## Documentation

- [Documentation index](docs/README.md)
- [Knowledge-serving architecture](docs/architecture/knowledge-serving.md)
- [Immutable generation serving](docs/development/generation-serving-preview.md)
- [Authoritative Memory developer preview](docs/development/authoritative-memory-preview.md)
- [Platform support](docs/platform/SUPPORT.md)
- [Operations runbook](docs/runbooks/operations.md)
- [Knowledge-serving runbook](docs/runbooks/knowledge-serving.md)
- [Ansible deployment](deploy/ansible/README.md)
- [Ubuntu AMD64 knowledge-cell qualification](docs/quality/linux-amd64-knowledge-cell-qualification.md)
- [Linux AMD64 Authoritative Memory qualification](docs/quality/linux-amd64-authoritative-memory-qualification.md)
- [Market-readiness qualification](docs/quality/market-readiness-qualification.md)
- [Vector quality gates](docs/quality/vector-quality.md)
- [One-node benchmark](docs/quality/one-mac-benchmark.md)
- [Native GraphRAG plan and status](docs/development/native-graphrag-plan.md)

## Current limitations

- Immutable generation serving provides PostgreSQL-led full-replica
  convergence and generation-aware read failover. The Ubuntu AMD64 cell is
  qualified for a bounded retrieval envelope (100k vectors × 768 dimensions
  with smaller deterministic generation/failover drills). It does not make
  PostgreSQL or SeaweedFS highly available; production must supply those durable
  HA services.
- Privileged single-node publication remains an opt-in preview. The PostgreSQL
  replica worker rebuilds deterministic post-bundle revisions from ordered
  mutations; multi-replica convergence is implemented and qualified only for
  the documented Ubuntu AMD64 profile and envelope.
- Market-aligned ANN, competitor parity (Milvus/Weaviate on SIFT1M), larger
  graph tiers, and full serving-system soak/failure gates are automated but
  not a completed release verdict. See
  [market-readiness qualification](docs/quality/market-readiness-qualification.md).
- The multi-shard coordinator is not a replication layer and does not provide
  automatic placement, failover, or rebalancing.
- Coordinator authentication/workspace propagation to shards is not complete,
  which is one reason it is not the agent-facing HA gateway.
- The storage crate includes WAL primitives, but the server write path does not
  yet use the configured WAL.
- The native BM25 index is rebuilt in memory from persisted records.
- Primary Linux packaging and knowledge-cell qualification evidence are
  AMD64-only; Mac Studio is the preferred Apple Silicon capacity host.
- Mac Mini / MacBook are supported standalone form factors, not substitutes
  for Studio or AMD64 enterprise capacity claims.
- Linux ARM64, NVIDIA Thor, and CUDA/GPU-accelerated vector-index paths are
  unsupported release paths.
- Four-Mac Thunderbolt validation tooling defines an experimental evidence
  path for Mac clustering; the enterprise design centers on Mac Studio or
  AMD64 cloud full-replica cells.
- Authoritative Memory is an experimental, single-process developer preview.
  Its immutable ledger, bitemporal/history APIs, retained replay, and
  reviewable deletion workflow are implemented for evaluation, but are not a
  production, system-of-record, multi-tenant, HA, or fleet qualification. A
  bounded synthetic Linux AMD64 systems envelope is published separately. See
  the [preview boundary](docs/development/authoritative-memory-preview.md) and
  [qualification report](docs/quality/linux-amd64-authoritative-memory-qualification.md).

## Contributing

1. Fork the repository and create a focused branch.
2. Add tests for behavior changes.
3. Run the validation commands above.
4. Open a pull request with a concise summary and the commands run.

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Support

Use [GitHub Issues](https://github.com/defai-digital/akidb/issues) for bug
reports, support questions, and feature requests.
