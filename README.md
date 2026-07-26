# AkiDB

**A portable retrieval engine for private AI systems.**

AkiDB combines durable vector storage, hybrid search, graph-aware retrieval,
and context assembly in one Rust service. It is designed for local and
on-premises RAG, agent memory, code intelligence, and other workloads where
source data should stay under the operator's control.

AkiDB has two deliberately different data-lifecycle profiles:

- **Mutable standalone:** clients write vectors and records directly to one
  AkiDB service. This is the primary supported profile.
- **Immutable generation serving:** AX Fabric publishes a complete,
  checksum-addressed knowledge generation and AkiDB materializes a disposable
  local retrieval projection. PostgreSQL-led full-replica convergence and the
  generation-aware AX read gateway are implemented; the Linux AMD64 cell is
  qualified separately from the primary Mac profile.

AkiDB v0.10.0 supports macOS 26 on Apple Silicon and Ubuntu 24.04 or newer on
AMD64. The default runtime is CPU-portable. Linux ARM64, macOS Intel, CUDA,
NVIDIA GPU, and Thor-specific paths are not supported release targets.

> **Project status:** the standalone database is the primary supported
> deployment. Immutable generation serving adds independently rebuilt full
> replicas, quorum activation, and generation-aware read failover. The Ubuntu
> AMD64 three-replica knowledge cell is qualified for a bounded 100k × 768
> envelope. Broader market ANN, graph, and competitor-parity claims remain an
> active release gate, not a completed verdict. AkiDB is not a consensus
> database: canonical data remains in MinIO/OpenWiki and PostgreSQL. The
> multi-shard coordinator remains a separate capacity path.

## Why AkiDB

Most RAG systems assemble a vector index, keyword engine, metadata store, graph
database, reranker, and context builder as separate services. AkiDB puts the
core retrieval path behind one API and one operational boundary:

- **Dense retrieval:** HNSW search with cosine, inner-product, or L2 distance
  and `f32` or `f16` vector storage.
- **Hybrid retrieval:** in-process BM25, dense search, Reciprocal Rank Fusion,
  optional reranking, MMR diversity, and token-budgeted context packing.
- **Native GraphRAG:** persisted graph nodes and edges, bounded traversal, and
  graph-expanded chunk retrieval without an external graph service.
- **Structured filtering:** typed metadata and tag filters, plus an optional
  SQLite metadata index; PostgreSQL support is feature-gated.
- **Two durability models:** RocksDB-backed mutable standalone state, or
  immutable knowledge generations rebuilt from canonical MinIO artifacts and
  control records.
- **Agent-ready interfaces:** gRPC, Python and TypeScript SDKs, an MCP stdio
  server, a terminal UI, and JSON-oriented operations commands.
- **Local-first security:** loopback-first defaults, bearer-token and workspace
  controls, redacted management output, and an optional, separately secured
  PostgreSQL control plane for the replica profile.

## Supported platforms

| Operating system | Architecture | Runtime status | Delivery path |
| --- | --- | --- | --- |
| macOS 26 | Apple Silicon (`arm64`, M2 or newer) | Supported | Release archive or source build |
| Ubuntu 24.04+ | AMD64 (`x86_64`) | Supported | Release archive, source build, Docker, and qualified Ansible artifact workflow |

Both supported targets use the portable HNSW backend. Linux ARM64 (including
NVIDIA Thor), macOS Intel, Ubuntu older than 24.04, CUDA/NVIDIA acceleration,
and other Linux distributions are outside the release support matrix. Thor
may be used for isolated experiments, but those results do not change the
supported platforms without a separate qualification decision. See
[Platform Support](docs/platform/SUPPORT.md) for the exact matrix.

## Architecture

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

The same retrieval core is used by both lifecycle profiles. In mutable
standalone mode, vectors and metadata are persisted in RocksDB and the HNSW
and lexical indexes are rebuilt from durable local state at startup. In
generation mode, a manifest binds vector, lexical, payload, and graph data to
one immutable generation; AkiDB builds that generation in a shadow directory
and atomically changes the local serving pointer only after verification. The
native graph index shares the retrieval boundary, so graph expansion does not
require a second database.

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

### Agentic knowledge-serving design

The target knowledge-serving cell separates canonical data, publication
authority, local retrieval state, and request routing:

```text
OpenWiki + source objects
            │
            ▼
  AX Fabric ingestion/distillation
            │
            ├── immutable logical bundles ──► MinIO
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

MinIO and OpenWiki remain canonical. PostgreSQL is the publication and ordered
checkpoint authority. Each AkiDB node owns an independent, rebuildable full
copy on local storage; live RocksDB or index files are never shared between
replicas. NATS may later accelerate notifications, but it is not the
correctness authority. Immediate agent/session memory stays on a strongly
consistent path rather than relying on asynchronous index convergence.

The checked-in implementation covers the full diagram: authoritative
publication, independent materialization/checkpoints, quorum activation,
bounded GraphRAG evidence, and read-only gateway failover. See the
[knowledge-serving architecture](docs/architecture/knowledge-serving.md) for
the ownership, consistency, and release boundaries.

### Deployment shapes

| Shape | Components | Status and intended use |
| --- | --- | --- |
| Mutable standalone | One `akidb` server and local storage | Primary supported path for local RAG, agent memory, development, and single-node deployments |
| Immutable single node | MinIO plus one generation-enabled AkiDB server | Opt-in atomic-publication preview; no replication or failover |
| Full-replica cell | HA PostgreSQL, MinIO, three independent AkiDB replicas, and two or more AX gateways | Qualified Ubuntu AMD64 knowledge-serving profile; PostgreSQL and object-store HA remain external deployment responsibilities |
| Multi-shard | Coordinator plus two or more independent shard servers | Fan-out search, capacity experiments, and qualified private-network clusters; not the HA replica design |
| Ingestion stack | Upload gateway, parsers, NATS, MinIO, ingestion workers, embedding service, and AkiDB | Document-processing and integration workflows; its NATS stream is separate from knowledge-generation authority |

The coordinator merges results across shards and applies backpressure, but it
is not yet a replication layer. The current coordinator also does not forward
bearer/workspace metadata to shards. The Ansible cluster profile therefore
runs only on an isolated WireGuard service network and must not expose AkiDB
ports publicly.

## Quick start

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
| `auth` / `auth.acl` | Loopback policy, bearer token source, default workspace, and workspace enforcement |
| `generation_serving` | Opt-in immutable generation paths, publication credential, S3 limits, and generation materialization |
| `generation_serving.replica_control` | Disabled-by-default PostgreSQL replica-worker settings for the Ubuntu AMD64 knowledge-serving profile |
| `index` | HNSW construction/search settings, metric, precision, filtering, and rebuild thresholds |
| `storage` | RocksDB and snapshot-related paths; WAL settings are reserved for the not-yet-wired server WAL path |
| `sql` | Optional SQLite or feature-gated PostgreSQL metadata index |
| `embedding` | Optional text embedding endpoint, model identity, dimensions, and timeouts |
| `observability` / `slo` | Logs, metrics, tracing, backpressure, and reference targets |

Security defaults and requirements:

- Keep `127.0.0.1` unless remote access is intentionally configured.
- A non-loopback server bind requires bearer authentication unless
  `auth.mode = "disabled"` is explicitly selected for an isolated network.
- Pass tokens through `AKIDB_AUTH_TOKEN` or a mode-`0600` file referenced by
  `AKIDB_AUTH_TOKEN_FILE`; never commit tokens or inventories.
- Single-node generation publication requires a distinct
  `AKIDB_GENERATION_CONTROL_TOKEN`; PostgreSQL replica mode removes that local
  control API and reads its database URL only from the configured environment
  variable, with verified TLS by default.
- Built-in server TLS is supported. The knowledge-cell profile also uses an
  encrypted private overlay, HTTPS at the gateway and MinIO, and verified
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
- [Platform support](docs/platform/SUPPORT.md)
- [Operations runbook](docs/runbooks/operations.md)
- [Knowledge-serving runbook](docs/runbooks/knowledge-serving.md)
- [Ansible deployment](deploy/ansible/README.md)
- [Ubuntu AMD64 knowledge-cell qualification](docs/quality/linux-amd64-knowledge-cell-qualification.md)
- [Market-readiness qualification](docs/quality/market-readiness-qualification.md)
- [Vector quality gates](docs/quality/vector-quality.md)
- [One-node benchmark](docs/quality/one-mac-benchmark.md)
- [Native GraphRAG plan and status](docs/development/native-graphrag-plan.md)

## Current limitations

- Immutable generation serving provides PostgreSQL-led full-replica
  convergence and generation-aware read failover. The Ubuntu AMD64 cell is
  qualified for a bounded retrieval envelope (100k vectors × 768 dimensions
  with smaller deterministic generation/failover drills). It does not make
  PostgreSQL or MinIO highly available; production must supply those durable
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
- Linux release artifacts and the knowledge-serving cell are AMD64-only.
- Linux ARM64, NVIDIA Thor, and CUDA builds are unsupported release paths.
- Four-Mac Thunderbolt validation tooling defines an experimental evidence
  path; it is not the primary product topology or a production-readiness claim.

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
