# AkiDB

**A portable retrieval database for private AI systems.**

AkiDB combines durable vector storage, hybrid search, graph-aware retrieval,
and context assembly in one Rust service. It is designed for local and
on-premises RAG, agent memory, code intelligence, and other workloads where
source data should stay under the operator's control.

AkiDB v0.10.0 supports macOS 26 on Apple Silicon and Ubuntu 24.04 or newer on
AMD64 and ARM64. The default runtime is CPU-portable; CUDA, NVIDIA GPU, and
Thor-specific paths are not required.

> **Project status:** the standalone database is the primary supported
> deployment. The multi-shard coordinator, ingestion stack, and Ansible
> cluster workflow are available for evaluation and qualification, but they do
> not yet provide automatic replication, failover, or rebalancing.

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
- **Durable local state:** RocksDB-backed vectors, metadata, graph state, ID
  mappings, local snapshot inventory, and rebuild state.
- **Agent-ready interfaces:** gRPC, Python and TypeScript SDKs, an MCP stdio
  server, a terminal UI, and JSON-oriented operations commands.
- **Local-first security:** loopback-first defaults, bearer-token and workspace
  controls, redacted management output, and no cloud control plane.

## Supported platforms

| Operating system | Architecture | Runtime status | Delivery path |
| --- | --- | --- | --- |
| macOS 26 | Apple Silicon (`arm64`, M2 or newer) | Supported | Release archive or source build |
| Ubuntu 24.04+ | AMD64 (`x86_64`) | Supported | Release archive, source build, Docker, and qualified Ansible artifact workflow |
| Ubuntu 24.04+ | ARM64 (`aarch64`) | Supported | Release archive or source build |

All supported targets use the portable HNSW backend. A homogeneous operating
system and architecture is recommended within a shard group. macOS Intel,
older Ubuntu releases, CUDA/NVIDIA acceleration, and other Linux
distributions are outside the tested support matrix.

The Ubuntu ARM64 runtime is supported for standalone servers, coordinators, and
shards. The checksum-pinned Ansible cluster artifact and the opt-in immutable
generation-serving qualification gate remain AMD64-specific; see
[Platform Support](docs/platform/SUPPORT.md) for the exact runtime, CI,
container, and deployment matrix.

## Architecture

### Service boundary

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

The server owns the storage and retrieval lifecycle. Vectors and metadata are
persisted in RocksDB; the HNSW and lexical indexes are rebuilt from durable
state at startup. The native graph index shares the storage boundary, so graph
expansion does not require a second database. Text-to-vector conversion stays
behind an OpenAI-compatible embedding interface and can be disabled when
clients provide vectors directly.

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

### Deployment shapes

| Shape | Components | Intended use |
| --- | --- | --- |
| Standalone | One `akidb` server and local storage | Primary supported path for local RAG, agent memory, development, and single-node deployments |
| Multi-shard | Coordinator plus two or more shard servers | Fan-out search, capacity experiments, and qualified private-network clusters |
| Ingestion stack | Upload gateway, parsers, NATS, MinIO, ingestion workers, embedding service, and AkiDB | Document-processing and integration workflows |

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

On Ubuntu 24.04 or newer, on either AMD64 or ARM64:

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
- Built-in server TLS is not wired in v0.10.0. Terminate TLS at a trusted local
  proxy or use a private encrypted overlay for remote traffic.
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
- [Platform support](docs/platform/SUPPORT.md)
- [Operations runbook](docs/runbooks/operations.md)
- [Ansible cluster deployment](deploy/ansible/README.md)
- [Vector quality gates](docs/quality/vector-quality.md)
- [One-node benchmark](docs/quality/one-mac-benchmark.md)
- [Native GraphRAG plan and status](docs/development/native-graphrag-plan.md)

## Current limitations

- No automatic shard replication, failover, placement, or rebalancing.
- Coordinator authentication/workspace propagation to shards is not complete.
- Built-in gRPC TLS is not active in the server binary.
- The storage crate includes WAL primitives, but the server write path does not
  yet use the configured WAL.
- The native BM25 index is rebuilt in memory from persisted records.
- The Docker image, checksum-pinned Ansible artifact, and immutable
  generation-serving qualification gate are currently AMD64-only even though
  the native Ubuntu runtime supports ARM64.
- NVIDIA Thor and CUDA builds are unsupported.
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
