# AkiDB

A Mac-first AI knowledge retrieval engine for private local RAG, Apple Silicon
appliance deployments, and four-Mac Thunderbolt cells.

## Features

- **One-Mac Appliance**: Production-capable single-node deployment on Apple Silicon
- **Four-Mac Cell Design**: Thunderbolt-connected shard and replica placement for local scale-out
- **Cell-Based Horizontal Scale**: Add four-Mac cells instead of growing an unbounded mesh
- **Portable Backend First**: CPU/portable backend for Mac M2 or later ARM64 systems
- **Sub-50ms Latency**: Optimized for real-time RAG applications
- **Hybrid Retrieval**: Dense vector search plus BM25 lexical retrieval and RRF fusion
- **Metadata Filtering**: Typed metadata/tag filters backed by RocksDB indexes
- **Native Graph Foundation**: RocksDB-backed graph retrieval primitives for GraphRAG expansion
- **Context Builder**: Source-grounded context packing with citation support
- **Rust Performance**: Memory-safe, async-first implementation

## Architecture

```
                 Client
                   │
                   ▼
          Logical AkiDB Endpoint
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
   One-Mac Appliance   Four-Mac Thunderbolt Cell
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
       Shards           Replicas          Snapshots
```

Retrieval is evolving toward an AI-native knowledge stack:

```
Text Query
   │
   ▼
Query Planner
   │
   ├── Vector Search
   ├── BM25 / Full Text
   ├── Metadata Filters
   ├── SQLite Metadata SQL
   └── Native Graph Index
           │
        Fusion
           │
      Rerank / MMR
           │
    Context Builder
```

The default hot path stays self-contained on Apple Silicon. SQLite can be
enabled as an optional metadata SQL adapter for exact structured filters.
PostgreSQL and external graph engines such as Kuzu or Apache AGE remain planned
as optional adapters, not default runtime dependencies.

The Kuzu evaluation entry point is currently compile-gated with
`akidb-graph/kuzu`. It exposes the adapter boundary and schema scaffold without
linking Kuzu into the default build; real Kuzu binding support remains behind
the benchmark and maintenance gates. Native-vs-Kuzu benchmark decisions should
be validated with `scripts/validate-kuzu-decision.py`.

## Quick Start

### Mac M2 Or Later

```bash
# Clone the repository
git clone https://github.com/defai-digital/akidb.git
cd akidb

# Build and validate the portable Apple Silicon path
./scripts/build-on-mac-arm64.sh

# Or run Cargo directly
cargo build
cargo test

# Run AkiDB through the single CLI entry point
cargo run -p akidb-cli -- server --standalone --config config/default.toml
cargo run -p akidb-cli -- coordinator --shards 127.0.0.1:50051
cargo run -p akidb-cli -- tui --coordinator 127.0.0.1:50050

# Format and lint
cargo fmt
cargo clippy
```

### Roadmap

Detailed product and architecture documents (PRD, ADRs, technical specification)
are maintained internally and are not published in this repository.

## Project Structure

```
akidb/
├── crates/
│   ├── common/          # Shared types, errors, config
│   ├── faiss-wrapper/   # Optional FAISS FFI bindings
│   ├── graph/           # Native GraphRAG graph index and traversal contract
│   ├── retrieval/       # BM25, RRF, rerank, context packing
│   ├── sql/             # Optional SQLite metadata SQL adapter
│   ├── storage/         # RocksDB, WAL, ID mapping
│   ├── grpc-server/     # gRPC API service
│   ├── coordinator/     # Fan-out search coordination
│   └── cli/             # Single akidb command entry point
├── services/            # Python sidecar services
├── config/              # Configuration files
├── deploy/              # Deployment manifests
├── docs/                # Product, architecture, runbooks, and archive
├── scripts/             # Utility scripts
└── samples/             # Sample documents and fixtures
```

## Configuration

See `config/default.toml` for all configuration options.

Key settings:
- `slo.reference.*`: SLO reference configuration
- `index.nprobe`: Search accuracy vs speed for FAISS-compatible backends
- `sql.*`: optional metadata SQL adapter for structured filters. SQLite is the default; PostgreSQL requires `akidb-server --features postgres`.
- `embedding.*`: optional local text embedding sidecar for `TextSearch`

### Local Text Embeddings

AkiDB uses a local OpenAI-compatible `/v1/embeddings` endpoint for `TextSearch`.
For current `ax-engine`, run the included sidecar against local Qwen embedding
native artifacts containing `model-manifest.json`; do not use
`ax-engine serve <embedding-alias>`.

```bash
python3 scripts/ax_engine_embedding_server.py \
  --model-dir /path/to/Qwen3-Embedding-4B \
  --model-id Qwen/Qwen3-Embedding-4B \
  --port 8081

AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-4B \
  ./scripts/validate-standalone.sh
```

For `Qwen3-Embedding-0.6B`, set `AX_ENGINE_MODEL=Qwen/Qwen3-Embedding-0.6B`
and `EMBEDDING_DIMENSIONS=1024` when running the validator.

### Vector Quality QA

Run all available local quality gates. The vector gate always runs; the semantic
`TextSearch` gate runs when `AX_ENGINE_MODEL_DIR` is configured.

```bash
./scripts/qa_all.sh --build
```

For release validation, require both gates:

```bash
AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-0.6B-4bit-DWQ \
AX_ENGINE_MODEL=mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ \
EMBEDDING_DIMENSIONS=1024 \
./scripts/qa_all.sh --build --require-text
```

Run the deterministic quality gate to compare AkiDB search results with exact
brute-force cosine ground truth:

```bash
python3 scripts/qa_vector_quality.py --build
```

Run semantic retrieval QA with local embedding artifacts:

```bash
AX_ENGINE_MODEL_DIR=/path/to/Qwen3-Embedding-0.6B-4bit-DWQ \
AX_ENGINE_MODEL=mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ \
EMBEDDING_DIMENSIONS=1024 \
python3 scripts/qa_text_retrieval.py
```

Quality gates report recall@k, nDCG, MRR, hit rate, and latency. See
`docs/quality/vector-quality.md` for thresholds and release artifact rules.

### One-Mac Benchmark

Run the clean standalone synthetic benchmark and write a JSON artifact:

```bash
./scripts/benchmark-one-mac.sh
```

For the reference target shape:

```bash
VECTORS=1000000 DIMENSIONS=768 QUERIES=5000 ./scripts/benchmark-one-mac.sh
```

See `docs/quality/one-mac-benchmark.md` for artifact requirements and
interpretation.

### Four-Mac Cell Validation

Before any production claim for a four-Mac Thunderbolt cell, validate a
machine-readable artifact:

```bash
python3 scripts/validate-four-mac-cell.py docs/reports/four-mac-cell-YYYYMMDD.json
```

See `docs/quality/four-mac-cell-validation.md` for the artifact schema and
default gates. This validator does not replace the real four-Mac hardware run;
it defines the evidence required to mark that validation complete.

## Performance Targets

| Metric | Target | Reference Config |
|--------|--------|------------------|
| One-Mac Search P95 | < 50ms | D=768, N=1M, topK=10 |
| One-Mac Search P99 | < 100ms | D=768, N=1M, topK=10 |
| Four-Mac Cell Throughput | >= 2.5x one Mac | Same dataset class |
| Recall@10 | > 95% | Approximate backend reference config |

## Documentation

- [Documentation Index](docs/README.md) - canonical docs and archive map
- [Platform Support](docs/platform/SUPPORT.md) - macOS Apple Silicon support matrix
- [One-Mac Benchmark](docs/quality/one-mac-benchmark.md) - reproducible benchmark artifact workflow
- [Four-Mac Cell Validation](docs/quality/four-mac-cell-validation.md) - Thunderbolt cell validation artifact gate
- [Kuzu Decision Gate](docs/quality/kuzu-decision-gate.md) - native-vs-Kuzu graph adapter benchmark gate
- [Vector Quality Gates](docs/quality/vector-quality.md) - recall and semantic retrieval QA

Product requirements, architecture decisions, and the technical specification are
maintained as internal documents and are not part of this public repository.

## Development Status

**Current Phase:** v2 Mac-first design reset

- [x] Cargo workspace initialized
- [x] CI/CD pipeline configured
- [x] Security baseline (cargo-audit, deny.toml)
- [x] Canonical PRD/ADR/technical specification
- [x] Native graph retrieval crate initialized
- [x] BM25 + RRF hybrid retrieval foundation
- [x] Deterministic query planner scaffolding
- [x] Graph-expanded context packing when a graph index is configured
- [x] Best-effort graph indexing from `parent_id` and `related_ids` metadata
- [x] MCP status reports graph stats when graph expansion is configured
- [x] TextSearch metadata/tag filtering across hybrid and graph-expanded context
- [x] Metadata-driven code graph edges for imports/calls/dependencies/owners/commits
- [x] Planner-driven TextSearch vector/BM25/hybrid routing with explicit mode overrides
- [x] Graph-expanded chunks participate in TextSearch results when graph routing is enabled
- [x] Server/MCP startup wires the native graph index by default
- [x] Local graph inspect CLI for stats, neighbors, and related chunks
- [x] Optional SQLite metadata SQL adapter design
- [x] Planner-driven SQL metadata retrieval mode for scalar JSON filters
- [x] Optional PostgreSQL metadata adapter behind `akidb-server/postgres`
- [x] Kuzu adapter evaluation scaffold behind optional `akidb-graph/kuzu` feature
- [x] Kuzu native-vs-Kuzu decision artifact validator
- [x] One-Mac benchmark artifact validator
- [x] Four-Mac cell validation artifact validator
- [ ] One-Mac reference benchmark
- [ ] Kuzu binding and native-vs-Kuzu benchmark artifact
- [ ] Four-Mac Thunderbolt cell validation

## License

Apache License 2.0 - See LICENSE for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run `cargo fmt` and `cargo clippy`
4. Submit a pull request

## Support

- GitHub Issues: Bug reports and feature requests
- Documentation: See `/docs` directory
