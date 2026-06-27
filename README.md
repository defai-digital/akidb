# AkiDB

A Mac-first vector search engine for private local RAG, Apple Silicon
appliance deployments, and four-Mac Thunderbolt cells.

## Features

- **One-Mac Appliance**: Production-capable single-node deployment on Apple Silicon
- **Four-Mac Cell Design**: Thunderbolt-connected shard and replica placement for local scale-out
- **Cell-Based Horizontal Scale**: Add four-Mac cells instead of growing an unbounded mesh
- **Portable Backend First**: CPU/portable backend for Mac M2 or later ARM64 systems
- **Sub-50ms Latency**: Optimized for real-time RAG applications
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

### Four-Mac Cell

The four-Mac Thunderbolt cell is the v2 distributed design target. See:

- [PRD](docs/product/PRD.md)
- [ADR](docs/adr/ADR-0001-mac-first-cell-architecture.md)
- [Technical Specification](docs/architecture/TECH_SPEC.md)

## Project Structure

```
akidb/
├── crates/
│   ├── common/          # Shared types, errors, config
│   ├── faiss-wrapper/   # Optional FAISS FFI bindings
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
- [ADR](docs/adr/ADR-0001-mac-first-cell-architecture.md) - Architecture Decision Records
- [PRD](docs/product/PRD.md) - Product Requirements
- [Technical Specification](docs/architecture/TECH_SPEC.md) - Mac appliance and Thunderbolt cell architecture
- [One-Mac Benchmark](docs/quality/one-mac-benchmark.md) - reproducible benchmark artifact workflow
- [Vector Quality Gates](docs/quality/vector-quality.md) - recall and semantic retrieval QA

## Development Status

**Current Phase:** v2 Mac-first design reset

- [x] Cargo workspace initialized
- [x] CI/CD pipeline configured
- [x] Security baseline (cargo-audit, deny.toml)
- [x] Canonical PRD/ADR/technical specification
- [ ] One-Mac reference benchmark
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
