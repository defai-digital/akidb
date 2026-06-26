# AkiDB

A distributed vector search engine for NVIDIA Jetson Thor edge clusters and
macOS Apple Silicon development hosts.

## Features

- **NVIDIA Thor GPU Search**: FAISS GPU IVF-Flat with optional cuVS shadow-mode scaffolding
- **Apple Silicon Development**: CPU/portable backend for Mac M2 or later ARM64 systems
- **Distributed Architecture**: Shard-based design with fan-out search
- **No-Replication Design**: Cost-effective edge deployment with MinIO snapshots
- **Sub-50ms Latency**: Optimized for real-time RAG applications
- **Rust Performance**: Memory-safe, async-first implementation

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Coordinator                              │
│                    (Stateless, Fan-out)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌─────────┐     ┌─────────┐     ┌─────────┐
        │ Shard 0 │     │ Shard 1 │     │ Shard 2 │
        │  (Thor) │     │  (Thor) │     │  (Thor) │
        └─────────┘     └─────────┘     └─────────┘
              │               │               │
              └───────────────┴───────────────┘
                              │
                    ┌─────────────────┐
                    │   MinIO Cluster  │
                    │   (Snapshots)    │
                    └─────────────────┘
```

## Quick Start

### Development (Mac M2 or later)

```bash
# Clone the repository
git clone https://github.com/defai-digital/akidb.git
cd akidb

# Build and validate the portable Apple Silicon path
./scripts/build-on-mac-arm64.sh

# Or run Cargo directly
cargo build --features cpu
cargo test --features cpu

# Format and lint
cargo fmt
cargo clippy
```

### Production (Jetson Thor)

```bash
# Validate hardware
./scripts/thor-validate.sh

# Run FAISS benchmark
./scripts/faiss-benchmark.sh

# Setup MinIO
sudo ./scripts/minio-setup.sh

# Build with GPU support
./scripts/build-on-thor.sh

# Run server
./target/release/akidb-server --config config/default.toml
```

## Project Structure

```
akidb-thor/
├── crates/
│   ├── common/          # Shared types, errors, config
│   ├── faiss-wrapper/   # FAISS FFI bindings
│   ├── storage/         # RocksDB, WAL, ID mapping
│   ├── grpc-server/     # gRPC API service
│   └── coordinator/     # Fan-out search coordination
├── config/              # Configuration files
├── deploy/              # Deployment manifests
├── docs/                # Documentation
├── scripts/             # Utility scripts
└── proto/               # Protobuf definitions
```

## Configuration

See `config/default.toml` for all configuration options.

Key settings:
- `index.gpu.memory_fraction`: GPU memory budget (default: 0.6)
- `index.nprobe`: Search accuracy vs speed (default: 32)
- `slo.reference.*`: SLO reference configuration

## Performance Targets

| Metric | Target | Reference Config |
|--------|--------|------------------|
| E2E Search P95 | < 50ms | D=768, N=1M, topK=10 |
| FAISS Search P95 | < 10ms | nprobe=32 |
| Recall@10 | > 95% | Reference config |
| Throughput | 100+ QPS | Per coordinator |

## Documentation

- [Platform Support](docs/PLATFORM_SUPPORT.md) - NVIDIA Thor and Mac ARM64 build matrix
- [ADR v1.1](automatosx/prd/AKIDB_ADR_v1.1.md) - Architecture Decision Records
- [PRD v1.1](automatosx/prd/AKIDB_PRD_v1.1.md) - Product Requirements
- [Implementation Plan](automatosx/prd/AKIDB_IMPLEMENTATION_PLAN_v1.0.md) - Development roadmap
- [CUDA Compatibility](docs/CUDA_COMPATIBILITY.md) - Hardware/software matrix

## Development Status

**Current Phase:** Phase 0 - Validation Sprint

- [x] Cargo workspace initialized
- [x] CI/CD pipeline configured
- [x] Security baseline (cargo-audit, deny.toml)
- [x] Thor validation scripts
- [ ] Hardware validation (pending Thor acquisition)
- [ ] FAISS GPU benchmark on Thor

## License

MIT License - See LICENSE for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run `cargo fmt` and `cargo clippy`
4. Submit a pull request

## Support

- GitHub Issues: Bug reports and feature requests
- Documentation: See `/docs` directory
