# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AkiDB Thor Edition is a distributed vector search engine optimized for NVIDIA Jetson Thor edge clusters. It provides GPU-accelerated FAISS-based vector search with sub-50ms latency targets for real-time RAG applications.

## Build Commands

```bash
# Development build (Mac - CPU mode)
cargo build --features cpu

# Production build (Jetson Thor - GPU mode)
cargo build --release --features gpu

# Run tests
cargo test --features cpu

# Run a single test
cargo test --features cpu test_name

# Format and lint
cargo fmt
cargo clippy

# Run specific crate tests
cargo test -p akidb-storage --features cpu
cargo test -p akidb-faiss --features cpu
```

## Architecture

### Crate Dependency Graph

```
akidb-server (binary)
├── akidb-grpc (gRPC service layer)
│   ├── akidb-faiss (vector index abstraction)
│   │   └── akidb-common (types, errors)
│   └── akidb-storage (persistence layer)
│       └── akidb-common
└── akidb-common

akidb-coordinator (binary - stateless fan-out coordinator)
├── akidb-grpc
└── akidb-common
```

### Key Crates

- **akidb-common**: Shared types (`Vector`, `VectorId`, `InternalId`, `SearchResult`), error types (`AkiDbError`), config parsing
- **akidb-faiss**: Vector index trait (`VectorIndex`) with implementations for CPU, GPU, cuVS, and Mock. Handles tombstone bitsets and index rebuild
- **akidb-storage**: RocksDB backend, WAL, ID mapping (external ID ↔ internal ID), S3/MinIO snapshot storage
- **akidb-grpc**: gRPC service implementing `akidb.v1.Akidb` proto service. Proto definition at `crates/grpc-server/proto/akidb.proto`
- **akidb-coordinator**: Stateless query coordinator handling fan-out search, result merging (min-heap), shard routing, backpressure, read-your-writes consistency, and embedding service integration

### Feature Flags

The `akidb-faiss` and `akidb-server` crates use feature flags for index implementation:
- `cpu` (default): CPU-only FAISS implementation for development
- `gpu`: CUDA-enabled GPU FAISS for production on Jetson Thor

### Distributed Design

- **Shards**: Each Thor node runs one shard server (`akidb-server`)
- **Coordinator**: Stateless coordinator fans out queries to shards, merges results
- **No replication**: Cost-effective edge design relies on MinIO snapshots for durability
- **Partial results**: Coordinator returns partial results with coverage metrics when shards are unavailable

### Key Patterns

- **ID Mapping**: External string IDs are mapped to internal i64 IDs for FAISS. Storage layer maintains bidirectional mapping
- **Tombstone Deletes**: Vectors are tombstone-deleted (GPU bitset filtering), not removed from index. Periodic rebuilds compact tombstones
- **WAL + Snapshots**: Write-ahead log for durability, periodic snapshots to MinIO for recovery

## Configuration

Main config file: `config/default.toml`

Key configuration sections:
- `[index]`: FAISS index type (IVF4096,Flat), nprobe for search accuracy
- `[index.gpu]`: GPU device ID, memory fraction (0.6 default)
- `[storage]`: RocksDB and WAL paths
- `[storage.minio]`: MinIO endpoint and credentials for snapshots
- `[slo]`: P95 latency targets (50ms reference) and backpressure thresholds

## Proto API

gRPC service definition: `crates/grpc-server/proto/akidb.proto`

Key operations: Insert, Search, Delete, Update, Get, InsertBatch, SearchBatch, Health

Proto regeneration happens automatically via `tonic-build` in `crates/grpc-server/build.rs`.

## Scripts

- `scripts/thor-validate.sh`: Validate Jetson Thor hardware
- `scripts/faiss-benchmark.sh`: Run FAISS GPU benchmarks
- `scripts/minio-setup.sh`: Setup MinIO for snapshot storage
- `scripts/build-on-thor.sh`: Build on Jetson Thor with GPU support
