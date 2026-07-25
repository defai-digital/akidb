# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AkiDB is a **Mac-first** vector search engine for private, local RAG. It targets
single-node Apple Silicon appliances and four-Mac Thunderbolt cells (the v2
distributed design). It uses the `usearch` HNSW index for CPU/portable vector
search with sub-50ms latency targets.

> **Important:** Thor, CUDA, NVIDIA GPU, and Linux ARM paths are **unsupported and
> deprecated**. Do not reintroduce them in code, CI, or active docs. (Some legacy
> references may still exist in history; ignore them.) The portable Apple Silicon
> path is the only supported target.

## Build & Test Commands

```bash
# Fast portable compile check (preferred first step)
cargo check --workspace

# Build
cargo build                              # debug, all crates
cargo build --release -p akidb-server    # release server binary

# Validate the full Apple Silicon dev path
./scripts/build-on-mac-arm64.sh

# Tests
cargo test --workspace                   # unit, integration, and doc tests
cargo test -p akidb-storage              # single crate
cargo test -p akidb-faiss test_name      # single test by name

# Format & lint (run fmt before focused changes; avoid broad format-only churn)
cargo fmt
cargo clippy --workspace --all-targets
```

There are **no `cpu`/`gpu` feature flags** anymore — crates build with default
(empty) features. The index backend is `usearch`, not raw FAISS, despite the
`faiss-wrapper` crate name (package `akidb-faiss`).

### Running the system (single `akidb` CLI entry point)

`akidb-cli` is the single entry binary; it dispatches to the server, coordinator,
and TUI:

```bash
cargo run -p akidb-cli -- server --standalone --config config/default.toml
cargo run -p akidb-cli -- coordinator --shards 127.0.0.1:50051
cargo run -p akidb-cli -- tui --coordinator 127.0.0.1:50050
```

### Python sidecar services

Python services live in `services/doc-parser` and `services/upload-gateway`. From
a service directory:

```bash
pip install -e ".[dev]" && pytest tests/ -v
ruff check .
```

The local embedding wrapper is `scripts/ax_engine_embedding_server.py`. Point it
at native artifacts via `AX_ENGINE_MODEL_DIR` (must contain `model-manifest.json`).
Do **not** wire AkiDB to `ax-engine serve <embedding-alias>`.

## Architecture

### Workspace crates (package name → purpose)

```
akidb-cli            Single `akidb` entry point; dispatches server/coordinator/tui
akidb-server         Shard server binary (run() takes Args)
akidb-coordinator    Stateless fan-out query coordinator (run_server() takes ServerArgs)
akidb-tui            Terminal operations UI
akidb-grpc           gRPC service layer (server impls, MCP bridge, admin, ingestion, webhook)
akidb-proto          Generated protobuf and gRPC bindings (proto in crates/proto/proto/)
akidb-embedding      Embedding service abstraction with caching, fallback, and ax-engine client
akidb-faiss          Vector index abstraction (usearch HNSW; crate dir is faiss-wrapper)
akidb-storage        RocksDB backend, WAL, ID mapping, S3/MinIO snapshots
akidb-common         Shared types (Vector, VectorId, InternalId, SearchResult), AkiDbError, config
akidb-contracts      Boundary validation: validation contracts + type-safe newtypes at gRPC/WAL/storage edges
akidb-invariants     debug_invariant! / critical_invariant! macros (debug-only vs always-on assertions)
akidb-ingestion      Document ingestion orchestrator (ingestion-orchestrator dir)
akidb-benchmark      Benchmark harness
```

### Dependency direction

`akidb-cli` sits on top of `akidb-server`, `akidb-coordinator`, and `akidb-tui`.
`akidb-server` pulls in `akidb-grpc`, `akidb-proto`, `akidb-embedding`,
`akidb-faiss`, `akidb-storage`, and `akidb-coordinator`. `akidb-faiss` and
`akidb-storage` depend on `akidb-common`; `akidb-faiss` and the boundary layers
also use `akidb-contracts` / `akidb-invariants`.

### Distributed design

- **Shards**: each Mac node runs one shard server (`akidb-server`).
- **Coordinator**: stateless; fans queries out to shards, merges results with a
  min-heap, handles shard routing, backpressure, and read-your-writes consistency.
- **Four-Mac cell**: a deferred (P2) distributed target using Thunderbolt-connected
  shards + replicas + MinIO snapshots. Detailed product/architecture docs (PRD,
  ADRs, tech spec) are internal under `ax-internal/` and not part of the public repo.
- **Partial results**: coordinator returns partial results with coverage metrics
  when shards are unavailable.

### Key patterns

- **ID mapping**: external string IDs map to internal i64 IDs for the index; the
  storage layer keeps the bidirectional mapping.
- **Tombstone deletes**: vectors are tombstone-deleted (bitset filtering), not
  removed from the index. Periodic rebuilds compact tombstones.
- **WAL + snapshots**: write-ahead log for durability; periodic MinIO snapshots
  for recovery.
- **Contracts at boundaries, invariants inside**: validate untrusted data with
  `akidb-contracts` at gRPC/WAL/storage edges; assert internal assumptions with
  `akidb-invariants` macros (zero-cost in release for `debug_invariant!`).

## Proto API

Service definition: `crates/proto/proto/akidb.proto` (package `akidb.v1`).
Regenerated automatically via `tonic-build` in `crates/proto/build.rs`.

Three services:
- **Akidb**: Insert, Search, Delete, Update, Get, Health, InsertBatch, SearchBatch,
  GetClusterState, TextSearch
- **IngestionService**: TriggerSync, GetSyncStatus, UpdateTags, ReindexCategory,
  DeleteCategory, ListCategories
- **AdminService**: background task status/history, TriggerSnapshot, TriggerRebuild,
  CancelTask, resource status, webhook config

## Configuration

Main config: `config/default.toml`. Sections: `[server]`, `[index]`,
`[index.rebuild]`, `[index.tombstone]`, `[storage]`, `[storage.minio]`,
`[observability]`, `[slo]` (with `[slo.reference]` 50ms P95 target and
`[slo.backpressure]`), `[embedding]`.

## Conventions

- Rust 2021: 4-space indent, `snake_case` functions/modules, `PascalCase` types,
  explicit `Result` error handling.
- Place unit tests near the module under `#[cfg(test)]`; integration tests in
  `crates/*/tests/`, benchmarks in `benches/`. Name tests by behavior
  (`test_wal_recovery`, `test_parser_routing`).
- Commits: short imperative subjects; keep scoped; don't mix formatting with
  behavior changes. PRs: summary, commands run, linked issues, screenshots only
  for TUI/UI changes.
- Don't commit secrets, `.env`, local data, or `deploy/compose/secrets/`.

## Scripts

- `scripts/build-on-mac-arm64.sh`: validate the Apple Silicon build path
- `scripts/benchmark-one-mac.sh`: single-Mac benchmark
- `scripts/validate-standalone.sh`: validate a standalone server
- `scripts/minio-setup.sh`: set up MinIO for snapshot storage
- `scripts/qa_all.sh`, `scripts/qa_*.py`: QA / retrieval-quality checks
- `scripts/ax_engine_embedding_server.py`: local embedding server wrapper
