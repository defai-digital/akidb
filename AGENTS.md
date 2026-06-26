# Repository Guidelines

## Project Structure & Module Organization

AkiDB is a Mac-only Rust workspace with Python sidecar services. Core Rust crates
live in `crates/`: `common` for shared types/config, `contracts` for validation,
`faiss-wrapper` for the portable vector index abstraction, `storage` for
RocksDB/WAL/snapshots, `grpc-server` for protobuf/gRPC APIs, `coordinator` for
fan-out routing, `server` for the shard binary, `tui` for terminal operations,
and `ingestion-orchestrator` for document ingestion. Python services live in
`services/doc-parser` and `services/upload-gateway`. Configuration is in
`config/`, scripts in `scripts/`, deployment files in `deploy/`, and canonical
docs in `docs/`.

## Build, Test, and Development Commands

- `./scripts/build-on-mac-arm64.sh`: validate the Apple Silicon development path.
- `cargo check --workspace --features cpu`: fast portable compile check.
- `cargo test --workspace --features cpu`: run Rust unit, integration, and doc tests.
- `cargo build --release -p akidb-server --features cpu`: build the Mac server.
- `pip install -e ".[dev]" && pytest tests/ -v`: run Python service tests from a service directory.
- `ruff check .`: lint a Python sidecar service.

## Coding Style & Naming Conventions

Use Rust 2021 conventions: four-space indentation, `snake_case`
modules/functions, `PascalCase` types, and explicit `Result` error handling. Run
`cargo fmt` before focused Rust changes, but avoid broad formatting-only churn.
Use `cargo clippy --workspace --all-targets --features cpu` for lint feedback.
Python code follows standard `pytest` conventions.

## Testing Guidelines

Place Rust unit tests near the module under `#[cfg(test)]`; use
`crates/*/tests/` for integration tests and `benches/` for benchmarks. Test names
should describe behavior, for example `test_wal_recovery` or
`test_parser_routing`. All supported tests should pass on macOS Apple Silicon in
CPU/portable mode.

## Commit & Pull Request Guidelines

Use short imperative commit subjects, such as `Remove Thor support` or `Update
Mac build docs`. Keep commits scoped and avoid mixing formatting with behavior
changes. Pull requests should include a concise summary, commands run, linked
issues, and screenshots only for TUI/UI-visible changes.

## Security & Configuration Tips

Do not commit secrets, `.env` files, local data, or `deploy/compose/secrets/`.
Use CPU features on Mac M2 or later. Thor, CUDA, NVIDIA GPU, and Linux ARM paths
are unsupported and should not be reintroduced in active docs or CI.
