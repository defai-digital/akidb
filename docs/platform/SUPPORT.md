# Platform Support

AkiDB v0.10.0 supports a CPU-portable runtime on macOS 26 and Ubuntu 24.04 or
newer. Platform support refers to the native Rust server, coordinator, CLI,
TUI, and SDK interoperability. Container images and deployment automation have
a narrower qualification matrix, documented separately below.

## Runtime matrix

| Operating system | Architecture | Rust target | Status | Notes |
| --- | --- | --- | --- | --- |
| macOS 26 | Apple Silicon ARM64 | `aarch64-apple-darwin` | Supported | M2 or newer. Primary local-development and private single-node path. |
| Ubuntu 24.04+ | AMD64 | `x86_64-unknown-linux-gnu` | Supported | Native standalone, shard, coordinator, release archive, and qualified Ansible artifact path. |

Both targets use the portable HNSW backend. Linux ARM64, CUDA, NVIDIA GPU,
Thor-specific acceleration, macOS Intel, Ubuntu older than 24.04, and other
Linux distributions are outside the release support matrix.

## Delivery and deployment matrix

| Path | macOS 26 ARM64 | Ubuntu 24.04+ AMD64 |
| --- | --- | --- |
| Source build and workspace tests | Supported | Supported |
| GitHub release archive | Supported | Supported |
| Standalone server | Supported | Supported |
| Shard and coordinator binaries | Supported | Supported |
| Immutable single-node generation serving | Preview | Supported |
| PostgreSQL-led full-replica knowledge cell | Not currently qualified | Supported for the documented three-replica, 100k × 768 envelope (market ANN/competitor parity remains a separate gate) |
| Docker images | Development use only | Supported image architecture |
| Checksum-pinned Ansible artifact | Not applicable | Supported |
| Four-Mac Thunderbolt evidence tooling | Experimental | Not applicable |

The runtime being supported does not imply that every packaging, cluster, or
preview path is supported on the same architecture. The Docker, immutable
Ansible artifact, and generation-serving qualification pipelines are
AMD64-only. The knowledge cell provides full-copy retrieval replicas and read
failover; it does not provide PostgreSQL or MinIO HA.

The `generation-postgres` build feature enables the replica worker, but a
feature-enabled binary by itself is not an HA deployment. The supported cell
also requires independent storage, authoritative activation policy,
generation-aware routing, blank-node rebuild evidence, and the checked-in
failure/recovery workflow. See the
[knowledge-serving architecture](../architecture/knowledge-serving.md).
The measured envelope, failure drills, and important control-plane/object-store
limitations are recorded in the
[Ubuntu AMD64 qualification report](../quality/linux-amd64-knowledge-cell-qualification.md).
Public-dataset ANN matrices, Milvus/Weaviate parity, and broader market soak
gates are tracked separately in
[market-readiness qualification](../quality/market-readiness-qualification.md)
and are not implied by the 100k × 768 cell pass.

## macOS 26 on Apple Silicon

Install Rust with `rustup`, then install the native prerequisites:

```bash
brew install cmake protobuf
./scripts/build-on-mac-arm64.sh
```

The build script verifies `Darwin/arm64`, checks and tests the workspace, and
builds the unified `akidb` CLI. Optional local text embeddings can use
`scripts/ax_engine_embedding_server.py` with native `ax-engine` model
artifacts.

## Ubuntu 24.04+ on AMD64

Install the native build dependencies:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential clang cmake libclang-dev libssl-dev \
  pkg-config protobuf-compiler
```

Then build and test:

```bash
cargo check --workspace --no-default-features
cargo test --workspace
cargo build --release -p akidb-cli
```

Use an OpenAI-compatible local embedding endpoint when `TextSearch` is
enabled.

## Cluster qualification

The coordinator can fan out requests to shards on any supported native
runtime. It does not yet provide replication, failover, placement, or
rebalancing, and it does not forward bearer/workspace metadata to shards.

The checked-in Ansible profile is therefore a qualification environment:

- Ubuntu 24.04+ AMD64 hosts;
- checksum-pinned native artifacts;
- a WireGuard-only AkiDB service network;
- rolling health gates and rollback;
- no public AkiDB bind;
- `auth.mode=disabled` only inside the explicitly trusted overlay.

See [`deploy/ansible/README.md`](../../deploy/ansible/README.md). Linux ARM64
cluster automation and release artifacts are intentionally not published.

This four-shard profile is separate from the knowledge-serving replica design.
The latter starts with full logical copies on two or three nodes and a
generation-aware AX gateway; it does not relabel independent hash-routed
shards as replicas.

## CI coverage

GitHub Actions exercises:

- workspace tests on macOS 26 ARM64;
- workspace tests on Ubuntu 24.04 AMD64;
- the Apple Silicon build script on macOS 26;
- release builds for macOS Apple Silicon and Linux AMD64;
- the immutable Linux cluster artifact on Ubuntu 24.04 AMD64;
- the real-MinIO immutable generation-serving gate on Ubuntu 24.04 AMD64;
- Ansible syntax checks for knowledge-cell and market-qualification playbooks
  (no live SIFT1M, competitor, or soak execution).

Platform-specific hardware, performance, and failure claims require their own
checked-in evidence; a successful compile or playbook syntax check is not a
production-readiness claim.
