# Platform Support

AkiDB v0.10.0 supports a CPU-portable runtime on macOS 26 and Ubuntu 24.04 or
newer. Platform support refers to the native Rust server, coordinator, CLI,
TUI, and SDK interoperability. Container images and deployment automation have
a narrower qualification matrix, documented separately below.

## Runtime matrix

| Operating system | Architecture | Rust target | Status | Notes |
| --- | --- | --- | --- | --- |
| macOS 26 | Apple Silicon ARM64 | `aarch64-apple-darwin` | Supported | M2 or newer. Primary local-development and private single-node path. |
| Ubuntu 24.04+ | AMD64 | `x86_64-unknown-linux-gnu` | Supported | Native standalone, shard, coordinator, and qualified Ansible cluster artifact path. |
| Ubuntu 24.04+ | ARM64 | `aarch64-unknown-linux-gnu` | Supported | Native standalone, shard, and coordinator. Build from source or use the matching release archive. |

All three targets use the portable usearch HNSW backend. CUDA, NVIDIA GPU,
Thor-specific acceleration, macOS Intel, Ubuntu older than 24.04, and other
Linux distributions are outside the tested support matrix.

## Delivery and deployment matrix

| Path | macOS 26 ARM64 | Ubuntu 24.04+ AMD64 | Ubuntu 24.04+ ARM64 |
| --- | --- | --- | --- |
| Source build and workspace tests | Supported | Supported | Supported |
| GitHub release archive | Supported | Supported | Supported |
| Standalone server | Supported | Supported | Supported |
| Shard and coordinator binaries | Supported | Supported | Supported |
| Docker images | Development use only | Supported image architecture | Not currently published |
| Checksum-pinned Ansible cluster artifact | Not applicable | Qualified | Not yet qualified |
| Four-Mac Thunderbolt evidence tooling | Experimental | Not applicable | Not applicable |

The runtime being supported does not imply that every packaging or cluster
automation path is supported on the same architecture. In particular, the
current Docker and immutable Ansible artifact pipelines are AMD64-only.

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

## Ubuntu 24.04+ on AMD64 or ARM64

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

The same commands apply to `x86_64` and `aarch64` hosts. Use an
OpenAI-compatible local embedding endpoint when `TextSearch` is enabled.

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

See [`deploy/ansible/README.md`](../../deploy/ansible/README.md). ARM64 cluster
automation remains unqualified even though the ARM64 server, shard, and
coordinator runtime is supported.

## CI coverage

GitHub Actions exercises:

- workspace tests on macOS 26 ARM64;
- workspace tests on Ubuntu 24.04 AMD64 and ARM64;
- the Apple Silicon build script on macOS 26;
- release builds for all three native target triples;
- the immutable Linux cluster artifact on Ubuntu 24.04 AMD64.

Platform-specific hardware, performance, and failure claims require their own
checked-in evidence; a successful compile is not a production-readiness claim.
