# Platform Support

AkiDB v0.10.0 supports a CPU-portable runtime on macOS 26 Apple Silicon.
Ubuntu 24.04 or newer on AMD64 is an active qualification target. Passing CI
or producing an artifact does not yet make it a production support claim.

## Runtime matrix

| Operating system | Architecture | Rust target | Status | Notes |
| --- | --- | --- | --- | --- |
| macOS 26 | Apple Silicon ARM64 | `aarch64-apple-darwin` | Supported | M2 or newer. Primary local-development and private single-node path. |
| Ubuntu 24.04+ | AMD64 | `x86_64-unknown-linux-gnu` | Qualification preview | Native standalone, shard, coordinator, generation preview, and Ansible test path. |

Both active paths use the portable HNSW backend. Linux ARM64, CUDA, NVIDIA GPU,
Thor-specific acceleration, macOS Intel, Ubuntu older than 24.04, and other
Linux distributions are outside the active support matrix.

## Delivery and deployment matrix

| Path | macOS 26 ARM64 | Ubuntu 24.04+ AMD64 |
| --- | --- | --- |
| Source build and workspace tests | Supported | Qualification gate |
| GitHub release archive | Supported | Qualification artifact |
| Standalone server | Supported | Qualification preview |
| Shard and coordinator binaries | Evaluation | Evaluation |
| Immutable generation serving | Single-node preview | Single-node qualification preview |
| Docker images | Development use only | Qualification image architecture |
| Checksum-pinned Ansible cluster artifact | Not applicable | Qualification only |
| Four-Mac Thunderbolt evidence tooling | Experimental | Not applicable |

The AMD64 rows are evidence-producing paths, not a statement that replication,
automatic failover, or Linux production support is complete.

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
enabled. These commands are qualification steps until the AMD64 gates pass.

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

See [`deploy/ansible/README.md`](../../deploy/ansible/README.md).

## CI coverage

GitHub Actions exercises:

- workspace tests on macOS 26 ARM64;
- workspace tests on Ubuntu 24.04 AMD64;
- the Apple Silicon build script on macOS 26;
- release builds for macOS ARM64 and Ubuntu AMD64;
- the immutable Linux cluster artifact on Ubuntu 24.04 AMD64.

Platform-specific hardware, performance, and failure claims require their own
checked-in evidence; a successful compile is not a production-readiness claim.
