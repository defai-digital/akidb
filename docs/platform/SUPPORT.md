# Platform Support

AkiDB v0.10.0 supports the CPU-portable runtime on macOS 26 Apple Silicon and
Ubuntu 24.04 or newer on AMD64. Platform support refers to the native Rust
server, coordinator, CLI, TUI, and SDK interoperability. Container images and
deployment automation have the narrower qualification matrix below.

## Target use cases

| Audience | Best-fit deployment | Why this is the product center |
| --- | --- | --- |
| Single user / private workstation | **Mac Studio** or **AMD64 PC** standalone | Enough RAM, local NVMe, and quiet always-on capacity for private RAG, agent memory, and code intelligence on one machine |
| Enterprise / team | **Mac Studio cluster** (on-prem target) or **AMD64 cluster in cloud** | Full-replica knowledge cells and generation-aware gateways; the current checked-in cluster qualification is Ubuntu AMD64 |

These are the deployments AkiDB optimizes for in architecture, sizing guidance,
and release packaging. A product target is not automatically a qualified
capacity claim: Mac cluster qualification remains separate from the measured
Ubuntu AMD64 cell.

## Also supported

| Form factor | Role | Notes |
| --- | --- | --- |
| **Mac Mini** or **MacBook** standalone | Supported secondary single-node path | Same macOS Apple Silicon portable runtime as Mac Studio. Prefer Studio when corpus, concurrency, or always-on duty cycle grows. |

Secondary support means the portable binary path is intended to build and run
there. It does **not** mean every cluster playbook, release artifact, or
capacity claim is qualified on that form factor.

## Runtime matrix

| Operating system | Architecture | Rust target | Support tier | Notes |
| --- | --- | --- | --- | --- |
| macOS 26 | Apple Silicon ARM64 | `aarch64-apple-darwin` | **Primary** on Mac Studio; also supported on Mac Mini / MacBook | M2 or newer. Preferred private single-node and Mac cluster hardware is Mac Studio. |
| Ubuntu 24.04+ | AMD64 | `x86_64-unknown-linux-gnu` | **Primary** on workstation PCs and cloud VMs | Standalone, shard/coordinator, release archive, Docker, and qualified Ansible knowledge-cell path. |

Outside the support matrix:

- macOS Intel;
- Linux ARM64, including NVIDIA Thor;
- Ubuntu older than 24.04;
- other Linux distributions; and
- CUDA / NVIDIA GPU-accelerated vector-index paths.

An unsupported platform may happen to compile, but that does not establish a
release, CI, security-update, or capacity commitment. Both supported targets
use the portable HNSW (`usearch`) backend.

## Delivery and deployment matrix

| Path | macOS 26 Apple Silicon | Ubuntu 24.04+ AMD64 |
| --- | --- | --- |
| Source build and workspace tests | Supported | Supported |
| GitHub release archive | Supported | Supported |
| Standalone server | Supported | Supported |
| Shard and coordinator binaries | Supported | Supported |
| Immutable single-node generation serving | Preview | Supported |
| PostgreSQL-led full-replica knowledge cell | Mac Studio cluster is an intended enterprise path; not yet qualified | Supported for the documented three-replica, 100k × 768 envelope |
| Docker images | Development use only | Supported image architecture |
| Checksum-pinned Ansible artifact | Not applicable | Supported |
| Four-Mac Thunderbolt evidence tooling | Experimental; not a production-readiness claim | Not applicable |

The runtime being supported does not imply that every packaging, cluster, or
preview path is qualified on every form factor. Docker, immutable Ansible
artifacts, and the measured knowledge-cell envelope are AMD64-only. Mac Studio
remains the preferred Apple Silicon capacity host; Mac Mini and MacBook are
valid standalone hosts with smaller practical envelopes.

The knowledge cell provides full-copy retrieval replicas and read failover; it
does not provide PostgreSQL or MinIO HA. The `generation-postgres` build
feature enables the replica worker, but a feature-enabled binary by itself is
not an HA deployment. See the
[knowledge-serving architecture](../architecture/knowledge-serving.md).

Measured AMD64 envelope, failure drills, and control-plane limits:
[Ubuntu AMD64 qualification report](../quality/linux-amd64-knowledge-cell-qualification.md).

Market ANN / competitor parity remains a separate gate:
[market-readiness qualification](../quality/market-readiness-qualification.md).

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

**Hardware guidance**

- **Mac Studio:** preferred single-user and cluster node for sustained private
  RAG and multi-collection work.
- **Mac Mini:** supported standalone appliance for lighter always-on loads.
- **MacBook:** supported for development and personal standalone use; not the
  preferred always-on cluster node.

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
enabled. For enterprise cloud cells, prefer checksum-pinned release archives
and the Ansible knowledge-serving playbooks rather than compiling on each host.

## Cluster qualification

Enterprise designs center on:

1. **Mac Studio cluster** — private on-prem full replicas / capacity cells; and
2. **AMD64 cloud cluster** — the checked-in Ansible knowledge-serving cell and
   independent-shard lab on Ubuntu 24.04+ AMD64.

The coordinator can fan out requests to shards on supported native runtimes.
Independent-shard deployments may run **two or more active-active
coordinators** for entrypoint redundancy (VIP/LB + static peer membership).
That path does **not** provide shard data replication, automatic rebalancing,
or placement. A failed shard host still makes that partition unavailable.
The coordinator also does not forward bearer/workspace metadata to shards.

For agent-facing read HA with generation barriers, use the knowledge-serving
cell (full replicas + gateways), not independent-shard fan-out alone.

The checked-in AMD64 Ansible profile is a qualification environment:

- Ubuntu 24.04+ AMD64 hosts;
- checksum-pinned native artifacts;
- a WireGuard-only AkiDB service network;
- rolling health gates and rollback;
- no public AkiDB bind;
- `auth.mode=disabled` only inside the explicitly trusted overlay.

See [`deploy/ansible/README.md`](../../deploy/ansible/README.md). Linux ARM64
and Thor cluster automation and release artifacts are intentionally not
published.

The four-shard coordinator profile is separate from the knowledge-serving
replica design. The latter starts with full logical copies on two or three
nodes and a generation-aware AX gateway; it does not relabel independent
hash-routed shards as replicas.

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
production-readiness claim. Lighter supported Mac form factors need their own
evidence for capacity claims.
