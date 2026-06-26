# Platform Support

AkiDB v2 is Mac-first. The primary runtime target is Apple Silicon macOS,
starting with a one-Mac appliance and extending to a four-Mac Thunderbolt cell.
NVIDIA Thor remains an optional accelerator and compatibility target.

| Platform | Target triple | Backend | Feature flags | Notes |
| --- | --- | --- | --- | --- |
| Mac M2 or later | `aarch64-apple-darwin` | CPU/portable | `--features cpu` or `--features portable` | Primary one-Mac appliance and development target. CUDA GPU mode is not supported on macOS. |
| Four-Mac Apple Silicon cell | `aarch64-apple-darwin` | CPU/portable cell | `--features cpu` or `--features portable` | Distributed design target. Requires validated Thunderbolt networking and homogeneous hot-cell hardware for production. |
| NVIDIA Jetson Thor | `aarch64-unknown-linux-gnu` | CUDA/FAISS GPU | `--no-default-features --features gpu` | Optional accelerator target. Requires CUDA, FAISS GPU, and NVIDIA runtime libraries. |

## Mac M2 Or Later

Use Apple Silicon Macs for the primary local runtime:

```bash
./scripts/build-on-mac-arm64.sh
```

This script verifies `Darwin/arm64`, checks the Rust workspace with CPU
features, runs tests, and builds `akidb-server`.

Do not enable the `gpu` feature on macOS. Apple Silicon does not provide NVIDIA
CUDA, and the build script fails early with an explicit error if CUDA mode is
requested for macOS.

## Four-Mac Thunderbolt Cell

The production distributed shape is a four-Mac cell:

- Same reference SKU inside a hot production cell.
- Thunderbolt networking validated before benchmark claims.
- Replication factor 2 recommended for hot collections.
- Horizontal scale by adding another four-Mac cell, not by adding a fifth node
  to an existing cell.

See:

- [PRD](../product/PRD.md)
- [ADR](../adr/ADR-0001-mac-first-cell-architecture.md)
- [Technical Specification](../architecture/TECH_SPEC.md)

## NVIDIA Thor

Use Thor only when intentionally validating the optional NVIDIA accelerator path:

```bash
./scripts/thor-validate.sh
./scripts/install-faiss-gpu.sh
./scripts/build-on-thor.sh
```

The `gpu` feature is intentionally Linux-only by default. It links the C++
FAISS wrapper against CUDA and FAISS libraries discovered through:

- `CUDA_HOME` or `CUDA_PATH`
- `FAISS_PATH`
- `/usr/local/cuda`
- `/opt/faiss`

Set `AKIDB_ALLOW_CUDA_ON_UNSUPPORTED_TARGET=1` only when intentionally testing a
non-Thor CUDA environment.

## CI Coverage

CI separates portable and GPU paths:

- Ubuntu and macOS runners run CPU/portable checks and tests.
- A macOS ARM64 job runs the Apple Silicon build script.
- Optional GPU jobs run only on self-hosted `jetson-thor` runners.
