# Platform Support

AkiDB supports two first-class runtime targets:

| Platform | Target triple | Backend | Feature flags | Notes |
| --- | --- | --- | --- | --- |
| NVIDIA Jetson Thor | `aarch64-unknown-linux-gnu` | CUDA/FAISS GPU | `--no-default-features --features gpu` | Production target. Requires CUDA, FAISS GPU, and NVIDIA runtime libraries. |
| Mac M2 or later | `aarch64-apple-darwin` | CPU/portable | `--features cpu` or `--features portable` | Development and local validation target. CUDA GPU mode is not supported on macOS. |

## NVIDIA Thor

Use Thor for production GPU-accelerated shards:

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

## Mac M2 Or Later

Use Apple Silicon Macs for portable CPU-mode development:

```bash
./scripts/build-on-mac-arm64.sh
```

This script verifies `Darwin/arm64`, checks the Rust workspace with CPU
features, runs tests, and builds `akidb-server`.

Do not enable the `gpu` feature on macOS. Apple Silicon does not provide NVIDIA
CUDA, and the build script fails early with an explicit error if CUDA mode is
requested for macOS.

## CI Coverage

CI separates portable and GPU paths:

- Ubuntu and macOS runners run CPU/portable checks and tests.
- A macOS ARM64 job runs the Apple Silicon build script.
- The GPU job runs only on self-hosted `jetson-thor` runners.
