# Platform Support

AkiDB v2 is Mac-first. The primary runtime target is Apple Silicon macOS,
starting with a one-Mac appliance and extending to a four-Mac Thunderbolt cell.
Thor, CUDA, NVIDIA GPU, and Linux ARM deployment paths are not supported.

| Platform | Target triple | Backend | Build path | Notes |
| --- | --- | --- | --- | --- |
| Mac M2 or later | `aarch64-apple-darwin` | CPU/portable | default Cargo features | Primary one-Mac appliance and development target. CUDA GPU mode is not supported on macOS. |
| Four-Mac Apple Silicon cell | `aarch64-apple-darwin` | CPU/portable cell | default Cargo features | Distributed design target. Requires validated Thunderbolt networking and homogeneous hot-cell hardware for production. |

## Mac M2 Or Later

Use Apple Silicon Macs for the primary local runtime:

```bash
./scripts/build-on-mac-arm64.sh
```

This script verifies `Darwin/arm64`, checks the Rust workspace, runs tests, and
builds the `akidb` single entry point.

Do not enable GPU/CUDA feature paths. Apple Silicon support is CPU/portable only.
Optional text embeddings use local `ax-engine` through
`scripts/ax_engine_embedding_server.py` with a local Qwen embedding model
native artifact directory containing `model-manifest.json`. Do not use
`ax-engine serve <embedding-alias>` for AkiDB embeddings.

## Four-Mac Thunderbolt Cell

The production distributed shape is a four-Mac cell:

- Same reference SKU inside a hot production cell.
- Thunderbolt networking validated before benchmark claims.
- Replication factor 2 recommended for hot collections.
- Horizontal scale by adding another four-Mac cell, not by adding a fifth node
  to an existing cell.

Product requirements, architecture decisions, and the technical specification are
maintained as internal documents (`ax-internal/`) and are not part of this public
repository.

## CI Coverage

CI validates only the Mac-supported portable path:

- Ubuntu and macOS runners run CPU/portable checks and tests.
- A macOS ARM64 job runs the Apple Silicon build script.
