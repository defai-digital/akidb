# CUDA Compatibility Matrix for AkiDB Thor

**Document Version:** 1.0
**Last Updated:** 2025-01-20
**Status:** Phase 0 Validation Required

---

## Target Platform: NVIDIA Jetson Thor

| Component | Expected Version | Notes |
|-----------|-----------------|-------|
| **JetPack** | 7.x | Thor-specific release |
| **L4T** | 36.x+ | Linux for Tegra |
| **CUDA** | 12.2+ | Required for FAISS GPU |
| **cuDNN** | 8.9+ | Deep learning acceleration |
| **TensorRT** | 8.6+ | Inference optimization |

---

## FAISS Compatibility

### FAISS Version Requirements

| FAISS Version | CUDA Requirement | Notes |
|---------------|------------------|-------|
| 1.7.4 | CUDA 11.x | Last version with CUDA 11 |
| **1.8.0+** | **CUDA 12.x** | **Recommended for Thor** |
| 1.9.0 (latest) | CUDA 12.x | Latest features |

### FAISS-rs (Rust Bindings)

| faiss-rs Version | FAISS Version | Status |
|------------------|---------------|--------|
| 0.2.x | 1.7.x | Stable, CUDA 11 |
| 0.3.x | 1.8.x | **Target for Thor** |

**Action Required:**
- [ ] Verify faiss-rs 0.3.x builds on ARM64
- [ ] Test GPU IVF-Flat on Thor
- [ ] Benchmark performance vs x86_64

---

## TensorRT Compatibility

### TensorRT-LLM Requirements

| Component | Version | Notes |
|-----------|---------|-------|
| TensorRT-LLM | 0.8+ | For Qwen3-Embedding |
| TensorRT | 8.6+ | Backend |
| CUDA | 12.x | Required |

### Alternative: vLLM

| Component | Version | Notes |
|-----------|---------|-------|
| vLLM | 0.3+ | Fallback option |
| CUDA | 11.8+ or 12.x | More flexible |

**Decision Point:** TensorRT-LLM offers better latency (5-10ms) but requires more build effort. vLLM is easier but slower (20-30ms).

---

## Validation Checklist

### Pre-Phase 1 (Week 0)

```bash
# 1. Verify CUDA version
nvidia-smi
nvcc --version

# 2. Check JetPack version
cat /etc/nv_tegra_release

# 3. Test FAISS GPU
python3 -c "import faiss; print(f'FAISS GPUs: {faiss.get_num_gpus()}')"

# 4. Check TensorRT
python3 -c "import tensorrt; print(f'TensorRT: {tensorrt.__version__}')"
```

### Compatibility Test Matrix

| Test | Command | Expected Result |
|------|---------|-----------------|
| CUDA available | `nvidia-smi` | Shows Thor GPU |
| FAISS GPU | `python3 -c "import faiss; assert faiss.get_num_gpus() > 0"` | No error |
| IVF-Flat build | See benchmark script | Completes |
| IVF-Flat search | See benchmark script | < 10ms P95 |

---

## Known Issues and Workarounds

### Issue 1: FAISS ARM64 Build

**Problem:** FAISS official wheels may not support ARM64 with GPU.

**Workaround:** Build from source:
```bash
git clone https://github.com/facebookresearch/faiss.git
cd faiss
cmake -B build -DFAISS_ENABLE_GPU=ON -DFAISS_ENABLE_PYTHON=ON
cmake --build build -j$(nproc)
```

### Issue 2: CUDA 12 + Old Libraries

**Problem:** Some Python libraries still require CUDA 11.

**Workaround:** Use NVIDIA's container images or conda environments with proper CUDA version.

### Issue 3: Unified Memory on Thor

**Problem:** Thor uses unified memory (CPU+GPU shared).

**Consideration:** Memory budgets in AkiDB config must account for this. Set `gpu_memory_fraction: 0.6` to leave headroom.

---

## Recommended Software Stack

```yaml
# Thor Software Stack for AkiDB

system:
  jetpack: "7.1+"
  l4t: "36.3+"
  kernel: "5.15+"

nvidia:
  cuda: "12.2"
  cudnn: "8.9.5"
  tensorrt: "8.6.1"

python:
  version: "3.10"
  faiss-gpu: "1.8.0"  # Build from source
  tensorrt-llm: "0.8+"  # Optional

rust:
  version: "1.75+"
  faiss-rs: "0.3+"  # Verify ARM64 support
```

---

## Version Pinning Strategy

For reproducible builds, pin these versions in your environment:

```bash
# environment.yml or requirements.txt
cuda==12.2
cudnn==8.9.5.29
tensorrt==8.6.1
faiss-gpu==1.8.0  # Built from source
```

```toml
# Cargo.toml
[dependencies]
# Pin to specific faiss-rs version after validation
# faiss = { version = "=0.3.0", features = ["gpu"] }
```

---

## Upgrade Path

When upgrading CUDA or FAISS:

1. **Test in isolation first** - Benchmark on single node
2. **Shadow deployment** - Run new version alongside old
3. **Validate recall** - Ensure search quality unchanged
4. **Check memory** - New versions may have different memory profiles
5. **Performance regression test** - Compare P95 latencies

---

## References

- [FAISS GPU Guide](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)
- [JetPack Documentation](https://developer.nvidia.com/embedded/jetpack)
- [TensorRT-LLM](https://github.com/NVIDIA/TensorRT-LLM)
- [faiss-rs](https://github.com/Enet4/faiss-rs)

---

*This document should be updated after Phase 0 hardware validation.*
