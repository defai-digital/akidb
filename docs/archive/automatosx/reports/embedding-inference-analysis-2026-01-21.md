# Embedding & Inference Server Analysis for AkiDB Thor Cluster

**Date**: 2026-01-21
**Hardware**: NVIDIA Jetson AGX Thor (2x nodes)
**JetPack**: 7.0 | **CUDA**: 13.0 | **Compute Capability**: 11.0

---

## Executive Summary

**TensorRT-Edge-LLM is NOT recommended** for AkiDB's embedding use case. It is designed for Large Language Model (LLM) text generation, not embedding inference.

**Recommended Solution**: **Hugging Face Text Embeddings Inference (TEI)** or **Infinity** for embedding model serving, paired with **Triton Inference Server** for production deployment flexibility.

---

## Hardware Capabilities

| Spec | Thor-01 | Thor-02 |
|------|---------|---------|
| Device | Jetson AGX Thor | Jetson AGX Thor |
| CUDA Compute | 11.0 | 11.0 |
| CUDA Version | 13.0 | 13.0 |
| JetPack | 7.0 | 7.0 |
| Memory | 122 GB | 122 GB |

The Thor cluster **exceeds requirements** for all embedding inference solutions (minimum CUDA 7.5 compute capability).

---

## Solution Comparison

### Option 1: TensorRT-Edge-LLM ❌ NOT RECOMMENDED

**Repository**: https://github.com/NVIDIA/TensorRT-Edge-LLM

| Aspect | Details |
|--------|---------|
| Purpose | LLM/VLM text generation inference |
| Supported Models | Llama 3.x, Qwen, DeepSeek (generative models) |
| Embedding Support | ❌ No dedicated embedding model support |
| Hardware Match | ✅ Designed for Jetson Thor |

**Why Not Suitable**:
- Designed for autoregressive text generation, not embedding extraction
- No support for sentence-transformers or embedding models
- Overkill complexity for embedding-only use case

---

### Option 2: Hugging Face Text Embeddings Inference (TEI) ✅ RECOMMENDED

**Repository**: https://github.com/huggingface/text-embeddings-inference

| Aspect | Details |
|--------|---------|
| Purpose | High-performance text embedding inference |
| Supported Models | BERT, E5, GTE, BGE, Nomic, Jina, Mistral-embed |
| Hardware Support | ✅ CUDA 12.2+ (Thor has CUDA 13.0) |
| Compute Requirement | ✅ >= 7.5 (Thor has 11.0) |
| Performance | Flash Attention, dynamic batching |

**Key Features**:
- Purpose-built for embedding models
- Token-based dynamic batching for optimal GPU utilization
- Flash Attention integration
- Prometheus metrics & OpenTelemetry tracing
- Safetensors support for fast model loading
- gRPC and HTTP/REST APIs

**Recommended Models for AkiDB**:
```
# High quality, 768-dim (matches AkiDB benchmark config)
BAAI/bge-large-en-v1.5
intfloat/e5-large-v2
nomic-ai/nomic-embed-text-v1.5

# Smaller, faster (384-dim)
BAAI/bge-small-en-v1.5
sentence-transformers/all-MiniLM-L6-v2
```

**Deployment Example**:
```bash
# On Thor nodes with Docker
docker run --gpus all -p 8080:80 \
  ghcr.io/huggingface/text-embeddings-inference:1.5 \
  --model-id BAAI/bge-large-en-v1.5 \
  --max-batch-tokens 16384

# API Usage
curl http://localhost:8080/embed \
  -X POST -H 'Content-Type: application/json' \
  -d '{"inputs": ["Hello world", "How are you?"]}'
```

---

### Option 3: Infinity ✅ RECOMMENDED (Alternative)

**Repository**: https://github.com/michaelfeil/infinity

| Aspect | Details |
|--------|---------|
| Purpose | Multi-model embedding/reranking server |
| Supported Models | All HuggingFace embedding models |
| Backends | PyTorch, ONNX, TensorRT, CTranslate2 |
| API | OpenAI-compatible |

**Key Features**:
- Multi-model support (serve multiple embedding models)
- OpenAI API compatible (drop-in replacement)
- TensorRT optimization for NVIDIA GPUs
- Dynamic batching
- Support for CLIP, CLAP, ColPali (multimodal)
- Reranking model support

**Deployment Example**:
```bash
# With TensorRT optimization
docker run -it --gpus all \
  -v infinity-data:/app/.cache \
  -p 7997:7997 \
  michaelf34/infinity:latest-trt-onnx v2 \
  --model-id BAAI/bge-large-en-v1.5 \
  --engine optimum \
  --port 7997

# OpenAI-compatible API
curl http://localhost:7997/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model": "BAAI/bge-large-en-v1.5", "input": ["Hello world"]}'
```

---

### Option 4: NVIDIA Triton Inference Server ✅ PRODUCTION READY

**Documentation**: https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/jetson.html

| Aspect | Details |
|--------|---------|
| Purpose | General-purpose model serving |
| JetPack Support | ✅ JetPack 7.x with Triton 2.61 |
| Backends | TensorRT, ONNX, PyTorch, Python |
| API | gRPC, HTTP, C API |

**Key Features**:
- Ensemble model pipelines (preprocessing → embedding → postprocessing)
- Model versioning and A/B testing
- Dynamic batching
- Prometheus metrics
- Direct C API for edge use cases (recommended for Jetson)

**Best For**: Production deployment with custom preprocessing pipelines

---

## Recommended Architecture for AkiDB

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                       │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              AkiDB Coordinator (thor-01:50050)              │
│  - Consistent hashing router                                 │
│  - Result merging                                            │
│  - Embedding client (calls TEI/Infinity)                     │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│  thor-01 (Shard 0)      │     │  thor-02 (Shard 1)      │
│  - AkiDB Server         │     │  - AkiDB Server         │
│  - TEI Server (8080)    │     │  - TEI Server (8080)    │
│  - FAISS GPU Index      │     │  - FAISS GPU Index      │
└─────────────────────────┘     └─────────────────────────┘
```

### Deployment Plan

**Phase 1: TEI Deployment**
```bash
# On both thor-01 and thor-02
docker run -d --name tei --gpus all \
  -p 8080:80 \
  --restart unless-stopped \
  ghcr.io/huggingface/text-embeddings-inference:1.5 \
  --model-id BAAI/bge-large-en-v1.5 \
  --max-batch-tokens 16384 \
  --max-concurrent-requests 512
```

**Phase 2: AkiDB Integration**
Add embedding client to coordinator:
```rust
// In akidb-coordinator
pub struct EmbeddingClient {
    endpoint: String,  // http://localhost:8080/embed
}

impl EmbeddingClient {
    pub async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        // Call TEI API
    }
}
```

**Phase 3: End-to-End Pipeline**
```
User Query → Coordinator → TEI (embed query) → Fan-out Search → Merge Results
```

---

## Performance Expectations

| Model | Dimension | Throughput (est.) | Latency (est.) |
|-------|-----------|-------------------|----------------|
| bge-large-en-v1.5 | 1024 | 500-1000 emb/sec | 2-5 ms |
| bge-small-en-v1.5 | 384 | 2000-4000 emb/sec | 1-2 ms |
| e5-large-v2 | 1024 | 500-800 emb/sec | 3-6 ms |

*Estimates based on Jetson Thor's compute capability. Actual performance to be benchmarked.*

---

## Recommendation Summary

| Priority | Solution | Use Case |
|----------|----------|----------|
| **1st** | **TEI** | Primary embedding server - simple, fast, purpose-built |
| **2nd** | **Infinity** | If OpenAI API compatibility or multi-model needed |
| **3rd** | **Triton** | Production with custom pipelines or ensemble models |
| ❌ | TensorRT-Edge-LLM | Not suitable - designed for LLM generation |

---

## Next Steps

1. **Deploy TEI** on both Thor nodes for testing
2. **Benchmark embedding throughput** with different models
3. **Integrate embedding client** into AkiDB coordinator
4. **Test end-to-end pipeline**: text → embedding → vector search → results

---

## Sources

- [TensorRT-Edge-LLM GitHub](https://github.com/NVIDIA/TensorRT-Edge-LLM)
- [Hugging Face TEI GitHub](https://github.com/huggingface/text-embeddings-inference)
- [Infinity GitHub](https://github.com/michaelfeil/infinity)
- [Triton on Jetson Documentation](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/jetson.html)
- [NVIDIA TensorRT Edge-LLM Blog](https://developer.nvidia.com/blog/accelerating-llm-and-vlm-inference-for-automotive-and-robotics-with-nvidia-tensorrt-edge-llm)
- [JetPack 7.1 Announcement](https://developer.nvidia.com/blog/accelerate-ai-inference-for-edge-and-robotics-with-nvidia-jetson-t4000-and-nvidia-jetpack-7-1)

*Report generated: 2026-01-21*
