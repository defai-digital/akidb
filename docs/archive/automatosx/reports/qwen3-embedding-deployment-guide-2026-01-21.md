# Qwen3 Embedding Deployment Guide for AkiDB Thor Cluster

**Date**: 2026-01-21
**Target Model**: Qwen3-Embedding (0.6B or 8B)
**Hardware**: Jetson AGX Thor (128GB) x 2

---

## Executive Summary

**Best Solution**: **vLLM on Jetson Thor** with NVIDIA's optimized containers

vLLM is officially supported on Jetson AGX Thor with 3.5x performance optimization, making it the ideal choice for deploying Qwen3-Embedding models.

---

## Qwen3 Embedding Model Comparison

| Spec | Qwen3-Embedding-0.6B | Qwen3-Embedding-8B |
|------|---------------------|-------------------|
| Parameters | 600M | 8B |
| Max Embedding Dim | 1024 | 4096 |
| Context Length | 32K tokens | 32K tokens |
| VRAM Required | ~2GB | ~16GB |
| MTEB Score | 64.33 | **70.58** (#1) |
| Languages | 100+ | 100+ |
| License | Apache 2.0 | Apache 2.0 |

**Recommendation**: Use **Qwen3-Embedding-8B** on Thor (128GB RAM handles it easily)

---

## Deployment Options Ranked

### 1. vLLM (RECOMMENDED) ⭐⭐⭐⭐⭐

**Why Best for Thor**:
- Official NVIDIA support with optimized kernels for Thor architecture
- 3.5x performance boost vs generic implementation
- Monthly container updates from NGC
- Native embedding task support

**Container**: `nvcr.io/nvidia/vllm:25.11-py3` (or latest)

```bash
# Pull the optimized vLLM container for Jetson Thor
docker pull nvcr.io/nvidia/vllm:25.11-py3

# Run Qwen3-Embedding-8B
docker run -d --name qwen3-embed \
  --gpus all \
  --ipc=host \
  -p 8000:8000 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  nvcr.io/nvidia/vllm:25.11-py3 \
  --model Qwen/Qwen3-Embedding-8B \
  --task embed \
  --dtype float16 \
  --max-model-len 8192 \
  --port 8000
```

**API Usage**:
```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8000/v1",
    api_key="dummy"
)

response = client.embeddings.create(
    model="Qwen/Qwen3-Embedding-8B",
    input=["What is machine learning?"]
)
embedding = response.data[0].embedding
```

---

### 2. Text Embeddings Inference (TEI) ⭐⭐⭐⭐

**Why Good**:
- Purpose-built for embeddings
- Flash Attention optimized
- Simple deployment

**Note**: May need ARM64 build for Jetson

```bash
# TEI deployment (if ARM64 image available)
docker run -d --name tei-qwen3 \
  --gpus all \
  -p 8080:80 \
  -v hf_cache:/data \
  ghcr.io/huggingface/text-embeddings-inference:1.7.2 \
  --model-id Qwen/Qwen3-Embedding-8B \
  --dtype float16 \
  --max-batch-tokens 16384

# API call
curl http://localhost:8080/embed \
  -X POST -H 'Content-Type: application/json' \
  -d '{"inputs": ["Instruct: Retrieve relevant documents\nQuery: What is AI?"]}'
```

---

### 3. Native Python (Development) ⭐⭐⭐

**For testing and development**:

```python
from sentence_transformers import SentenceTransformer
import torch

# Load with Flash Attention 2 for best performance
model = SentenceTransformer(
    "Qwen/Qwen3-Embedding-8B",
    model_kwargs={
        "attn_implementation": "flash_attention_2",
        "torch_dtype": torch.float16,
        "device_map": "auto"
    },
    tokenizer_kwargs={"padding_side": "left"}
)

# Encode with instruction (recommended for 1-5% improvement)
queries = ["What is the capital of France?"]
query_embeddings = model.encode(queries, prompt_name="query")

# Encode documents
docs = ["Paris is the capital of France.", "London is in England."]
doc_embeddings = model.encode(docs, prompt_name="document")
```

---

## Recommended Architecture for AkiDB

```
┌─────────────────────────────────────────────────────────────────┐
│                      Client Applications                         │
│                   (REST API / gRPC requests)                     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                AkiDB Coordinator (thor-01:50050)                │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  EmbeddingClient → vLLM (localhost:8000)                │   │
│   │  - Converts text queries to embeddings                  │   │
│   │  - Uses Qwen3-Embedding-8B                              │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│   ┌──────────────────────────┴──────────────────────────┐       │
│   │              Fan-out to Shards                       │       │
│   └──────────────────────────┬──────────────────────────┘       │
└──────────────────────────────┼──────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
┌──────────────────────────┐     ┌──────────────────────────┐
│   thor-01 (Shard 0)      │     │   thor-02 (Shard 1)      │
│   ├─ AkiDB Server        │     │   ├─ AkiDB Server        │
│   ├─ vLLM Embedding      │     │   ├─ vLLM Embedding      │
│   │  (Qwen3-8B)          │     │   │  (Qwen3-8B)          │
│   └─ FAISS GPU Index     │     │   └─ FAISS GPU Index     │
└──────────────────────────┘     └──────────────────────────┘
```

---

## Deployment Script

```bash
#!/bin/bash
# deploy-qwen3-embedding.sh

THOR_01="devop@192.168.1.61"
THOR_02="devop@192.168.1.62"

# Deploy on both nodes
for HOST in $THOR_01 $THOR_02; do
  echo "=== Deploying Qwen3-Embedding on $HOST ==="

  ssh $HOST '
    # Pull the optimized vLLM container
    docker pull nvcr.io/nvidia/vllm:25.11-py3

    # Stop existing container if any
    docker stop qwen3-embed 2>/dev/null
    docker rm qwen3-embed 2>/dev/null

    # Create cache directory
    mkdir -p ~/.cache/huggingface

    # Run Qwen3-Embedding-8B
    docker run -d --name qwen3-embed \
      --gpus all \
      --ipc=host \
      --restart unless-stopped \
      -p 8000:8000 \
      -v ~/.cache/huggingface:/root/.cache/huggingface \
      nvcr.io/nvidia/vllm:25.11-py3 \
      --model Qwen/Qwen3-Embedding-8B \
      --task embed \
      --dtype float16 \
      --max-model-len 8192 \
      --port 8000

    echo "Waiting for model to load..."
    sleep 30

    # Health check
    curl -s http://localhost:8000/health || echo "Still loading..."
  '
done

echo "=== Deployment complete ==="
```

---

## AkiDB Integration Code

Add embedding client to the coordinator:

```rust
// crates/coordinator/src/embedding.rs

use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct EmbeddingRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

pub struct Qwen3EmbeddingClient {
    client: Client,
    endpoint: String,
    model: String,
}

impl Qwen3EmbeddingClient {
    pub fn new(endpoint: &str) -> Self {
        Self {
            client: Client::new(),
            endpoint: format!("{}/v1/embeddings", endpoint),
            model: "Qwen/Qwen3-Embedding-8B".to_string(),
        }
    }

    pub async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        let request = EmbeddingRequest {
            model: self.model.clone(),
            input: texts,
        };

        let response: EmbeddingResponse = self.client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await?
            .json()
            .await?;

        Ok(response.data.into_iter().map(|d| d.embedding).collect())
    }

    /// Embed with instruction prefix for better retrieval (1-5% improvement)
    pub async fn embed_query(&self, query: &str) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        let instruction = format!(
            "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: {}",
            query
        );
        let embeddings = self.embed(vec![instruction]).await?;
        Ok(embeddings.into_iter().next().unwrap())
    }
}
```

---

## Expected Performance

### Qwen3-Embedding-8B on Jetson Thor

| Metric | Estimated Value |
|--------|-----------------|
| Throughput | 200-500 embeddings/sec |
| Latency (single) | 5-15 ms |
| Latency (batch 32) | 20-50 ms |
| VRAM Usage | ~16 GB |
| Embedding Dimension | 4096 (or custom 32-4096) |

### Quality Benchmarks (Official)

| Benchmark | Score |
|-----------|-------|
| MTEB Multilingual | **70.58** (#1 worldwide) |
| MTEB English v2 | **75.22** |
| C-MTEB (Chinese) | **73.84** |
| Retrieval | **86.40** |

---

## Dimension Optimization

Qwen3-Embedding supports **Matryoshka Representation Learning (MRL)** - you can use smaller dimensions with minimal quality loss:

| Dimension | Storage per Vector | Quality Retention |
|-----------|-------------------|-------------------|
| 4096 | 16 KB | 100% |
| 2048 | 8 KB | ~99% |
| 1024 | 4 KB | ~97% |
| 512 | 2 KB | ~94% |
| 256 | 1 KB | ~90% |

**Recommendation for AkiDB**: Use **1024 dimensions** for good balance of quality and storage.

```python
# Truncate embeddings to desired dimension
embedding_4096 = model.encode(["text"])[0]
embedding_1024 = embedding_4096[:1024]  # MRL-compatible truncation
```

---

## Quick Test Commands

```bash
# Test embedding endpoint
curl http://192.168.1.61:8000/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3-Embedding-8B",
    "input": ["Hello world", "Machine learning is amazing"]
  }'

# Check model status
curl http://192.168.1.61:8000/v1/models

# Health check
curl http://192.168.1.61:8000/health
```

---

## Summary

| Component | Choice | Reason |
|-----------|--------|--------|
| **Model** | Qwen3-Embedding-8B | #1 MTEB, 100+ languages |
| **Inference** | vLLM (NVIDIA container) | 3.5x optimized for Thor |
| **Dimension** | 1024 (from 4096) | MRL truncation, 4x storage savings |
| **Deployment** | Docker on both nodes | High availability |

---

## Sources

- [Qwen3-Embedding-8B on Hugging Face](https://huggingface.co/Qwen/Qwen3-Embedding-8B)
- [Qwen3-Embedding-0.6B on Hugging Face](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B)
- [Qwen3 Embedding Blog](https://qwenlm.github.io/blog/qwen3-embedding/)
- [vLLM on Jetson Thor - Aetherix](https://blog.aetherix.com/how-to-run-vllm-on-jetson-agx-thor/)
- [Jetson Thor 3.5x Performance - NVIDIA Forums](https://forums.developer.nvidia.com/t/announcing-new-vllm-container-3-5x-increase-in-gen-ai-performance-in-just-5-weeks-of-jetson-agx-thor-launch/346634)
- [NVIDIA vLLM Containers on NGC](https://catalog.ngc.nvidia.com/)

*Report generated: 2026-01-21*
