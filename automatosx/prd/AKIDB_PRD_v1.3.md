# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.3

**Version:** 1.3
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved
**Changes from v1.2:** Added Ingestion Pipeline with Python sidecar for document parsing
**Review:** Multi-model synthesis (Claude, Gemini, Grok) addressing document ingestion architecture

---

## Change Log from v1.2

| Section | Change | Rationale |
|---------|--------|-----------|
| §13 | NEW: Ingestion Pipeline Architecture | Document-to-vector pipeline design |
| §13 | Python sidecar for document parsing | Mature libraries, faster development |
| §13 | Supported file types defined | PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL |
| §7 | Updated ingestion-related FRs | Align with batch processing strategy |
| §8 | Updated ingestion NFRs | 30-minute batch mode SLOs |

---

## Table of Contents

*Sections 1-12 remain largely unchanged from v1.2. New section 13 added.*

1. [Executive Summary](#1-executive-summary) *(minor update)*
2-12. *(See v1.2 for unchanged sections)*
13. [Ingestion Pipeline Architecture (NEW)](#13-ingestion-pipeline-architecture)
14-20. *(Renumbered from v1.2)*

---

## 1. Executive Summary

### 1.1 Product Vision

**AkiDB Thor Edition** is a distributed vector search engine for **NVIDIA Jetson Thor** edge clusters with **integrated document ingestion**.

### 1.2 Key Performance Targets (v1.3)

> **IMPORTANT:** All targets apply ONLY at the reference configuration. See §8 for SLO boundary conditions.

| Metric | Target | Reference Config | Validation Status |
|--------|--------|------------------|-------------------|
| E2E Search Latency (P95) | < 50ms | D=768, N=1M, topK=10 | **ESTIMATED** |
| FAISS Search (per shard, P95) | < 10ms | nprobe=32, batch=1 | **ESTIMATED** |
| Embedding Latency (P95) | < 10ms | TensorRT-LLM | **ESTIMATED** |
| Throughput | 100 QPS | Reference config | **ESTIMATED** |
| Recall@10 | > 95% | Reference config | **ESTIMATED** |
| Recovery Time (RTO) | < 60s | 1M vectors | **ESTIMATED** |
| Read-Your-Writes Visibility | < 100ms | After insert success | **SPECIFIED** |
| Container Restart | < 30s | Podman + systemd | **SPECIFIED** |
| Rolling Update | Zero downtime | Per-node sequential | **SPECIFIED** |
| **Document Ingestion Latency** | < 30 min | Batch mode | **SPECIFIED** |
| **Upload to Searchable** | < 30 min | End-to-end | **SPECIFIED** |

### 1.3 v1.3 Key Additions

1. **Ingestion Pipeline:** Python sidecar for document parsing and chunking
2. **Supported File Types:** PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL
3. **Batch Processing:** 30-minute SLO for document-to-searchable
4. **Event-Driven Architecture:** MinIO → NATS → Ingestion Worker → AkiDB

---

## 13. Ingestion Pipeline Architecture (NEW in v1.3)

### 13.1 Overview

AkiDB Thor Edition includes an **Ingestion Pipeline** that converts raw documents into searchable vectors. The pipeline uses a **Python sidecar service** for document parsing and chunking, leveraging mature Python libraries.

#### Why Python Sidecar (Not Rust)

| Consideration | Python | Rust |
|---------------|--------|------|
| PDF parsing libraries | Mature (pypdf, pdfplumber) | Limited (pdfium-render) |
| DOCX/XLSX support | Excellent (python-docx, openpyxl) | Limited |
| Development speed | Fast | Slower |
| LangChain integration | Native | FFI required |
| Maintenance | Easier | More complex |
| Migration path | Can migrate to Rust later | - |

#### Decision Rationale

1. **Library maturity:** Python document parsing libraries are battle-tested
2. **Time-to-market:** Faster development with Python
3. **Flexibility:** Easy to add new file types
4. **Migration path:** Can migrate hot paths to Rust later if needed

### 13.2 Supported File Types

| Format | Extension | Parser | Chunking Strategy |
|--------|-----------|--------|-------------------|
| PDF | .pdf | pypdf, pdfplumber | Page-based + semantic |
| Word | .docx | python-docx | Paragraph-based |
| Excel | .xlsx, .xls | openpyxl, xlrd | Row-based with headers |
| CSV | .csv | pandas | Row-based with headers |
| HTML | .html, .htm | beautifulsoup4 | Tag-based (p, div, section) |
| XML | .xml | lxml | Element-based |
| JSON | .json | stdlib | Key-value flattening |
| EndNote | .enl | Custom parser | Record-based |

### 13.3 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      AKIDB INGESTION PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐                                                            │
│  │   Client    │                                                            │
│  │  (Browser/  │                                                            │
│  │    CLI)     │                                                            │
│  └──────┬──────┘                                                            │
│         │ HTTP POST                                                         │
│         ▼                                                                   │
│  ┌─────────────────┐                                                        │
│  │ Upload Gateway  │  FastAPI service                                       │
│  │ (Python)        │  - Pre-signed URL generation                          │
│  │                 │  - File validation                                     │
│  │ Port: 8000      │  - Upload status tracking                             │
│  └────────┬────────┘                                                        │
│           │ Pre-signed URL                                                  │
│           ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │     MinIO       │  Object storage                                        │
│  │                 │  - Bucket: uploads/                                    │
│  │ Port: 9000      │  - Event notifications enabled                        │
│  └────────┬────────┘                                                        │
│           │ S3 Event (s3:ObjectCreated:*)                                   │
│           ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │ NATS JetStream  │  Message queue                                         │
│  │                 │  - Stream: AKIDB_INGEST                               │
│  │ Port: 4222      │  - Subject: akidb.uploads.>                           │
│  └────────┬────────┘                                                        │
│           │ Pull subscription                                               │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │              Ingestion Workers (Python)                      │           │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │           │
│  │  │  Worker 1   │  │  Worker 2   │  │  Worker 3   │          │           │
│  │  │  (Thor 1)   │  │  (Thor 2)   │  │  (Thor 3)   │          │           │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │           │
│  │         │                │                │                  │           │
│  │         └────────────────┴────────────────┘                  │           │
│  │                          │                                   │           │
│  │  ┌───────────────────────┼───────────────────────────────┐  │           │
│  │  │                       ▼                               │  │           │
│  │  │  1. FETCH ─────► 2. PARSE ─────► 3. CHUNK            │  │           │
│  │  │     │                │                │                │  │           │
│  │  │     │                ▼                ▼                │  │           │
│  │  │     │           [Raw Text]      [Text Chunks]         │  │           │
│  │  │     │                                 │                │  │           │
│  │  │     │                                 ▼                │  │           │
│  │  │     │           4. EMBED ◄────────────┘               │  │           │
│  │  │     │                │                                 │  │           │
│  │  │     │                ▼                                 │  │           │
│  │  │     │           [Embeddings]                          │  │           │
│  │  │     │                │                                 │  │           │
│  │  │     │                ▼                                 │  │           │
│  │  │     │           5. INSERT ─────► AkiDB gRPC           │  │           │
│  │  │     │                                                  │  │           │
│  │  │     └──────────► 6. CLEANUP (delete from uploads/)    │  │           │
│  │  └───────────────────────────────────────────────────────┘  │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                          │                                                  │
│                          ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                    AkiDB Cluster                             │           │
│  │   ┌─────────────┐                                            │           │
│  │   │ Coordinator │ ──► Shard 0 / Shard 1 / Shard 2           │           │
│  │   └─────────────┘                                            │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13.4 Component Specifications

#### Upload Gateway (Python FastAPI)

| Attribute | Specification |
|-----------|---------------|
| Framework | FastAPI |
| Port | 8000 |
| Image | `ghcr.io/akidb/upload-gateway:latest` |
| Endpoints | POST /upload, GET /status/{job_id} |
| Pre-signed URL TTL | 15 minutes |
| Max file size | 100MB (configurable) |
| Supported formats | PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL |

#### Ingestion Worker (Python)

| Attribute | Specification |
|-----------|---------------|
| Framework | asyncio + NATS client |
| Image | `ghcr.io/akidb/ingestion-worker:latest` |
| Parallelism | 1 worker per Thor node (configurable) |
| Batch size | 10 documents (configurable) |
| Retry policy | 3 retries with exponential backoff |
| Dead letter queue | akidb.uploads.dlq |

#### NATS JetStream

| Attribute | Specification |
|-----------|---------------|
| Stream name | AKIDB_INGEST |
| Subjects | akidb.uploads.*, akidb.uploads.dlq |
| Retention | WorkQueue (delete after ack) |
| Max age | 24 hours |
| Replicas | 1 (edge deployment) |

### 13.5 Document Processing Pipeline

#### Stage 1: File Fetch

```python
async def fetch_document(bucket: str, key: str) -> bytes:
    """Download document from MinIO."""
    async with get_minio_client() as client:
        response = await client.get_object(bucket, key)
        return await response.read()
```

#### Stage 2: Document Parsing

| File Type | Parser | Output |
|-----------|--------|--------|
| PDF | pdfplumber | List[PageText] |
| DOCX | python-docx | List[Paragraph] |
| XLSX | openpyxl | List[Row] with headers |
| CSV | pandas | List[Row] with headers |
| HTML | beautifulsoup4 | List[TextBlock] |
| XML | lxml | List[Element] |
| JSON | stdlib | Flattened key-values |
| ENL | Custom | List[Record] |

#### Stage 3: Text Chunking

```python
from langchain.text_splitter import RecursiveCharacterTextSplitter

splitter = RecursiveCharacterTextSplitter(
    chunk_size=512,        # tokens
    chunk_overlap=50,      # overlap for context
    length_function=tiktoken_counter,
    separators=["\n\n", "\n", ". ", " ", ""]
)

chunks = splitter.split_text(document_text)
```

**Chunking Parameters:**

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| chunk_size | 512 tokens | Optimal for embedding models |
| chunk_overlap | 50 tokens | Preserve context at boundaries |
| min_chunk_size | 100 tokens | Avoid tiny fragments |

#### Stage 4: Embedding Generation

```python
async def generate_embeddings(chunks: List[str]) -> List[List[float]]:
    """Call TensorRT-LLM embedding service."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{TENSORRT_URL}/v1/embeddings",
            json={"input": chunks, "model": "bge-base-en-v1.5"}
        )
        return [item["embedding"] for item in response.json()["data"]]
```

**Embedding Model:**

| Model | Dimensions | Latency (P95) |
|-------|------------|---------------|
| BGE-base-en-v1.5 | 768 | < 10ms (TensorRT) |
| E5-base-v2 | 768 | < 10ms (TensorRT) |

#### Stage 5: Vector Insertion

```python
async def insert_vectors(vectors: List[Vector], metadata: List[dict]):
    """Insert vectors into AkiDB via gRPC."""
    async with grpc.aio.insecure_channel(AKIDB_COORDINATOR) as channel:
        stub = AkiDBStub(channel)
        request = InsertBatchRequest(
            vectors=[
                VectorData(
                    id=str(uuid4()),
                    embedding=v,
                    metadata=json.dumps(m)
                )
                for v, m in zip(vectors, metadata)
            ]
        )
        await stub.InsertBatch(request)
```

### 13.6 Metadata Schema

Each vector stores the following metadata:

```json
{
  "source_file": "report.pdf",
  "source_bucket": "uploads",
  "source_key": "user123/report.pdf",
  "file_type": "pdf",
  "chunk_index": 5,
  "total_chunks": 42,
  "page_number": 3,
  "upload_time": "2026-01-21T10:30:00Z",
  "process_time": "2026-01-21T10:31:15Z",
  "correlation_id": "abc-123-def",
  "text_preview": "First 200 characters of chunk..."
}
```

### 13.7 Error Handling

| Error Type | Handling | Retry |
|------------|----------|-------|
| Parse failure | Log + skip file + notify | No |
| Embedding timeout | Retry with backoff | 3x |
| AkiDB unavailable | Retry with backoff | 5x |
| Invalid file format | Reject + notify user | No |
| File too large | Reject + notify user | No |

**Dead Letter Queue:**

Failed messages after retries are moved to `akidb.uploads.dlq` for manual review.

### 13.8 Batch Mode Operation

Documents are processed in **batch mode** with a 30-minute SLO:

```
┌─────────────────────────────────────────────────────────────────┐
│                     BATCH PROCESSING TIMELINE                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  T+0        T+5min      T+10min     T+25min     T+30min        │
│   │           │           │           │           │             │
│   ▼           ▼           ▼           ▼           ▼             │
│ Upload → Parse/Chunk → Embed → Insert → Searchable             │
│   │           │           │           │           │             │
│   └───────────┴───────────┴───────────┴───────────┘             │
│                     30-minute SLO                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 13.9 Deployment Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION DEPLOYMENT                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Thor 1    │  │   Thor 2    │  │   Thor 3    │             │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤             │
│  │ akidb-shard │  │ akidb-shard │  │ akidb-shard │             │
│  │ ingest-worker│ │ ingest-worker│ │ ingest-worker│            │
│  │ minio       │  │ minio       │  │ minio       │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │               │               │                       │
│         └───────────────┴───────────────┘                       │
│                         │                                       │
│                    ┌─────────────┐                              │
│                    │   Thor 4    │                              │
│                    ├─────────────┤                              │
│                    │akidb-coord  │                              │
│                    │upload-gateway│                             │
│                    │ nats        │                              │
│                    │ minio       │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 13.10 Container Specifications

#### Upload Gateway Container

| Attribute | Specification |
|-----------|---------------|
| Image | `ghcr.io/akidb/upload-gateway:latest` |
| Base | `python:3.11-slim-bookworm` |
| Ports | 8000 (HTTP) |
| Environment | MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY |
| Health check | `curl -f http://localhost:8000/health` |
| Memory limit | 512MB |

#### Ingestion Worker Container

| Attribute | Specification |
|-----------|---------------|
| Image | `ghcr.io/akidb/ingestion-worker:latest` |
| Base | `python:3.11-slim-bookworm` |
| GPU | Not required (embedding via TensorRT service) |
| Environment | NATS_URL, MINIO_ENDPOINT, AKIDB_COORDINATOR, TENSORRT_URL |
| Health check | NATS connection status |
| Memory limit | 2GB |

#### NATS Container

| Attribute | Specification |
|-----------|---------------|
| Image | `nats:2.10-alpine` |
| Ports | 4222 (client), 8222 (monitoring) |
| Config | JetStream enabled |
| Storage | 10GB for message persistence |

---

## 7. Functional Requirements (UPDATED)

### 7.7 Ingestion Requirements (NEW in v1.3)

| ID | Requirement | Priority | Specification |
|----|-------------|----------|---------------|
| FR-I01 | Document upload API | P0 | HTTP POST with pre-signed URL |
| FR-I02 | Multi-format parsing | P0 | PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL |
| FR-I03 | Automatic chunking | P0 | 512-token chunks with 50-token overlap |
| FR-I04 | Batch processing | P0 | Event-driven via NATS JetStream |
| FR-I05 | Upload status tracking | P1 | GET /status/{job_id} endpoint |
| FR-I06 | Error notification | P1 | Failed uploads reported to client |
| FR-I07 | Correlation ID tracing | P1 | End-to-end tracking from upload to search |
| FR-I08 | File validation | P0 | Type, size, malware scanning (optional) |

---

## 8. Non-Functional Requirements (UPDATED)

### 8.8 Ingestion Performance Requirements (NEW in v1.3)

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-I01 | Upload to searchable (batch) | < 30 min | 95th percentile |
| NFR-I02 | Document parse time | < 30s | Per document (< 100 pages) |
| NFR-I03 | Chunking throughput | > 1000 chunks/s | Per worker |
| NFR-I04 | Worker recovery | < 30s | After container restart |
| NFR-I05 | Message processing | At-least-once | With deduplication |
| NFR-I06 | Max file size | 100MB | Configurable |
| NFR-I07 | Concurrent uploads | 100 | Per gateway instance |

---

## 14. Success Metrics (UPDATED)

### 14.4 Ingestion Metrics (NEW in v1.3)

| Metric | Target | Phase |
|--------|--------|-------|
| Upload success rate | > 99% | Phase 2+ |
| Parse success rate | > 95% | Phase 2+ |
| Batch SLO compliance | > 95% | Phase 3+ |
| Worker uptime | > 99.5% | Phase 3+ |

---

## 18. Risks and Mitigations (UPDATED)

### 18.4 Ingestion Risks (NEW in v1.3)

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| PDF parsing failures | Medium | Medium | Multiple parser fallback (pdfplumber → pypdf) |
| Large file OOM | Medium | High | Streaming parser, file size limits |
| NATS message loss | Low | High | JetStream persistence, redelivery |
| TensorRT unavailable | Low | High | Circuit breaker, queue backpressure |
| Python sidecar overhead | Low | Low | Monitor memory, optimize hot paths |

---

## Summary of v1.3 Changes

| Section | Key Change | User Impact |
|---------|------------|-------------|
| §13 | Ingestion pipeline architecture defined | Document upload capability |
| §13 | Python sidecar for parsing | Supports 8 file formats |
| §13 | 30-minute batch SLO | Predictable search availability |
| §7 | Ingestion FRs added | Upload API requirements |
| §8 | Ingestion NFRs added | Performance targets |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, delete/update contracts, consistency guarantees, cuVS gate |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets), deployment architecture, infrastructure requirements |
| 1.3 | 2026-01-21 | AkiDB Team | Ingestion pipeline (Python sidecar), document parsing, batch processing |

---

*End of PRD v1.3*
