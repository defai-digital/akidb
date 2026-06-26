# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.4

**Version:** 1.4
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved
**Changes from v1.3:** Updated Ingestion Pipeline to Hybrid Architecture (Rust + Python)
**Review:** Multi-model synthesis (Claude, Gemini, Grok) validating hybrid approach

---

## Change Log from v1.3

| Section | Change | Rationale |
|---------|--------|-----------|
| §13 | Updated to Hybrid Architecture | Multi-model consensus: Rust orchestration + Python parsing |
| §13 | Added format-aware routing | Parse simple formats in Rust (40-60% of workload) |
| §13 | Updated component specifications | Rust orchestrator + Python parser service |
| §7 | Updated ingestion FRs | Reflect hybrid architecture |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2-12. *(See v1.3 for unchanged sections)*
13. [Ingestion Pipeline Architecture (UPDATED)](#13-ingestion-pipeline-architecture)
14-20. *(See v1.3 for unchanged sections)*

---

## 1. Executive Summary

### 1.1 Product Vision

**AkiDB Thor Edition** is a distributed vector search engine for **NVIDIA Jetson Thor** edge clusters with **hybrid document ingestion** (Rust orchestration + Python parsing).

### 1.2 Key Performance Targets (v1.4)

| Metric | Target | Reference Config | Validation Status |
|--------|--------|------------------|-------------------|
| E2E Search Latency (P95) | < 50ms | D=768, N=1M, topK=10 | **ESTIMATED** |
| FAISS Search (per shard, P95) | < 10ms | nprobe=32, batch=1 | **ESTIMATED** |
| Embedding Latency (P95) | < 10ms | TensorRT-LLM | **ESTIMATED** |
| Throughput | 100 QPS | Reference config | **ESTIMATED** |
| Recall@10 | > 95% | Reference config | **ESTIMATED** |
| Document Ingestion Latency | < 30 min | Batch mode | **SPECIFIED** |
| Upload to Searchable | < 30 min | End-to-end | **SPECIFIED** |
| **Rust Orchestrator Memory** | < 50MB | Baseline | **SPECIFIED** |
| **Python Parser Memory** | < 2GB | Peak per document | **SPECIFIED** |

### 1.3 v1.4 Key Updates

1. **Hybrid Ingestion Architecture:** Rust orchestrator + Python parser service
2. **Format-Aware Routing:** Simple formats (JSON, CSV, HTML, XML) parsed in Rust
3. **Fault Isolation:** Python parsing failures isolated from orchestrator
4. **Clear Migration Path:** Incremental Rust adoption based on profiling

---

## 13. Ingestion Pipeline Architecture (UPDATED in v1.4)

### 13.1 Overview

AkiDB Thor Edition uses a **Hybrid Ingestion Pipeline** with:
- **Rust Orchestrator:** Memory-efficient, long-running NATS consumer
- **Python Parser Service:** Isolated service for complex document formats
- **Format-Aware Routing:** Simple formats parsed directly in Rust

#### Why Hybrid (Not Pure Python)

| Consideration | Pure Python | Hybrid (Rust + Python) |
|---------------|-------------|------------------------|
| Orchestrator memory | ~200MB (unpredictable) | ~50MB (predictable) |
| Fault isolation | None (crash affects all) | Process isolation |
| Concurrency | GIL limited | Tokio async |
| Simple format speed | Adequate | 2-3x faster (Rust) |
| Complex format support | Excellent | Excellent (Python service) |
| Migration path | Rewrite needed | Incremental adoption |

#### Decision Rationale (Multi-Model Consensus)

1. **Memory efficiency:** Unified memory (64GB) is precious; Rust's 50MB baseline leaves max for FAISS/TensorRT-LLM
2. **Fault isolation:** Python parser crashes don't affect the orchestrator; container restarts automatically
3. **Format-aware routing:** 40-60% of typical documents are simple formats parseable directly in Rust
4. **Clear migration path:** Replace Python parsers with Rust incrementally as crates mature

### 13.2 Supported File Types

| Format | Extension | Parser Location | Library |
|--------|-----------|-----------------|---------|
| JSON | .json | **Rust** | serde_json |
| CSV | .csv | **Rust** | csv crate |
| HTML | .html, .htm | **Rust** | scraper |
| XML | .xml | **Rust** | quick-xml |
| PDF | .pdf | Python | pdfplumber |
| Word | .docx | Python | python-docx |
| Excel | .xlsx, .xls | Python | openpyxl |
| EndNote | .enl | Python | Custom parser |

### 13.3 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID INGESTION PIPELINE (v1.4)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐                                                            │
│  │   Client    │  HTTP POST /upload                                         │
│  └──────┬──────┘                                                            │
│         ▼                                                                   │
│  ┌─────────────────┐                                                        │
│  │ Upload Gateway  │  Pre-signed URL → MinIO                                │
│  │ (FastAPI)       │                                                        │
│  └────────┬────────┘                                                        │
│           ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │     MinIO       │  S3 Event Notification                                 │
│  └────────┬────────┘                                                        │
│           ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │ NATS JetStream  │  Stream: AKIDB_INGEST                                  │
│  └────────┬────────┘                                                        │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    RUST ORCHESTRATOR (tokio)                         │   │
│  │                    Memory: ~50MB baseline                            │   │
│  │                                                                      │   │
│  │  ┌──────────────┐                                                   │   │
│  │  │    NATS      │                                                   │   │
│  │  │  Consumer    │                                                   │   │
│  │  └──────┬───────┘                                                   │   │
│  │         │                                                            │   │
│  │         ▼                                                            │   │
│  │  ┌──────────────────────────────────────────────────────────────┐   │   │
│  │  │                    FORMAT ROUTER                              │   │   │
│  │  │  if (json|csv|html|xml) → Rust Parsers                       │   │   │
│  │  │  else                   → Python Parser Service              │   │   │
│  │  └────────────┬────────────────────────┬────────────────────────┘   │   │
│  │               │                        │                            │   │
│  │               ▼                        ▼                            │   │
│  │  ┌───────────────────┐    ┌───────────────────────────────────┐   │   │
│  │  │   RUST PARSERS    │    │   PYTHON PARSER SERVICE           │   │   │
│  │  │   (in-process)    │    │   (HTTP localhost:8001)           │   │   │
│  │  │                   │    │                                    │   │   │
│  │  │ • serde_json      │    │ • pdfplumber                      │   │   │
│  │  │ • csv crate       │    │ • python-docx                     │   │   │
│  │  │ • scraper         │    │ • openpyxl                        │   │   │
│  │  │ • quick-xml       │    │ • ENL custom                      │   │   │
│  │  │                   │    │                                    │   │   │
│  │  │ Memory: ~0        │    │ Memory: 500MB-2GB (capped)        │   │   │
│  │  │ Latency: <10ms    │    │ Latency: 100ms-30s                │   │   │
│  │  └─────────┬─────────┘    └─────────────┬─────────────────────┘   │   │
│  │            │                            │                          │   │
│  │            └────────────┬───────────────┘                          │   │
│  │                         ▼                                          │   │
│  │  ┌──────────────────────────────────────────────────────────────┐ │   │
│  │  │                    TEXT CHUNKING                              │ │   │
│  │  │  512 tokens / 50 overlap / tiktoken                          │ │   │
│  │  └────────────────────────────┬─────────────────────────────────┘ │   │
│  │                               ▼                                    │   │
│  │  ┌──────────────────────────────────────────────────────────────┐ │   │
│  │  │                 TENSORRT-LLM EMBEDDING                        │ │   │
│  │  │  Batch: 32-64 chunks → embeddings                            │ │   │
│  │  └────────────────────────────┬─────────────────────────────────┘ │   │
│  │                               ▼                                    │   │
│  │  ┌──────────────────────────────────────────────────────────────┐ │   │
│  │  │                    AKIDB GRPC INSERT                          │ │   │
│  │  │  tonic client → Coordinator → Shards                         │ │   │
│  │  └──────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13.4 Component Specifications

#### Rust Orchestrator

| Attribute | Specification |
|-----------|---------------|
| Language | Rust (tokio async runtime) |
| Image | `ghcr.io/akidb/ingestion-orchestrator:arm64` |
| Port | 9091 (metrics/health) |
| Memory Limit | 512MB |
| NATS Client | async-nats 0.33+ |
| gRPC Client | tonic 0.11+ |
| In-process Parsers | serde_json, csv, quick-xml, scraper |

**Responsibilities:**
1. Consume MinIO events from NATS JetStream
2. Download files from MinIO
3. Route documents by format
4. Parse simple formats directly (JSON, CSV, HTML, XML)
5. Call Python service for complex formats
6. Manage retry logic (3 retries, exponential backoff)
7. Batch embedding requests (32-64 chunks)
8. Insert vectors via AkiDB gRPC
9. Handle dead-letter queue for failed documents

#### Python Parser Service

| Attribute | Specification |
|-----------|---------------|
| Framework | FastAPI |
| Image | `ghcr.io/akidb/document-parser:arm64` |
| Port | 8001 |
| Memory Limit | 2GB |
| Timeout | 60s per document |
| Parsers | pdfplumber, python-docx, openpyxl, custom ENL |

**Single Endpoint:**
```
POST /parse
Content-Type: application/octet-stream
X-Filename: report.pdf
```

**Responsibilities:**
1. Accept document bytes via HTTP
2. Detect and parse document format
3. Extract text with page/section metadata
4. Return structured JSON response
5. Fail fast on malformed documents (60s timeout)

#### Upload Gateway (unchanged from v1.3)

| Attribute | Specification |
|-----------|---------------|
| Framework | FastAPI |
| Port | 8000 |
| Endpoints | POST /upload, GET /status/{job_id}, GET /health |

### 13.5 Format-Aware Routing

```rust
// Rust orchestrator routing logic
pub enum ParserTarget {
    RustNative,      // In-process parsing
    PythonService,   // HTTP call to Python
}

pub fn route_document(filename: &str) -> ParserTarget {
    let ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        // Simple formats → Rust (40-60% of typical workload)
        "json" => ParserTarget::RustNative,
        "csv"  => ParserTarget::RustNative,
        "html" | "htm" => ParserTarget::RustNative,
        "xml"  => ParserTarget::RustNative,

        // Complex formats → Python
        "pdf"  => ParserTarget::PythonService,
        "docx" | "doc" => ParserTarget::PythonService,
        "xlsx" | "xls" => ParserTarget::PythonService,
        "enl"  => ParserTarget::PythonService,

        // Unknown → default to Python (safer for edge cases)
        _ => ParserTarget::PythonService,
    }
}
```

### 13.6 Fault Isolation

| Failure Scenario | Handling |
|------------------|----------|
| Python parser crash | Container auto-restarts; orchestrator retries from queue |
| Python parser OOM | 2GB limit kills container; orchestrator moves to DLQ |
| Malformed document | Python returns error; orchestrator logs and moves to DLQ |
| TensorRT-LLM timeout | Retry with backoff; circuit breaker if persistent |
| AkiDB unavailable | Retry with backoff; messages stay in NATS |

**Dead Letter Queue:**
- Subject: `akidb.uploads.dlq`
- Retention: 7 days
- Manual review for failed documents

### 13.7 Memory Budget

| Component | Memory | Notes |
|-----------|--------|-------|
| Rust Orchestrator | 50MB | Baseline, predictable |
| Python Parser (idle) | 150MB | Libs loaded |
| Python Parser (active) | 500MB-2GB | Capped by container |
| **Total Ingestion** | **~200MB-2GB** | Peak during large doc |
| FAISS + TensorRT | ~40GB | Primary consumers |
| **Remaining for OS** | ~22GB | Ample headroom |

### 13.8 Deployment Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                HYBRID INGESTION DEPLOYMENT                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Thor 1    │  │   Thor 2    │  │   Thor 3    │             │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤             │
│  │ akidb-shard │  │ akidb-shard │  │ akidb-shard │             │
│  │ ingestion-  │  │ ingestion-  │  │ ingestion-  │             │
│  │ orchestrator│  │ orchestrator│  │ orchestrator│ (optional)  │
│  │ doc-parser  │  │ doc-parser  │  │ doc-parser  │ (optional)  │
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
│                    │nats         │                              │
│                    │ingestion-   │                              │
│                    │orchestrator │ (primary)                    │
│                    │doc-parser   │                              │
│                    │minio        │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Scaling Notes:**
- Primary orchestrator on Thor 4 (coordinator node)
- Additional orchestrators on shard nodes for high volume (optional)
- NATS consumer groups enable horizontal scaling
- Each orchestrator has co-located Python parser (localhost HTTP)

### 13.9 Migration Path

| Phase | Timeframe | Actions |
|-------|-----------|---------|
| **Phase 1** | Initial | Rust orchestrator + Python parser for all complex formats |
| **Phase 2** | Post-MVP | Profile production traffic; identify actual bottlenecks |
| **Phase 3** | As needed | Migrate XLSX to calamine (Rust) when mature |
| **Phase 4** | As needed | Migrate DOCX to docx-rs (Rust) when table support added |
| **Long-term** | Indefinite | Keep PDF and ENL in Python (no Rust alternatives) |

---

## 7. Functional Requirements (UPDATED)

### 7.7 Ingestion Requirements (UPDATED in v1.4)

| ID | Requirement | Priority | Specification |
|----|-------------|----------|---------------|
| FR-I01 | Document upload API | P0 | HTTP POST with pre-signed URL |
| FR-I02 | Multi-format parsing | P0 | PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL |
| FR-I03 | Automatic chunking | P0 | 512-token chunks with 50-token overlap |
| FR-I04 | Batch processing | P0 | Event-driven via NATS JetStream |
| FR-I05 | Upload status tracking | P1 | GET /status/{job_id} endpoint |
| FR-I06 | Error notification | P1 | Failed uploads reported to client |
| FR-I07 | Correlation ID tracing | P1 | End-to-end tracking from upload to search |
| FR-I08 | File validation | P0 | Type, size validation |
| **FR-I09** | **Hybrid architecture** | **P0** | **Rust orchestrator + Python parser (NEW)** |
| **FR-I10** | **Format-aware routing** | **P0** | **Simple formats in Rust (NEW)** |
| **FR-I11** | **Fault isolation** | **P0** | **Parser crashes isolated from orchestrator (NEW)** |

---

## Summary of v1.4 Changes

| Section | Key Change | User Impact |
|---------|------------|-------------|
| §13 | Hybrid architecture adopted | Memory-efficient, fault-tolerant ingestion |
| §13 | Format-aware routing | 40-60% of docs parsed faster in Rust |
| §13 | Fault isolation model | Parsing failures don't crash pipeline |
| §7 | Updated ingestion FRs | Reflect hybrid architecture requirements |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, consistency guarantees |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Ingestion pipeline (Python sidecar) |
| 1.4 | 2026-01-21 | AkiDB Team | Hybrid ingestion (Rust orchestrator + Python parser) |

---

*End of PRD v1.4*
