# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.5 (Final)

**Version:** 1.5
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved (Final for Production Release)
**Changes from v1.4:** Added critical resilience patterns, NATS 3-node, comprehensive monitoring
**Review:** Multi-model synthesis (Claude, Gemini, Grok) - Final validation

---

## Change Log from v1.4

| Section | Change | Rationale |
|---------|--------|-----------|
| §11 | NATS cluster: 4-node → 3-node | Anti-pattern identified; same fault tolerance |
| §13 | Added circuit breaker, backpressure, memory coordination | Critical resilience gaps |
| §13 | XLSX moved to Rust (calamine) | Library maturity confirmed |
| §13 | Semantic chunking added | 15-20% retrieval quality improvement |
| §8 | Updated operational NFRs | Resilience metrics |
| New | Comprehensive monitoring requirements | Production observability |

---

## Executive Summary

### Product Vision

**AkiDB Thor Edition** is a production-ready distributed vector search engine for **NVIDIA Jetson Thor** edge clusters with:
- **Hybrid document ingestion** (Rust orchestration + Python parsing)
- **Fault-tolerant architecture** (circuit breaker, backpressure, memory coordination)
- **30-minute batch SLO** from upload to searchable

### v1.5 Key Features

| Feature | Description |
|---------|-------------|
| **Hybrid Ingestion** | Rust orchestrator (60-70% of formats) + Python parser (complex formats) |
| **Circuit Breaker** | Python parser failures isolated from orchestrator |
| **Backpressure** | AkiDB saturation throttles NATS consumption |
| **Memory Coordination** | Unified memory pressure detection and mitigation |
| **Semantic Chunking** | Sentence-boundary-aware for improved retrieval |
| **Dynamic Batching** | 16-64 chunks based on queue depth |

---

## 11. Deployment Architecture (UPDATED)

### 11.1 NATS Cluster Configuration (UPDATED in v1.5)

**Change:** Reduced from 4-node to 3-node cluster.

**Rationale:** A 4-node Raft cluster provides the same fault tolerance as 3-node (can only lose 1 node) but with more overhead.

```
┌─────────────────────────────────────────────────────────────────┐
│                    AKIDB THOR CLUSTER (v1.5)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Thor 1    │  │   Thor 2    │  │   Thor 3    │             │
│  │  (Shard 0)  │  │  (Shard 1)  │  │  (Shard 2)  │             │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤             │
│  │ akidb-shard │  │ akidb-shard │  │ akidb-shard │             │
│  │ ingestion-  │  │             │  │             │             │
│  │ orchestrator│  │             │  │             │             │
│  │ doc-parser  │  │             │  │             │             │
│  │ NATS (R1)   │  │ NATS (R2)   │  │ NATS (R3)   │             │
│  │ minio       │  │ minio       │  │ minio       │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │               │               │                       │
│         └───────────────┴───────────────┘                       │
│              NATS Raft Cluster (3-node)                         │
│              Quorum: 2 | Can lose: 1 node                       │
│                         │                                       │
│                    ┌─────────────┐                              │
│                    │   Thor 4    │                              │
│                    │(Coordinator)│                              │
│                    ├─────────────┤                              │
│                    │akidb-coord  │                              │
│                    │upload-gateway│                             │
│                    │(NATS client)│ ← Connects to Thor 1-3      │
│                    │minio        │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 13. Ingestion Pipeline Architecture (FINAL)

### 13.1 Overview

The v1.5 Hybrid Ingestion Pipeline includes:

1. **Rust Orchestrator** - Memory-efficient, long-running core
2. **Python Parser Service** - Complex formats with fault isolation
3. **Circuit Breaker** - Prevents cascade failures
4. **Backpressure Controller** - Throttles when AkiDB saturated
5. **Memory Coordinator** - Manages unified memory contention
6. **Semantic Chunker** - Sentence-boundary-aware splitting
7. **Dynamic Batcher** - Queue-depth-adaptive embedding batches

### 13.2 Architecture Diagram (v1.5 Final)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HYBRID INGESTION PIPELINE (v1.5 FINAL)                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MinIO Event                                                                │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    NATS JetStream (3-node)                           │   │
│  │  Stream: AKIDB_INGEST | Replicas: 3 | max_deliver: 3                │   │
│  └──────────────────────────────────┬──────────────────────────────────┘   │
│                                     │                                       │
│                                     ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    RUST ORCHESTRATOR (tokio)                         │   │
│  │                    Memory: 512MB-2GB (dynamic)                       │   │
│  │                                                                      │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  MEMORY COORDINATOR                             │ │   │
│  │  │  Monitor: tegrastats | Pause threshold: 70% unified memory     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                                     │                                │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  BACKPRESSURE CONTROLLER                        │ │   │
│  │  │  Monitor: AkiDB insert latency | Pause if >500ms               │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                                     │                                │   │
│  │  ┌──────────────────────────────────┴───────────────────────────┐   │   │
│  │  │                    FORMAT ROUTER                              │   │   │
│  │  │  Route by extension → Rust (60-70%) or Python (30-40%)       │   │   │
│  │  └─────────────┬────────────────────────────┬───────────────────┘   │   │
│  │                │                            │                        │   │
│  │                ▼                            ▼                        │   │
│  │  ┌─────────────────────────┐  ┌─────────────────────────────────┐  │   │
│  │  │   RUST PARSERS (60-70%) │  │   CIRCUIT BREAKER               │  │   │
│  │  │   • JSON (serde_json)   │  │   ┌───────────────────────────┐ │  │   │
│  │  │   • CSV (csv crate)     │  │   │ State: CLOSED/OPEN/HALF  │ │  │   │
│  │  │   • HTML (scraper)      │  │   │ Failures: 0/3            │ │  │   │
│  │  │   • XML (quick-xml)     │  │   │ Reset: 30s               │ │  │   │
│  │  │   • XLSX (calamine) NEW │  │   └───────────┬───────────────┘ │  │   │
│  │  │   • DOCX-simple (docx-rs)│ │               │                  │  │   │
│  │  └──────────┬──────────────┘  │               ▼                  │  │   │
│  │             │                  │   ┌───────────────────────────┐ │  │   │
│  │             │                  │   │ PYTHON PARSER (30-40%)   │ │  │   │
│  │             │                  │   │ • PDF (pdfplumber)       │ │  │   │
│  │             │                  │   │ • DOCX-complex           │ │  │   │
│  │             │                  │   │ • ENL (custom)           │ │  │   │
│  │             │                  │   │ Memory: 2GB | Timeout: 60s│ │  │   │
│  │             │                  │   └───────────┬───────────────┘ │  │   │
│  │             │                  └───────────────┼─────────────────┘  │   │
│  │             │                                  │                     │   │
│  │             └──────────────┬──────────────────┘                     │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  SEMANTIC CHUNKER                               │ │   │
│  │  │  Target: ~512 tokens | Boundary: sentence | Overlap: 20-50     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  DYNAMIC BATCHER                                │ │   │
│  │  │  Range: 16-64 | Based on: queue depth + GPU utilization        │ │   │
│  │  │                                                                 │ │   │
│  │  │                  TensorRT-LLM Embedding                         │ │   │
│  │  │                  Model: BGE-base-en-v1.5 (768-dim)             │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  IDEMPOTENCY LAYER                              │ │   │
│  │  │  Key: content_hash | Dedup: skip if exists                     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  │                            │                                         │   │
│  │                            ▼                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │                  AKIDB GRPC CLIENT (tonic)                      │ │   │
│  │  │  Backpressure signal → throttle NATS ack rate                  │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                            │                                               │
│          ┌─────────────────┼─────────────────┐                            │
│          ▼                 ▼                 ▼                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                    │
│  │  DEAD LETTER │  │  DOCUMENT    │  │  MONITORING  │                    │
│  │    QUEUE     │  │   STATE      │  │  (Prometheus)│                    │
│  │  Auto-retry  │  │  TRACKER     │  │              │                    │
│  │  (exp backoff)│ │  (SQLite)    │  │  • Latency   │                    │
│  └──────────────┘  └──────────────┘  │  • GPU util  │                    │
│                                       │  • Memory    │                    │
│                                       │  • Errors    │                    │
│                                       └──────────────┘                    │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### 13.3 Resilience Components (NEW in v1.5)

#### Circuit Breaker

| Attribute | Value |
|-----------|-------|
| Failure threshold | 3 consecutive failures |
| Reset timeout | 30 seconds |
| Half-open calls | 1 test call |
| Fallback | Move to DLQ with `circuit_open` reason |

**State Diagram:**
```
CLOSED ──(3 failures)──► OPEN ──(30s timeout)──► HALF-OPEN
   ▲                       │                         │
   │                       │                         │
   └──────(success)────────┴────────(failure)────────┘
```

#### Backpressure Controller

| Attribute | Value |
|-----------|-------|
| Insert latency threshold | 500ms |
| Queue depth high water | 10,000 messages |
| Pause duration | 5 seconds |
| Throttle delay | 100ms per message |

#### Memory Coordinator

| Attribute | Value |
|-----------|-------|
| Unified memory limit | 64GB |
| Ingestion budget | 5% (3.2GB) |
| Pause threshold | 70% unified memory |
| Monitor | tegrastats (Jetson-specific) |

### 13.4 Updated Format Routing

| Format | Extension | Location | Library | Parse Ratio |
|--------|-----------|----------|---------|-------------|
| JSON | .json | Rust | serde_json | |
| CSV | .csv | Rust | csv crate | |
| HTML | .html, .htm | Rust | scraper | |
| XML | .xml | Rust | quick-xml | |
| **XLSX** | **.xlsx, .xls** | **Rust** | **calamine** | **60-70%** |
| DOCX (simple) | .docx | Rust | docx-rs | |
| PDF | .pdf | Python | pdfplumber | |
| DOCX (complex) | .docx | Python | python-docx | **30-40%** |
| ENL | .enl | Python | Custom | |

### 13.5 Semantic Chunking

**Strategy:** Sentence-boundary-aware chunking instead of fixed-size.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Target tokens | 512 | BGE optimal context |
| Min overlap | 20 tokens | Minimum context preservation |
| Max overlap | 50 tokens | Maximum for sentence boundaries |
| Boundary detection | unicode-segmentation | Cross-language support |

**Benefits:**
- 15-20% improvement in retrieval quality (vs. fixed chunking)
- Chunks are semantically coherent
- Overlap preserves context without redundancy

### 13.6 Dynamic Embedding Batching

| Queue Depth | Batch Size | Rationale |
|-------------|------------|-----------|
| <100 messages | 16 | Low load: minimize latency |
| 100-1000 | Linear scale | Balance throughput/latency |
| >1000 messages | 64 | High load: maximize throughput |
| GPU util >80% | Reduce 50% | Prevent memory pressure |

---

## 7. Functional Requirements (v1.5 Final)

### 7.7 Ingestion Requirements

| ID | Requirement | Priority | Specification |
|----|-------------|----------|---------------|
| FR-I01 | Document upload API | P0 | HTTP POST with pre-signed URL (15-min expiry) |
| FR-I02 | Multi-format parsing | P0 | PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL |
| FR-I03 | Semantic chunking | P0 | Sentence-boundary-aware, ~512 tokens |
| FR-I04 | Batch processing | P0 | Event-driven via NATS JetStream (3-node) |
| FR-I05 | Upload status tracking | P1 | GET /status/{job_id} with full history |
| FR-I06 | Error notification | P1 | Failed uploads in DLQ with reason |
| FR-I07 | Correlation ID tracing | P0 | End-to-end tracking |
| FR-I08 | File validation | P0 | Type, size (100MB max), Content-Type check |
| FR-I09 | Circuit breaker | P0 | Python parser fault isolation |
| FR-I10 | Backpressure | P0 | AkiDB latency → NATS throttling |
| FR-I11 | Memory coordination | P0 | Unified memory pressure detection |
| FR-I12 | Idempotency | P0 | Content-hash deduplication |

---

## 8. Non-Functional Requirements (v1.5 Final)

### 8.8 Ingestion Performance Requirements

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-I01 | Upload to searchable (batch) | < 30 min | 95th percentile |
| NFR-I02 | Document parse time | < 30s | Per document (< 100 pages) |
| NFR-I03 | Rust parsing throughput | > 10 docs/s | Simple formats |
| NFR-I04 | Python parsing throughput | > 1 doc/s | Complex formats |
| NFR-I05 | Message processing | At-least-once | With idempotent dedup |
| NFR-I06 | Max file size | 100MB | Configurable |
| NFR-I07 | Concurrent uploads | 100 | Per gateway instance |

### 8.9 Resilience Requirements (NEW in v1.5)

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-R01 | Circuit breaker recovery | < 60s | From OPEN to CLOSED |
| NFR-R02 | Backpressure response | < 5s | From detection to throttle |
| NFR-R03 | Memory pressure response | < 10s | From detection to pause |
| NFR-R04 | DLQ processing | < 1 hour | Auto-retry with backoff |
| NFR-R05 | Python parser restart | < 30s | Container auto-restart |

---

## 14. Monitoring Requirements (NEW in v1.5)

### 14.1 Required Metrics

```yaml
# Ingestion Throughput
ingestion_documents_total{format, status}
ingestion_chunks_total{format}
embedding_batches_total{size_bucket}

# Latency (histograms)
ingestion_e2e_duration_seconds{format, quantile}
parser_duration_seconds{format, parser}
embedding_batch_duration_seconds{quantile}

# Resource Utilization
memory_usage_bytes{component}
gpu_utilization_percent
gpu_memory_used_bytes
unified_memory_used_bytes

# Queue Health
nats_pending_messages{stream}
dead_letter_queue_depth
queue_processing_rate

# Resilience State
circuit_breaker_state{service}
backpressure_active
memory_pressure_level

# Errors
parser_failures_total{format, error_type}
embedding_failures_total
insert_failures_total
```

### 14.2 Alerting Thresholds

| Alert | Condition | Severity |
|-------|-----------|----------|
| SLO Breach | `ingestion_e2e_duration_seconds{p95} > 1800` | Critical |
| High DLQ | `dead_letter_queue_depth > 100` | Warning |
| Memory Pressure | `unified_memory_used_bytes > 0.7 * 64GB` | Warning |
| Circuit Open | `circuit_breaker_state == "open"` | Warning |
| Parser Down | `parser_failures_total rate > 10/min` | Critical |
| Queue Backlog | `nats_pending_messages > 10000` | Warning |

### 14.3 Required Dashboards

1. **Ingestion Overview** - Documents/min, success rate, latency percentiles
2. **Resource Utilization** - CPU, GPU, unified memory, per-component memory
3. **Queue Health** - NATS depth, DLQ depth, processing rate
4. **Resilience Status** - Circuit breaker states, backpressure events, memory pauses

---

## 15. Security Requirements (v1.5 Final)

### 15.1 Pre-signed URL Security

| Control | Specification |
|---------|---------------|
| URL expiry | 15 minutes (configurable) |
| Permissions | PUT-only to specific key |
| Size validation | Reject files > 100MB |
| Content-Type | Validate matches declared type |
| Path traversal | Sanitize filenames before storage |

### 15.2 Container Security

| Control | Specification |
|---------|---------------|
| Rootless | All containers run as non-root |
| Memory limits | Python: 2GB, Rust: 2GB |
| Network isolation | Host mode (consider CNI for v2) |
| Secrets | Files only, not environment variables |

---

## 16. Production Readiness Checklist (v1.5)

### Critical (Must Pass for Release)

| ID | Item | Owner | Status |
|----|------|-------|--------|
| C-01 | NATS 3-node cluster deployed | Infra | [ ] |
| C-02 | Circuit breaker implemented | Dev | [ ] |
| C-03 | Backpressure mechanism tested | QA | [ ] |
| C-04 | Memory coordinator active | Dev | [ ] |
| C-05 | Core metrics exported | Ops | [ ] |
| C-06 | 30-min SLO validated | QA | [ ] |
| C-07 | GPU passthrough working | Infra | [ ] |

### High Priority (Strongly Recommended)

| ID | Item | Owner | Status |
|----|------|-------|--------|
| H-01 | Semantic chunking | Dev | [ ] |
| H-02 | Dynamic batching | Dev | [ ] |
| H-03 | XLSX in Rust (calamine) | Dev | [ ] |
| H-04 | Idempotency layer | Dev | [ ] |
| H-05 | Document state tracking | Dev | [ ] |
| H-06 | Pre-signed URL hardening | Security | [ ] |
| H-07 | GPU metrics via DCGM | Ops | [ ] |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, consistency guarantees |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Ingestion pipeline (Python sidecar) |
| 1.4 | 2026-01-21 | AkiDB Team | Hybrid ingestion (Rust orchestrator + Python parser) |
| 1.5 | 2026-01-21 | AkiDB Team | NATS 3-node, resilience patterns, monitoring (Final) |

---

*End of PRD v1.5 (Final)*
