# AkiDB Thor Edition - Implementation Plan

**Version:** 1.3
**Date:** 2026-01-21
**Status:** Approved
**Based On:** ADR v1.4 (Final), PRD v1.5 (Final)
**Review:** Multi-model synthesis (Claude, Gemini, Grok)
**Changes from v1.2:** Hybrid architecture, NATS 3-node, resilience patterns, semantic chunking

---

## Change Log from v1.2

| Section | Change | Rationale |
|---------|--------|-----------|
| Phase 1 | Marked as ~70% complete | Verified from codebase |
| Phase 2 | Replaced Python-only ingestion with Hybrid architecture | ADR-018 decision |
| Phase 2 | NATS 4-node → 3-node | ADR-019 (Raft anti-pattern) |
| Phase 2 | Added circuit breaker, backpressure, memory coordinator | ADR-020 |
| Phase 2 | Added semantic chunking, dynamic batching | PRD v1.5 requirements |
| Phase 2 | Added calamine XLSX in Rust | 60-70% Rust parsing ratio |
| Phase 4 | Updated quadlets for hybrid architecture | Production deployment |
| Timeline | Phase 2 extended to 10 weeks | Hybrid complexity |

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition over **~26 weeks (~6.5 months)** across 4 phases plus a validation sprint. This version incorporates the **Hybrid Ingestion Pipeline** with Rust orchestration and Python parsing sidecars.

**Key Updates in v1.3:**
- Hybrid ingestion: Rust orchestrator (60-70%) + Python parser (30-40%)
- NATS 3-node JetStream cluster (not 4)
- Circuit breaker for Python parser fault isolation
- Backpressure controller with AkiDB latency awareness
- Memory coordinator with tegrastats integration
- Semantic chunking (sentence-boundary-aware)
- Dynamic batching (16-64 based on queue depth)
- Calamine XLSX parsing in Rust

---

## Current Progress Assessment

### Completed (Phase 0-1)

| Component | Status | Location |
|-----------|--------|----------|
| FAISS GPU wrapper | **DONE** | `crates/faiss-wrapper/` |
| RocksDB storage | **DONE** | `crates/storage/` |
| gRPC server | **DONE** | `crates/grpc-server/` |
| Coordinator (fanout, merger) | **DONE** | `crates/coordinator/` |
| Generic backpressure | **DONE** | `crates/coordinator/src/backpressure.rs` |
| Embedding service | **DONE** | `crates/coordinator/src/embedding.rs` |
| Batch processing | **DONE** | `crates/coordinator/src/batch.rs` |
| Common types/config | **DONE** | `crates/common/` |
| Benchmark crate | **DONE** | `crates/benchmark/` |
| K8s manifests (reference) | **DONE** | `deploy/kubernetes/` |
| Ansible structure | **PARTIAL** | `deploy/ansible/` |

### Not Started (Phase 2 Hybrid Ingestion)

| Component | Status | New Location |
|-----------|--------|--------------|
| Ingestion orchestrator (Rust) | **NOT STARTED** | `crates/ingestion-orchestrator/` |
| Rust parsers (JSON, CSV, HTML, XML, XLSX) | **NOT STARTED** | `crates/ingestion-orchestrator/src/parsers/` |
| Semantic chunker | **NOT STARTED** | `crates/ingestion-orchestrator/src/chunker/` |
| Circuit breaker | **NOT STARTED** | `crates/ingestion-orchestrator/src/circuit_breaker.rs` |
| Memory coordinator | **NOT STARTED** | `crates/ingestion-orchestrator/src/memory.rs` |
| AkiDB-latency backpressure | **NOT STARTED** | `crates/ingestion-orchestrator/src/backpressure.rs` |
| Dynamic batcher | **NOT STARTED** | `crates/ingestion-orchestrator/src/batcher.rs` |
| NATS client | **NOT STARTED** | `crates/ingestion-orchestrator/src/nats.rs` |
| Python parser service | **NOT STARTED** | `services/doc-parser/` |
| Upload gateway | **NOT STARTED** | `services/upload-gateway/` |
| Quadlet files | **NOT STARTED** | `deploy/quadlets/` |

---

## Timeline Overview

```
------------------------------------------------------------------------------
                      AKIDB THOR IMPLEMENTATION TIMELINE v1.3
------------------------------------------------------------------------------

  Week 0       | Weeks 1-6     | Weeks 7-16     | Weeks 17-22  | Weeks 23-26
  +---------+  | +----------+  | +------------+ | +----------+ | +----------+
  |VALIDATION|  | | PHASE 1  |  | |  PHASE 2   | | | PHASE 3  | | | PHASE 4  |
  | SPRINT  |  | |Foundation|  | |  HYBRID    | | | Optimize | | |Production|
  | (1 week)|  | | (6 weeks)|  | | INGESTION  | | | (6 weeks)| | | (4 weeks)|
  +---------+  | +----------+  | | (10 weeks) | | +----------+ | +----------+
               |               | +------------+ |              |
  Hardware     | ~70% COMPLETE | Rust Orch.    | TensorRT     | cuVS
  Podman + CDI | (verify only) | + Python Parse| Rebuild      | Production
  NATS 3-node  |               | + Resilience  | Performance  | Quadlets
  Dockerfile   |               | + NATS 3-node |              |

------------------------------------------------------------------------------
```

**Total Duration:** 26-27 weeks (~6.5 months) - *Extended by 2 weeks for hybrid complexity*

---

## Phase 0: Validation Sprint (Week 0)

### Objectives
- Validate hardware environment
- Verify existing Phase 1 implementation
- Test NATS on ARM64

### Validation Tasks

| ID | Task | Owner | Duration | Exit Criteria |
|----|------|-------|----------|---------------|
| V-01 | Verify Thor hardware specs | DevOps | 0.5 day | 64GB unified memory confirmed |
| V-02 | Test GPU passthrough with CDI | DevOps | 1 day | nvidia-smi works inside container |
| V-03 | Validate Podman 4.0+ on Thor | DevOps | 0.5 day | podman run hello-world succeeds |
| V-04 | Test NATS 2.10 on ARM64 | DevOps | 0.5 day | NATS server runs, JetStream enabled |
| V-05 | Verify existing FAISS build | Rust Eng | 1 day | cargo test in faiss-wrapper passes |
| V-06 | Verify existing coordinator | Rust Eng | 1 day | cargo test in coordinator passes |
| V-07 | Benchmark single-node FAISS | ML Eng | 1 day | IVF-Flat search <10ms for 1M vectors |
| V-08 | Test Python 3.11 runtime | DevOps | 0.5 day | python3 --version succeeds |
| V-09 | Validate network latency | DevOps | 0.5 day | <1ms inter-node latency |
| V-10 | Test tegrastats availability | DevOps | 0.5 day | tegrastats returns memory stats |

### Phase 0 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| GPU passthrough | nvidia-smi inside container | [ ] |
| NATS on ARM64 | JetStream operational | [ ] |
| Existing tests pass | cargo test all-green | [ ] |
| Network latency | <1ms inter-node | [ ] |
| tegrastats access | Memory stats readable | [ ] |

---

## Phase 1: Foundation Completion (Weeks 1-6)

### Status: ~70% Complete

Most of Phase 1 is already implemented. This phase focuses on verification, documentation, and completing missing pieces.

### Objectives
- Verify and document existing implementation
- Complete any missing Phase 1 items
- Prepare for Phase 2 hybrid ingestion

### Sprint 1-2 (Weeks 1-4): Verification & Completion

| ID | Task | Priority | Estimate | Status |
|----|------|----------|----------|--------|
| P1-01 | Document existing FAISS wrapper | P1 | 2d | NEW |
| P1-02 | Document coordinator architecture | P1 | 2d | NEW |
| P1-03 | Verify gRPC API contract | P0 | 1d | NEW |
| P1-04 | Complete Dockerfile for server | P0 | 2d | VERIFY |
| P1-05 | Test distributed fan-out | P0 | 3d | VERIFY |
| P1-06 | Performance baseline benchmarks | P0 | 3d | NEW |
| P1-07 | Fix any failing tests | P0 | 2d | AS NEEDED |
| P1-08 | CI/CD pipeline setup | P0 | 3d | NEW |

### Sprint 3 (Weeks 5-6): Pre-Ingestion Setup

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-09 | Create ingestion-orchestrator crate scaffold | P0 | 2d | - |
| P1-10 | Setup services/ directory structure | P0 | 1d | - |
| P1-11 | Create deploy/quadlets/ directory | P0 | 1d | - |
| P1-12 | NATS configuration for 3-node cluster | P0 | 2d | V-04 |
| P1-13 | MinIO bucket notification setup | P0 | 2d | - |
| P1-14 | Prometheus/Grafana dashboards | P1 | 2d | - |

### Phase 1 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| All existing tests pass | 100% green | [ ] |
| Single-node search P95 | < 10ms | [ ] |
| gRPC API documented | OpenAPI spec | [ ] |
| CI/CD operational | GitHub Actions | [ ] |
| Baseline benchmarks | Documented | [ ] |
| ingestion-orchestrator scaffold | Cargo builds | [ ] |

---

## Phase 2: Hybrid Ingestion Pipeline (Weeks 7-16) - MAJOR UPDATE

### Objectives
- Implement Rust ingestion orchestrator
- Create Python parser service sidecar
- Deploy NATS 3-node JetStream cluster
- Implement all resilience patterns (circuit breaker, backpressure, memory)
- Create upload gateway
- Achieve 30-minute upload-to-searchable SLO

### Crate Structure

```
crates/ingestion-orchestrator/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── main.rs              # Binary entry point
│   ├── config.rs            # Configuration
│   ├── nats/
│   │   ├── mod.rs
│   │   ├── consumer.rs      # NATS JetStream consumer
│   │   └── publisher.rs     # DLQ publisher
│   ├── parsers/
│   │   ├── mod.rs           # Parser trait + router
│   │   ├── json.rs          # serde_json
│   │   ├── csv.rs           # csv crate
│   │   ├── html.rs          # scraper
│   │   ├── xml.rs           # quick-xml
│   │   ├── xlsx.rs          # calamine
│   │   └── docx.rs          # docx-rs (simple)
│   ├── python_client/
│   │   ├── mod.rs
│   │   └── http.rs          # HTTP client to Python parser
│   ├── circuit_breaker.rs   # ADR-020 circuit breaker
│   ├── backpressure.rs      # ADR-020 AkiDB-latency backpressure
│   ├── memory.rs            # ADR-020 tegrastats memory coordinator
│   ├── chunker/
│   │   ├── mod.rs
│   │   └── semantic.rs      # Sentence-boundary chunking
│   ├── batcher/
│   │   ├── mod.rs
│   │   └── dynamic.rs       # Queue-depth adaptive batching
│   ├── embedding.rs         # TensorRT-LLM client
│   ├── idempotency.rs       # Content-hash deduplication
│   ├── state.rs             # SQLite document state tracker
│   ├── metrics.rs           # Prometheus metrics
│   └── pipeline.rs          # Main orchestration logic
```

### Python Parser Service Structure

```
services/doc-parser/
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── parser/
│   ├── __init__.py
│   ├── main.py           # FastAPI app
│   ├── config.py         # Settings
│   ├── routers/
│   │   └── parse.py      # POST /parse endpoint
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base.py       # Abstract parser
│   │   ├── pdf.py        # pdfplumber
│   │   ├── docx.py       # python-docx (complex)
│   │   └── enl.py        # EndNote parser
│   └── health.py         # Health endpoints
└── tests/
    └── test_parsers.py
```

### Sprint Breakdown

#### Sprint 4 (Weeks 7-8): NATS + Rust Orchestrator Foundation

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-01 | Deploy NATS 3-node JetStream cluster | P0 | 2d | P1-12 |
| P2-02 | Create NATS quadlet files | P0 | 1d | P2-01 |
| P2-03 | NATS consumer in Rust (async_nats) | P0 | 3d | P2-01 |
| P2-04 | MinIO event notification → NATS | P0 | 1d | P2-01 |
| P2-05 | Basic orchestrator pipeline scaffold | P0 | 2d | P1-09 |
| P2-06 | Configuration loading (envconfig) | P1 | 1d | P2-05 |

**Sprint 4 Exit Criteria:**
- [ ] NATS 3-node cluster running on Thor 1-3
- [ ] MinIO uploads trigger NATS events
- [ ] Rust consumer receives messages

#### Sprint 5 (Weeks 9-10): Rust Parsers + Format Router

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-07 | Format router (extension-based) | P0 | 1d | P2-05 |
| P2-08 | JSON parser (serde_json) | P0 | 1d | P2-07 |
| P2-09 | CSV parser (csv crate) | P0 | 1d | P2-07 |
| P2-10 | HTML parser (scraper) | P0 | 2d | P2-07 |
| P2-11 | XML parser (quick-xml) | P0 | 2d | P2-07 |
| P2-12 | XLSX parser (calamine) | P0 | 2d | P2-07 |
| P2-13 | Simple DOCX parser (docx-rs) | P1 | 2d | P2-07 |
| P2-14 | Parser unit tests | P0 | 1d | P2-08..P2-13 |

**Sprint 5 Exit Criteria:**
- [ ] JSON, CSV, HTML, XML, XLSX parsed in Rust
- [ ] Format router correctly routes by extension
- [ ] Parser tests passing

#### Sprint 6 (Weeks 11-12): Python Parser Service

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-15 | Create doc-parser FastAPI scaffold | P0 | 1d | - |
| P2-16 | PDF parser (pdfplumber) | P0 | 2d | P2-15 |
| P2-17 | Complex DOCX parser (python-docx) | P0 | 2d | P2-15 |
| P2-18 | ENL parser (custom) | P1 | 2d | P2-15 |
| P2-19 | HTTP client in Rust orchestrator | P0 | 2d | P2-15 |
| P2-20 | Python parser Dockerfile | P0 | 1d | P2-15 |
| P2-21 | Python parser quadlet | P0 | 0.5d | P2-20 |

**Sprint 6 Exit Criteria:**
- [ ] PDF, complex DOCX, ENL parsed in Python
- [ ] Rust orchestrator calls Python parser via HTTP
- [ ] Python parser container runs

#### Sprint 7 (Weeks 13-14): Resilience Patterns (ADR-020)

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-22 | Circuit breaker for Python parser | P0 | 3d | P2-19 |
| P2-23 | Circuit breaker state metrics | P0 | 1d | P2-22 |
| P2-24 | AkiDB-latency backpressure controller | P0 | 3d | P2-05 |
| P2-25 | NATS consumption throttling | P0 | 1d | P2-24 |
| P2-26 | Memory coordinator (tegrastats) | P0 | 2d | - |
| P2-27 | Resilience integration tests | P0 | 2d | P2-22..P2-26 |

**Circuit Breaker Implementation:**
```rust
// crates/ingestion-orchestrator/src/circuit_breaker.rs
pub struct CircuitBreaker {
    state: AtomicU8,              // 0=Closed, 1=Open, 2=HalfOpen
    failure_count: AtomicUsize,
    last_failure: AtomicU64,
    config: CircuitBreakerConfig,
}

pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,     // 3 consecutive failures
    pub reset_timeout: Duration,      // 30 seconds
    pub half_open_max_calls: usize,   // 1 test call
}
```

**Backpressure Implementation:**
```rust
// crates/ingestion-orchestrator/src/backpressure.rs
pub struct IngestionBackpressure {
    akidb_client: AkiDBClient,
    nats_consumer: Consumer,
    config: BackpressureConfig,
}

pub struct BackpressureConfig {
    pub insert_latency_threshold_ms: u64,  // 500ms
    pub queue_depth_high_water: usize,     // 10000
    pub pause_duration: Duration,          // 5 seconds
}
```

**Sprint 7 Exit Criteria:**
- [ ] Circuit breaker transitions work (CLOSED → OPEN → HALF-OPEN → CLOSED)
- [ ] Backpressure pauses on high AkiDB latency
- [ ] Memory coordinator pauses at 70% unified memory

#### Sprint 8 (Weeks 15-16): Chunking, Batching, Integration

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-28 | Semantic chunker (unicode-segmentation) | P0 | 3d | - |
| P2-29 | Dynamic batcher (queue-depth adaptive) | P0 | 2d | - |
| P2-30 | TensorRT-LLM embedding client | P0 | 2d | - |
| P2-31 | Idempotency layer (content-hash) | P0 | 2d | - |
| P2-32 | Document state tracker (SQLite) | P1 | 2d | - |
| P2-33 | Dead letter queue handler | P1 | 1d | P2-03 |
| P2-34 | Upload gateway (FastAPI) | P0 | 3d | - |
| P2-35 | Pre-signed URL generation | P0 | 1d | P2-34 |
| P2-36 | End-to-end integration test | P0 | 3d | ALL |

**Semantic Chunker:**
```rust
// crates/ingestion-orchestrator/src/chunker/semantic.rs
pub struct SemanticChunker {
    target_tokens: usize,      // 512
    min_overlap: usize,        // 20
    max_overlap: usize,        // 50
}

impl SemanticChunker {
    pub fn chunk(&self, text: &str) -> Vec<Chunk> {
        // 1. Split into sentences (unicode-segmentation)
        // 2. Group sentences into chunks near target_tokens
        // 3. Apply overlap at sentence boundaries
    }
}
```

**Dynamic Batcher:**
```rust
// crates/ingestion-orchestrator/src/batcher/dynamic.rs
pub struct DynamicBatcher {
    min_batch: usize,   // 16
    max_batch: usize,   // 64
}

impl DynamicBatcher {
    pub fn optimal_size(&self, queue_depth: usize, gpu_util: f32) -> usize {
        // Linear interpolation based on queue depth
        // Reduce by 50% if GPU util > 80%
    }
}
```

**Sprint 8 Exit Criteria:**
- [ ] Semantic chunking produces sentence-boundary chunks
- [ ] Dynamic batching adjusts to queue depth
- [ ] End-to-end: Upload → Parse → Chunk → Embed → Search works
- [ ] 30-minute SLO validated

### Phase 2 Deliverables

| Deliverable | Description |
|-------------|-------------|
| `crates/ingestion-orchestrator/` | Rust orchestrator with all resilience patterns |
| `services/doc-parser/` | Python parser service (FastAPI) |
| `services/upload-gateway/` | Upload gateway with pre-signed URLs |
| `deploy/quadlets/nats.container` | NATS JetStream quadlet |
| `deploy/quadlets/doc-parser.container` | Python parser quadlet |
| `deploy/quadlets/ingestion-orchestrator.container` | Rust orchestrator quadlet |
| `deploy/quadlets/upload-gateway.container` | Upload gateway quadlet |

### Phase 2 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| NATS 3-node cluster | Quorum operational | [ ] |
| Rust parsing ratio | 60-70% of documents | [ ] |
| Circuit breaker | All state transitions tested | [ ] |
| Backpressure | Throttles on AkiDB latency >500ms | [ ] |
| Memory coordinator | Pauses at 70% unified memory | [ ] |
| Semantic chunking | ~512 tokens, sentence boundaries | [ ] |
| Upload → Search SLO | < 30 minutes (P95) | [ ] |
| Prometheus metrics | All metrics exported | [ ] |

---

## Phase 3: Optimization (Weeks 17-22)

### Objectives
- Integrate TensorRT-optimized models
- Implement index rebuild
- Performance tuning
- Ingestion optimization

### Sprint 9-10 (Weeks 17-20): TensorRT + Index Rebuild

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-01 | TensorRT model optimization | P0 | 4d | Phase 2 |
| P3-02 | Index rebuild strategy | P0 | 5d | Phase 1 |
| P3-03 | Async rebuild with zero downtime | P0 | 4d | P3-02 |
| P3-04 | Compaction scheduling | P1 | 3d | P3-02 |
| P3-05 | Performance profiling | P0 | 2d | ALL |

### Sprint 11-12 (Weeks 21-22): Ingestion Optimization + Hardening

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-06 | Optimize embedding batch sizes | P0 | 2d | Phase 2 |
| P3-07 | Add ENL parser to Python service | P2 | 2d | Phase 2 |
| P3-08 | Ingestion load testing (1000 docs/hr) | P0 | 3d | Phase 2 |
| P3-09 | Thermal throttling (batch reduction) | P1 | 2d | P2-26 |
| P3-10 | Cold start handling (503 until ready) | P1 | 1d | - |
| P3-11 | DLQ auto-recovery cron | P1 | 1d | P2-33 |
| P3-12 | Security hardening (pre-signed URLs) | P0 | 2d | P2-35 |

### Phase 3 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Search P95 latency | < 10ms | [ ] |
| Ingestion throughput | 1000 docs/hr | [ ] |
| Index rebuild | Zero-downtime | [ ] |
| TensorRT inference | < 20ms per batch | [ ] |
| Security review | Passed | [ ] |

---

## Phase 4: Production Deployment (Weeks 23-26)

### Objectives
- Complete quadlet deployment
- Production monitoring
- Documentation
- Handoff

### Sprint 13-14 (Weeks 23-26): Production Readiness

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-01 | Finalize all quadlet files | P0 | 2d | Phase 2 |
| P4-02 | Ansible playbook for full deployment | P0 | 3d | P4-01 |
| P4-03 | Grafana dashboards (all 4 required) | P0 | 2d | - |
| P4-04 | Alerting rules (all thresholds from PRD) | P0 | 2d | P4-03 |
| P4-05 | Production load testing | P0 | 3d | ALL |
| P4-06 | Runbook documentation | P0 | 2d | - |
| P4-07 | cuVS evaluation gate | P1 | 3d | - |
| P4-08 | Security penetration test | P0 | 2d | - |
| P4-09 | Final sign-off checklist | P0 | 1d | ALL |

### Quadlet Files (v1.3)

#### nats.container

```ini
[Unit]
Description=NATS JetStream Server (Thor Cluster)
After=network-online.target
Wants=network-online.target

[Container]
Image=nats:2.10-alpine
ContainerName=nats
Environment=NATS_CONFIG=/etc/nats/nats.conf
Volume=/etc/akidb/nats:/etc/nats:ro,Z
Volume=/var/lib/nats:/data:Z
PublishPort=4222:4222
PublishPort=6222:6222
PublishPort=8222:8222
Network=host
HealthCmd=wget -q --spider http://localhost:8222/healthz
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=5
TimeoutStartSec=30

[Install]
WantedBy=multi-user.target
```

#### ingestion-orchestrator.container

```ini
[Unit]
Description=AkiDB Ingestion Orchestrator (Rust)
After=network-online.target nats.service akidb-shard.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/ingestion-orchestrator:latest
ContainerName=ingestion-orchestrator
Environment=NATS_URL=nats://localhost:4222
Environment=MINIO_ENDPOINT=localhost:9000
Environment=MINIO_ACCESS_KEY_FILE=/run/secrets/minio-access-key
Environment=MINIO_SECRET_KEY_FILE=/run/secrets/minio-secret-key
Environment=AKIDB_COORDINATOR=localhost:50051
Environment=TENSORRT_URL=http://localhost:8001
Environment=DOC_PARSER_URL=http://localhost:8080
Environment=CIRCUIT_BREAKER_THRESHOLD=3
Environment=CIRCUIT_BREAKER_RESET_SECS=30
Environment=BACKPRESSURE_LATENCY_THRESHOLD_MS=500
Environment=MEMORY_PAUSE_THRESHOLD_PCT=70
Volume=/etc/akidb/secrets:/run/secrets:ro,Z
Volume=/var/lib/akidb/state:/var/lib/akidb:Z
Network=host
AddDevice=nvidia.com/gpu=all
SecurityLabelDisable=true
HealthCmd=curl -f http://localhost:9090/health
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=10
TimeoutStartSec=120
MemoryMax=2G

[Install]
WantedBy=multi-user.target
```

#### doc-parser.container

```ini
[Unit]
Description=AkiDB Document Parser (Python)
After=network-online.target
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/doc-parser:latest
ContainerName=doc-parser
Environment=PYTHONUNBUFFERED=1
Environment=MAX_FILE_SIZE_MB=100
Environment=PARSE_TIMEOUT_SECS=60
Volume=/tmp/doc-parser:/tmp:Z
Network=host
HealthCmd=curl -f http://localhost:8080/health
HealthInterval=30s
HealthTimeout=10s
HealthRetries=3

[Service]
Restart=always
RestartSec=10
TimeoutStartSec=60
MemoryMax=2G

[Install]
WantedBy=multi-user.target
```

#### upload-gateway.container

```ini
[Unit]
Description=AkiDB Upload Gateway
After=network-online.target minio.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/upload-gateway:latest
ContainerName=upload-gateway
Environment=MINIO_ENDPOINT=localhost:9000
Environment=MINIO_ACCESS_KEY_FILE=/run/secrets/minio-access-key
Environment=MINIO_SECRET_KEY_FILE=/run/secrets/minio-secret-key
Environment=UPLOAD_BUCKET=uploads
Environment=PRESIGNED_URL_EXPIRY=900
Environment=MAX_FILE_SIZE_MB=100
Volume=/etc/akidb/secrets:/run/secrets:ro,Z
Network=host
HealthCmd=curl -f http://localhost:8000/health
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=5
TimeoutStartSec=60

[Install]
WantedBy=multi-user.target
```

### Phase 4 Exit Gate (Production Readiness Checklist)

#### Critical (Must Pass)

| ID | Item | Owner | Status |
|----|------|-------|--------|
| C-01 | NATS 3-node cluster deployed | Infra | [ ] |
| C-02 | Circuit breaker implemented | Dev | [ ] |
| C-03 | Backpressure mechanism tested | QA | [ ] |
| C-04 | Memory coordinator active | Dev | [ ] |
| C-05 | Core metrics exported | Ops | [ ] |
| C-06 | 30-min SLO validated | QA | [ ] |
| C-07 | GPU passthrough working | Infra | [ ] |

#### High Priority (Strongly Recommended)

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

## Critical Path Dependencies (v1.3)

```
------------------------------------------------------------------------------
                  CRITICAL PATH DEPENDENCY DAG v1.3
------------------------------------------------------------------------------

  Week 0: Hardware + Podman/CDI + NATS + tegrastats Validation
              │
              ▼
  Phase 1: Verify existing code ──► Scaffold ingestion-orchestrator
              │
              ▼
  Phase 2: ┌──────────────────────────────────────────────────────────┐
           │                                                          │
           │  NATS 3-node ──► MinIO Events ──┐                       │
           │                                  │                       │
           │  Rust Parsers ──────────────────┼──► Format Router      │
           │                                  │         │             │
           │  Python Parser ─────────────────┼──► Circuit Breaker    │
           │                                  │         │             │
           │                                  ▼         ▼             │
           │               Orchestrator Pipeline                      │
           │                       │                                  │
           │   ┌───────────────────┼───────────────────┐             │
           │   │                   │                   │             │
           │   ▼                   ▼                   ▼             │
           │  Backpressure   Memory Coord    Semantic Chunker        │
           │       │               │               │                  │
           │       └───────────────┴───────────────┘                  │
           │                       │                                  │
           │                       ▼                                  │
           │               Dynamic Batcher ──► TensorRT              │
           │                       │                                  │
           │                       ▼                                  │
           │               AkiDB Insert                               │
           │                                                          │
           └──────────────────────────────────────────────────────────┘
              │
              ▼
  Phase 3: TensorRT Optimization ──► Index Rebuild ──► Load Testing
              │
              ▼
  Phase 4: Quadlets ──► Ansible ──► Monitoring ──► Sign-off

------------------------------------------------------------------------------
```

---

## Team Allocation (v1.3)

| Role | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| **Rust Engineer 1** | Verify FAISS | Doc, CI/CD | **Orchestrator, Parsers** | Rebuild | Quadlets |
| **Rust Engineer 2** | Verify coord | Tests | **Circuit breaker, Backpressure** | Perf | Hardening |
| **Python Engineer** | - | - | **doc-parser, upload-gateway** | ENL | Testing |
| **ML Engineer** | FAISS bench | - | Chunker, Batcher | TensorRT | cuVS |
| **DevOps** | HW, Podman, **NATS** | CI/CD | **Memory coord, tegrastats** | Load test | Ansible |

---

## Technology Stack (v1.3)

### Core (Rust)
- FAISS 1.8+ (GPU IVF-Flat)
- RocksDB 7.8+
- Tonic (gRPC)
- Tokio (async runtime)
- async_nats (NATS client)
- calamine (XLSX)
- scraper (HTML)
- quick-xml (XML)
- unicode-segmentation (sentence splitting)
- SQLite (document state)

### Ingestion Services (Python)
- Python 3.11
- FastAPI 0.109+
- pdfplumber 0.10+
- python-docx 1.1+
- Uvicorn

### Infrastructure
- Podman 4.0+
- Systemd quadlets
- NATS JetStream 2.10+ (3-node)
- MinIO (distributed)
- Prometheus + Grafana

---

## Deliverables Summary (v1.3)

| Phase | Deliverable | Description |
|-------|-------------|-------------|
| 0 | Validation report | Hardware + NATS + tegrastats confirmed |
| 1 | Verified codebase | Existing code documented + tested |
| 2 | Rust orchestrator | `crates/ingestion-orchestrator/` |
| 2 | Python parser | `services/doc-parser/` |
| 2 | Upload gateway | `services/upload-gateway/` |
| 2 | NATS 3-node | JetStream cluster operational |
| 2 | Resilience patterns | Circuit breaker, backpressure, memory |
| 3 | TensorRT models | Optimized inference |
| 3 | Index rebuild | Zero-downtime rebuild |
| 4 | Quadlets | Full production deployment |
| 4 | Dashboards | 4 Grafana dashboards |
| 4 | Runbook | Operations documentation |

---

## Open Questions (v1.3)

### Resolved

| ID | Question | Resolution |
|----|----------|------------|
| Q7 | Document parsing approach? | Hybrid: Rust orchestrator + Python parser |
| Q8 | Message queue? | NATS JetStream 3-node |
| Q9 | Chunking strategy? | Semantic (sentence-boundary) |
| Q10 | NATS cluster size? | 3-node (not 4) |
| Q11 | XLSX parser? | Rust (calamine) |

### Remaining

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q12 | TensorRT vs vLLM for embedding? | TensorRT (primary) | Phase 3 |
| Q13 | Malware scanning for uploads? | ClamAV vs cloud API | Phase 3 |
| Q14 | OCR for scanned PDFs? | Tesseract vs cloud | Phase 3 |
| Q15 | cuVS replacement for FAISS? | Depends on benchmarks | Phase 4 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial implementation plan |
| 1.1 | 2026-01-21 | AkiDB Team | Added Podman + quadlets deployment |
| 1.2 | 2026-01-21 | AkiDB Team | Added Python Ingestion Service, NATS, Upload Gateway |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid architecture, NATS 3-node, resilience patterns, semantic chunking |

---

*End of Implementation Plan v1.3*
