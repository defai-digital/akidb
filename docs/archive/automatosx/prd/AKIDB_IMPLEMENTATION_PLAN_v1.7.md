# AkiDB Thor Edition - Implementation Plan

**Version:** 1.7
**Date:** 2026-01-21
**Status:** Approved
**Based On:** ADR v1.6, PRD v1.6, Phase 0 Validation Report
**Review:** Multi-model synthesis (Claude, Gemini, Grok)
**Changes from v1.6:** Phase 0 COMPLETE with validated metrics, actual hardware/software specs

---

## Change Log from v1.6

| Section | Change | Rationale |
|---------|--------|-----------|
| Phase 0 | Marked as **✅ COMPLETE** | All 10 validation tasks passed |
| Hardware | Updated to **actual specs** (128GB, not 64GB) | Phase 0 V-01 validation |
| Software | Actual versions documented | CUDA 13.0, Python 3.12.3, Docker 28.2.2 |
| Metrics | **Validated performance** incorporated | 2.9ms search, 344 QPS, 15.9K vec/sec |
| Tests | **84 tests passing** documented | All crates verified |
| Phase 1 | Updated to **75% complete** | Based on actual codebase review |
| Timeline | Adjusted to **24 weeks remaining** | Phase 0 complete, Phase 1 at 75% |
| Services | Actual running services documented | Coordinator, servers, vLLM, MinIO |

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition. **Phase 0 Validation Sprint is COMPLETE** with all criteria met. The project uses **Docker + docker-compose** deployment aligned with NVIDIA's official Jetson Thor documentation.

**Validated Configuration (Phase 0):**
- Hardware: 2x NVIDIA Jetson AGX Thor, **128GB** unified memory each
- Container Runtime: Docker 28.2.2 + nvidia-container-runtime
- GPU: NVIDIA Thor, **CUDA 13.0**, Driver 580.00
- Embedding: Qwen3-Embedding-8B via vLLM 25.11 (deployed)
- Message Queue: NATS 2.10.29 JetStream (validated on ARM64)
- Network Latency: **0.81ms** inter-node (target <1ms)

**Validated Performance (Mock Mode):**
- Search Latency: **2.90ms** avg, 4.74ms P95 (target <10ms)
- Search QPS: **344** (target >100)
- Insert Throughput: **15,905 vec/sec** (target >10K)
- Test Suite: **84 tests passing**, 0 failures

---

## Progress Summary

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 0: Validation | ✅ **COMPLETE** | 10/10 tasks |
| Phase 1: Foundation | 🔄 In Progress | ~75% |
| Phase 2: Hybrid Ingestion | ❌ Not Started | 0% |
| Phase 3: Optimization | ❌ Blocked | 0% |
| Phase 4: Production | ❌ Blocked | 0% |

---

## Phase 0: Validation Sprint ✅ COMPLETE

### Exit Gate: ALL PASSED

| ID | Task | Target | Actual | Status |
|----|------|--------|--------|--------|
| V-01 | Thor hardware specs | 64GB memory | **128GB** | ✅ EXCEEDED |
| V-02 | GPU passthrough | nvidia-smi in container | CUDA 13.0 works | ✅ PASS |
| V-03 | Container runtime | Podman/Docker | Docker 28.2.2 | ✅ PASS |
| V-04 | NATS on ARM64 | JetStream operational | v2.10.29 works | ✅ PASS |
| V-05 | FAISS build | Tests pass | 23/23 pass | ✅ PASS |
| V-06 | Coordinator build | Tests pass | 45/45 pass | ✅ PASS |
| V-07 | FAISS benchmark | <10ms search | **2.9ms** | ✅ EXCEEDED |
| V-08 | Python runtime | 3.11+ | **3.12.3** | ✅ EXCEEDED |
| V-09 | Network latency | <1ms | **0.81ms** | ✅ PASS |
| V-10 | tegrastats | Available | /usr/bin/tegrastats | ✅ PASS |

### Validated Hardware Specifications

| Spec | thor-01 | thor-02 |
|------|---------|---------|
| IP Address | 192.168.1.61 | 192.168.1.62 |
| Memory | 128,790,772 KB (123 GB) | 128,790,776 KB (123 GB) |
| CPU Cores | 14 | 14 |
| GPU | NVIDIA Thor | NVIDIA Thor |
| CUDA Version | 13.0 | 13.0 |
| Driver Version | 580.00 | 580.00 |
| Ubuntu | 24.04 | 24.04 |
| Docker | 28.2.2 | 28.2.2 |
| Python | 3.12.3 | 3.12.3 |

### Validated Performance Metrics

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Search Avg Latency | **2.90 ms** | < 10 ms | ✅ PASS |
| Search P50 | 2.45 ms | - | ✅ |
| Search P95 | 4.74 ms | - | ✅ |
| Search P99 | 6.26 ms | - | ✅ |
| Search QPS | **344** | > 100 | ✅ PASS |
| SLO Compliance | **100%** | > 99% | ✅ PASS |
| Insert Throughput | **15,905 vec/sec** | > 10,000 | ✅ PASS |
| Insert P50 | 254 µs | < 5 ms | ✅ PASS |
| Network Latency | **0.81 ms** | < 1 ms | ✅ PASS |

*Note: Metrics from mock mode. GPU mode expected to be faster.*

### Running Services (Validated)

| Node | Service | Port | Status |
|------|---------|------|--------|
| thor-01 | akidb-coordinator | 50050 | ✅ Running |
| thor-01 | akidb-server | 50051 | ✅ Running |
| thor-01 | qwen3-embed (vLLM) | 8000 | ✅ Running |
| thor-01 | MinIO | 9000/9001 | ✅ Running |
| thor-02 | akidb-server | 50051 | ✅ Running |
| thor-02 | qwen3-embed (vLLM) | 8000 | ✅ Running |
| thor-02 | MinIO | 9000/9001 | ✅ Running |

---

## Phase 1: Foundation ~75% COMPLETE

### Test Suite Summary

| Package | Tests | Status |
|---------|-------|--------|
| akidb-faiss | 23 | ✅ PASS |
| akidb-coordinator | 45 | ✅ PASS |
| akidb-storage | 12 | ✅ PASS |
| akidb-common | 4 | ✅ PASS |
| akidb-grpc | 0* | ✅ (integration tested) |
| akidb-server | 0* | ✅ (binary) |
| akidb-benchmark | 0* | ✅ (binary) |
| **TOTAL** | **84** | **✅ ALL PASS** |

### Crate Structure (VERIFIED)

```
crates/
├── faiss-wrapper/          ✅ 23 tests
│   ├── src/
│   │   ├── lib.rs          # Feature flags (cpu, gpu, cuvs)
│   │   ├── cpu.rs          # CPU FAISS implementation
│   │   ├── gpu.rs          # GPU FAISS FFI bindings
│   │   ├── ffi.rs          # C++ FFI declarations
│   │   ├── mock.rs         # Mock index for testing
│   │   ├── index.rs        # Index traits
│   │   ├── tombstone.rs    # Tombstone bitset
│   │   ├── rebuild.rs      # Index rebuild logic
│   │   └── cuvs.rs         # cuVS preparation
│   └── cpp/
│       ├── faiss_wrapper.h
│       └── faiss_wrapper.cpp
├── storage/                 ✅ 12 tests
│   └── src/
│       ├── lib.rs
│       ├── backend.rs      # RocksDB backend
│       ├── id_mapping.rs   # External→Internal ID map
│       ├── wal.rs          # Write-ahead log
│       └── snapshot.rs     # Snapshot management
├── grpc-server/             ✅ Implemented
│   ├── src/
│   │   ├── lib.rs
│   │   ├── service.rs      # gRPC service impl
│   │   └── metrics.rs      # Prometheus metrics
│   └── proto/
│       └── akidb.proto     # 8 RPCs defined
├── coordinator/             ✅ 45 tests
│   ├── src/
│   │   ├── lib.rs
│   │   ├── router.rs       # Consistent hash (150 vnodes)
│   │   ├── fanout.rs       # Parallel shard queries
│   │   ├── merger.rs       # Min-heap result merge
│   │   ├── batch.rs        # Adaptive batch processor
│   │   ├── embedding.rs    # vLLM client + cache
│   │   ├── backpressure.rs # Generic backpressure
│   │   ├── consistency.rs  # Write tracking
│   │   ├── compaction.rs   # Compaction scheduler
│   │   ├── slo.rs          # SLO estimation
│   │   └── metrics.rs      # Coordinator metrics
│   └── bin/
│       └── coordinator.rs
├── common/                  ✅ 4 tests
│   └── src/
│       ├── lib.rs
│       ├── types.rs        # VectorId, SearchResult
│       ├── config.rs       # AkiDbConfig
│       └── error.rs        # AkiDbError
├── server/                  ✅ Binary
│   └── src/main.rs
└── benchmark/               ✅ Binary
    └── src/main.rs
```

### gRPC API (8 RPCs - ALL IMPLEMENTED)

```protobuf
service Akidb {
  rpc Insert(InsertRequest) returns (InsertResponse);           ✅
  rpc Search(SearchRequest) returns (SearchResponse);           ✅
  rpc Delete(DeleteRequest) returns (DeleteResponse);           ✅
  rpc Update(UpdateRequest) returns (UpdateResponse);           ✅
  rpc Get(GetRequest) returns (GetResponse);                    ✅
  rpc Health(HealthRequest) returns (HealthResponse);           ✅
  rpc InsertBatch(InsertBatchRequest) returns (InsertBatchResponse);   ✅
  rpc SearchBatch(SearchBatchRequest) returns (SearchBatchResponse);   ✅
}
```

### Deployment Structure (VERIFIED)

```
deploy/
├── ansible/                 ✅ Partial
│   ├── inventory.yml
│   ├── ansible.cfg
│   ├── playbooks/
│   │   ├── setup.yml
│   │   ├── deploy.yml
│   │   └── validate.yml
│   └── templates/
│       ├── akidb.service.j2
│       ├── akidb.toml.j2
│       └── minio.service.j2
├── kubernetes/              ✅ Reference only
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── shard-statefulset.yaml
│   ├── coordinator-deployment.yaml
│   ├── monitoring.yaml
│   └── kustomization.yaml
├── grafana/                 ✅ Basic dashboard
│   └── akidb-dashboard.json
├── prometheus/              ✅ Config + rules
│   ├── prometheus.yml
│   └── akidb_rules.yml
├── minio/                   ✅ Compose file
│   └── docker-compose.yml
├── bin/                     ✅ Deployed binaries
│   ├── akidb-coordinator
│   └── akidb-bench
└── deploy-coordinator.sh    ✅ Deployment script
```

### Remaining Phase 1 Tasks

| ID | Task | Priority | Estimate | Status |
|----|------|----------|----------|--------|
| P1-01 | Scaffold `crates/ingestion-orchestrator/` | P0 | 1d | TODO |
| P1-02 | Create `services/doc-parser/` structure | P0 | 0.5d | TODO |
| P1-03 | Create `services/upload-gateway/` structure | P0 | 0.5d | TODO |
| P1-04 | Create `deploy/compose/` directory | P0 | 0.5d | TODO |
| P1-05 | NATS 3-node configuration | P0 | 1d | TODO |
| P1-06 | MinIO bucket notification setup | P0 | 1d | TODO |
| P1-07 | CI/CD pipeline (GitHub Actions) | P1 | 2d | TODO |
| P1-08 | Enable GPU mode on Thor | P0 | 2d | TODO |

### Phase 1 Exit Gate

| Criteria | Target | Current | Status |
|----------|--------|---------|--------|
| All tests pass | 100% green | 84/84 ✅ | ✅ MET |
| Search P95 | < 10ms | 4.74ms | ✅ MET |
| Insert throughput | > 10K/sec | 15,905/sec | ✅ MET |
| gRPC API complete | All 8 RPCs | 8/8 ✅ | ✅ MET |
| ingestion-orchestrator scaffold | Cargo builds | ❌ | PENDING |
| NATS config ready | 3-node template | ❌ | PENDING |
| GPU mode enabled | Using GPU: true | ❌ | PENDING |

---

## Timeline Overview (Updated)

```
------------------------------------------------------------------------------
                      AKIDB THOR IMPLEMENTATION TIMELINE v1.7
------------------------------------------------------------------------------

  Week 0       | Weeks 1-2     | Weeks 3-12     | Weeks 13-18  | Weeks 19-24
  +---------+  | +----------+  | +------------+ | +----------+ | +----------+
  |VALIDATION|  | | PHASE 1  |  | |  PHASE 2   | | | PHASE 3  | | | PHASE 4  |
  | SPRINT  |  | |Complete  |  | |  HYBRID    | | | Optimize | | |Production|
  |✅COMPLETE|  | | (2 weeks)|  | | INGESTION  | | | (6 weeks)| | | (6 weeks)|
  +---------+  | +----------+  | | (10 weeks) | | +----------+ | +----------+
               |               | +------------+ |              |
  ✅ 2026-01-21| Scaffolds     | Rust Orch.    | GPU Mode     | Docker
               | NATS config   | Python Parse  | Rebuild      | Compose
               | GPU enable    | Resilience    | Performance  | Production

------------------------------------------------------------------------------
```

**Total Remaining:** ~24 weeks from Phase 1 completion
**Current Date:** 2026-01-21

---

## Phase 2: Hybrid Ingestion Pipeline (Weeks 3-12)

### Objectives
- Implement Rust ingestion orchestrator
- Create Python parser service sidecar
- Deploy NATS 3-node JetStream cluster
- Implement all resilience patterns
- Achieve 30-minute upload-to-searchable SLO

### New Crate: ingestion-orchestrator

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
│   │   └── http.rs          # HTTP client to Python parser
│   ├── circuit_breaker.rs   # Fault isolation
│   ├── backpressure.rs      # AkiDB-latency aware
│   ├── memory.rs            # tegrastats coordinator
│   ├── chunker/
│   │   └── semantic.rs      # Sentence-boundary chunking
│   ├── batcher/
│   │   └── dynamic.rs       # Queue-depth adaptive
│   ├── embedding.rs         # vLLM client
│   ├── idempotency.rs       # Content-hash dedup
│   ├── state.rs             # SQLite tracker
│   ├── metrics.rs           # Prometheus
│   └── pipeline.rs          # Orchestration logic
```

### New Service: doc-parser (Python)

```
services/doc-parser/
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── parser/
│   ├── __init__.py
│   ├── main.py           # FastAPI app
│   ├── config.py
│   ├── routers/
│   │   └── parse.py      # POST /parse
│   ├── parsers/
│   │   ├── base.py       # Abstract parser
│   │   ├── pdf.py        # pdfplumber
│   │   ├── docx.py       # python-docx
│   │   └── enl.py        # EndNote
│   └── health.py
└── tests/
```

### New Service: upload-gateway (Python)

```
services/upload-gateway/
├── Dockerfile
├── requirements.txt
├── gateway/
│   ├── __init__.py
│   ├── main.py           # FastAPI app
│   ├── routers/
│   │   ├── upload.py     # POST /upload
│   │   └── presign.py    # GET /presign
│   └── health.py
└── tests/
```

### Sprint Breakdown

| Sprint | Weeks | Focus |
|--------|-------|-------|
| Sprint 1 | 3-4 | NATS + Orchestrator Foundation |
| Sprint 2 | 5-6 | Rust Parsers + Format Router |
| Sprint 3 | 7-8 | Python Parser Service |
| Sprint 4 | 9-10 | Resilience Patterns |
| Sprint 5 | 11-12 | Chunking, Batching, Integration |

### Phase 2 Exit Gate

| Criteria | Target |
|----------|--------|
| NATS 3-node cluster | Quorum operational |
| Rust parsing ratio | 60-70% of documents |
| Circuit breaker | All state transitions tested |
| Backpressure | Throttles on >500ms latency |
| Memory coordinator | Pauses at 70% memory |
| Semantic chunking | ~512 tokens, sentence boundaries |
| Upload → Search SLO | < 30 minutes (P95) |

---

## Phase 3: Optimization (Weeks 13-18)

### Objectives
- Enable GPU FAISS mode
- Implement index rebuild
- Performance tuning
- TensorRT evaluation (optional)

### Key Tasks

| ID | Task | Priority |
|----|------|----------|
| P3-01 | Enable GPU FAISS mode | P0 |
| P3-02 | Index rebuild strategy | P0 |
| P3-03 | Zero-downtime rebuild | P0 |
| P3-04 | Compaction scheduling | P1 |
| P3-05 | Embedding batch optimization | P0 |
| P3-06 | Load testing (1000 docs/hr) | P0 |
| P3-07 | Thermal throttling | P1 |
| P3-08 | TensorRT evaluation | P2 |

### Phase 3 Exit Gate

| Criteria | Target |
|----------|--------|
| Search P95 (GPU) | < 5ms |
| Ingestion throughput | 1000 docs/hr |
| Index rebuild | Zero-downtime |
| GPU utilization | > 60% during search |

---

## Phase 4: Production (Weeks 19-24)

### Objectives
- Docker Compose production deployment
- Monitoring and alerting
- Documentation and runbooks
- Security hardening

### Key Tasks

| ID | Task | Priority |
|----|------|----------|
| P4-01 | Docker Compose files | P0 |
| P4-02 | Ansible deployment playbooks | P0 |
| P4-03 | Grafana dashboards (4) | P0 |
| P4-04 | Alerting rules | P0 |
| P4-05 | Production load testing | P0 |
| P4-06 | Runbook documentation | P0 |
| P4-07 | cuVS evaluation | P1 |
| P4-08 | Security review | P0 |

### Production Readiness Checklist

| ID | Item | Status |
|----|------|--------|
| C-01 | NATS 3-node cluster | [ ] |
| C-02 | Circuit breaker | [ ] |
| C-03 | Backpressure tested | [ ] |
| C-04 | Memory coordinator | [ ] |
| C-05 | Core metrics | [ ] |
| C-06 | 30-min SLO validated | [ ] |
| C-07 | GPU mode active | [ ] |
| C-08 | Runbook complete | [ ] |
| C-09 | Security review | [ ] |

---

## Technology Stack (v1.7 - Validated)

### Core (Rust)
- FAISS 1.8+ (GPU IVF-Flat)
- RocksDB 7.8+
- Tonic (gRPC)
- Tokio (async runtime)
- async_nats (NATS client)
- calamine (XLSX), scraper (HTML), quick-xml (XML)
- unicode-segmentation (sentence splitting)
- SQLite (document state)

### Ingestion Services (Python)
- **Python 3.12.3** (validated)
- FastAPI 0.109+
- pdfplumber, python-docx
- Uvicorn

### Infrastructure (Validated)
- **Docker 28.2.2** (validated on Thor)
- **CUDA 13.0** (validated)
- **NVIDIA Driver 580.00** (validated)
- NATS JetStream **2.10.29** (validated on ARM64)
- MinIO (deployed)
- Prometheus + Grafana (deployed)
- **vLLM 25.11** (Qwen3-Embedding-8B deployed)

---

## Risk Register (v1.7)

| Risk | Probability | Impact | Mitigation | Status |
|------|-------------|--------|------------|--------|
| GPU FAISS FFI issues | Medium | High | C++ service fallback | Monitored |
| Thor thermal throttling | Medium | Medium | tegrastats monitoring ✅ | Mitigated |
| NATS 3rd node unavailable | Low | Medium | 2-node acceptable | Accepted |
| vLLM latency spikes | Low | Medium | Circuit breaker | Planned |
| Memory pressure | Medium | High | Memory coordinator | Planned |

---

## Open Questions (v1.7)

### Resolved

| ID | Question | Resolution |
|----|----------|------------|
| Q1 | Document parsing approach? | Hybrid: Rust + Python |
| Q2 | Message queue? | NATS JetStream 3-node |
| Q3 | Container runtime? | **Docker 28.2.2** |
| Q4 | Hardware specs? | **128GB** validated |
| Q5 | Network latency? | **0.81ms** validated |
| Q6 | Search performance? | **2.9ms** validated |

### Remaining

| ID | Question | Decision By |
|----|----------|-------------|
| Q7 | TensorRT vs vLLM? | Phase 3 |
| Q8 | Malware scanning? | Phase 3 |
| Q9 | OCR for scanned PDFs? | Phase 3 |
| Q10 | cuVS replacement? | Phase 4 |
| Q11 | Third NATS node? | Phase 2 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial plan |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid architecture, NATS, resilience |
| 1.6 | 2026-01-21 | AkiDB Team | Docker deployment, version unified |
| **1.7** | **2026-01-21** | **AkiDB Team** | **Phase 0 validated, actual metrics** |

---

## Summary

| Aspect | Value |
|--------|-------|
| Phase 0 | ✅ **COMPLETE** (10/10 tasks) |
| Phase 1 | 🔄 **75%** (scaffolds remaining) |
| Phase 2 | ❌ Not started |
| Total Tests | **84 passing** |
| Search Latency | **2.9ms** (mock mode) |
| Hardware | **128GB** per node |
| Timeline | **~24 weeks** remaining |

**Immediate Next Steps:**
1. Create `crates/ingestion-orchestrator/` scaffold
2. Create `services/` directory structure
3. Setup NATS 3-node configuration
4. Enable GPU mode on Thor nodes
5. Begin Phase 2 development

---

*End of Implementation Plan v1.7*
