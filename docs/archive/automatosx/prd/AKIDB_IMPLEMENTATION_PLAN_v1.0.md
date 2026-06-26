# AkiDB Thor Edition - Implementation Plan

**Version:** 1.0
**Date:** 2025-01-20
**Status:** Approved
**Based On:** ADR v1.1, PRD v1.1
**Review:** Multi-model synthesis (Claude, Grok)

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition over **~19 weeks (~5 months)** across 4 phases plus a validation sprint. The plan prioritizes early hardware validation, FAISS-rs GPU binding stability, and distributed systems correctness over ML optimizations.

**Key Insight:** Treat AkiDB Thor as a **distributed systems project that uses ML** (FAISS/TensorRT), not an ML project that needs distribution.

---

## Timeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AKIDB THOR IMPLEMENTATION TIMELINE                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 0          │ Weeks 1-6        │ Weeks 7-12      │ Weeks 13-18  │ 19-22│
│  ┌─────────────┐ │ ┌──────────────┐ │ ┌─────────────┐ │ ┌──────────┐ │ ┌───┐│
│  │ VALIDATION  │ │ │   PHASE 1    │ │ │   PHASE 2   │ │ │  PHASE 3 │ │ │P4 ││
│  │   SPRINT    │ │ │  Foundation  │ │ │ Distribution│ │ │Optimization│ │ │   ││
│  │  (1 week)   │ │ │  (6 weeks)   │ │ │  (6 weeks)  │ │ │ (6 weeks)│ │ │4wk││
│  └─────────────┘ │ └──────────────┘ │ └─────────────┘ │ └──────────┘ │ └───┘│
│                                                                             │
│  Hardware        │ Single-node      │ Multi-node      │ TensorRT     │ cuVS │
│  CI/CD           │ FAISS GPU        │ Fan-out         │ Rebuild      │ Prod │
│  Security        │ gRPC + RocksDB   │ Tombstones      │ Performance  │      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Total Duration:** 22-23 weeks (~5.5 months)

---

## Phase 0: Validation Sprint (Week 0)

### Objectives
- Validate hardware compatibility before writing application code
- Establish CI/CD pipeline with GPU support
- Confirm CUDA version compatibility matrix
- Set security baseline

### Tasks

| ID | Task | Owner | Duration | Exit Criteria |
|----|------|-------|----------|---------------|
| V-01 | Procure Jetson Thor hardware (4 units) | Infra | - | Hardware delivered |
| V-02 | GPU driver + CUDA installation | Infra | 1 day | nvidia-smi reports expected GPU |
| V-03 | FAISS standalone benchmark (IVF-Flat, 1M vectors) | Dev | 2 days | Benchmark completes, latencies recorded |
| V-04 | MinIO cluster deployment (4 nodes) | Infra | 1 day | S3 API responds, latency < 10ms |
| V-05 | CUDA compatibility matrix validation | ML | 1 day | FAISS 1.8+ and TensorRT compatible |
| V-06 | CI/CD pipeline with GPU runners | DevOps | 2 days | GPU tests run in CI |
| V-07 | Security baseline (TLS config, cargo-audit) | DevOps | 1 day | No critical vulnerabilities |

### Deliverables
- [ ] Hardware benchmark report (FAISS IVF-Flat latencies at reference config)
- [ ] MinIO latency baseline document
- [ ] CUDA compatibility matrix (FAISS ↔ TensorRT)
- [ ] CI/CD pipeline operational
- [ ] Security scan report

### Exit Gate
All tasks complete. FAISS GPU IVF-Flat confirmed working on Thor hardware.

---

## Phase 1: Foundation (Weeks 1-6)

### Objectives
- Establish single-node vector search with GPU acceleration
- Implement core gRPC API
- Set up RocksDB for metadata storage
- Instrument observability from day one

### Sprint Breakdown

#### Sprint 1 (Weeks 1-2): Scaffolding & FAISS Integration

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-01 | Initialize Cargo workspace with modular crates | P0 | 2d | - |
| P1-02 | Create `faiss-wrapper` crate with GPU IVF-Flat bindings | P0 | 5d | P1-01 |
| P1-03 | Implement basic insert/search operations via FFI | P0 | 3d | P1-02 |
| P1-04 | Define gRPC protobuf schemas (v1) | P0 | 2d | - |
| P1-05 | Design storage abstraction interface | P1 | 2d | - |

**Crate Structure:**
```
akidb-thor/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── faiss-wrapper/            # FAISS FFI bindings
│   ├── grpc-server/              # tonic gRPC service
│   ├── storage/                  # Storage abstraction
│   ├── coordinator/              # Fan-out coordinator (Phase 2)
│   └── common/                   # Shared types, errors
```

**Sprint 1 Exit Criteria:**
- [ ] FFI calls to FAISS GPU succeed
- [ ] Basic insert/search works in unit tests
- [ ] Protobuf schemas defined and compiling

#### Sprint 2 (Weeks 3-4): RocksDB & gRPC Service

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-06 | Implement RocksDB storage backend | P0 | 4d | P1-05 |
| P1-07 | ID mapping: external → internal | P0 | 3d | P1-06 |
| P1-08 | Implement gRPC InsertVector endpoint | P0 | 2d | P1-03, P1-04 |
| P1-09 | Implement gRPC SearchVector endpoint | P0 | 2d | P1-03, P1-04 |
| P1-10 | Minimal gRPC streaming prototype (fan-out stub) | P1 | 2d | P1-09 |
| P1-11 | Error propagation across FFI boundary | P0 | 2d | P1-03 |

**Sprint 2 Exit Criteria:**
- [ ] Persistence verified (restart preserves data)
- [ ] gRPC endpoints functional
- [ ] Streaming POC demonstrates fan-out semantics
- [ ] FFI errors propagate to gRPC responses

#### Sprint 3 (Weeks 5-6): GPU Memory & Observability

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P1-12 | GPU memory management (60% budget enforcement) | P0 | 3d | P1-02 |
| P1-13 | Memory pressure handling (CPU fallback trigger) | P1 | 2d | P1-12 |
| P1-14 | Observability: OpenTelemetry tracing | P0 | 3d | P1-08, P1-09 |
| P1-15 | Metrics: GPU memory, latency percentiles | P0 | 2d | P1-14 |
| P1-16 | Benchmarking at reference config (D=768, N=1M) | P0 | 3d | P1-12 |
| P1-17 | Load test: 10M+ vectors without OOM | P0 | 2d | P1-12 |

**Sprint 3 Exit Criteria:**
- [ ] 10M+ vectors without GPU OOM
- [ ] P50/P95/P99 latencies baselined
- [ ] Tracing spans visible in Jaeger/similar
- [ ] Metrics exported to Prometheus

### Phase 1 Deliverables
- [ ] Single-node AkiDB binary with gRPC API
- [ ] Insert, Search, Get operations functional
- [ ] RocksDB persistence layer
- [ ] GPU memory management with CPU fallback
- [ ] Observability (tracing + metrics)
- [ ] Benchmark report at reference configuration

### Phase 1 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Single-node insert latency | < 5ms | [ ] |
| Single-node search P95 (ref config) | < 10ms | [ ] |
| Vectors without OOM | 10M+ | [ ] |
| Tracing instrumented | Yes | [ ] |
| Storage abstraction | Interface defined | [ ] |
| Security | TLS on gRPC | [ ] |

---

## Phase 2: Distribution (Weeks 7-12)

### Objectives
- Implement multi-node fan-out search
- Add tombstone-based deletes
- Define consistency model
- Handle partition tolerance

### Sprint Breakdown

#### Sprint 4 (Weeks 7-8): Fan-out Coordinator

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-01 | Implement coordinator service (separate binary) | P0 | 4d | Phase 1 |
| P2-02 | Shard routing logic (hash-based) | P0 | 2d | P2-01 |
| P2-03 | Parallel search fan-out to N shards | P0 | 3d | P2-02 |
| P2-04 | Min-heap result merging | P0 | 2d | P2-03 |
| P2-05 | Protobuf schema versioning strategy | P1 | 2d | P1-04 |
| P2-06 | Partial results handling (missing shards) | P0 | 2d | P2-03 |

**Sprint 4 Exit Criteria:**
- [ ] Fan-out search works across 4 shards
- [ ] Partial results returned when shard unavailable
- [ ] Schema versioning documented

#### Sprint 5 (Weeks 9-10): Tombstone Deletes & Consistency

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-07 | Tombstone bitset implementation (GPU) | P0 | 4d | P1-02 |
| P2-08 | FAISS IDSelector integration | P0 | 2d | P2-07 |
| P2-09 | Delete API endpoint (gRPC) | P0 | 2d | P2-07 |
| P2-10 | Update API endpoint (delete + insert) | P0 | 2d | P2-09 |
| P2-11 | Consistency model documentation | P0 | 1d | - |
| P2-12 | Read-your-writes validation (< 100ms) | P0 | 2d | P2-09, P2-10 |
| P2-13 | Tombstone log format design | P1 | 2d | P2-07 |

**Tombstone Bitset Implementation:**
```rust
// crates/faiss-wrapper/src/tombstone.rs
pub struct TombstoneBitset {
    /// GPU-resident bit array (1 bit per vector)
    /// 0 = active, 1 = deleted
    bitset: cuda::DeviceBuffer<u8>,
    /// Reader-writer lock for concurrent access
    lock: RwLock<()>,
    /// Count of deleted vectors (for compaction trigger)
    deleted_count: AtomicU64,
}

impl TombstoneBitset {
    pub fn mark_deleted(&self, internal_id: i64) -> Result<()>;
    pub fn is_deleted(&self, internal_id: i64) -> bool;
    pub fn create_selector(&self) -> faiss::IDSelectorBitmap;
}
```

**Sprint 5 Exit Criteria:**
- [ ] Deleted vectors never appear in search results
- [ ] Update = delete + insert works correctly
- [ ] Read-your-writes < 100ms validated
- [ ] Consistency model documented

#### Sprint 6 (Weeks 11-12): Load Testing & Fault Tolerance

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-14 | Multi-node load testing (100 QPS target) | P0 | 3d | P2-03 |
| P2-15 | Partition tolerance testing | P0 | 2d | P2-06 |
| P2-16 | Backpressure implementation | P0 | 3d | P2-14 |
| P2-17 | Health checking and circuit breakers | P0 | 2d | P2-01 |
| P2-18 | Graceful degradation (partial results, topK/2) | P1 | 2d | P2-06 |
| P2-19 | MinIO snapshot integration (basic) | P1 | 3d | P1-05 |

**Sprint 6 Exit Criteria:**
- [ ] 100 QPS sustained under load
- [ ] System survives node failures gracefully
- [ ] Backpressure triggers at soft breach (P95 > 50ms)
- [ ] Snapshots persist to MinIO

### Phase 2 Deliverables
- [ ] Coordinator binary with fan-out search
- [ ] Delete and Update API endpoints
- [ ] Tombstone bitset (GPU-resident)
- [ ] Consistency guarantees documented
- [ ] Partition tolerance tested
- [ ] MinIO snapshot persistence

### Phase 2 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Fan-out search (4 shards) | < 50ms E2E P95 | [ ] |
| Delete visibility | Immediate | [ ] |
| Read-your-writes | < 100ms | [ ] |
| Throughput | 100 QPS | [ ] |
| Partition survival | Partial results | [ ] |
| Schema versioning | Documented | [ ] |

---

## Phase 3: Optimization (Weeks 13-18)

### Objectives
- Integrate TensorRT-LLM for embeddings
- Implement zero-downtime index rebuilds
- Performance tuning and cost optimization

### Sprint Breakdown

#### Sprint 7 (Weeks 13-14): TensorRT Embedding

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-01 | TensorRT-LLM model loading (Qwen3-Embedding) | P0 | 4d | V-05 |
| P3-02 | Embedding service integration | P0 | 3d | P3-01 |
| P3-03 | Embedding caching layer | P1 | 2d | P3-02 |
| P3-04 | vLLM fallback implementation | P1 | 3d | P3-02 |
| P3-05 | Embedding latency benchmarking | P0 | 2d | P3-02 |

**Sprint 7 Exit Criteria:**
- [ ] Embeddings generated at < 10ms P95
- [ ] vLLM fallback works when TensorRT fails
- [ ] Model loading < 30s

#### Sprint 8 (Weeks 15-16): Index Rebuild

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-06 | WAL implementation for rebuild replay | P0 | 4d | P1-06 |
| P3-07 | Dual-index swap mechanism | P0 | 4d | P3-06 |
| P3-08 | Shadow index building (excludes tombstones) | P0 | 3d | P3-07, P2-07 |
| P3-09 | Concurrent reads during rebuild | P0 | 2d | P3-07 |
| P3-10 | Rebuild memory management (2x peak) | P0 | 2d | P3-07 |

**Dual-Index Swap Process:**
```
1. PRE-REBUILD
   ├── Record WAL position (LSN_start)
   ├── Allocate shadow index memory
   └── Set rebuild_in_progress = true

2. DURING REBUILD
   ├── READS: Served by OLD index
   ├── WRITES: Go to BOTH old index + WAL
   └── SHADOW: Built from RocksDB snapshot

3. POST-REBUILD
   ├── Replay WAL (LSN > LSN_start)
   ├── Validate shadow index
   └── Atomic pointer swap

4. CLEANUP
   ├── Deallocate old index
   ├── Clear replayed WAL
   └── Reset tombstone bitset
```

**Sprint 8 Exit Criteria:**
- [ ] Rebuild completes without serving degradation
- [ ] Concurrent reads during rebuild verified
- [ ] WAL replay correctness tested

#### Sprint 9 (Weeks 17-18): Performance Tuning

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P3-11 | Compaction trigger implementation (10% tombstones) | P0 | 2d | P3-08 |
| P3-12 | Rebuild automation (scheduled + triggered) | P0 | 3d | P3-11 |
| P3-13 | SLO estimation API | P1 | 3d | Phase 2 |
| P3-14 | Performance profiling (nsys, perf) | P0 | 3d | All |
| P3-15 | Cost optimization analysis | P1 | 2d | P3-14 |
| P3-16 | Batch processing optimization | P1 | 2d | P1-09 |

**Sprint 9 Exit Criteria:**
- [ ] Compaction triggers automatically
- [ ] /slo/estimate API operational
- [ ] Performance report with optimization recommendations
- [ ] Batch queries improve throughput 2x+

### Phase 3 Deliverables
- [ ] TensorRT-LLM embedding integration
- [ ] vLLM fallback path
- [ ] Zero-downtime index rebuild
- [ ] WAL for rebuild replay
- [ ] Compaction automation
- [ ] SLO estimation API
- [ ] Performance optimization report

### Phase 3 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| Embedding P95 | < 10ms | [ ] |
| Rebuild duration (1M vectors) | < 5 min | [ ] |
| Rebuild serving degradation | None | [ ] |
| Compaction automation | Triggers at 10% | [ ] |
| Batch throughput improvement | > 2x | [ ] |

---

## Phase 4: Production (Weeks 19-22)

### Objectives
- Evaluate and integrate cuVS (if gate criteria met)
- Production hardening
- Security audit and documentation

### Sprint Breakdown

#### Sprint 10 (Weeks 19-20): cuVS Integration

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-01 | cuVS integration behind feature flag | P1 | 4d | Phase 1 |
| P4-02 | Shadow mode validation (24h) | P0 | 3d | P4-01 |
| P4-03 | cuVS vs FAISS benchmark comparison | P0 | 2d | P4-01 |
| P4-04 | Rollback mechanism testing | P0 | 2d | P4-01 |
| P4-05 | cuVS gate decision documentation | P0 | 1d | P4-02, P4-03 |

**cuVS Gate Criteria:**
```yaml
enablement_requirements:
  latency_improvement: ≥ 25%    # vs FAISS baseline P95
  recall_maintained: ≥ 95%      # No regression
  shadow_validation: 24h        # Parallel execution
  result_divergence: < 0.1%     # vs FAISS results
  thermal_stability: < 85°C     # 30min sustained

decision:
  if_gate_passed: Enable cuVS with feature flag
  if_gate_failed: Document reasons, remain on FAISS
```

**Sprint 10 Exit Criteria:**
- [ ] cuVS gate decision made and documented
- [ ] Rollback mechanism tested
- [ ] Performance comparison report published

#### Sprint 11 (Weeks 21-22): Production Hardening

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P4-06 | Security audit (external or internal) | P0 | 3d | All |
| P4-07 | Penetration testing | P1 | 2d | P4-06 |
| P4-08 | Deployment automation (Kubernetes manifests) | P0 | 3d | All |
| P4-09 | Runbook documentation | P0 | 2d | All |
| P4-10 | Operational playbooks (incident response) | P0 | 2d | All |
| P4-11 | Final load testing (production simulation) | P0 | 2d | All |

**Sprint 11 Exit Criteria:**
- [ ] Security audit passed
- [ ] Kubernetes deployment automated
- [ ] Runbooks complete
- [ ] Production simulation successful

### Phase 4 Deliverables
- [ ] cuVS integration (if gate passed) or documented exclusion
- [ ] Security audit report
- [ ] Kubernetes deployment manifests
- [ ] Operational runbooks
- [ ] Production readiness certification

### Phase 4 Exit Gate

| Criteria | Target | Validated |
|----------|--------|-----------|
| cuVS decision | Documented | [ ] |
| Security audit | Passed | [ ] |
| Production simulation | 100 QPS, < 50ms P95 | [ ] |
| Runbooks | Complete | [ ] |
| Deployment automation | Kubernetes ready | [ ] |

---

## Critical Path Dependencies

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CRITICAL PATH DEPENDENCY DAG                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 0: Hardware Validation ────────────────────────────────────────┐      │
│              │                                                       │      │
│              ▼                                                       │      │
│  Phase 1: FAISS-rs GPU ──► Storage Abstraction ──► gRPC Service     │      │
│              │                    │                    │             │      │
│              │                    ▼                    │             │      │
│              │             RocksDB Integration ◄───────┘             │      │
│              │                    │                                  │      │
│              ▼                    ▼                                  │      │
│  Phase 2: Fan-out Coordinator ◄───┘                                  │      │
│              │                                                       │      │
│              ├──► Tombstone Deletes ──► Tombstone Log Format        │      │
│              │                              │                        │      │
│              ▼                              ▼                        │      │
│         Schema Versioning ────────► Rebuild Design (Phase 3)        │      │
│              │                              │                        │      │
│              ▼                              ▼                        │      │
│  Phase 3: TensorRT ◄────────────────────────┤ (CUDA compat) ◄───────┘      │
│              │                              │                               │
│              ├──► Index Rebuild ──► Rebuild Automation                      │
│              │                                                              │
│              ▼                                                              │
│  Phase 4: cuVS Gate (quantitative threshold decision)                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

LONGEST POLE: FAISS-rs GPU bindings → Single-node correctness → Fan-out
```

---

## Risk Mitigation Timeline

| Risk | Phase | Mitigation | Checkpoint |
|------|-------|------------|------------|
| FAISS-rs GPU bindings unstable | 1 | Early validation, fallback plan (C++ service) | Sprint 1 |
| CUDA version incompatibility | 0 | Compatibility matrix validation | Week 0 |
| GPU OOM under load | 1 | Memory budgets, CPU fallback | Sprint 3 |
| Fan-out latency too high | 2 | Optimize network, reduce shards | Sprint 4 |
| Tombstone accumulation | 2 | Compaction triggers | Sprint 5 |
| Rebuild memory pressure | 3 | Unload embedding model, schedule off-peak | Sprint 8 |
| TensorRT compilation fails | 3 | vLLM fallback ready | Sprint 7 |
| cuVS regression | 4 | Feature flag, rollback mechanism | Sprint 10 |

---

## Team Allocation

| Role | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| **Rust Engineer 1** | FAISS bench | FAISS wrapper, FFI | Coordinator | Rebuild | cuVS |
| **Rust Engineer 2** | - | gRPC, RocksDB | Tombstones | WAL | Hardening |
| **ML Engineer** | CUDA compat | - | - | TensorRT | cuVS validation |
| **DevOps** | CI/CD, MinIO | Observability | Load testing | Automation | Deployment |

---

## Success Metrics

### Technical Metrics

| Metric | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|--------|---------|---------|---------|---------|
| FAISS Search P95 | < 10ms | < 10ms | < 10ms | < 8ms (cuVS) |
| E2E Search P95 | N/A | < 50ms | < 50ms | < 50ms |
| Throughput | 50 QPS | 100 QPS | 150 QPS | 200 QPS |
| Recall@10 | > 95% | > 95% | > 95% | > 95% |
| Vectors supported | 10M | 10M | 10M | 10M |

### Operational Metrics

| Metric | Target | Phase |
|--------|--------|-------|
| Mean Time to Recovery | < 60s | Phase 2+ |
| Deployment frequency | Weekly | Phase 4 |
| Change failure rate | < 5% | Phase 4 |
| SLO compliance | > 99% | Phase 4 |

---

## Budget Considerations

### Hardware (One-time)

| Item | Quantity | Est. Cost | Phase |
|------|----------|-----------|-------|
| Jetson Thor | 4 | $TBD | 0 |
| NVMe SSD 500GB | 4 | ~$400 | 0 |
| 10Gbps Switch | 1 | ~$500 | 0 |

### Cloud/CI (Monthly)

| Item | Est. Cost | Notes |
|------|-----------|-------|
| GPU CI runners | ~$500/mo | GitHub Actions or similar |
| MinIO storage (dev) | ~$100/mo | S3-compatible |
| Monitoring | ~$100/mo | Prometheus, Grafana |

---

## Open Questions for Phase 1

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q1 | FAISS-rs vs custom FFI? | faiss-rs crate vs hand-rolled | Sprint 1 |
| Q2 | Combined coordinator+shard binary? | Single vs separate | Sprint 2 |
| Q3 | RocksDB vs alternative (sled)? | RocksDB (recommended) | Sprint 2 |
| Q4 | Sync insert for immediate visibility? | Yes (optional flag) vs No | Sprint 6 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial implementation plan |

---

*End of Implementation Plan v1.0*
