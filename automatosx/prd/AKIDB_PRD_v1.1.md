# AkiDB Thor Edition - Product Requirements Document (PRD)
## Version 1.1

**Version:** 1.1
**Date:** 2025-01-20
**Author:** AkiDB Team
**Status:** Approved
**Changes from v1.0:** SLO boundary conditions, delete/update API contracts, ID management, consistency guarantees, cuVS as optional accelerator
**Review:** Multi-model synthesis (Claude, Grok) addressing reviewer feedback

---

## Change Log from v1.0

| Section | Change | Rationale |
|---------|--------|-----------|
| §8 | Added SLO Assumption Table with reference configuration | Latency targets meaningless without parameters |
| §8 | Added Degradation Matrix | Document performance outside reference |
| §8 | Added Backpressure Policy | Define behavior when SLO exceeded |
| §7 | Added Delete/Update API contracts with specific behaviors | Engineering gap |
| §7 | Added ID collision handling specification | Prevent mapping bugs |
| §7 | Added Consistency/Visibility guarantees | Read-your-writes undefined |
| §9 | Added SLO Estimation API | Enable capacity planning |
| §6 | cuVS moved to "optional accelerator" with gate criteria | Unvalidated on Thor |
| All | Performance numbers marked "ESTIMATED" | Avoid misleading claims |

---

## Table of Contents

*Only sections with significant changes are fully reproduced. Unchanged sections reference v1.0.*

1. [Executive Summary](#1-executive-summary) *(updated)*
2. [Problem Statement](#2-problem-statement) *(unchanged, see v1.0)*
3. [Goals and Non-Goals](#3-goals-and-non-goals) *(updated)*
4. [Target Users](#4-target-users) *(unchanged, see v1.0)*
5. [Use Cases](#5-use-cases) *(updated with delete/update)*
6. [System Architecture](#6-system-architecture) *(updated with cuVS gate)*
7. [Functional Requirements](#7-functional-requirements) *(significantly updated)*
8. [Non-Functional Requirements](#8-non-functional-requirements) *(significantly updated)*
9. [API Specification](#9-api-specification) *(updated with SLO API)*
10. [Data Model](#10-data-model) *(updated with ID mapping)*
11-19. *(See v1.0 for unchanged sections)*

---

## 1. Executive Summary

### 1.1 Product Vision

**AkiDB Thor Edition** is a distributed vector search engine for **NVIDIA Jetson Thor** edge clusters.

### 1.2 Key Performance Targets (v1.1 REVISED)

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

### 1.3 v1.1 Key Additions

1. **SLO Boundary Conditions:** All latency claims now tied to explicit reference configuration
2. **cuVS as Optional Accelerator:** Not enabled by default, requires validation gate
3. **Delete/Update Contracts:** Explicit tombstone filtering and visibility semantics
4. **Consistency Guarantees:** Bounded read-your-writes (100ms)
5. **ID Management:** External→internal mapping with collision handling

---

## 3. Goals and Non-Goals (UPDATED)

### 3.1 Goals (MUST Have) - v1.1 Updates

| ID | Goal | Success Criteria | v1.1 Change |
|----|------|------------------|-------------|
| G1 | GPU-accelerated vector search | FAISS GPU IVF-Flat < 10ms/shard at reference config | Added "at reference config" |
| G2 | Sub-50ms end-to-end latency | P95 < 50ms at reference config | Added "at reference config" |
| G11 | **Explicit consistency model** | Read-your-writes < 100ms documented | **NEW** |
| G12 | **Delete visibility** | Deleted vectors never in search results | **NEW** |

### 3.2 Goals (SHOULD Have) - v1.1 Updates

| ID | Goal | Success Criteria | v1.1 Change |
|----|------|------------------|-------------|
| G8 | TensorRT-LLM embedding | < 10ms; vLLM fallback available | Added fallback |
| G13 | **cuVS acceleration** | ≥25% latency improvement when enabled | **NEW** (was assumed in v1.0) |
| G14 | **SLO estimation API** | /slo/estimate endpoint operational | **NEW** |

### 3.3 Non-Goals (Will NOT Do) - v1.1 Additions

| ID | Non-Goal | Rationale |
|----|----------|-----------|
| NG8 | **Strong consistency** | Eventual consistency with bounded lag is sufficient |
| NG9 | **External ID reuse** | Deleted IDs never recycled (prevents confusion) |
| NG10 | **cuVS as default** | Unvalidated on Thor; requires explicit gate |

---

## 5. Use Cases (UPDATED)

### UC-7: Delete Vector (NEW)

```
Actor: Application
Precondition: Vector with ID exists in collection

Flow:
  1. Application calls AkiDB Delete API with vector ID
  2. Shard marks internal ID in tombstone bitset
  3. Shard updates RocksDB mapping (deleted=true)
  4. Shard returns DeleteResponse with status

Postcondition:
  - Vector immediately excluded from search results
  - External ID cannot be reused (permanent)
  - Vector reclaimed on next compaction

Performance: < 5ms
Visibility: Immediate (same request)
```

### UC-8: Update Vector (NEW)

```
Actor: Application
Precondition: Vector with ID exists in collection

Flow:
  1. Application calls AkiDB Update API with ID and new vector
  2. Shard marks old internal ID in tombstone bitset (immediate)
  3. Shard allocates new internal ID
  4. Shard adds new vector to FAISS index
  5. Shard updates RocksDB mapping
  6. Shard returns UpdateResponse

Postcondition:
  - Old vector immediately excluded from search
  - New vector searchable within 100ms

Performance: < 10ms
Visibility:
  - Delete part: Immediate
  - Insert part: Within 100ms
```

### UC-9: Search Respecting Consistency (NEW)

```
Actor: Application
Precondition: Application just inserted vector

Flow:
  1. Application inserts vector, receives success (T0)
  2. Application waits < 100ms
  3. Application searches for inserted vector (T1)
  4. If T1 - T0 < 100ms: Vector MAY or MAY NOT appear
  5. If T1 - T0 ≥ 100ms: Vector WILL appear

Postcondition: Application understands visibility lag

Guidance:
  - For immediate reads, use get(id) not search(vector)
  - For guaranteed search visibility, wait 100ms or use sync flag
```

---

## 6. System Architecture (UPDATED)

### 6.1 Index Layer - cuVS as Optional Accelerator

```
┌─────────────────────────────────────────────────────────────┐
│                    INDEX LAYER                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  PRIMARY (Always enabled):                                  │
│  ┌───────────────────────────────────────────────────────┐ │
│  │            FAISS GPU IVF-Flat                         │ │
│  │  • Mature, well-tested                                │ │
│  │  • Fallback to CPU if GPU unavailable                 │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  OPTIONAL ACCELERATOR (Feature flagged):                   │
│  ┌───────────────────────────────────────────────────────┐ │
│  │            cuVS Acceleration                          │ │
│  │  • NOT enabled by default                             │ │
│  │  • Requires: AKIDB_USE_CUVS=true                     │ │
│  │  • Requires: Validation gate passed                   │ │
│  │  • Auto-rollback on performance regression            │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  cuVS Enablement Gate:                                      │
│  • ≥25% latency improvement over FAISS baseline            │
│  • ≥95% recall@10 maintained                               │
│  • 24h shadow mode validation passed                        │
│  • Thermal stability confirmed (85°C limit)                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 7. Functional Requirements (SIGNIFICANTLY UPDATED)

### 7.1 Vector Operations (UPDATED)

| ID | Requirement | Priority | v1.1 Specification |
|----|-------------|----------|-------------------|
| FR-V01 | Insert single vector | P0 | Visible in search within 100ms |
| FR-V02 | Batch insert (≤10,000) | P0 | Visible within 100ms of batch complete |
| FR-V03 | Search top-k | P0 | At reference config; see SLO table |
| FR-V04 | Search with filter | P0 | Pre-filter via RocksDB |
| FR-V05 | **Delete vector** | P0 | **Immediate visibility; tombstone bitset** |
| FR-V06 | **Update vector** | P1 | **Delete (immediate) + Insert (100ms)** |
| FR-V07 | Get vector by ID | P1 | Immediate (from RocksDB/WAL) |
| FR-V08 | **Upsert semantics** | P1 | **Insert existing ID = update, not error** |

### 7.2 Delete Contract (NEW)

```
DELETE /v1/collections/{collection}/vectors/{id}

Request: None (ID in path)

Response:
{
  "success": true,
  "id": "vec_123",
  "status": "DELETED",     // or "NOT_FOUND", "ALREADY_DELETED"
  "visibility": "immediate"
}

Status Codes:
  200: Success (DELETED, NOT_FOUND, ALREADY_DELETED)
  404: Collection not found
  500: Internal error

Behavior:
  - DELETED: Vector existed, now tombstoned
  - NOT_FOUND: ID never existed (no-op, still success)
  - ALREADY_DELETED: Previously deleted (no-op, still success)

Invariants:
  - Deleted IDs NEVER reused
  - Deleted vectors NEVER appear in search results
  - Delete is idempotent
```

### 7.3 Update Contract (NEW)

```
PUT /v1/collections/{collection}/vectors/{id}

Request:
{
  "vector": [0.1, 0.2, ...],
  "metadata": {...}          // Optional, replaces existing
}

Response:
{
  "success": true,
  "id": "vec_123",
  "status": "UPDATED",       // or "CREATED" if ID didn't exist
  "visibility": {
    "delete": "immediate",
    "insert": "within_100ms"
  }
}

Behavior:
  - If ID exists: Delete old + Insert new (upsert)
  - If ID doesn't exist: Insert new (create)
  - Old vector immediately unsearchable
  - New vector searchable within 100ms
```

### 7.4 ID Management Contract (NEW)

| Requirement | Specification |
|-------------|---------------|
| External ID format | String, 1-256 bytes, UTF-8 |
| External ID uniqueness | Required per collection |
| External ID reuse | **NEVER** (deleted IDs are permanent) |
| Internal ID format | int64 (FAISS index) |
| Internal ID stability | May change on compaction |
| Collision handling | Insert existing = upsert |

### 7.5 Consistency Requirements (NEW)

| ID | Requirement | Priority | Specification |
|----|-------------|----------|---------------|
| FR-C01 | Read-your-writes | P0 | search() finds insert() within 100ms |
| FR-C02 | Delete visibility | P0 | Immediate (same request) |
| FR-C03 | Get consistency | P0 | get(id) returns immediately after insert |
| FR-C04 | Cross-shard consistency | P1 | None guaranteed (each shard independent) |

---

## 8. Non-Functional Requirements (SIGNIFICANTLY UPDATED)

### 8.1 SLO Assumption Table (NEW in v1.1)

> **CRITICAL:** All performance SLOs apply ONLY within this reference configuration. Deviations require explicit capacity planning using the degradation matrix.

#### Reference Configuration

| Parameter | Symbol | Reference Value | Valid Range | Notes |
|-----------|--------|-----------------|-------------|-------|
| Dimensions | D | **768** | 128–1024 | LLM embedding standard |
| Vectors per shard | N | **1,000,000** | 100K–2M | Thor memory constraint |
| Retrieval depth | topK | **10** | 1–100 | Standard retrieval |
| IVF probes | nprobe | **32** | 16–64 | Recall/latency balance |
| Query batch size | batch | **1** | 1–256 | Single-query baseline |
| Cluster count | nlist | **4096** | √N heuristic | IVF parameter |
| Filter selectivity | sel | **≥1%** | ≥0.1% | Metadata filter hit rate |
| Shard count | shards | **4** | 2–16 | Fan-out overhead |

#### SLO Targets at Reference Configuration

| Metric | P50 | P95 | P99 | Measurement |
|--------|-----|-----|-----|-------------|
| FAISS search (per shard) | 3ms | **10ms** | 20ms | ESTIMATED |
| Fan-out + merge | 2ms | 5ms | 10ms | ESTIMATED |
| Embedding | 5ms | **10ms** | 15ms | ESTIMATED |
| **E2E search** | 15ms | **50ms** | 75ms | ESTIMATED |
| Recall@10 | — | **≥95%** | — | vs brute-force |

### 8.2 Degradation Matrix (NEW in v1.1)

> **WARNING:** These multipliers are estimates. Actual performance must be validated on Jetson Thor hardware.

| Deviation from Reference | Latency Multiplier | Recall Impact | Notes |
|--------------------------|-------------------|---------------|-------|
| D = 1536 (2x dimensions) | **1.5x** | None | Double embedding size |
| N = 2M (2x vectors) | **1.4x** | None | Linear scaling |
| N = 5M (5x vectors) | **2.0x** | None | Requires validation |
| topK = 50 (5x depth) | **1.2x** | None | More candidates |
| topK = 100 (10x depth) | **1.3x** | None | Diminishing overhead |
| nprobe = 64 (2x probes) | **1.7x** | +2% recall | Trade latency for recall |
| nprobe = 128 (4x probes) | **2.5x** | +3% recall | Diminishing returns |
| batch = 32 | **0.4x per query** | None | Amortization benefit |
| batch = 64 | **0.3x per query** | None | GPU throughput optimized |
| filter selectivity = 0.1% | **2.0x** | None | More vectors scanned |
| shards = 8 (2x shards) | **+5ms** | None | Fan-out overhead |
| shards = 16 (4x shards) | **+10ms** | None | Network dominated |

#### Combined Deviation Example

```
Configuration:
  D = 768 (reference)
  N = 2M (1.4x)
  topK = 50 (1.2x)
  nprobe = 64 (1.7x)

Estimated P95:
  Base:       10ms
  × N factor: 10ms × 1.4 = 14ms
  × topK:     14ms × 1.2 = 16.8ms
  × nprobe:   16.8ms × 1.7 = 28.6ms

  Estimated per-shard P95: ~29ms
  Estimated E2E P95: ~65ms (outside 50ms SLO)

Recommendation: Reduce nprobe to 32 or N to 1M
```

### 8.3 Backpressure Policy (NEW in v1.1)

| Condition | Behavior | HTTP Response |
|-----------|----------|---------------|
| **Normal** (P95 < 50ms) | Full operation | 200 OK |
| **Soft breach** (P95 50-75ms) | Log warning, emit metric | 200 OK + `X-SLO-Warning: true` |
| **Hard breach** (P95 > 75ms) | Reject new queries, drain queue | 503 Service Unavailable |
| **Degraded mode** (optional) | Return partial results (topK/2) | 200 OK + `X-Degraded: true` |

### 8.4 Performance Requirements (UPDATED)

| ID | Requirement | Target | Boundary Condition | Status |
|----|-------------|--------|-------------------|--------|
| NFR-P01 | FAISS search P95 | < 10ms | Reference config | **ESTIMATED** |
| NFR-P02 | E2E search P95 | < 50ms | Reference config | **ESTIMATED** |
| NFR-P03 | Embedding P95 | < 10ms | TensorRT-LLM | **ESTIMATED** |
| NFR-P04 | Ingest throughput | > 10K/s | Per node | **ESTIMATED** |
| NFR-P05 | **Read-your-writes** | < 100ms | After insert success | **SPECIFIED** |
| NFR-P06 | **Delete visibility** | Immediate | Same request | **SPECIFIED** |

### 8.5 Consistency Requirements (NEW)

| ID | Requirement | Target | Notes |
|----|-------------|--------|-------|
| NFR-CON01 | Insert visibility | < 100ms | After success response |
| NFR-CON02 | Delete visibility | Immediate | Same request |
| NFR-CON03 | Get after insert | Immediate | From WAL/RocksDB |
| NFR-CON04 | Search consistency | Snapshot | Within single query |
| NFR-CON05 | Cross-shard | None | Each shard independent |

---

## 9. API Specification (UPDATED)

### 9.1 SLO Estimation API (NEW)

```
GET /v1/slo/estimate

Query Parameters:
  d:       Dimensions (default: 768)
  n:       Vectors per shard (default: 1000000)
  topK:    Retrieval depth (default: 10)
  nprobe:  IVF probes (default: 32)
  batch:   Query batch size (default: 1)
  shards:  Shard count (default: 4)
  filter:  Filter selectivity % (default: 100)

Response:
{
  "request": {
    "d": 768,
    "n": 2000000,
    "topK": 50,
    "nprobe": 64,
    "batch": 1,
    "shards": 4,
    "filter_selectivity": 1.0
  },

  "estimates": {
    "faiss_p50_ms": 12,
    "faiss_p95_ms": 29,
    "faiss_p99_ms": 45,
    "e2e_p50_ms": 25,
    "e2e_p95_ms": 65,
    "e2e_p99_ms": 95,
    "recall_at_10": 0.97
  },

  "slo_compliance": {
    "faiss_p95_within_slo": false,
    "e2e_p95_within_slo": false,
    "recall_within_slo": true
  },

  "deviation_factors": {
    "n": 1.4,
    "topK": 1.2,
    "nprobe": 1.7,
    "combined": 2.86
  },

  "recommendations": [
    "Reduce nprobe from 64 to 32 (-1.7x latency, -2% recall)",
    "Reduce N from 2M to 1M (-1.4x latency)",
    "Or accept degraded SLO with documented expectations"
  ],

  "confidence": 0.75,
  "note": "Estimates based on projections. Validate on Thor hardware."
}
```

### 9.2 Delete API (NEW)

```protobuf
message DeleteRequest {
  string collection = 1;
  string id = 2;
}

message DeleteResponse {
  bool success = 1;
  string id = 2;
  DeleteStatus status = 3;
  string visibility = 4;      // "immediate"
}

enum DeleteStatus {
  DELETED = 0;
  NOT_FOUND = 1;
  ALREADY_DELETED = 2;
}
```

### 9.3 Update API (NEW)

```protobuf
message UpdateRequest {
  string collection = 1;
  string id = 2;
  Vector vector = 3;
  bytes metadata = 4;         // Optional, replaces existing
}

message UpdateResponse {
  bool success = 1;
  string id = 2;
  UpdateStatus status = 3;
  VisibilityInfo visibility = 4;
}

enum UpdateStatus {
  UPDATED = 0;                // Existing ID updated
  CREATED = 1;                // New ID created (upsert)
}

message VisibilityInfo {
  string delete_visibility = 1;   // "immediate"
  string insert_visibility = 2;   // "within_100ms"
}
```

### 9.4 Search Response (UPDATED)

```protobuf
message SearchResponse {
  repeated SearchResult results = 1;

  // Degradation metadata
  bool partial = 2;
  repeated string missing_shards = 3;
  float coverage = 4;

  // SLO metadata (NEW in v1.1)
  SLOInfo slo = 5;
}

message SLOInfo {
  uint64 latency_us = 1;          // Actual latency
  bool within_slo = 2;            // P95 < 50ms?
  bool degraded_mode = 3;         // Partial results returned?
  string slo_warning = 4;         // Warning if soft breach
}
```

---

## 10. Data Model (UPDATED)

### 10.1 ID Mapping (NEW in v1.1)

```
┌─────────────────────────────────────────────────────────────┐
│                    ID MAPPING                               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  RocksDB (Persistent):                                      │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Key:   {collection}:{external_id}                    │ │
│  │  Value: {                                             │ │
│  │    internal_id: int64,                               │ │
│  │    created_at: timestamp,                             │ │
│  │    updated_at: timestamp,                             │ │
│  │    deleted: bool,                                     │ │
│  │    deleted_at: timestamp | null                       │ │
│  │  }                                                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  Memory (Reverse Map):                                      │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Type: Dense array                                    │ │
│  │  Index: internal_id                                   │ │
│  │  Value: external_id pointer                           │ │
│  │  Use: Return external IDs in search results           │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  Tombstone Bitset (GPU):                                    │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Type: Bit array (1 bit per vector)                   │ │
│  │  Size: N / 8 bytes (125KB for 1M vectors)            │ │
│  │  0 = active, 1 = deleted                              │ │
│  │  Use: Exclude deleted from FAISS search               │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 10.2 External ID Constraints

| Constraint | Specification |
|------------|---------------|
| Format | String, UTF-8 |
| Length | 1-256 bytes |
| Uniqueness | Required per collection |
| Allowed characters | a-z, A-Z, 0-9, -, _, . |
| Reserved prefixes | `_akidb_` (internal use) |
| **Reuse after delete** | **NEVER ALLOWED** |

---

## 14. Success Metrics (UPDATED)

### 14.1 Technical Metrics (UPDATED)

| Metric | Target | Boundary | Status |
|--------|--------|----------|--------|
| FAISS Search P95 | < 10ms | Reference config | **ESTIMATED** |
| E2E Search P95 | < 50ms | Reference config | **ESTIMATED** |
| Embedding P95 | < 10ms | TensorRT-LLM | **ESTIMATED** |
| Recall@10 | > 95% | Reference config | **ESTIMATED** |
| **Read-your-writes** | < 100ms | After insert | **SPECIFIED** |
| **Delete visibility** | Immediate | Same request | **SPECIFIED** |
| **cuVS improvement** | ≥ 25% | If enabled | **GATE CRITERIA** |

### 14.2 Contract Compliance Metrics (NEW)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Read-your-writes violations | < 0.1% | Queries < 100ms missing recent inserts |
| Delete leakage | 0% | Deleted vectors in search results |
| ID collision errors | 0% | Duplicate external IDs accepted |
| SLO breaches | < 5% | Queries exceeding P95 target |

---

## 17. Risks and Mitigations (UPDATED)

### 17.1 Technical Risks (UPDATED)

| Risk | Probability | Impact | Mitigation | v1.1 Change |
|------|-------------|--------|------------|-------------|
| cuVS on Thor untested | High | High | Gate criteria, shadow mode, feature flag | **NEW** |
| SLO outside reference config | High | Medium | Degradation matrix, /slo/estimate API | **NEW** |
| Tombstone accumulation | Medium | Medium | Compaction triggers at 10% | **NEW** |
| Read-your-writes violation | Medium | Low | 100ms bound, flush triggers | **NEW** |
| ID mapping corruption | Low | High | RocksDB checksums, snapshot recovery | **NEW** |

---

## 18. Open Questions (UPDATED)

### 18.1 Resolved in v1.1

| ID | Question | Resolution |
|----|----------|------------|
| Q1 | cuVS vs FAISS default? | FAISS default; cuVS requires validation gate |
| Q2 | Delete visibility? | Immediate via tombstone bitset |
| Q3 | Read-your-writes timing? | Within 100ms or next batch flush |
| Q4 | ID reuse after delete? | Never allowed |

### 18.2 Remaining Questions

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q5 | GPU bitset vs oversampling? | Bitset (recommended) vs oversample | Week 2 |
| Q6 | WAL replay vs pause during rebuild? | WAL replay (recommended) | Week 4 |
| Q7 | Sync insert option for immediate visibility? | Yes/No | Week 6 |

---

## Summary of v1.1 Changes

| Section | Key Change | User Impact |
|---------|------------|-------------|
| SLO | Reference configuration required | Must capacity plan for deviations |
| SLO | Degradation matrix provided | Can estimate non-reference performance |
| SLO | /slo/estimate API added | Automated capacity planning |
| Delete | Immediate visibility specified | Predictable behavior |
| Delete | Tombstone bitset strategy | Efficient GPU implementation |
| Update | Upsert semantics clarified | Insert existing ID = update |
| Consistency | 100ms read-your-writes bound | Application design guidance |
| ID | External ID never reused | Prevents confusion |
| cuVS | Optional with gate criteria | Reduced deployment risk |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial PRD |
| 1.1 | 2025-01-20 | AkiDB Team | SLO boundaries, delete/update contracts, consistency guarantees, cuVS gate |

---

*End of PRD v1.1*
