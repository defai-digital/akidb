# AkiDB Thor Edition - Architecture Decision Records (ADR)
## Version 1.1

**Version:** 1.1
**Date:** 2025-01-20
**Status:** Approved
**Changes from v1.0:** cuVS gating criteria, SLO boundary conditions, delete/update contract, ID management
**Review:** Multi-model synthesis (Claude, Grok) addressing reviewer feedback

---

## Change Log from v1.0

| Section | Change | Rationale |
|---------|--------|-----------|
| ADR-002 | cuVS moved from "decided" to "optional accelerator" with gate criteria | Unvalidated on Thor ARM64 |
| ADR-002 | Added SLO boundary conditions and reference configuration | Latency targets meaningless without parameters |
| ADR-009 | Added explicit delete/update contract with tombstone strategy | Missing engineering details |
| ADR-009 | Added dual-index swap rebuild strategy | Ingest during rebuild undefined |
| ADR-015 | NEW: ID Management contract | Critical gap identified |
| ADR-016 | NEW: Consistency and Visibility Guarantees | Read-your-writes undefined |
| All | Performance projections marked as "ESTIMATED" with assumptions | Avoid misleading claims |

---

## Table of Contents

- [ADR-002: Vector Index Strategy (FAISS GPU IVF-Flat)](#adr-002-vector-index-strategy-revised)
- [ADR-009: Index Lifecycle - Delete, Update, Rebuild](#adr-009-index-lifecycle-revised)
- [ADR-015: ID Management Contract (NEW)](#adr-015-id-management-contract)
- [ADR-016: Consistency and Visibility Guarantees (NEW)](#adr-016-consistency-guarantees)

*Note: Only revised and new ADRs included. Unchanged ADRs (001, 003-008, 010-014) remain as in v1.0.*

---

## ADR-002: Vector Index Strategy (REVISED)

### Status
**Accepted** (Revised from v1.0)

### Context

We need a vector index for NVIDIA Jetson Thor that maximizes GPU utilization while maintaining recall guarantees. The v1.0 decision positioned cuVS as the primary accelerator, but reviewer feedback correctly identified that cuVS on ARM64 Blackwell is **unvalidated** and should not be treated as decided.

### Decision

We adopt a **two-layer decision** with explicit gating:

```
┌─────────────────────────────────────────────────────────────┐
│                   DECISION LAYER 1 (STABLE)                 │
│                                                             │
│   PRIMARY: FAISS GPU IVF-Flat                              │
│   • Mature, well-tested on NVIDIA GPUs                     │
│   • Known behavior, regression testable                     │
│   • Fallback: CPU IVF-Flat if GPU unavailable              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                DECISION LAYER 2 (EXPERIMENTAL)              │
│                                                             │
│   OPTIONAL ACCELERATOR: cuVS                               │
│   • NOT enabled by default                                  │
│   • Requires explicit validation gate                       │
│   • Must pass enablement criteria before production         │
│   • Rollback available via feature flag                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Index Configuration

```yaml
index:
  # Primary configuration (FAISS GPU IVF-Flat)
  type: IVF4096,Flat
  nlist: 4096
  nprobe: 32                    # Default, tunable per query
  metric: cosine                # Or L2, inner_product
  gpu_memory_fraction: 0.60    # 60% of available unified memory

  # Fallback configuration
  fallback:
    type: cpu_ivf_flat
    enabled: true
    trigger: gpu_oom OR gpu_unavailable
```

### cuVS Gate Criteria

cuVS may ONLY be enabled in production when ALL of the following criteria are met:

```yaml
cuvs_gate:
  # ═══════════════════════════════════════════════════════════
  # ENABLEMENT THRESHOLDS
  # ═══════════════════════════════════════════════════════════

  latency_improvement:
    minimum: 0.25              # ≥25% P95 reduction (MUST meet)
    target: 0.40               # ≥40% recommended for production
    measurement: "P95 latency vs FAISS baseline, identical queries"

  recall_tiers:
    high_fidelity: 0.98        # For RAG, transactional
    balanced: 0.95             # DEFAULT for Thor edge
    speed_critical: 0.90       # For real-time, approximate OK
    default: balanced

  memory_overhead_max: 0.15    # ≤15% additional GPU memory

  power_budget_watts: 40       # Thor-specific: sustained average

  # ═══════════════════════════════════════════════════════════
  # VALIDATION PROTOCOL (REQUIRED BEFORE ENABLEMENT)
  # ═══════════════════════════════════════════════════════════

  validation:
    shadow_mode:
      required: true
      duration_hours: 24       # Minimum parallel execution
      result_divergence_max: 0.001  # <0.1% difference from FAISS

    benchmark_suite: "mlperf_edge"  # Standardized
    thermal_test:
      duration_minutes: 30
      max_temperature_celsius: 85

    test_configuration:
      D: 768
      N: 1000000
      topK: 10
      nprobe: 32
      batch_sizes: [1, 32, 64]

  # ═══════════════════════════════════════════════════════════
  # FEATURE FLAG AND ROLLBACK
  # ═══════════════════════════════════════════════════════════

  feature_flag:
    name: "AKIDB_USE_CUVS"
    default: false             # OFF by default
    hot_toggle: true           # No restart required

  rollback_triggers:           # ANY condition for 30s+ triggers rollback
    p99_latency_multiplier: 3.0    # 3x baseline
    recall_drop_absolute: 0.05     # 5% recall loss
    temperature_celsius: 85
    oom_detected: true
    power_watts_exceeded: 60       # Thor thermal limit

  rollback_behavior:
    action: "disable_cuvs_flag"
    logging: "error"
    alert: "pagerduty"
    cooldown_minutes: 30       # Before re-enablement attempt
```

### SLO Boundary Conditions (NEW in v1.1)

**CRITICAL:** All latency targets apply ONLY within the reference configuration. Deviations require explicit capacity planning.

#### Reference Configuration

| Parameter | Reference Value | Valid Range | Notes |
|-----------|-----------------|-------------|-------|
| **D** (dimensions) | 768 | 128–1024 | LLM embedding standard |
| **N** (vectors/shard) | 1,000,000 | 100K–2M | Thor memory constraint |
| **topK** | 10 | 1–100 | Standard retrieval |
| **nprobe** | 32 | 16–64 | Recall/latency balance |
| **batch** | 1 | 1–256 | Single-query baseline |
| **nlist** | 4096 | √N heuristic | Cluster count |
| **filter_selectivity** | ≥1% | ≥0.1% | Metadata filter hit rate |

#### SLO at Reference Configuration

| Metric | Target | Measurement |
|--------|--------|-------------|
| P50 Latency (per shard) | < 5ms | FAISS search only |
| P95 Latency (per shard) | < 10ms | FAISS search only |
| P99 Latency (per shard) | < 20ms | FAISS search only |
| Recall@10 | ≥ 0.95 | vs brute-force baseline |

#### Degradation Matrix (ESTIMATED)

> **WARNING:** These are estimates. Validate on Thor hardware.

| Deviation from Reference | Latency Multiplier | Recall Impact |
|--------------------------|-------------------|---------------|
| D = 1536 (2x dims) | 1.5x | None |
| N = 5M (5x vectors) | 2.0x | None |
| topK = 100 (10x depth) | 1.3x | None |
| nprobe = 64 (2x probes) | 1.7x | +2% recall |
| batch = 32 | 0.4x per query | None (amortized) |
| filter_selectivity = 0.1% | 2.0x | None |

#### SLO Estimation API

```
GET /v1/slo/estimate?d=768&n=1500000&topK=50&nprobe=32

Response:
{
  "p95_estimate_ms": 67,
  "p99_estimate_ms": 95,
  "recall_estimate": 0.96,
  "confidence": 0.85,
  "within_slo": false,
  "reference_config": {...},
  "deviation_factors": {
    "n": 1.5,
    "topK": 1.3
  },
  "recommendation": "Reduce N to 1M or topK to 10 for SLO compliance"
}
```

### Performance Projections (ESTIMATED)

> **DISCLAIMER:** These projections are estimates based on similar hardware. **Actual performance must be validated on Jetson Thor.** Assumes D=768, topK=10, nprobe=32, batch≥32.

| Vectors | GPU Memory | Build Time | Search P50 | Search P95 |
|---------|------------|------------|------------|------------|
| 100K | ~400MB | ~10s | 2ms | 5ms |
| 1M | ~4GB | ~2min | 5ms | 10ms |
| 10M | ~40GB | ~20min | 8ms | 15ms |

### Validation Requirements

**CRITICAL: Before finalizing architecture:**

1. **Hardware validation:**
   - [ ] Acquire Jetson Thor hardware
   - [ ] Verify FAISS GPU IVF-Flat builds and runs
   - [ ] Benchmark at reference configuration
   - [ ] Document actual latency/recall numbers

2. **cuVS validation (if pursuing):**
   - [ ] Run 24h shadow mode
   - [ ] Confirm <0.1% result divergence
   - [ ] Verify ≥25% latency improvement
   - [ ] Validate thermal stability (30min sustained)

3. **Comparative benchmark:**
   - [ ] Benchmark HNSW (CPU) vs IVF (GPU) under Thor unified memory
   - [ ] Test batch=1 and batch=64 scenarios
   - [ ] Report P95/P99 and recall@10
   - [ ] Document power consumption

### Consequences

**Positive:**
- Conservative approach reduces deployment risk
- Clear criteria for cuVS adoption
- Explicit SLO boundaries prevent overpromising
- Rollback mechanism provides safety net

**Negative:**
- May not capture full cuVS potential initially
- Two codepaths increase testing burden
- SLO estimation adds complexity

---

## ADR-009: Index Lifecycle - Delete, Update, Rebuild (REVISED)

### Status
**Accepted** (Revised from v1.0)

### Context

v1.0 mentioned "soft delete" and "shadow rebuild" without specifying critical engineering details:
- How are deleted vectors excluded from search results?
- What happens to ingests during rebuild?
- How are external IDs mapped to internal FAISS indices?

These gaps would cause immediate engineering problems.

### Decision

We define explicit contracts for delete, update, and rebuild operations.

### Delete Contract

#### Tombstone Filtering Strategy: GPU Bitset

```cpp
// GPU bitset: 1 bit per vector
// Memory: 125KB for N=1M vectors (trivial)
// 0 = active, 1 = deleted

thrust::device_vector<uint8_t> tombstone_bitset((N + 7) / 8);

// Thread-safe access via reader-writer lock
std::shared_mutex bitset_mutex;

// Integration with FAISS search
void search_with_tombstones(
    const float* query,
    int k,
    float* distances,
    int64_t* labels
) {
    // Acquire read lock
    std::shared_lock lock(bitset_mutex);

    // Set FAISS search parameters with ID selector
    faiss::IDSelectorBitmap selector(N, tombstone_bitset.data().get());
    faiss::SearchParametersIVF params;
    params.sel = &selector;

    // Execute search (deleted vectors excluded)
    index->search(1, query, k, distances, labels, &params);
}
```

**Why bitset over oversampling:**
- Oversampling (fetch 2x topK, filter CPU-side) wastes GPU bandwidth
- Bitset filtering is O(1) memory per vector
- Thor's unified memory makes bitset updates efficient
- FAISS IDSelector integrates natively

#### Delete Visibility

| Aspect | Contract |
|--------|----------|
| **Immediate visibility** | Within same request (bitset updated before response) |
| **Cross-node visibility** | Eventual (coordinator caches health, not tombstones) |
| **Search behavior** | Deleted vectors NEVER returned in results |

#### Delete API Response

```protobuf
message DeleteResponse {
  bool success = 1;
  string id = 2;
  DeleteStatus status = 3;
}

enum DeleteStatus {
  DELETED = 0;           // Vector existed and was deleted
  NOT_FOUND = 1;         // Vector ID did not exist (no-op, success=true)
  ALREADY_DELETED = 2;   // Vector was already deleted (no-op, success=true)
}
```

### Update Contract

**Semantics:** Update = Delete + Insert (not in-place modification)

```
Update(id, new_vector, new_metadata):
1. Mark old internal_id in tombstone bitset
2. Allocate new internal_id
3. Add new vector to FAISS index
4. Update external→internal mapping
5. Return success

Atomicity: NOT atomic. Concurrent reads may see:
- Old vector (before step 1)
- No vector (between steps 1-3)
- New vector (after step 4)

Read-your-writes: Guaranteed within 100ms (see ADR-016)
```

### Tombstone Compaction

Tombstones accumulate and degrade performance. Compaction reclaims space.

```yaml
compaction:
  # Trigger conditions (ANY triggers compaction)
  triggers:
    tombstone_ratio: 0.10      # 10% of vectors deleted
    tombstone_count: 100000    # 100K absolute

  # Scheduling
  schedule: "off_peak"         # Prefer low-traffic windows
  max_duration_seconds: 120    # Abort if exceeded, retry later

  # Behavior
  method: "full_rebuild"       # Rebuild index excluding tombstones
  concurrent_reads: true       # Serve queries during compaction
```

### Rebuild Strategy: Dual-Index Swap

**Goal:** Zero-downtime rebuilds with no data loss.

```
┌─────────────────────────────────────────────────────────────┐
│                    REBUILD PROCESS                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  PHASE 1: PRE-REBUILD                                       │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  1. Record WAL position (LSN_start)                   │ │
│  │  2. Allocate memory for shadow index                  │ │
│  │  3. Set rebuild_in_progress = true                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  PHASE 2: DURING REBUILD                                    │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  READS: Served by OLD index (unchanged)               │ │
│  │  WRITES: Go to BOTH:                                  │ │
│  │    - Old index (immediate searchability)              │ │
│  │    - WAL (for replay into new index)                  │ │
│  │                                                       │ │
│  │  SHADOW INDEX: Built from:                            │ │
│  │    - RocksDB snapshot (source of truth)              │ │
│  │    - Excludes tombstoned vectors                      │ │
│  │    - Retrains IVF clusters if drift detected          │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  PHASE 3: POST-REBUILD                                      │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  1. Replay WAL entries where LSN > LSN_start          │ │
│  │  2. Validate shadow index (sample queries)            │ │
│  │  3. Atomic pointer swap: index_ptr = shadow_index     │ │
│  │  4. Set rebuild_in_progress = false                   │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  PHASE 4: CLEANUP                                           │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  1. Deallocate old index memory                       │ │
│  │  2. Clear replayed WAL entries                        │ │
│  │  3. Reset tombstone bitset (all zeros)                │ │
│  │  4. Snapshot new index to MinIO                       │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Ingest During Rebuild

**Strategy:** Dual-write with WAL replay (NOT pause)

```yaml
rebuild:
  ingest_strategy: dual_write_wal_replay

  wal_queue:
    # Hybrid threshold: floor + memory-based scaling
    pause_threshold: "max(10000, available_memory_bytes * 0.01 / avg_vector_bytes)"

    # Backpressure behavior
    backpressure:
      soft_limit: 0.5            # 50% of threshold: slow down
      hard_limit: 1.0            # 100% of threshold: pause ingest
      resume_at: 0.3             # Resume when queue at 30%

  # Timeout
  max_rebuild_duration_seconds: 300  # 5 minutes
  timeout_behavior: "abort_and_retry_later"
```

### Memory Requirements During Rebuild

```
During rebuild, memory usage peaks at ~2x normal:

  Normal operation:
    FAISS index:     40% (12.8 GB on 32GB node)
    Embedding:       30% (9.6 GB)
    System:          20% (6.4 GB)
    Headroom:        10% (3.2 GB)

  During rebuild:
    Old index:       40% (12.8 GB)
    Shadow index:    40% (12.8 GB)   ← ADDITIONAL
    Embedding:       Reduced to 15% (temporarily)
    System:          5% (minimum)

  Mitigation:
    - Unload embedding model during rebuild
    - Schedule rebuilds during low-traffic
    - Abort if memory pressure exceeds 95%
```

### Fault Tolerance

| Failure | Detection | Recovery |
|---------|-----------|----------|
| GPU OOM mid-rebuild | CUDA error | Abort rebuild, restore old index |
| Power loss during rebuild | Marker file check at startup | Restore from last MinIO snapshot |
| Corrupted shadow index | Validation fails | Discard shadow, keep old index |
| WAL corruption | Checksum mismatch | Restore from snapshot, accept data loss |

### Consequences

**Positive:**
- Zero-downtime rebuilds
- Explicit delete/update contracts
- Clear failure recovery paths
- Tombstone accumulation bounded

**Negative:**
- Dual-write adds complexity
- 2x memory during rebuild
- WAL replay adds recovery time

---

## ADR-015: ID Management Contract (NEW)

### Status
**Accepted**

### Context

FAISS uses internal contiguous indices (0, 1, 2, ...). Users provide external IDs (UUIDs, strings). The mapping between them is critical for correctness.

### Decision

We implement a **two-tier ID mapping** with explicit collision handling.

```
┌─────────────────────────────────────────────────────────────┐
│                    ID MAPPING TIERS                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  TIER 1: External ID (User-facing)                          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Type:       String (UUID, alphanumeric)              │ │
│  │  Uniqueness: REQUIRED (per collection)                │ │
│  │  Immutable:  YES (never reused after delete)          │ │
│  │  Storage:    RocksDB                                  │ │
│  │  Max Length: 256 bytes                                │ │
│  └───────────────────────────────────────────────────────┘ │
│                           │                                 │
│                           ▼                                 │
│  TIER 2: Internal ID (FAISS)                               │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Type:       int64                                    │ │
│  │  Uniqueness: REQUIRED                                 │ │
│  │  Mutable:    YES (may change on compaction)           │ │
│  │  Storage:    Memory (dense array)                     │ │
│  │  Range:      0 to N-1 (contiguous)                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Mapping Storage

```
Forward Map (RocksDB):
  Key:   external_id (string)
  Value: {
    internal_id: int64,
    created_at: timestamp,
    deleted: bool
  }

Reverse Map (Memory):
  Type:  Dense array [internal_id] → external_id
  Size:  N * sizeof(pointer)
  Use:   Return external IDs in search results
```

### Collision Handling

| Operation | Existing ID? | Behavior |
|-----------|--------------|----------|
| `insert(id, vec)` | No | Create new mapping, allocate internal_id |
| `insert(id, vec)` | Yes, active | **UPSERT**: Update vector, preserve internal_id |
| `insert(id, vec)` | Yes, deleted | **ERROR**: External IDs never reused |
| `delete(id)` | Yes, active | Mark tombstone, preserve mapping |
| `delete(id)` | No | **NO-OP**: Return success, log warning |
| `delete(id)` | Yes, deleted | **NO-OP**: Return success |

### Internal ID Lifecycle

```
ALLOCATION:
  - New vector → Allocate next available internal_id
  - Use free list if available (from compaction)
  - If free list empty, increment max_internal_id

DELETION:
  - Mark tombstone bitset
  - Do NOT add to free list immediately
  - Internal ID remains "occupied" until compaction

COMPACTION:
  - Rebuild index with contiguous internal IDs
  - Update all internal_id mappings
  - Reset free list
  - Internal IDs 0 to N-1 (no gaps)
```

### Invariants

1. **External ID uniqueness:** No two active vectors share external ID
2. **External ID immortality:** Deleted external IDs NEVER reused
3. **Internal ID density:** After compaction, internal IDs are contiguous
4. **Mapping consistency:** Forward and reverse maps always agree

### Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Forward map corrupted | RocksDB checksum | Restore from snapshot |
| Reverse map corrupted | Startup validation | Rebuild from forward map |
| ID exhaustion (int64) | Allocation check | Compaction required (practically impossible) |

---

## ADR-016: Consistency and Visibility Guarantees (NEW)

### Status
**Accepted**

### Context

Users need to know when operations become visible to queries. v1.0 did not specify read-your-writes behavior.

### Decision

We adopt **eventual consistency** with **bounded visibility lag**.

### Visibility Guarantees

| Operation | Visibility | Maximum Lag |
|-----------|------------|-------------|
| `insert` | Eventual | **100ms** or next batch flush |
| `delete` | Immediate | Same request |
| `update` | Eventual | **100ms** (delete immediate, insert eventual) |
| `search` | Snapshot | Consistent within single query |

### Read-Your-Writes Contract

```
After insert(id, vec) returns success:
  - search(vec) MAY NOT find id immediately
  - search(vec) WILL find id within 100ms
  - get(id) WILL return vec immediately (from WAL/RocksDB)

After delete(id) returns success:
  - search(*) WILL NOT return id (immediate)
  - get(id) WILL return NOT_FOUND (immediate)

After update(id, new_vec) returns success:
  - search(old_vec) WILL NOT return id (immediate, delete part)
  - search(new_vec) MAY NOT find id immediately (insert part)
  - search(new_vec) WILL find id within 100ms
```

### Batch Flush Behavior

```yaml
batch_flush:
  # Trigger conditions (ANY triggers flush)
  triggers:
    interval_ms: 50            # Flush every 50ms
    batch_size: 1000           # Or every 1000 vectors
    memory_pressure: 0.80      # Or at 80% buffer usage

  # Flush makes vectors searchable
  behavior:
    pre_flush: "vectors in write buffer, not searchable"
    post_flush: "vectors in FAISS index, searchable"
```

### Cross-Shard Consistency

```
Scenario: Insert to shard A, then search across shards A, B, C

Timeline:
  T0: insert(shard_A, id, vec) → success
  T1: search(vec) → fans out to A, B, C
      - Shard A: MAY or MAY NOT include id (depends on flush)
      - Shard B, C: Irrelevant (don't have id)

Guarantee:
  - If T1 > T0 + 100ms, shard A WILL include id
  - Coordinator does NOT provide cross-shard consistency
  - Each shard provides independent eventual consistency
```

### Conflict Resolution

```
Scenario: Concurrent insert and delete of same ID

Timeline:
  T0: Client A calls insert(id, vec1)
  T0: Client B calls delete(id)

Resolution: Last-writer-wins based on server receipt time
  - If delete received after insert: Vector deleted
  - If insert received after delete: ERROR (ID was deleted, cannot reuse)

Recommendation: Use optimistic locking at application layer if needed
```

### Observability

```yaml
metrics:
  # Visibility lag monitoring
  akidb_write_buffer_size:
    type: gauge
    description: "Vectors pending flush"

  akidb_flush_lag_ms:
    type: histogram
    description: "Time from insert to searchable"
    buckets: [10, 25, 50, 100, 200, 500]

  akidb_read_your_writes_violations:
    type: counter
    description: "Queries within 100ms that missed recently inserted vectors"
```

---

## Summary of v1.1 Changes

| ADR | Key Change | Impact |
|-----|------------|--------|
| **002** | cuVS gated, not default | Reduces deployment risk |
| **002** | SLO reference configuration added | Enables meaningful benchmarks |
| **002** | Degradation matrix added | Sets expectations for non-reference configs |
| **009** | Tombstone bitset strategy specified | Enables delete implementation |
| **009** | Dual-index swap rebuild detailed | Enables zero-downtime rebuilds |
| **009** | Ingest during rebuild defined | Prevents data loss |
| **015** | ID management contract (NEW) | Prevents mapping bugs |
| **016** | Consistency guarantees (NEW) | Sets user expectations |

---

## Validation Checklist for v1.1

Before signing off on architecture:

- [ ] **Hardware:** Jetson Thor acquired and operational
- [ ] **FAISS:** GPU IVF-Flat benchmark at reference config
- [ ] **SLO:** Actual latency/recall documented
- [ ] **cuVS:** 24h shadow mode (if pursuing)
- [ ] **Delete:** Tombstone filtering validated
- [ ] **Rebuild:** Dual-index swap tested with concurrent ingest
- [ ] **Consistency:** Read-your-writes <100ms validated

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial ADRs |
| 1.1 | 2025-01-20 | AkiDB Team | cuVS gate, SLO boundaries, delete/update contract, ID management, consistency guarantees |

---

*End of ADR v1.1*
