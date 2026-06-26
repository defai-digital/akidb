# PRD: AutomatosX Principles Adoption for AkiDB

## Document Information
- **Version**: 1.0
- **Date**: 2026-01-22
- **Author**: Engineering Team
- **Status**: Draft - Pending Approval

---

## 1. Executive Summary

### 1.1 Overview
This PRD outlines the adoption of AutomatosX architectural principles (Contracts, Domains, Workflows, Invariants, Guards) into AkiDB, a high-performance vector database. The goal is to systematically prevent classes of bugs rather than fixing individual instances.

### 1.2 Problem Statement
Recent bug hunting sessions revealed 9 significant bugs (3 critical, 5 high, 1 medium) in AkiDB. While all bugs were fixed, analysis shows that 7 of 9 bugs could have been prevented with explicit architectural safeguards.

### 1.3 Proposed Solution
Incrementally adopt AutomatosX principles over 10-12 weeks, starting with contracts at system boundaries and progressing to state machine guards for the rebuild process.

### 1.4 Success Metrics
| Metric | Target |
|--------|--------|
| Query P95 latency | <1% regression |
| Bug prevention rate | >70% of similar bug classes |
| Test coverage on new code | >90% |
| Time to detect invariant violation | <1 minute |

---

## 2. Background

### 2.1 Current State
AkiDB is a production vector database with:
- 241 passing tests
- Sub-50ms P95 query latency target
- Dual-index rebuild for GPU vector indexing
- Distributed shard coordination

### 2.2 Recent Bugs Discovered

| Bug ID | Severity | Root Cause | Preventable By |
|--------|----------|------------|----------------|
| BUG-HUNT-001 | CRITICAL | TOCTOU race condition | Invariant/Domain |
| BUG-HUNT-002 | CRITICAL | No input validation | Contract |
| BUG-HUNT-003 | HIGH | Unbounded data structure | Invariant |
| BUG-HUNT-004 | HIGH | Encoding collision | Contract/Newtype |
| BUG-HUNT-005 | HIGH | Wrong memory ordering | Guard/Policy |
| BUG-HUNT-006 | HIGH | Panic instead of error | Contract |
| BUG-HUNT-201 | CRITICAL | No panic recovery | Guard/Workflow |
| BUG-HUNT-202 | HIGH | State synchronization | Invariant |
| BUG-HUNT-203 | MEDIUM | Silent failure | Observability |

### 2.3 AutomatosX Principles

1. **Contracts**: Explicit validation rules at system boundaries
2. **Domains**: Encapsulated modules with atomic APIs
3. **Workflows**: Formalized multi-step processes
4. **Invariants**: Machine-checked state assumptions
5. **Guards**: Policy enforcement for state transitions

---

## 3. Requirements

### 3.1 Functional Requirements

#### FR-1: Contract Validation
- **FR-1.1**: All WAL entries MUST be validated before write
- **FR-1.2**: Maximum vector dimensions: 4,096
- **FR-1.3**: Maximum metadata size: 64KB
- **FR-1.4**: gRPC requests MUST be validated at entry point
- **FR-1.5**: Collection keys MUST use length-prefixed encoding

#### FR-2: Invariant Checking
- **FR-2.1**: ID mapping MUST maintain bijectivity
- **FR-2.2**: Search results MUST be sorted by distance
- **FR-2.3**: Tombstoned vectors MUST NOT appear in results
- **FR-2.4**: Heap size MUST NOT exceed 1.5x capacity

#### FR-3: Rebuild State Machine
- **FR-3.1**: Rebuild MUST follow defined state transitions
- **FR-3.2**: States: Idle → Preparing → Rebuilding → Swapping → Finishing → Idle
- **FR-3.3**: Each transition MUST pass guard checks
- **FR-3.4**: Panic during rebuild MUST trigger rollback

#### FR-4: Query Workflow
- **FR-4.1**: Fan-out/merge MUST have timeout handling
- **FR-4.2**: Partial results MUST report coverage ratio
- **FR-4.3**: Results MUST be deduplicated across shards

#### FR-5: Observability
- **FR-5.1**: Invariant violations MUST emit metrics
- **FR-5.2**: Critical violations MUST trigger alerts
- **FR-5.3**: Rebuild state changes MUST be logged

### 3.2 Non-Functional Requirements

#### NFR-1: Performance
- Query P95 latency: <50ms (no regression >1%)
- Insert throughput: No regression >5%
- Rebuild time: No regression >10%

#### NFR-2: Reliability
- Data durability: No data loss during rebuild
- Recovery time: <5 minutes from any failure state

#### NFR-3: Maintainability
- Code coverage: >90% on new modules
- Documentation: All contracts documented in code
- Review: All guard logic peer-reviewed

---

## 4. Technical Design

### 4.1 New Crate Structure

```
crates/
├── akidb-contracts/        # NEW: Boundary validation
│   ├── src/
│   │   ├── lib.rs
│   │   ├── wal.rs          # WAL entry contracts
│   │   ├── grpc.rs         # Request validation
│   │   └── storage.rs      # Key encoding newtypes
│   └── Cargo.toml
└── akidb-invariants/       # NEW: Test utilities
    ├── src/
    │   ├── lib.rs
    │   └── macros.rs       # debug_invariant! macro
    └── Cargo.toml
```

### 4.2 Contract Implementation

```rust
// crates/akidb-contracts/src/wal.rs
pub const MAX_VECTOR_DIMENSIONS: usize = 4096;
pub const MAX_METADATA_BYTES: usize = 65536;

#[derive(Debug)]
pub struct ContractViolation {
    pub field: &'static str,
    pub message: String,
}

pub fn validate_wal_entry(entry: &WalEntry) -> Result<(), ContractViolation> {
    if entry.vector.len() > MAX_VECTOR_DIMENSIONS {
        return Err(ContractViolation {
            field: "vector",
            message: format!("Vector dimensions {} exceed max {}",
                entry.vector.len(), MAX_VECTOR_DIMENSIONS),
        });
    }
    if entry.metadata.len() > MAX_METADATA_BYTES {
        return Err(ContractViolation {
            field: "metadata",
            message: format!("Metadata size {} exceeds max {}",
                entry.metadata.len(), MAX_METADATA_BYTES),
        });
    }
    Ok(())
}
```

### 4.3 Newtype Pattern

```rust
// crates/akidb-contracts/src/storage.rs

/// Collection key with guaranteed length-prefixed encoding.
/// Prevents BUG-HUNT-004 class of bugs at compile time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionKey(String);

impl CollectionKey {
    pub fn new(collection: &str, id: &str) -> Self {
        // Length-prefixed encoding: "{col_len}:{collection}{id}"
        Self(format!("{}:{}{}", collection.len(), collection, id))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

// Cannot create CollectionKey any other way - encoding is guaranteed
```

### 4.4 Invariant Macro

```rust
// crates/akidb-invariants/src/macros.rs

/// Debug-only invariant check. Compiles to no-op in release builds.
#[macro_export]
macro_rules! debug_invariant {
    ($cond:expr, $msg:expr) => {
        #[cfg(debug_assertions)]
        {
            if !$cond {
                panic!("INVARIANT VIOLATED: {}", $msg);
            }
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            if !$cond {
                panic!("INVARIANT VIOLATED: {}", format!($fmt, $($arg)*));
            }
        }
    };
}

/// Release-mode invariant for critical checks (use sparingly).
#[macro_export]
macro_rules! invariant {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            tracing::error!(invariant = $msg, "CRITICAL INVARIANT VIOLATED");
            INVARIANT_VIOLATIONS.with_label_values(&[$msg, "critical"]).inc();
        }
    };
}
```

### 4.5 Typestate Rebuild FSM

```rust
// crates/faiss-wrapper/src/rebuild_fsm.rs
use std::marker::PhantomData;

// Zero-sized state types
pub struct Idle;
pub struct Preparing { pub snapshot_id: SnapshotId }
pub struct Rebuilding { pub old: IndexHandle, pub new: IndexHandle }
pub struct Swapping { pub new: IndexHandle }
pub struct Finishing { pub old: IndexHandle }

pub struct RebuildMachine<S> {
    _state: PhantomData<S>,
    index: Arc<RwLock<Index>>,
}

impl RebuildMachine<Idle> {
    pub fn start_rebuild(self) -> Result<RebuildMachine<Preparing>, GuardViolation> {
        // Guard G1: Must have recovery point
        let snapshot_id = self.ensure_snapshot_exists()?;
        self.ensure_wal_flushed()?;

        Ok(RebuildMachine {
            _state: PhantomData,
            index: self.index,
        })
    }
}

impl RebuildMachine<Rebuilding> {
    pub fn ready_to_swap(self) -> Result<RebuildMachine<Swapping>, GuardViolation> {
        // Guard G2: New index must be healthy
        self.verify_new_index_healthy()?;
        self.verify_vector_count()?;

        Ok(RebuildMachine {
            _state: PhantomData,
            index: self.index,
        })
    }
}

impl RebuildMachine<Swapping> {
    pub fn complete_swap(self) -> Result<RebuildMachine<Finishing>, GuardViolation> {
        // Guard G3: No data loss
        self.verify_no_data_loss()?;

        Ok(RebuildMachine {
            _state: PhantomData,
            index: self.index,
        })
    }
}
```

### 4.6 Observability

```rust
// crates/akidb-common/src/metrics.rs
use lazy_static::lazy_static;
use prometheus::{IntCounterVec, register_int_counter_vec};

lazy_static! {
    pub static ref INVARIANT_VIOLATIONS: IntCounterVec = register_int_counter_vec!(
        "akidb_invariant_violations_total",
        "Total count of invariant violations",
        &["invariant_id", "severity"]
    ).unwrap();

    pub static ref REBUILD_STATE_TRANSITIONS: IntCounterVec = register_int_counter_vec!(
        "akidb_rebuild_state_transitions_total",
        "Count of rebuild state transitions",
        &["from_state", "to_state", "result"]
    ).unwrap();
}
```

---

## 5. Implementation Plan

### Phase 1: Contracts at Boundaries (Weeks 1-2)

| Task | Owner | Effort |
|------|-------|--------|
| Create `akidb-contracts` crate | TBD | 2 days |
| Implement WAL entry validation | TBD | 2 days |
| Implement gRPC request validation | TBD | 2 days |
| Add `CollectionKey` newtype | TBD | 1 day |
| Integration tests | TBD | 2 days |
| Performance benchmarking | TBD | 1 day |

**Deliverables**:
- `crates/akidb-contracts/` with validation logic
- Integration with existing WAL and gRPC code
- Benchmark report showing <1% latency impact

### Phase 2: Invariants (Weeks 3-4)

| Task | Owner | Effort |
|------|-------|--------|
| Create `akidb-invariants` crate | TBD | 1 day |
| Implement `debug_invariant!` macro | TBD | 1 day |
| Add invariants to ID mapping | TBD | 2 days |
| Add invariants to result merger | TBD | 2 days |
| Property-based tests with proptest | TBD | 3 days |
| Update CI for invariant checks | TBD | 1 day |

**Deliverables**:
- `crates/akidb-invariants/` with macro
- Invariant checks in critical paths
- Property-based test suite

### Phase 3: Rebuild State Machine (Weeks 5-8)

| Task | Owner | Effort |
|------|-------|--------|
| Design typestate FSM | TBD | 3 days |
| Implement state types | TBD | 2 days |
| Implement guard checks | TBD | 5 days |
| Add panic recovery | TBD | 3 days |
| Integrate with existing rebuild | TBD | 5 days |
| Comprehensive testing | TBD | 5 days |

**Deliverables**:
- Typestate-based rebuild FSM
- Guard checks for all transitions
- Panic recovery with rollback

### Phase 4: Query Workflow (Weeks 9-10)

| Task | Owner | Effort |
|------|-------|--------|
| Design workflow structure | TBD | 2 days |
| Implement fan-out logic | TBD | 2 days |
| Implement merge with coverage | TBD | 2 days |
| Add timeout handling | TBD | 2 days |
| Integration testing | TBD | 2 days |

**Deliverables**:
- Formalized query workflow
- Coverage reporting
- Timeout and fallback handling

### Phase 5: Observability (Ongoing)

| Task | Owner | Effort |
|------|-------|--------|
| Add Prometheus metrics | TBD | 2 days |
| Create Grafana dashboards | TBD | 2 days |
| Configure alerts | TBD | 1 day |
| Documentation | TBD | 2 days |

**Deliverables**:
- Metrics for invariant violations
- Rebuild state dashboard
- Alert configuration

---

## 6. Risks and Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Performance regression | Medium | High | Benchmark each phase; rollback if >1% |
| New bugs from refactoring | Medium | Medium | Incremental changes; existing tests as safety net |
| Team learning curve | Low | Medium | Documentation; pair programming |
| Scope creep | Medium | Medium | Strict phase boundaries; defer domains to future |
| CI time increase | Low | Low | Parallelize tests; cache dependencies |

---

## 7. Dependencies

### External Dependencies
- `proptest` crate for property-based testing
- `prometheus` crate for metrics
- `tracing` crate for structured logging

### Internal Dependencies
- Existing test suite (241 tests)
- CI/CD pipeline
- Benchmark infrastructure

---

## 8. Acceptance Criteria

### Phase 1 Complete When:
- [ ] All WAL writes pass contract validation
- [ ] All gRPC requests validated at entry
- [ ] `CollectionKey` newtype in use
- [ ] Benchmark shows <1% latency regression

### Phase 2 Complete When:
- [ ] `debug_invariant!` macro available
- [ ] ID mapping has bijectivity invariant
- [ ] Result merger has ordering invariant
- [ ] Property tests pass in CI

### Phase 3 Complete When:
- [ ] Rebuild uses typestate FSM
- [ ] All state transitions have guards
- [ ] Panic during rebuild triggers rollback
- [ ] No data loss in chaos testing

### Phase 4 Complete When:
- [ ] Query workflow has timeout handling
- [ ] Partial results report coverage
- [ ] Deduplication works across shards

### Phase 5 Complete When:
- [ ] Metrics exported to Prometheus
- [ ] Dashboard shows rebuild states
- [ ] Alerts configured for critical violations

---

## 9. Appendix

### A. Glossary
- **Contract**: Explicit validation rule at system boundary
- **Invariant**: Assumption about system state that must always hold
- **Guard**: Check that must pass before state transition
- **Typestate**: Rust pattern using phantom types to encode state
- **TOCTOU**: Time-of-check to time-of-use race condition

### B. References
- AutomatosX GitHub: https://github.com/defai-digital/AutomatosX
- Rust Typestate Pattern: https://docs.rust-lang.org/nomicon/phantom-data.html
- Property-Based Testing: https://proptest-rs.github.io/proptest/

### C. Related Documents
- ADR-002: AutomatosX Principles Adoption
- Bug Hunt Report: BUG-HUNT-001 through BUG-HUNT-203
