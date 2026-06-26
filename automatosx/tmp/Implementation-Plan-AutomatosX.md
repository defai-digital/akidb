# Implementation Plan: AutomatosX Principles Adoption

## Document Information
- **Date**: 2026-01-22
- **Version**: 1.0
- **Status**: Ready for Review

---

## Executive Summary

This implementation plan details the incremental adoption of AutomatosX architectural principles for AkiDB over 10-12 weeks. The plan prioritizes low-risk, high-value changes first, with continuous validation against performance targets.

---

## Timeline Overview

```
Week 1-2    Week 3-4    Week 5-8         Week 9-10    Ongoing
   │           │           │                │           │
   ▼           ▼           ▼                ▼           ▼
┌──────┐   ┌──────┐   ┌──────────┐   ┌──────────┐   ┌──────┐
│Phase1│───│Phase2│───│  Phase3  │───│  Phase4  │───│Phase5│
│Contrc│   │Invari│   │ Rebuild  │   │  Query   │   │Observ│
│ acts │   │ ants │   │   FSM    │   │ Workflow │   │ ability
└──────┘   └──────┘   └──────────┘   └──────────┘   └──────┘
```

---

## Phase 1: Contracts at System Boundaries

**Duration**: Weeks 1-2 (10 working days)
**Risk Level**: Low
**Expected Impact**: Prevents BUG-HUNT-002, BUG-HUNT-004, BUG-HUNT-006

### Week 1: Foundation

#### Day 1-2: Create akidb-contracts Crate
```bash
# Create new crate
cargo new --lib crates/akidb-contracts
```

**Deliverables**:
- `Cargo.toml` with minimal dependencies
- Basic crate structure
- Integration with workspace

#### Day 3-4: WAL Entry Validation
```rust
// Implement in crates/akidb-contracts/src/wal.rs
pub const MAX_VECTOR_DIMENSIONS: usize = 4096;
pub const MAX_METADATA_BYTES: usize = 65536;

pub fn validate_wal_entry(entry: &WalEntry) -> Result<(), ContractViolation>;
```

**Integration Point**: `crates/storage/src/wal.rs:append_insert()`

#### Day 5: gRPC Request Validation
```rust
// Implement in crates/akidb-contracts/src/grpc.rs
pub fn validate_insert_request(req: &InsertRequest) -> Result<(), ContractViolation>;
pub fn validate_search_request(req: &SearchRequest) -> Result<(), ContractViolation>;
```

**Integration Point**: gRPC service handlers

### Week 2: Newtypes and Testing

#### Day 6-7: CollectionKey Newtype
```rust
// Implement in crates/akidb-contracts/src/storage.rs
pub struct CollectionKey(String);

impl CollectionKey {
    pub fn new(collection: &str, id: &str) -> Self {
        Self(format!("{}:{}{}", collection.len(), collection, id))
    }
}
```

**Migration**: Update `crates/storage/src/id_mapping.rs` to use `CollectionKey`

#### Day 8-9: Integration Tests
- Test boundary validation with valid inputs
- Test boundary validation with invalid inputs
- Test CollectionKey encoding uniqueness

#### Day 10: Performance Benchmark
```bash
cargo bench --bench search_benchmark
cargo bench --bench insert_benchmark
```

**Gate**: Proceed only if latency regression <1%

### Phase 1 Checklist
- [ ] `akidb-contracts` crate created
- [ ] WAL validation implemented
- [ ] gRPC validation implemented
- [ ] CollectionKey newtype in use
- [ ] All existing tests pass
- [ ] Benchmark shows <1% regression

---

## Phase 2: Invariants as Debug Assertions

**Duration**: Weeks 3-4 (10 working days)
**Risk Level**: Low
**Expected Impact**: Catches BUG-HUNT-001, BUG-HUNT-003, BUG-HUNT-202 in tests

### Week 3: Macro and Core Invariants

#### Day 11-12: Create akidb-invariants Crate
```rust
// crates/akidb-invariants/src/macros.rs
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
}
```

#### Day 13-14: ID Mapping Invariants
```rust
// Add to crates/storage/src/id_mapping.rs
debug_invariant!(
    self.get(external_id)
        .and_then(|i| self.reverse_lookup(i))
        == Some(external_id.clone()),
    "ID mapping bijectivity"
);
```

#### Day 15: Result Merger Invariants
```rust
// Add to crates/coordinator/src/merger.rs
debug_invariant!(
    results.windows(2).all(|w| w[0].score >= w[1].score),
    "Results must be sorted by score descending"
);
debug_invariant!(
    self.heap.len() <= (self.capacity * 3) / 2,
    "Heap size must not exceed 1.5x capacity"
);
```

### Week 4: Property Testing

#### Day 16-18: proptest Integration
```rust
// crates/akidb-invariants/tests/property_tests.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn id_mapping_bijectivity(id in "[a-z0-9]{1,100}") {
        let mapping = IdMapping::new();
        let internal = mapping.insert(&VectorId::new(&id)).unwrap();
        let external = mapping.reverse_lookup(internal).unwrap();
        prop_assert_eq!(external.as_str(), id);
    }

    #[test]
    fn merger_respects_capacity(
        results in prop::collection::vec(any::<(String, f32)>(), 1..1000),
        capacity in 1usize..100
    ) {
        let mut merger = ResultMerger::new(capacity);
        for (id, score) in results {
            merger.add(make_result(&id, score));
        }
        let final_results = merger.finish();
        prop_assert!(final_results.len() <= capacity);
    }
}
```

#### Day 19: CI Integration
- Add property tests to CI pipeline
- Configure proptest for reasonable runtime

#### Day 20: Documentation
- Document all invariants in code comments
- Update CLAUDE.md with invariant policy

### Phase 2 Checklist
- [ ] `akidb-invariants` crate created
- [ ] `debug_invariant!` macro implemented
- [ ] ID mapping invariants added
- [ ] Result merger invariants added
- [ ] Property tests pass
- [ ] CI runs property tests

---

## Phase 3: Rebuild State Machine with Guards

**Duration**: Weeks 5-8 (20 working days)
**Risk Level**: Medium
**Expected Impact**: Prevents BUG-HUNT-201; makes rebuild panic-safe

### Week 5: Design and State Types

#### Day 21-23: FSM Design
```
States:
  Idle → Preparing → Rebuilding → Swapping → Finishing → Idle

Guards:
  G1: Idle→Preparing:     snapshot_exists() && wal_flushed()
  G2: Preparing→Building: resources_available()
  G3: Building→Swapping:  new_index_healthy() && vector_count_valid()
  G4: Swapping→Finishing: verify_no_data_loss()
  G5: Finishing→Idle:     cleanup_complete()
```

#### Day 24-25: Implement State Types
```rust
// crates/faiss-wrapper/src/rebuild/states.rs
pub struct Idle;
pub struct Preparing { pub snapshot_id: SnapshotId }
pub struct Rebuilding { pub old: IndexHandle, pub new: IndexHandle }
pub struct Swapping { pub new: IndexHandle }
pub struct Finishing { pub old: IndexHandle }
```

### Week 6: Guard Implementation

#### Day 26-28: Pre-Transition Guards
```rust
// crates/faiss-wrapper/src/rebuild/guards.rs
pub fn pre_build_guard(state: &IndexState) -> Result<(), GuardViolation> {
    ensure!(state.snapshot_exists(), "No recovery snapshot");
    ensure!(state.wal_flushed(), "WAL not flushed");
    Ok(())
}

pub fn pre_swap_guard(old: &Index, new: &Index, tombstones: usize)
    -> Result<(), GuardViolation>
{
    let expected = old.count().saturating_sub(tombstones);
    ensure!(new.count() >= expected, "New index missing vectors");
    ensure!(new.is_healthy(), "New index unhealthy");
    Ok(())
}
```

#### Day 29-30: Post-Transition Guards
```rust
pub fn post_swap_guard(old_count: usize, new: &Index, compacted: usize)
    -> Result<(), GuardViolation>
{
    ensure!(new.count() >= old_count - compacted, "Data loss detected");
    Ok(())
}
```

### Week 7: Panic Recovery

#### Day 31-33: catch_unwind Integration
```rust
// crates/faiss-wrapper/src/rebuild/safe_rebuild.rs
use std::panic::{self, AssertUnwindSafe};

pub fn safe_rebuild<F>(f: F) -> Result<(), RebuildError>
where
    F: FnOnce() -> Result<(), RebuildError> + panic::UnwindSafe,
{
    match panic::catch_unwind(f) {
        Ok(result) => result,
        Err(panic_info) => {
            tracing::error!(?panic_info, "Rebuild panicked, initiating rollback");
            rollback_to_snapshot()?;
            Err(RebuildError::PanicRecovered)
        }
    }
}
```

#### Day 34-35: Rollback Logic
```rust
fn rollback_to_snapshot() -> Result<(), RebuildError> {
    // 1. Identify last good snapshot
    // 2. Discard partial new index
    // 3. Restore state to snapshot
    // 4. Log recovery action
}
```

### Week 8: Integration and Testing

#### Day 36-38: Integrate with Existing Code
- Replace existing rebuild logic with FSM
- Ensure backward compatibility
- Update all callers

#### Day 39-40: Testing
- Unit tests for each guard
- Integration tests for full rebuild cycle
- Chaos testing: inject panics at each state

### Phase 3 Checklist
- [ ] State types defined
- [ ] Guards for all transitions
- [ ] Panic recovery implemented
- [ ] Rollback logic tested
- [ ] Integration complete
- [ ] Chaos tests pass

---

## Phase 4: Coordinator Query Workflow

**Duration**: Weeks 9-10 (10 working days)
**Risk Level**: Low
**Expected Impact**: Improves BUG-HUNT-202 prevention; formalizes partial results

### Week 9: Workflow Design

#### Day 41-43: QueryWorkflow Structure
```rust
// crates/coordinator/src/workflow.rs
pub struct QueryWorkflow {
    timeout: Duration,
    min_coverage: f64,
    max_retries: usize,
}

impl QueryWorkflow {
    pub async fn execute(&self, query: SearchRequest) -> SearchResponse {
        let shard_futures = self.fan_out(&query);
        let results = self.collect_with_timeout(shard_futures).await;
        let merged = self.merge_and_deduplicate(results, query.top_k);

        SearchResponse {
            results: merged.results,
            coverage: merged.coverage,
            partial: merged.coverage < 1.0,
        }
    }
}
```

#### Day 44-45: Timeout Handling
```rust
async fn collect_with_timeout(
    &self,
    futures: Vec<impl Future<Output = ShardResult>>,
) -> CollectedResults {
    let deadline = Instant::now() + self.timeout;

    let mut results = Vec::new();
    let mut responding = 0;
    let total = futures.len();

    for future in futures {
        match tokio::time::timeout_at(deadline, future).await {
            Ok(Ok(result)) => {
                results.push(result);
                responding += 1;
            }
            Ok(Err(e)) => tracing::warn!(?e, "Shard error"),
            Err(_) => tracing::warn!("Shard timeout"),
        }
    }

    CollectedResults {
        results,
        coverage: responding as f64 / total as f64,
    }
}
```

### Week 10: Integration and Testing

#### Day 46-48: Merge with Coverage
```rust
fn merge_and_deduplicate(
    &self,
    collected: CollectedResults,
    top_k: usize,
) -> MergedResults {
    let mut merger = ResultMerger::new(top_k);

    for shard_result in collected.results {
        merger.add_results(shard_result.results);
    }

    MergedResults {
        results: merger.finish(),
        coverage: collected.coverage,
    }
}
```

#### Day 49-50: Integration Testing
- Test with all shards responding
- Test with partial shard failure
- Test with complete timeout
- Verify deduplication across shards

### Phase 4 Checklist
- [ ] QueryWorkflow implemented
- [ ] Timeout handling works
- [ ] Coverage reporting accurate
- [ ] Deduplication verified
- [ ] Integration tests pass

---

## Phase 5: Observability (Ongoing)

**Duration**: Ongoing after Phase 4
**Risk Level**: Low
**Expected Impact**: Enables proactive detection of invariant violations

### Metrics Implementation

```rust
// crates/akidb-common/src/metrics.rs
use lazy_static::lazy_static;
use prometheus::{IntCounterVec, Histogram, register_int_counter_vec, register_histogram};

lazy_static! {
    pub static ref INVARIANT_VIOLATIONS: IntCounterVec = register_int_counter_vec!(
        "akidb_invariant_violations_total",
        "Total invariant violations",
        &["invariant_id", "severity"]
    ).unwrap();

    pub static ref REBUILD_DURATION: Histogram = register_histogram!(
        "akidb_rebuild_duration_seconds",
        "Rebuild operation duration"
    ).unwrap();

    pub static ref GUARD_CHECK_FAILURES: IntCounterVec = register_int_counter_vec!(
        "akidb_guard_check_failures_total",
        "Guard check failures by transition",
        &["from_state", "to_state", "guard_id"]
    ).unwrap();
}
```

### Alert Configuration

```yaml
# alerts/akidb_alerts.yml
groups:
  - name: akidb_invariants
    rules:
      - alert: InvariantViolation
        expr: increase(akidb_invariant_violations_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "AkiDB invariant violation detected"

      - alert: RebuildGuardFailure
        expr: increase(akidb_guard_check_failures_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Rebuild guard check failed"
```

### Phase 5 Checklist
- [ ] Prometheus metrics exported
- [ ] Grafana dashboard created
- [ ] Alerts configured
- [ ] Documentation complete

---

## Validation Gates

### Performance Gate (Required at Each Phase)
```bash
# Must pass before proceeding to next phase
cargo bench --bench search_benchmark -- --save-baseline phase_N
# Compare with baseline: regression must be <1%
```

### Test Gate (Required at Each Phase)
```bash
# All existing tests must pass
cargo test --all

# Property tests (Phase 2+)
cargo test --test property_tests

# Chaos tests (Phase 3+)
cargo test --test chaos_tests
```

### Review Gate (Required at Each Phase)
- [ ] Code review by 2 team members
- [ ] Architecture review for Phase 3
- [ ] Security review for contracts

---

## Rollback Plan

### If Phase Causes Issues:
1. **Immediate**: Revert commits for that phase
2. **Within 24h**: Full rollback to pre-phase state
3. **Validate**: Run full test suite
4. **Analyze**: Document what went wrong
5. **Retry**: Fix issues and re-attempt

### Rollback Commands:
```bash
# Identify last good commit
git log --oneline

# Revert to last good state
git revert --no-commit HEAD~N..HEAD
git commit -m "Rollback Phase X due to [reason]"

# Verify
cargo test --all
cargo bench
```

---

## Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Query P95 latency | <1% regression | Benchmark comparison |
| Test coverage | >90% new code | cargo-tarpaulin |
| Bug prevention | >70% similar bugs | Property test coverage |
| Detection time | <1 minute | Alert latency |
| All existing tests | 100% pass | CI pipeline |

---

## Resource Requirements

### Development
- 1-2 engineers for 10-12 weeks
- Access to benchmark infrastructure
- CI/CD pipeline modifications

### Infrastructure
- Prometheus instance for metrics
- Grafana for dashboards
- Alert manager for notifications

### External Dependencies
- `proptest` crate
- `prometheus` crate
- `criterion` crate for benchmarking
