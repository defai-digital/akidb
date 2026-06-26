# ADR-002: Adopting AutomatosX Architectural Principles

## Status
**Proposed** - Pending final decision

## Date
2026-01-22

## Context

### Problem Statement
AkiDB, a high-performance vector database written in Rust, has experienced 9 significant bugs discovered through systematic bug hunting:

| Bug ID | Severity | Description |
|--------|----------|-------------|
| BUG-HUNT-001 | CRITICAL | TOCTOU race in ID mapping |
| BUG-HUNT-002 | CRITICAL | Unbounded WAL entry size |
| BUG-HUNT-003 | HIGH | Merger heap unbounded growth |
| BUG-HUNT-004 | HIGH | Collection name key collision |
| BUG-HUNT-005 | HIGH | Relaxed atomic ordering on ARM |
| BUG-HUNT-006 | HIGH | Snapshot load panic |
| BUG-HUNT-201 | CRITICAL | Data loss during rebuild with panic |
| BUG-HUNT-202 | HIGH | Result merger incomplete results |
| BUG-HUNT-203 | MEDIUM | Silent tombstone clear failure |

While all bugs have been fixed, the pattern suggests systemic architectural gaps that could lead to similar issues in the future.

### AutomatosX Principles Under Consideration

1. **Contracts**: Explicit validation at system boundaries (gRPC, WAL, storage)
2. **Domains**: Encapsulated modules with clear boundaries and atomic APIs
3. **Workflows**: Formalized multi-step processes (rebuild, query fan-out)
4. **Invariants**: Machine-checked assumptions about system state
5. **Guards**: Policy enforcement for state transitions and operations

### Current System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AkiDB                                │
├─────────────────────────────────────────────────────────────┤
│  akidb-grpc          │  Coordinator       │  Router         │
│  (gRPC Server)       │  (Query Routing)   │  (Shard Mgmt)   │
├──────────────────────┼──────────────────────────────────────┤
│  faiss-wrapper       │  storage                             │
│  (GPU Vector Index)  │  (WAL, Snapshots, ID Mapping)        │
│  - Dual-index rebuild│  - Striped locking                   │
│  - Tombstone mgmt    │  - Length-prefixed keys              │
└─────────────────────────────────────────────────────────────┘
```

## Decision Drivers

1. **Prevent bug classes, not just instances**: Fix systemic issues
2. **Performance constraints**: Sub-50ms P95 query latency required
3. **Development velocity**: 241 tests passing; avoid disruption
4. **Rust ecosystem alignment**: Leverage type system where possible
5. **Production readiness**: System is operational; changes must be incremental

## Considered Options

### Option 1: Full AutomatosX Adoption (Large Refactor)
- Complete restructuring around domains, contracts, workflows
- Estimated effort: 16-20 weeks
- Risk: High (production disruption, new bugs)

### Option 2: Incremental Adoption (Recommended)
- Phased approach: Contracts → Invariants → Guards → Domains
- Estimated effort: 10-12 weeks
- Risk: Low (each phase is independently valuable)

### Option 3: Status Quo (No Adoption)
- Continue with ad-hoc bug fixes
- Estimated effort: None
- Risk: Similar bugs will recur

## Decision

**Adopt Option 2: Incremental Adoption** with the following phased approach:

### Phase 1: Contracts at System Boundaries (Weeks 1-2)
- Create `crates/akidb-contracts/` with validation logic
- Implement boundary validation for WAL entries, gRPC requests
- Use newtype patterns for compile-time guarantees

### Phase 2: Invariants as Debug Assertions (Weeks 3-4)
- Add `debug_invariant!` macro to critical paths
- Implement property-based testing with `proptest`
- Target: ID mapping bijectivity, search result ordering

### Phase 3: Rebuild State Machine with Guards (Weeks 5-8)
- Implement typestate pattern for rebuild FSM
- Add explicit guards for state transitions
- Wrap in `catch_unwind` for panic safety

### Phase 4: Coordinator Query Workflow (Weeks 9-10)
- Formalize fan-out/merge as workflow
- Add coverage contracts for partial results
- Implement timeout and fallback policies

### Phase 5: Observability (Ongoing)
- Add metrics for invariant violations
- Create alerts for critical failures
- Dashboard for rebuild state monitoring

## Consequences

### Positive
- **Prevents bug classes**: 7 of 9 bugs would have been caught/prevented
- **Self-documenting code**: Contracts and invariants serve as specs
- **Compile-time safety**: Typestate prevents invalid state transitions
- **Zero prod overhead**: Debug assertions compile out in release
- **Incremental value**: Each phase delivers measurable improvements

### Negative
- **Development overhead**: ~10-12 weeks of architectural work
- **Learning curve**: Team must understand new patterns
- **Code volume increase**: Additional validation and guard code
- **Testing burden**: New invariants require new tests

### Neutral
- **Performance**: Control plane accepts overhead; data plane unchanged
- **Existing tests**: All 241 tests remain valid and passing

## Bug-to-Principle Mapping

| Bug | Preventing Principle | Implementation |
|-----|---------------------|----------------|
| BUG-HUNT-001 | Invariant | Contract tests for atomic guarantees |
| BUG-HUNT-002 | Contract | `validate_entry()` at write boundary |
| BUG-HUNT-003 | Invariant | `debug_invariant!(heap.len() <= k * 1.5)` |
| BUG-HUNT-004 | Contract | `CollectionKey` newtype |
| BUG-HUNT-005 | Guard/Policy | Document policy, enforce in code review |
| BUG-HUNT-006 | Contract | Result types, not panics |
| BUG-HUNT-201 | Guard | `catch_unwind` + typestate FSM |
| BUG-HUNT-202 | Invariant | `debug_invariant!` on result completeness |
| BUG-HUNT-203 | Observability | Metrics + alerts, not just logs |

## Technical Approach

### Compile-Time Guarantees (Zero Runtime Cost)

```rust
// Newtype for collection keys - BUG-HUNT-004 impossible
pub struct CollectionKey(String);

impl CollectionKey {
    pub fn new(name: &str) -> Self {
        Self(format!("{}:{}", name.len(), name))
    }
}

// Typestate for rebuild - invalid transitions impossible
pub struct RebuildMachine<S> {
    _state: PhantomData<S>,
}

impl RebuildMachine<Idle> {
    pub fn start_rebuild(self) -> Result<RebuildMachine<Preparing>, GuardViolation>;
}
```

### Runtime Checks (Debug-Only)

```rust
#[macro_export]
macro_rules! debug_invariant {
    ($cond:expr, $msg:expr) => {
        #[cfg(debug_assertions)]
        {
            if !$cond {
                panic!("Invariant violated: {}", $msg);
            }
        }
    };
}
```

### Control Plane Guards (Always-On)

```rust
// Acceptable overhead for rebuild operations
fn pre_swap_guard(old: &Index, new: &Index) -> Result<(), GuardViolation> {
    ensure!(new.count() >= old.count() - tombstones, "Data loss detected");
    Ok(())
}
```

## Validation Criteria

1. **Performance**: <1% latency regression on query P95
2. **Test coverage**: All new code has >90% coverage
3. **Bug prevention**: Property tests catch simulated bugs
4. **Observability**: Invariant violations trigger alerts within 1 minute

## Related Decisions

- ADR-001: [Previous architectural decisions if any]
- Future: Domain boundary formalization (evaluate after Phase 3)

## Notes

This decision was informed by multi-model discussion (Claude, Gemini, Grok) via AutomatosX ax_discuss tool. All three models reached consensus on incremental adoption.
