# ADR-001: Internal Reliability Enhancement Over External Workflow Engines

## Status

Accepted

## Date

2026-01-22

## Context

AkiDB Thor Edition is a distributed vector search engine optimized for NVIDIA Jetson Thor edge clusters. The system currently lacks robust handling for background operations like:

1. **Snapshot Management**: MinIO uploads can fail mid-transfer, leaving partial state
2. **Index Rebuilds**: Tombstone compaction rebuilds are not crash-safe or resumable
3. **Background Tasks**: Ad-hoc scheduling with no unified resource management

A multi-model AI discussion (Claude, Gemini, Grok) evaluated whether to add Temporal workflow engine to address these reliability concerns.

### Options Considered

#### Option 1: Add Temporal Workflow Engine

**Pros:**
- Industry-standard workflow orchestration
- Built-in retries, checkpointing, and observability
- Handles complex multi-step workflows declaratively

**Cons:**
- Adds a new distributed system (server, workers, database, SDK)
- Resource contention on Jetson Thor's unified memory
- Overkill for internal database maintenance tasks
- Increased operational complexity at the edge
- ~50-100MB memory footprint plus active execution overhead

#### Option 2: Enhance Internal Capabilities (Selected)

**Pros:**
- Uses existing RocksDB for state persistence
- No new runtime dependencies
- Resource-aware scheduling respects P95 latency targets
- Self-contained, operationally simple
- Edge-appropriate minimal footprint

**Cons:**
- More initial development effort
- Must maintain custom state machines

## Decision

We will enhance AkiDB's internal reliability capabilities rather than adding Temporal or other external workflow engines.

The decision is based on the following principles:

1. **Edge Simplicity**: Jetson Thor deployments prioritize minimal dependencies and predictable resource usage
2. **Commensurate Complexity**: The problems (file transfers, state machines) don't require full workflow orchestration
3. **Self-Containment**: A vector database should handle its own maintenance without external orchestrators
4. **Latency Protection**: Internal scheduling can be resource-aware; external schedulers cannot

## Consequences

### Positive

- No new operational dependencies to manage at edge sites
- Full control over resource scheduling to protect P95 latency
- State machines are testable and debuggable within the codebase
- Smaller deployment footprint appropriate for Jetson Thor

### Negative

- Must implement and maintain state machine logic ourselves
- No off-the-shelf workflow visualization (must build observability)
- Learning curve for contributors unfamiliar with internal patterns

### Neutral

- Existing compaction scheduler patterns will be generalized
- RocksDB column families will store task state

## Implementation Approach

### Phase 1: Robust Snapshot Management
- Add `SnapshotState` enum persisted to RocksDB
- Implement resumable S3 uploads with chunk tracking
- Atomic rename pattern for MinIO completion

### Phase 2: Resumable Index Rebuild
- Persist `RebuildState` and checkpoint data to RocksDB
- Resume from checkpoint after coordinator restart
- Resource-aware scheduling (defer during high traffic)

### Phase 3: Unified Background Task Scheduler
- Generic `BackgroundTask` trait
- Resource governor with configurable budgets
- Priority queue with defer-on-pressure semantics

### Phase 4: Operational Observability
- Prometheus metrics for task states
- gRPC admin endpoints for manual triggers
- Webhook alerting on failures

## References

- Multi-model discussion trace: `automatosx/tmp/temporal-discussion-trace.md`
- Existing compaction scheduler: `crates/coordinator/src/compaction.rs`
- Existing rebuild manager: `crates/faiss-wrapper/src/rebuild.rs`
- Existing snapshot module: `crates/storage/src/snapshot.rs`

## Review

- **Proposed by**: AI Architecture Review (Claude, Gemini, Grok consensus)
- **Accepted by**: [Pending team review]
