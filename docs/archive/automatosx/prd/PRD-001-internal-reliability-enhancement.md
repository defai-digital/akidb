# PRD-001: AkiDB Internal Reliability Enhancement

## Document Information

| Field | Value |
|-------|-------|
| PRD ID | PRD-001 |
| Title | Internal Reliability Enhancement |
| Author | AI Architecture Team |
| Created | 2026-01-22 |
| Status | Draft |
| Related ADR | ADR-001-internal-reliability-over-temporal |

---

## 1. Executive Summary

This PRD defines the requirements for enhancing AkiDB Thor Edition's internal reliability for background operations. Rather than adding external workflow engines like Temporal, we will enhance the existing codebase to provide crash-safe, resumable background operations while maintaining edge-appropriate simplicity.

### Goals
- Make snapshot uploads resilient to failures and restarts
- Enable index rebuilds to resume from checkpoints after crashes
- Unify background task scheduling with resource awareness
- Provide operational visibility without external monitoring dependencies

### Non-Goals
- Adding external workflow orchestration (Temporal, Cadence)
- Kubernetes integration (AkiDB targets bare-metal Jetson)
- Distributed consensus for task coordination (single-coordinator model)

---

## 2. Background

### Current State

AkiDB Thor Edition has partial infrastructure for background operations:

| Component | Current State | Gap |
|-----------|--------------|-----|
| Snapshot uploads | S3 backend exists | No resumability; failures restart from scratch |
| Index rebuilds | RebuildManager with state machine | State is in-memory only; crashes lose progress |
| Compaction scheduling | CompactionScheduler exists | Not unified with other tasks; no resource awareness |
| Observability | Basic logging | No metrics; no admin endpoints |

### Problem Statement

1. **Snapshot reliability**: A coordinator restart during MinIO upload loses all progress
2. **Rebuild crashes**: A system crash during GPU index rebuild wastes compute time
3. **Resource contention**: Background tasks can cause P95 latency spikes
4. **Operational blindness**: No visibility into background task progress

---

## 3. Requirements

### 3.1 Functional Requirements

#### FR-1: Resumable Snapshot Uploads

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1.1 | Snapshot upload state must survive coordinator restart | P0 |
| FR-1.2 | Partial uploads must resume from last successful chunk | P0 |
| FR-1.3 | Failed uploads must be automatically retried with backoff | P0 |
| FR-1.4 | Completed snapshots must use atomic rename pattern | P0 |
| FR-1.5 | Orphaned temporary files must be cleaned up | P1 |

#### FR-2: Crash-Safe Index Rebuilds

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-2.1 | Rebuild state must be persisted to RocksDB | P0 |
| FR-2.2 | Rebuilds must resume from checkpoint after crash | P0 |
| FR-2.3 | Old index must be retained until new index is verified | P0 |
| FR-2.4 | Rebuild progress must be queryable via API | P1 |
| FR-2.5 | Manual rebuild trigger must be available | P1 |

#### FR-3: Unified Background Task Scheduler

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-3.1 | All background tasks must use common scheduler | P0 |
| FR-3.2 | Tasks must declare resource requirements | P0 |
| FR-3.3 | Scheduler must defer tasks when resources constrained | P0 |
| FR-3.4 | Task execution history must be persisted | P1 |
| FR-3.5 | Failed tasks must retry with configurable backoff | P0 |

#### FR-4: Operational Observability

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-4.1 | Task states must be exposed via Prometheus metrics | P1 |
| FR-4.2 | Admin gRPC endpoints must allow manual task triggers | P1 |
| FR-4.3 | Task failures must support webhook notifications | P2 |
| FR-4.4 | Task history must be queryable | P2 |

### 3.2 Non-Functional Requirements

#### NFR-1: Performance

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1.1 | Background tasks must not cause P95 latency > 50ms | Hard limit |
| NFR-1.2 | State persistence overhead < 1ms per operation | Target |
| NFR-1.3 | Memory overhead for scheduler < 10MB | Target |

#### NFR-2: Reliability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-2.1 | No data loss on coordinator crash during any operation | Mandatory |
| NFR-2.2 | Tasks must be idempotent (safe to retry) | Mandatory |
| NFR-2.3 | State machine transitions must be atomic | Mandatory |

#### NFR-3: Operability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-3.1 | Zero new external dependencies | Mandatory |
| NFR-3.2 | All configuration via existing TOML config | Mandatory |
| NFR-3.3 | Graceful shutdown must complete in-flight tasks | Target |

---

## 4. Technical Design

### 4.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     AkiDB Coordinator                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                Background Task Scheduler                 │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │  Snapshot   │  │   Rebuild   │  │   Cleanup   │     │   │
│  │  │    Task     │  │    Task     │  │    Task     │     │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │   │
│  │         │                │                │             │   │
│  │  ┌──────▼────────────────▼────────────────▼──────┐     │   │
│  │  │              Resource Governor                 │     │   │
│  │  │  (CPU budget, Memory budget, Latency check)   │     │   │
│  │  └───────────────────────┬───────────────────────┘     │   │
│  └──────────────────────────┼───────────────────────────────┘   │
│                             │                                   │
│  ┌──────────────────────────▼───────────────────────────────┐   │
│  │                    State Persistence                      │   │
│  │              (RocksDB Column Families)                   │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │   │
│  │  │ task_state   │  │ task_history │  │ checkpoints  │   │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 State Machine Definitions

#### Snapshot State Machine

```
┌───────┐     ┌────────────┐     ┌───────────┐     ┌──────────┐
│ Idle  │────▶│ Compressing│────▶│ Uploading │────▶│ Verifying│
└───────┘     └────────────┘     └───────────┘     └──────────┘
                   │                   │                 │
                   ▼                   ▼                 ▼
              ┌────────┐          ┌────────┐       ┌────────────┐
              │ Failed │◀─────────│ Failed │◀──────│ Completing │
              └────────┘          └────────┘       └────────────┘
                                                        │
                                                        ▼
                                                   ┌───────┐
                                                   │ Idle  │
                                                   └───────┘
```

#### Rebuild State Machine (Extended)

Extends existing `RebuildState` with persistence:

```rust
pub enum PersistentRebuildState {
    Idle,
    Preparing { wal_lsn: u64 },
    Scanning { vectors_scanned: u64, total: u64 },
    Building { vectors_built: u64, temp_path: String },
    Replaying { entries_replayed: u64 },
    Validating { samples_checked: u64 },
    Swapping,
    Cleaning { old_index_path: String },
}
```

### 4.3 Data Model

#### RocksDB Column Families

```rust
// New column families for task management
const CF_TASK_STATE: &str = "task_state";      // Current task states
const CF_TASK_HISTORY: &str = "task_history";  // Completed task records
const CF_CHECKPOINTS: &str = "checkpoints";    // Resumable operation checkpoints

// Key formats
// task_state:{task_type}:{task_id} -> TaskState (serialized)
// task_history:{timestamp}:{task_id} -> TaskResult (serialized)
// checkpoints:{task_type}:{task_id}:{checkpoint_id} -> CheckpointData
```

### 4.4 API Additions

#### Proto Definitions

```protobuf
// Addition to akidb.proto or new akidb_admin.proto

service AkidbAdmin {
  // Get status of all background tasks
  rpc GetBackgroundTaskStatus(GetBackgroundTaskStatusRequest)
      returns (GetBackgroundTaskStatusResponse);

  // Trigger a manual snapshot
  rpc TriggerSnapshot(TriggerSnapshotRequest)
      returns (TriggerSnapshotResponse);

  // Trigger a manual rebuild
  rpc TriggerRebuild(TriggerRebuildRequest)
      returns (TriggerRebuildResponse);

  // Get task execution history
  rpc GetTaskHistory(GetTaskHistoryRequest)
      returns (GetTaskHistoryResponse);
}

message BackgroundTaskStatus {
  string task_id = 1;
  string task_type = 2;  // "snapshot", "rebuild", "cleanup"
  string state = 3;
  double progress = 4;   // 0.0 - 1.0
  uint64 started_at = 5;
  string error = 6;      // Empty if no error
}
```

---

## 5. Implementation Phases

### Phase 1: Robust Snapshot Management (P0)

**Scope:**
- Implement `SnapshotStateMachine` with RocksDB persistence
- Add resumable multi-part uploads for S3
- Atomic completion pattern
- Cleanup job for orphaned files

**Deliverables:**
- `crates/storage/src/snapshot/state_machine.rs`
- `crates/storage/src/snapshot/resumable_upload.rs`
- `crates/storage/src/snapshot/cleanup.rs`
- Updated `crates/storage/src/snapshot.rs` (mod file)

**Acceptance Criteria:**
- [ ] Snapshot survives coordinator restart mid-upload
- [ ] Failed uploads resume from last chunk
- [ ] Orphaned temp files cleaned up after 24h

### Phase 2: Resumable Index Rebuild (P0)

**Scope:**
- Extend `RebuildManager` with persistent state
- Add checkpoint persistence to RocksDB
- Implement recovery on startup
- Resource-aware scheduling

**Deliverables:**
- `crates/faiss-wrapper/src/rebuild/persistent_state.rs`
- `crates/faiss-wrapper/src/rebuild/checkpoint.rs`
- Updates to `crates/faiss-wrapper/src/rebuild.rs`

**Acceptance Criteria:**
- [ ] Rebuild resumes from checkpoint after crash
- [ ] Old index retained until new verified
- [ ] Rebuild defers during high query load

### Phase 3: Background Task Scheduler (P1)

**Scope:**
- Create unified `BackgroundTaskScheduler`
- Implement `ResourceGovernor`
- Migrate compaction scheduler to new framework
- Add task persistence

**Deliverables:**
- `crates/common/src/scheduler/mod.rs`
- `crates/common/src/scheduler/task.rs`
- `crates/common/src/scheduler/governor.rs`
- `crates/common/src/scheduler/persistence.rs`

**Acceptance Criteria:**
- [ ] All background tasks use common scheduler
- [ ] Tasks defer when P95 latency rises
- [ ] Task state survives restart

### Phase 4: Observability (P1)

**Scope:**
- Add Prometheus metrics for tasks
- Implement admin gRPC endpoints
- Add webhook alerting

**Deliverables:**
- `crates/grpc-server/src/admin.rs`
- `crates/common/src/metrics/tasks.rs`
- Proto updates

**Acceptance Criteria:**
- [ ] Task metrics exposed at `/metrics`
- [ ] Manual triggers work via gRPC
- [ ] Webhooks fire on task failure

---

## 6. Configuration

### New Configuration Options

```toml
[background_tasks]
enabled = true
max_concurrent_tasks = 2

[background_tasks.snapshot]
schedule = "0 */6 * * *"  # Every 6 hours
retry_attempts = 3
retry_backoff_secs = [60, 300, 900]
chunk_size_mb = 64  # For resumable uploads

[background_tasks.rebuild]
schedule = "0 2 * * *"  # Daily at 2 AM
tombstone_threshold = 0.1
checkpoint_interval_vectors = 100000
min_query_idle_ms = 100

[background_tasks.cleanup]
schedule = "0 3 * * *"  # Daily at 3 AM
snapshot_retention_days = 7
temp_file_max_age_hours = 24

[resource_governor]
enabled = true
max_background_cpu_percent = 30
max_background_memory_mb = 4096
defer_when_p95_above_ms = 40
check_interval_ms = 1000
```

---

## 7. Testing Strategy

### Unit Tests
- State machine transition tests
- Checkpoint serialization/deserialization
- Resource governor threshold tests

### Integration Tests
- Simulate crash during snapshot upload, verify resume
- Simulate crash during rebuild, verify checkpoint recovery
- Verify resource governor defers tasks under load

### Stress Tests
- Concurrent search + background tasks
- Verify P95 stays under 50ms during rebuild

---

## 8. Rollout Plan

1. **Alpha**: Deploy to single test Jetson Thor node
2. **Beta**: Deploy to 3-node test cluster with synthetic workload
3. **GA**: Rolling deployment to production clusters

### Rollback Strategy
- Feature flags for new scheduler (`background_tasks.enabled`)
- Old compaction scheduler remains available
- State migration tool if needed

---

## 9. Success Metrics

| Metric | Baseline | Target |
|--------|----------|--------|
| Snapshot success rate | ~90% (estimated) | >99.9% |
| Rebuild crash recovery | 0% (restart from scratch) | 100% resume |
| P95 latency during background tasks | Variable, spikes | <50ms always |
| Time to detect failed background task | Minutes (log review) | <1 minute (metrics/webhook) |

---

## 10. Open Questions

1. **Q**: Should we support distributed task coordination for multi-coordinator setups?
   **A**: Deferred. Current design is single-coordinator. Will revisit if multi-coordinator becomes a priority.

2. **Q**: Should checkpoint interval be time-based or progress-based?
   **A**: Progress-based (every N vectors) for predictable resume points.

3. **Q**: How long to retain task history?
   **A**: 30 days default, configurable.

---

## 11. Appendix

### A. Rejected Alternatives

| Alternative | Reason for Rejection |
|-------------|---------------------|
| Temporal | Adds distributed system complexity inappropriate for edge |
| Redis job queues | New dependency; not needed for internal tasks |
| Kubernetes CronJobs | AkiDB targets bare-metal; K8s not assumed |
| Bash scripts + cron | Fragile; no state persistence |

### B. References

- ADR-001: Internal Reliability Over Temporal
- Multi-model discussion (Claude/Gemini/Grok)
- Existing codebase: `crates/coordinator/src/compaction.rs`
- Existing codebase: `crates/faiss-wrapper/src/rebuild.rs`
- Existing codebase: `crates/storage/src/snapshot.rs`
