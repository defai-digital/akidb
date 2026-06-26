# AkiDB Internal Reliability Enhancement Plan

## Overview

Based on multi-model analysis, this plan improves AkiDB Thor Edition's operational reliability **without** adding Temporal or other external workflow engines. The approach prioritizes simplicity, predictability, and self-contained robustness appropriate for edge deployment on Jetson Thor clusters.

## Goals

- Improve snapshot reliability with atomic operations and automatic retries
- Add resumable index rebuild with internal state machine
- Implement lightweight background task scheduling
- Enhance observability for operational tasks
- Maintain sub-50ms P95 search latency (zero impact on hot path)

---

## Phase 1: Robust Snapshot Management

**Objective:** Make MinIO snapshot uploads reliable and resumable without external orchestration.

### Tasks

1. **Implement atomic snapshot upload pattern**
   - Upload to temporary location (`snapshots/.tmp/<shard_id>_<timestamp>`)
   - Verify checksum after upload
   - Atomic rename to final location on success
   - Cleanup failed/partial uploads on startup

2. **Add rsync-style resumable uploads**
   - Track upload progress in RocksDB (`snapshot_upload_state` column family)
   - Resume from last successful chunk on restart
   - Implement exponential backoff for transient failures

3. **Create snapshot state machine**
   ```
   States: Idle → Compressing → Uploading → Verifying → Completing → Idle
                      ↓              ↓            ↓
                   Failed ←──────────────────────────
   ```
   - Persist state transitions in RocksDB
   - Auto-resume from any state on coordinator restart

4. **Add snapshot cleanup job**
   - Garbage collect orphaned `.tmp` files older than 24h
   - Prune old snapshots based on retention policy

### Deliverables
- `crates/akidb-storage/src/snapshot/state_machine.rs`
- `crates/akidb-storage/src/snapshot/resumable_upload.rs`
- `crates/akidb-storage/src/snapshot/cleanup.rs`

---

## Phase 2: Resumable Index Rebuild

**Objective:** Make tombstone compaction and index rebuilds crash-safe and resumable.

### Tasks

1. **Define rebuild state machine**
   ```
   States: 
     Idle → Scanning → Exporting → Building → Swapping → Cleanup → Idle
                ↓          ↓           ↓          ↓
             Failed ←─────────────────────────────────
   ```

2. **Implement checkpoint persistence**
   - Store rebuild state in RocksDB (`rebuild_state` key)
   - Track: current phase, temp index path, vectors exported count, last processed ID
   - On restart: read state, resume from checkpoint

3. **Add resource-aware scheduling**
   - Monitor current query load before starting rebuild
   - Pause/throttle rebuild during high-traffic periods
   - Expose rebuild progress via gRPC Health endpoint

4. **Implement safe index swap**
   - Build new index to temp path
   - Atomic pointer swap (no downtime)
   - Keep old index for rollback until new index verified
   - Clean old index after verification window (e.g., 5 minutes)

### Deliverables
- `crates/akidb-faiss/src/rebuild/state_machine.rs`
- `crates/akidb-faiss/src/rebuild/checkpoint.rs`
- `crates/akidb-coordinator/src/scheduler/rebuild_scheduler.rs`

---

## Phase 3: Lightweight Background Task Scheduler

**Objective:** Replace ad-hoc cron/scripts with internal, resource-aware task scheduling.

### Tasks

1. **Create internal task scheduler**
   - Simple priority queue with scheduling
   - Resource budget awareness (don't starve search path)
   - Task types: `Snapshot`, `Rebuild`, `Cleanup`, `HealthCheck`

2. **Implement task registry**
   ```rust
   pub trait BackgroundTask: Send + Sync {
       fn name(&self) -> &str;
       fn schedule(&self) -> Schedule; // Cron-like or interval
       fn resource_cost(&self) -> ResourceCost;
       async fn execute(&self, ctx: &TaskContext) -> Result<()>;
       fn on_failure(&self, error: &AkiDbError) -> FailureAction;
   }
   ```

3. **Add resource governor**
   - Track current CPU/memory/GPU usage
   - Defer low-priority tasks when resources constrained
   - Configurable thresholds in `config/default.toml`

4. **Task state persistence**
   - Store task execution history in RocksDB
   - Track: last run, last success, failure count, next scheduled
   - Auto-retry failed tasks with backoff

### Deliverables
- `crates/akidb-common/src/scheduler/mod.rs`
- `crates/akidb-common/src/scheduler/task.rs`
- `crates/akidb-common/src/scheduler/governor.rs`
- `crates/akidb-coordinator/src/tasks/` (task implementations)

---

## Phase 4: Operational Observability

**Objective:** Provide visibility into background operations without external tools.

### Tasks

1. **Add structured logging for state transitions**
   - Log every state machine transition with context
   - Include: task_id, from_state, to_state, duration_ms, error (if any)

2. **Expose metrics via Prometheus endpoint**
   ```
   akidb_snapshot_state{shard="0"} = "uploading"
   akidb_snapshot_last_success_timestamp{shard="0"} = 1706000000
   akidb_rebuild_progress{shard="0"} = 0.75
   akidb_rebuild_state{shard="0"} = "building"
   akidb_task_failures_total{task="snapshot"} = 2
   ```

3. **Add gRPC admin endpoints**
   - `GetBackgroundTaskStatus` - current state of all tasks
   - `TriggerSnapshot` - manual snapshot trigger
   - `TriggerRebuild` - manual rebuild trigger
   - `GetTaskHistory` - recent task executions

4. **Implement alerting hooks**
   - Configurable webhook on task failure
   - Simple HTTP POST with JSON payload
   - No external dependencies (just `reqwest`)

### Deliverables
- `crates/akidb-grpc/src/admin.rs`
- `crates/akidb-common/src/metrics/background_tasks.rs`
- Proto updates: `crates/grpc-server/proto/akidb_admin.proto`

---

## Configuration Updates

Add to `config/default.toml`:

```toml
[background_tasks]
enabled = true
max_concurrent_tasks = 2

[background_tasks.snapshot]
schedule = "0 */6 * * *"  # Every 6 hours
retry_attempts = 3
retry_backoff_secs = [60, 300, 900]

[background_tasks.rebuild]
schedule = "0 2 * * *"  # Daily at 2 AM
tombstone_threshold = 0.1  # Rebuild when >10% tombstones
min_query_idle_ms = 100  # Only run when queries taking <100ms

[background_tasks.cleanup]
schedule = "0 3 * * *"  # Daily at 3 AM
snapshot_retention_days = 7
temp_file_max_age_hours = 24

[resource_governor]
max_background_cpu_percent = 30
max_background_memory_mb = 4096
defer_when_p95_above_ms = 40  # Defer tasks when latency rising
```

---

## Implementation Order

| Phase | Effort | Priority | Dependencies |
|-------|--------|----------|--------------|
| Phase 1: Snapshots | Medium | **High** | None |
| Phase 2: Rebuild | Medium | **High** | None |
| Phase 3: Scheduler | Medium | Medium | Phase 1, 2 |
| Phase 4: Observability | Low | Medium | Phase 3 |

**Recommended sequence:** Phase 1 → Phase 2 → Phase 3 → Phase 4

Phases 1 and 2 can be developed in parallel as they're independent.

---

## Success Criteria

- [ ] Snapshot uploads survive coordinator restarts mid-upload
- [ ] Index rebuilds resume from checkpoint after crash
- [ ] Background tasks never cause P95 latency to exceed 50ms
- [ ] All task state transitions logged and queryable
- [ ] Zero external runtime dependencies added

---

## What We're NOT Doing (And Why)

| Rejected Approach | Reason |
|-------------------|--------|
| Temporal/Cadence | Overkill for edge; adds distributed system complexity |
| External job queues (Redis, RabbitMQ) | New dependency; not needed for internal tasks |
| Bash scripts + cron | Fragile; no state persistence; poor observability |
| Kubernetes CronJobs | AkiDB targets bare-metal Jetson; K8s not assumed |

---

## Risk Mitigation

1. **State machine bugs** → Comprehensive unit tests for all transitions
2. **Resource contention** → Governor with configurable, conservative defaults
3. **Data corruption during rebuild** → Keep old index until new verified
4. **Upload failures** → Idempotent operations; safe to retry

---

## References

- AkiDB Architecture: `CLAUDE.md`
- Discussion trace: `automatosx/tmp/temporal-discussion-trace.md`
- RocksDB column families: `crates/akidb-storage/src/rocks/mod.rs`
