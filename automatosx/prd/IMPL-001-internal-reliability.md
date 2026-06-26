# Implementation Plan: Internal Reliability Enhancement

## Overview

This document provides detailed implementation steps for PRD-001.

---

## Phase 1: Robust Snapshot Management

### Step 1.1: Create Snapshot State Machine Types

**File**: `crates/storage/src/snapshot/state_machine.rs`

```rust
// Key types to implement:
pub enum SnapshotState {
    Idle,
    Compressing { progress: f64 },
    Uploading { chunks_completed: u64, total_chunks: u64 },
    Verifying,
    Completing,
    Failed { error: String, retry_count: u32 },
}

pub struct SnapshotStateMachine {
    state: SnapshotState,
    snapshot_id: String,
    started_at: Option<u64>,
    // ... persistence handle
}
```

### Step 1.2: Implement Resumable Upload

**File**: `crates/storage/src/snapshot/resumable_upload.rs`

- Multi-part upload tracking
- Chunk-level checkpointing
- Resume from last successful chunk

### Step 1.3: Update Snapshot Module Structure

Convert `snapshot.rs` to `snapshot/mod.rs` directory structure:
- `mod.rs` - re-exports
- `state_machine.rs` - state machine
- `resumable_upload.rs` - resumable uploads
- `backend.rs` - existing backend trait + impls
- `manager.rs` - existing manager
- `cleanup.rs` - orphan cleanup

### Step 1.4: Add RocksDB Persistence

- Add `snapshot_state` column family
- Serialize/deserialize state machine
- Atomic state transitions

---

## Phase 2: Resumable Index Rebuild

### Step 2.1: Create Persistent Rebuild State

**File**: `crates/faiss-wrapper/src/rebuild/persistent_state.rs`

```rust
pub struct PersistentRebuildState {
    pub state: RebuildState,
    pub wal_start_lsn: u64,
    pub vectors_processed: u64,
    pub vectors_total: u64,
    pub temp_index_path: Option<String>,
    pub checkpoint_data: Option<Vec<u8>>,
}
```

### Step 2.2: Implement Checkpoint System

**File**: `crates/faiss-wrapper/src/rebuild/checkpoint.rs`

- Periodic checkpoint during vector export
- Store checkpoint to RocksDB
- Load checkpoint on startup

### Step 2.3: Add Resource-Aware Scheduling

- Check current query latency before starting rebuild
- Pause rebuild if latency exceeds threshold
- Resume when load decreases

---

## Phase 3: Background Task Scheduler

### Step 3.1: Define Core Traits

**File**: `crates/common/src/scheduler/task.rs`

```rust
#[async_trait]
pub trait BackgroundTask: Send + Sync {
    fn task_type(&self) -> &'static str;
    fn task_id(&self) -> &str;
    fn schedule(&self) -> TaskSchedule;
    fn resource_requirements(&self) -> ResourceRequirements;
    async fn execute(&self, ctx: &TaskContext) -> Result<TaskResult>;
    fn on_failure(&self, error: &AkiDbError) -> FailureAction;
}

pub enum TaskSchedule {
    Cron(String),
    Interval(Duration),
    Once,
    Manual,
}

pub struct ResourceRequirements {
    pub cpu_weight: u32,      // 1-100
    pub memory_mb: u32,
    pub io_weight: u32,       // 1-100
}
```

### Step 3.2: Implement Resource Governor

**File**: `crates/common/src/scheduler/governor.rs`

```rust
pub struct ResourceGovernor {
    config: ResourceGovernorConfig,
    current_tasks: Vec<RunningTask>,
    metrics_source: Arc<dyn MetricsSource>,
}

impl ResourceGovernor {
    pub fn can_start(&self, requirements: &ResourceRequirements) -> bool;
    pub fn should_pause(&self) -> bool;
    pub fn register_task(&mut self, task: RunningTask);
    pub fn unregister_task(&mut self, task_id: &str);
}
```

### Step 3.3: Implement Scheduler

**File**: `crates/common/src/scheduler/mod.rs`

- Priority queue for pending tasks
- Task state persistence
- Graceful shutdown handling

### Step 3.4: Migrate Compaction Scheduler

- Wrap existing CompactionScheduler logic as BackgroundTask
- Use new scheduler for execution
- Maintain backward compatibility

---

## Phase 4: Observability

### Step 4.1: Add Prometheus Metrics

**File**: `crates/common/src/metrics/tasks.rs`

```rust
// Metrics to add:
// akidb_background_task_state{task_type, task_id} gauge
// akidb_background_task_progress{task_type, task_id} gauge
// akidb_background_task_duration_seconds{task_type} histogram
// akidb_background_task_failures_total{task_type} counter
```

### Step 4.2: Add Admin gRPC Endpoints

**File**: `crates/grpc-server/src/admin.rs`

- GetBackgroundTaskStatus
- TriggerSnapshot
- TriggerRebuild
- GetTaskHistory

### Step 4.3: Add Webhook Alerting

- Simple HTTP POST on task failure
- Configurable webhook URL
- JSON payload with task details

---

## File Change Summary

### New Files

| File | Purpose |
|------|---------|
| `crates/storage/src/snapshot/mod.rs` | Module structure |
| `crates/storage/src/snapshot/state_machine.rs` | Snapshot state machine |
| `crates/storage/src/snapshot/resumable_upload.rs` | Resumable uploads |
| `crates/storage/src/snapshot/cleanup.rs` | Orphan cleanup |
| `crates/faiss-wrapper/src/rebuild/persistent_state.rs` | Persistent rebuild state |
| `crates/faiss-wrapper/src/rebuild/checkpoint.rs` | Checkpoint system |
| `crates/common/src/scheduler/mod.rs` | Scheduler module |
| `crates/common/src/scheduler/task.rs` | Task traits |
| `crates/common/src/scheduler/governor.rs` | Resource governor |
| `crates/common/src/scheduler/persistence.rs` | State persistence |
| `crates/common/src/metrics/tasks.rs` | Task metrics |
| `crates/grpc-server/src/admin.rs` | Admin endpoints |
| `crates/grpc-server/proto/akidb_admin.proto` | Admin proto |

### Modified Files

| File | Changes |
|------|---------|
| `crates/storage/src/lib.rs` | Update exports for new snapshot structure |
| `crates/storage/src/snapshot.rs` | Move to snapshot/backend.rs |
| `crates/faiss-wrapper/src/rebuild.rs` | Move to rebuild/mod.rs, add persistence |
| `crates/faiss-wrapper/src/lib.rs` | Update rebuild exports |
| `crates/common/src/lib.rs` | Add scheduler module |
| `crates/coordinator/src/compaction.rs` | Integrate with new scheduler |
| `crates/grpc-server/src/lib.rs` | Add admin module |
| `config/default.toml` | Add new configuration sections |

---

## Implementation Order

```
Week 1:
├── Step 1.1: Snapshot state machine types
├── Step 1.2: Resumable upload implementation
└── Step 1.3: Restructure snapshot module

Week 2:
├── Step 1.4: RocksDB persistence for snapshots
├── Step 2.1: Persistent rebuild state
└── Step 2.2: Checkpoint system

Week 3:
├── Step 2.3: Resource-aware rebuild scheduling
├── Step 3.1: Core task traits
└── Step 3.2: Resource governor

Week 4:
├── Step 3.3: Scheduler implementation
├── Step 3.4: Migrate compaction
└── Step 4.1: Prometheus metrics

Week 5:
├── Step 4.2: Admin gRPC endpoints
├── Step 4.3: Webhook alerting
└── Integration testing
```

---

## Testing Checklist

### Phase 1 Tests
- [ ] State machine transitions correctly
- [ ] Upload survives simulated crash
- [ ] Resume picks up from last chunk
- [ ] Cleanup removes old temp files
- [ ] Atomic rename completes properly

### Phase 2 Tests
- [ ] Rebuild state persists to RocksDB
- [ ] Checkpoint written at intervals
- [ ] Recovery loads correct checkpoint
- [ ] Resource check defers rebuild
- [ ] Old index retained until verified

### Phase 3 Tests
- [ ] Task scheduling works correctly
- [ ] Governor limits concurrent tasks
- [ ] P95 check defers new tasks
- [ ] State persists across restart
- [ ] Graceful shutdown completes tasks

### Phase 4 Tests
- [ ] Metrics exposed correctly
- [ ] Admin endpoints work
- [ ] Webhook fires on failure
- [ ] History queryable

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| State machine bugs | Comprehensive unit tests, property-based testing |
| Data corruption | Atomic operations, checksums on checkpoints |
| Resource starvation | Conservative default limits, monitoring |
| Migration issues | Feature flags, gradual rollout |
