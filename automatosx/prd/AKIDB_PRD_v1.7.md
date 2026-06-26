# PRD: Scheduled Ingestion and Document Lifecycle Management

**Version:** 1.7
**Date:** 2026-01-21
**Status:** Draft
**Authors:** AkiDB Team + automatosx (Multi-model synthesis: Claude, Gemini, Grok)
**Related ADR:** ADR-024

---

## 1. Executive Summary

This PRD defines the requirements for scheduled ingestion and document lifecycle management in AkiDB Thor Edition. The feature adds hourly synchronization with MinIO, soft delete with source tracking, and UID-based document categorization for selective operations.

### Goals
1. Ensure all files in MinIO are eventually ingested (hourly reconciliation)
2. Automatically detect and handle deleted source files (soft delete with confirmation)
3. Enable document categorization for selective removal and reindexing
4. Maintain search availability during all lifecycle operations

### Non-Goals
- Real-time ingestion (handled by existing NATS event-driven pipeline)
- Multi-tenant isolation (future scope)
- Cross-bucket synchronization (single bucket per deployment)

---

## 2. Problem Statement

### Current Limitations

| Issue | Impact | Severity |
|-------|--------|----------|
| Event-driven only | Files missed during NATS outages never ingested | HIGH |
| No deletion sync | Orphaned vectors consume storage and pollute search results | HIGH |
| No document grouping | Cannot selectively remove or reindex document sets | MEDIUM |
| Hard delete only | Accidental deletions are unrecoverable | MEDIUM |

### User Pain Points

1. **Operations Team**: "We need to remove all vectors from a specific data source, but there's no way to identify which vectors belong to which source."

2. **Data Engineers**: "When we update documents, we have to manually track which vectors to delete before re-ingesting."

3. **Platform Team**: "Files uploaded during maintenance windows are never processed. We have to manually trigger re-ingestion."

---

## 3. Proposed Solution

### 3.1 Scheduled Ingestion

**Hourly Synchronization Job**

```
┌─────────────────────────────────────────────────────────────┐
│                     Sync Cycle Flow                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  :00 ────▶ Acquire Lock ────▶ Compare MinIO vs Manifest     │
│                                       │                     │
│            ┌──────────────────────────┼──────────────────┐  │
│            ▼                          ▼                  ▼  │
│         NEW FILES              UPDATED FILES      MISSING   │
│            │                          │              │      │
│            ▼                          ▼              ▼      │
│         Ingest              Reindex (v+1)    Increment Miss │
│            │                          │              │      │
│            └──────────────────────────┴──────────────┘      │
│                                       │                     │
│                                       ▼                     │
│                             Update Checkpoint               │
│                                       │                     │
│                                       ▼                     │
│  :55 ◀──────────────────── Release Lock ◀──────────────────┘│
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| SCH-001 | Scheduler runs every hour (configurable) | P0 |
| SCH-002 | Random jitter (0-5 min) prevents thundering herd | P1 |
| SCH-003 | Mutex prevents overlapping runs | P0 |
| SCH-004 | Checkpoint enables crash recovery and resumption | P0 |
| SCH-005 | gRPC `/trigger` endpoint for manual runs | P1 |
| SCH-006 | Prometheus metrics for run counts, latency, failures | P1 |

### 3.2 Document Identifier (UID) System

**Composite Identifier Structure:**

```
┌─────────────────────────────────────────────────────────────┐
│                    DocumentIdentifier                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  content_hash (32 bytes)          ← SHA-256 of file content │
│  ├── Deduplication                                          │
│  └── Integrity verification                                 │
│                                                             │
│  category_uid (optional string)   ← User-provided tag       │
│  ├── "legal-docs/contracts"                                 │
│  ├── "support-tickets/2026-q1"                              │
│  └── Enables selective operations                           │
│                                                             │
│  source_path (string)             ← MinIO object key        │
│  └── Lineage tracking                                       │
│                                                             │
│  instance_id (UUID v7)            ← Time-ordered unique ID  │
│  ├── Efficient RocksDB range scans                          │
│  └── OpenTelemetry trace ID                                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| UID-001 | System generates content_hash (SHA-256) automatically | P0 |
| UID-002 | User can optionally provide category_uid via metadata | P1 |
| UID-003 | source_path extracted from MinIO object key | P0 |
| UID-004 | instance_id is UUIDv7 (time-ordered) | P0 |
| UID-005 | All UID fields stored in vector metadata | P0 |
| UID-006 | RocksDB index enables O(1) lookup by any UID field | P1 |

### 3.3 Soft Delete with Confirmation

**Delete State Machine:**

```
┌─────────┐     miss #1      ┌───────────────────┐
│ Active  │ ────────────────▶│ MarkedForDeletion │
└─────────┘                  └─────────┬─────────┘
     ▲                                 │
     │ file reappears                  │ miss #2, #3...
     │                                 ▼
     │                       ┌───────────────────┐
     └───────────────────────│ (missing_count<3) │
                             └─────────┬─────────┘
                                       │ miss #3 (threshold)
                                       ▼
                             ┌───────────────────┐
                             │ ConfirmedMissing  │──▶ Tombstone bit set
                             └─────────┬─────────┘    (excluded from search)
                                       │
                                       │ 7 days (retention)
                                       ▼
                             ┌───────────────────┐
                             │ HardDeleteScheduled│──▶ Compaction removes
                             └───────────────────┘
```

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| DEL-001 | Soft delete requires 3 consecutive misses (configurable) | P0 |
| DEL-002 | Tombstone bit excludes vectors from search immediately | P0 |
| DEL-003 | Hard delete only after 7-day retention (configurable) | P0 |
| DEL-004 | Compaction job removes hard-deleted vectors | P1 |
| DEL-005 | Deletion can be reversed if file reappears before hard delete | P1 |
| DEL-006 | DLQ entry created for all deletions (audit trail) | P2 |

### 3.4 Partial Reindexing

**Version-Based Reindex Flow:**

```
┌─────────────────────────────────────────────────────────────┐
│              Reindex by category_uid="legal-docs"           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Query max version for category  ──────▶  version = 3    │
│                                                             │
│  2. Fetch source files from MinIO                           │
│     └── legal-docs/contract1.pdf                            │
│     └── legal-docs/contract2.pdf                            │
│                                                             │
│  3. Embed and insert with version = 4                       │
│     ├── Vectors immediately searchable                      │
│     └── Old version=3 vectors still serve queries           │
│                                                             │
│  4. After all inserts confirmed:                            │
│     └── Tombstone all vectors where version < 4             │
│                                                             │
│  5. Schedule compaction                                     │
│                                                             │
│  Result: Zero downtime, atomic transition                   │
└─────────────────────────────────────────────────────────────┘
```

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| REI-001 | gRPC `/reindex/{category_uid}` triggers reindexing | P1 |
| REI-002 | Version-based insertion maintains search availability | P0 |
| REI-003 | Old vectors tombstoned only after new vectors confirmed | P0 |
| REI-004 | Reindex job creates checkpoint for crash recovery | P1 |
| REI-005 | Bulk reindex uses backpressure to protect query SLO | P0 |

### 3.5 Backpressure Integration

**Requirements:**

| ID | Requirement | Priority |
|----|-------------|----------|
| BP-001 | Pause ingestion when P95 latency > 80% of SLO (40ms) | P0 |
| BP-002 | Resume after 5 seconds (configurable) | P1 |
| BP-003 | Expose current backpressure state via `/status` | P1 |

---

## 4. User Interface

### 4.1 gRPC API Additions

```protobuf
service IngestionService {
  // Manual trigger for sync job
  rpc TriggerSync(TriggerSyncRequest) returns (TriggerSyncResponse);

  // Get sync job status
  rpc GetSyncStatus(GetSyncStatusRequest) returns (SyncStatusResponse);

  // Reindex documents by category
  rpc ReindexCategory(ReindexCategoryRequest) returns (ReindexCategoryResponse);

  // Soft delete by category
  rpc DeleteCategory(DeleteCategoryRequest) returns (DeleteCategoryResponse);

  // List categories with vector counts
  rpc ListCategories(ListCategoriesRequest) returns (ListCategoriesResponse);
}

message TriggerSyncRequest {
  bool force = 1;  // Run even if previous job still active
}

message ReindexCategoryRequest {
  string category_uid = 1;
  bool dry_run = 2;  // Preview without actual reindex
}

message DeleteCategoryRequest {
  string category_uid = 1;
  bool hard_delete = 2;  // Skip soft delete, immediate removal
}
```

### 4.2 Category UID Specification

Users specify `category_uid` via MinIO object metadata:

```bash
# Upload with category tag
mc cp document.pdf myminio/documents/ \
  --attr "x-amz-meta-category-uid=legal-docs/contracts"

# Or via HTTP header
curl -X PUT \
  -H "x-amz-meta-category-uid: support-tickets/2026-q1" \
  -T ticket.pdf \
  http://minio:9000/documents/ticket.pdf
```

---

## 5. Configuration

```toml
# /etc/akidb/ingestion.toml

[scheduler]
# Interval between sync runs
interval_hours = 1
# Random jitter to prevent thundering herd (0 to this value)
jitter_minutes = 5
# Enable manual trigger via gRPC
manual_trigger_enabled = true

[change_detection]
# Number of consecutive misses before soft delete
deletion_threshold = 3
# Days to retain soft-deleted vectors before hard delete
hard_delete_delay_days = 7
# Maximum files to process per sync run (0 = unlimited)
max_files_per_run = 0

[backpressure]
# Pause ingestion when P95 latency exceeds this (ms)
latency_threshold_ms = 40
# How long to pause before retrying (seconds)
pause_duration_secs = 5

[uid]
# Generate SHA-256 hash for deduplication
generate_content_hash = true
# Require category_uid in metadata (reject if missing)
require_category_uid = false
# Default category for files without metadata
default_category = "uncategorized"

[observability]
# Enable OpenTelemetry tracing
opentelemetry_enabled = true
# Prometheus metrics endpoint
prometheus_endpoint = "/metrics"
# Trace sample rate (0.0 to 1.0)
trace_sample_rate = 0.1
```

---

## 6. Metrics and Observability

### Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `ingestion_sync_runs_total` | Counter | `status` | Total sync runs (success/failed/skipped) |
| `ingestion_sync_duration_seconds` | Histogram | - | Duration of sync runs |
| `ingestion_files_processed_total` | Counter | `action` | Files processed (new/updated/deleted) |
| `ingestion_vectors_inserted_total` | Counter | - | Vectors inserted |
| `ingestion_vectors_tombstoned_total` | Counter | - | Vectors soft-deleted |
| `ingestion_vectors_compacted_total` | Counter | - | Vectors hard-deleted |
| `ingestion_backpressure_pauses_total` | Counter | - | Times ingestion paused for backpressure |
| `ingestion_manifest_size` | Gauge | - | Number of objects in manifest |

### OpenTelemetry Traces

```
Trace: ingestion_sync
├── Span: list_minio_objects
├── Span: compare_manifest
├── Span: process_new_files
│   ├── Span: fetch_file (instance_id=abc123)
│   ├── Span: embed_file (instance_id=abc123)
│   └── Span: insert_vectors (instance_id=abc123)
├── Span: process_updated_files
└── Span: process_deletions
```

---

## 7. Testing Requirements

### Unit Tests
- [ ] Scheduler jitter distribution
- [ ] Manifest comparison logic
- [ ] Delete state machine transitions
- [ ] Version incrementing for reindex

### Integration Tests
- [ ] End-to-end sync cycle with MinIO
- [ ] Crash recovery from checkpoint
- [ ] Backpressure pause/resume
- [ ] Category-based reindexing

### Performance Tests
- [ ] Sync 100k files in <30 minutes
- [ ] Manifest lookup <1ms P99
- [ ] Zero query latency impact during reindex

---

## 8. Rollout Plan

### Phase 1: Core Infrastructure (Week 1-2)
- [ ] Implement `IngestionScheduler` with tokio timer
- [ ] Add `ObjectManifest` to RocksDB
- [ ] Implement change detection logic
- [ ] Add checkpoint/recovery

### Phase 2: UID System (Week 2-3)
- [ ] Implement `DocumentIdentifier` struct
- [ ] Add UID fields to vector metadata
- [ ] Create RocksDB indexes for UID lookup
- [ ] Extract category_uid from MinIO metadata

### Phase 3: Lifecycle Management (Week 3-4)
- [ ] Implement delete state machine
- [ ] Add version-based reindexing
- [ ] Create gRPC endpoints
- [ ] Add Prometheus metrics

### Phase 4: Observability & Polish (Week 4)
- [ ] OpenTelemetry integration
- [ ] Configuration documentation
- [ ] Performance tuning
- [ ] Security review

---

## 9. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Files missed by event-driven | 0% | Compare MinIO listing vs ingested count |
| Orphaned vectors (source deleted) | 0 | Query vectors with missing source |
| Reindex availability gap | 0 ms | Monitor query errors during reindex |
| Sync job success rate | >99.9% | `ingestion_sync_runs_total{status="success"}` |
| Time to detect deleted file | <3 hours | 3 sync cycles × 1 hour |

---

## 10. Open Questions

1. **Multi-bucket support**: Should sync support multiple MinIO buckets with different schedules?

2. **Category inheritance**: Should nested categories (e.g., `legal-docs/contracts/2026`) inherit operations from parent?

3. **Quota enforcement**: Should category_uid have configurable storage quotas?

4. **Cross-shard consistency**: How to ensure category operations are atomic across AkiDB shards?

---

## Appendix A: Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Rust Ingestion Orchestrator                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────────────┐     ┌─────────────────┐     ┌──────────────────┐   │
│  │ Tokio Scheduler│────▶│ MinIO Differ    │────▶│ GPU Embedder     │   │
│  │ (hourly+jitter)│     │ (streaming)     │     │ (CUDA on Thor)   │   │
│  └───────┬────────┘     └────────┬────────┘     └────────┬─────────┘   │
│          │                       │                       │             │
│          │              ┌────────▼────────┐              │             │
│          │              │ RocksDB         │              │             │
│          │              │ - Manifest      │              │             │
│          │              │ - Checkpoints   │              │             │
│          │              │ - Delete States │              │             │
│          │              │ - UID Indexes   │              │             │
│          │              └────────┬────────┘              │             │
│          │                       │                       │             │
│  ┌───────▼───────────────────────▼───────────────────────▼──────────┐  │
│  │                    Backpressure Controller                        │  │
│  │              (pause if P95 > 40ms / 80% of SLO)                   │  │
│  └───────────────────────────────┬──────────────────────────────────┘  │
│                                  │                                     │
└──────────────────────────────────┼─────────────────────────────────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           │                       │                       │
           ▼                       ▼                       ▼
    ┌─────────────┐        ┌─────────────┐         ┌─────────────┐
    │ AkiDB Shard │        │ AkiDB Shard │         │ NATS        │
    │ (Thor GPU)  │        │ (Thor GPU)  │         │ JetStream   │
    └─────────────┘        └─────────────┘         └─────────────┘
```

---

*Generated by automatosx multi-model discussion (Claude, Gemini, Grok)*
