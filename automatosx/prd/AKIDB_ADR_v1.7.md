# ADR-024: Scheduled Ingestion and Document Lifecycle Management

**Status:** Proposed
**Date:** 2026-01-21
**Authors:** AkiDB Team + automatosx (Claude, Gemini, Grok synthesis)
**Supersedes:** None
**Related:** ADR-019 (Ingestion Pipeline), ADR-021 (Security Hardening)

---

## Context

The current AkiDB ingestion pipeline is event-driven via NATS JetStream, triggered by MinIO bucket notifications. While this provides real-time ingestion, it has limitations:

1. **Event Loss**: NATS events can be missed during outages or network issues
2. **No Periodic Reconciliation**: Files added during downtime may never be ingested
3. **No Deletion Sync**: Files removed from MinIO remain as orphaned vectors in AkiDB
4. **Limited Categorization**: No systematic way to group, remove, or reindex documents by category
5. **No Soft Delete**: Accidental deletions cannot be recovered

### Requirements

1. **Hourly Triggered Ingestion**: Automatically ingest newly added files from MinIO every hour
2. **Soft Delete with Sync**: Mark vectors as deleted when source files are removed from MinIO; actual deletion only after confirmation
3. **UID Tagging**: Unique identifier for document categorization, selective removal, and reindexing

---

## Decision

### 1. Scheduler Implementation

**Decision: Internal `tokio::time::interval` with mutex-based overlap prevention**

```rust
pub struct IngestionScheduler {
    interval: Duration,          // Default: 1 hour
    jitter_max: Duration,        // Default: 5 minutes
    run_lock: Arc<Mutex<()>>,    // Prevent overlapping runs
    checkpoint_store: Arc<RocksDB>,
}
```

**Rationale:**
- Lightweight for Jetson Thor edge hardware (avoids K8s overhead)
- Self-contained within Rust orchestrator
- Jitter prevents thundering herd across shards
- Optional gRPC `/trigger` endpoint for manual runs
- Kubernetes CronJob can complement as external backup if desired

**Rejected Alternatives:**
- Kubernetes CronJob (too heavyweight for edge deployment)
- Pure cron job (external dependency, no crash recovery)
- Event-only via NATS (unreliable during outages)

### 2. Composite UID System with Optional Tags

**Decision: Document Identifier with Typed Tags**

```rust
use std::collections::HashMap;

/// Maximum constraints for tag validation
pub const MAX_TAGS: usize = 50;
pub const MAX_TAG_KEY_LEN: usize = 64;
pub const MAX_TAG_VALUE_LEN: usize = 256;

/// Typed tag values supporting multiple use cases
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TagValue {
    Text(String),           // General text labels
    Number(f64),            // Access levels, scores
    Boolean(bool),          // Flags (is_public, verified)
    TextList(Vec<String>),  // Multi-label ML (spam, urgent, important)
}

struct DocumentIdentifier {
    /// SHA-256 of content for deduplication
    content_hash: [u8; 32],

    /// User-provided hierarchical categorization (optional)
    category_uid: Option<String>,  // e.g., "legal-docs/contracts"

    /// MinIO object key for lineage tracking
    source_path: String,

    /// Time-ordered unique ID (UUIDv7)
    instance_id: Uuid,

    /// Optional typed key-value tags for filtering, access control, and ML labeling.
    /// Keys should use namespaced conventions: "access:level", "ml:label", "review:status"
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    tags: HashMap<String, TagValue>,
}
```

**Tag Use Cases:**
| Use Case | Example Key | Example Value | Type |
|----------|-------------|---------------|------|
| Access Control | `access:level` | `3` | Number |
| Access Control | `access:classification` | `"confidential"` | Text |
| ML Labeling | `ml:sentiment` | `"positive"` | Text |
| ML Labeling | `ml:labels` | `["spam", "urgent"]` | TextList |
| Review Status | `review:verified` | `true` | Boolean |
| Custom | `project:name` | `"alpha"` | Text |

**Rationale:**
- `content_hash`: Enables deduplication (same file = same hash) and integrity verification
- `category_uid`: Enables grouping, selective deletion, and reindexing by user-defined categories
- `source_path`: Maintains lineage to source file in MinIO
- `instance_id`: Time-ordered for efficient RocksDB range scans, unique per ingestion
- `tags`: **Optional** typed key-value pairs for:
  - Access control (numeric levels, classification labels)
  - ML labeling (single or multi-label)
  - General metadata filtering

**Key Design Principles:**
- Tags are **optional** - users who don't need them incur no overhead
- Tags are **mutable** without re-embedding (update metadata only)
- Namespaced keys (e.g., `access:`, `ml:`) provide semantic grouping
- Validation limits prevent abuse (50 tags max, 64-char keys, 256-char values)

**Storage:**
- Embedded directly in vector metadata (not a side index)
- Indexed in RocksDB for O(1) lookup of vectors by any identifier
- Secondary inverted index for tag-based filtering:
  ```
  "tag:txt:{key}:{value}"  -> RoaringBitmap<InternalId>
  "tag:num:{key}:{value}"  -> RoaringBitmap<InternalId>
  "tag:bool:{key}:{value}" -> RoaringBitmap<InternalId>
  "tag:lst:{key}:{element}" -> RoaringBitmap<InternalId>
  ```

### 3. Change Detection via Manifest Comparison

**Decision: Streaming ETag diff against RocksDB manifest**

```rust
struct ObjectManifest {
    key: String,
    etag: String,
    content_hash: [u8; 32],
    last_seen_epoch: u64,       // Sync cycle counter
    missing_count: u8,          // Consecutive misses
    doc_id: DocumentIdentifier,
    delete_state: DeleteState,
}
```

**Detection Flow:**
1. Stream MinIO `ListObjectsV2` (sorted by key)
2. Stream RocksDB manifest (sorted by key)
3. Parallel comparison:
   - **In MinIO, not manifest** → NEW (ingest)
   - **In both, same ETag** → SKIP
   - **In both, different ETag** → UPDATED (reindex)
   - **In manifest, not MinIO** → INCREMENT missing_count

**Rationale:**
- More reliable than event-only (handles NATS outages)
- O(n) streaming comparison scales to large buckets
- Consecutive-miss threshold prevents false positives from transient MinIO issues
- Merkle trees rejected as over-engineered for typical bucket sizes

### 4. Two-Phase Soft Delete

**Decision: Tombstone-based with confirmation threshold**

```rust
enum DeleteState {
    Active,
    MarkedForDeletion { detected_at: SystemTime },
    ConfirmedMissing { confirmed_at: SystemTime },  // 3+ consecutive misses
    HardDeleteScheduled { scheduled_for: SystemTime },
}
```

**Deletion Flow:**
1. File missing in MinIO → `MarkedForDeletion` (increment `missing_count`)
2. After 3 consecutive misses → `ConfirmedMissing`, set tombstone bit in AkiDB
3. Tombstoned vectors excluded from search immediately
4. After retention period (7 days default) → `HardDeleteScheduled`
5. Background compaction removes hard-deleted vectors

**Rationale:**
- Prevents data loss from accidental MinIO deletions
- Allows recovery window before permanent deletion
- Aligns with AkiDB's existing tombstone bitset architecture in FAISS
- Configurable thresholds for different use cases

### 5. Version-Based Partial Reindexing

**Decision: Shadow insert then tombstone old**

```rust
struct VectorMetadata {
    doc_id: DocumentIdentifier,
    version: u64,           // Monotonically increasing
    tombstone: bool,
    ingested_at: SystemTime,
}
```

**Reindexing Flow (for category_uid group):**
1. Query current max version for category
2. Fetch and re-embed source files from MinIO
3. Insert new vectors with incremented version (old vectors still serve queries)
4. After all new vectors confirmed, tombstone old version
5. Schedule compaction to remove tombstoned vectors

**Rationale:**
- Zero search availability gaps during reindexing
- Atomic transition from old to new vectors
- Version tracking enables rollback if needed

### 6. Backpressure Integration

**Decision: Pause ingestion at 80% SLO threshold**

```rust
async fn ingest_with_backpressure(vectors: Vec<Vector>, akidb: &AkiDB) -> Result<()> {
    for chunk in vectors.chunks(100) {
        while akidb.current_p95_latency() > Duration::from_millis(40) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        akidb.insert_batch(chunk).await?;
    }
    Ok(())
}
```

**Rationale:**
- Protects query SLO (50ms P95 target) during bulk ingestion
- 80% threshold (40ms) provides headroom before SLO breach
- Configurable pause duration

---

## Consequences

### Positive
- **Reliability**: Hourly reconciliation catches files missed by event-driven ingestion
- **Data Safety**: Two-phase delete prevents accidental data loss
- **Flexibility**: UID system enables selective operations on document groups
- **Availability**: Version-based reindexing maintains search availability
- **Observability**: OpenTelemetry traces with `instance_id` for lineage tracking

### Negative
- **Complexity**: Additional state management in RocksDB manifest
- **Latency**: Hourly sync adds up to 1 hour delay for non-event files
- **Storage**: Tombstoned vectors consume space until compaction

### Risks and Mitigations
| Risk | Mitigation |
|------|------------|
| Hourly job takes >1 hour | Mutex prevents overlap; checkpoint enables resumption |
| RocksDB manifest grows unbounded | Periodic compaction removes hard-deleted entries |
| False positive deletions | 3-consecutive-miss threshold with configurable retention |

---

## Configuration

```toml
[scheduler]
interval_hours = 1
jitter_minutes = 5
manual_trigger_enabled = true

[change_detection]
deletion_threshold = 3          # Consecutive misses before soft delete
hard_delete_delay_days = 7      # Retention before compaction

[backpressure]
latency_threshold_ms = 40       # Pause at 80% of 50ms SLO
pause_duration_secs = 5

[uid]
generate_content_hash = true    # SHA-256 for deduplication
require_category_uid = false    # User-provided optional

[observability]
opentelemetry_enabled = true
prometheus_endpoint = "/metrics"
trace_sample_rate = 0.1
```

---

## Implementation Notes

### New Components
1. `IngestionScheduler` in `akidb-ingestion` crate
2. `ObjectManifest` table in RocksDB
3. `DocumentIdentifier` and `VectorMetadata` structs
4. gRPC endpoints: `/trigger`, `/status`, `/reindex/{category_uid}`

### Modified Components
1. `IngestionPipeline` - integrate scheduler and manifest
2. `StateTracker` - add delete state tracking
3. AkiDB vector metadata - add UID fields

### New Dependencies
- None (uses existing tokio, RocksDB, NATS stack)

---

## References

- [MinIO ListObjectsV2 API](https://min.io/docs/minio/linux/developers/go/API.html)
- [NATS JetStream Durability](https://docs.nats.io/nats-concepts/jetstream)
- [UUIDv7 Specification](https://www.ietf.org/archive/id/draft-peabody-dispatch-new-uuid-format-04.html)
- [Tombstone Pattern in Distributed Systems](https://martinfowler.com/articles/patterns-of-distributed-systems/versioned-value.html)
