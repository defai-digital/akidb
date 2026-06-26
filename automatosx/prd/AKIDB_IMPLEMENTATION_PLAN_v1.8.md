# Implementation Plan: Scheduled Ingestion, Document Lifecycle & Tags

**Version:** 1.8
**Date:** 2026-01-21
**Related:** ADR-024, PRD v1.7
**Estimated Duration:** 6 weeks

---

## Executive Summary

This plan implements three interconnected features:
1. **Scheduled Ingestion** - Hourly synchronization with MinIO
2. **Document Lifecycle Management** - Soft delete, version-based reindexing
3. **Optional Tags/Labels** - Access control, ML labeling, general metadata

---

## Phase 1: Core Data Structures (Week 1)

### 1.1 Add TagValue Enum to akidb-common

**File:** `crates/common/src/types/tag.rs` (new)

```rust
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

pub const MAX_TAGS: usize = 50;
pub const MAX_TAG_KEY_LEN: usize = 64;
pub const MAX_TAG_VALUE_LEN: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TagValue {
    Text(String),
    Number(f64),
    Boolean(bool),
    TextList(Vec<String>),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Tags(pub HashMap<String, TagValue>);

impl Tags {
    pub fn validate(&self) -> Result<(), TagValidationError> {
        if self.0.len() > MAX_TAGS {
            return Err(TagValidationError::TooManyTags(self.0.len()));
        }
        for (key, value) in &self.0 {
            if key.len() > MAX_TAG_KEY_LEN {
                return Err(TagValidationError::KeyTooLong(key.clone()));
            }
            match value {
                TagValue::Text(s) if s.len() > MAX_TAG_VALUE_LEN => {
                    return Err(TagValidationError::ValueTooLong(key.clone()));
                }
                TagValue::TextList(list) => {
                    for item in list {
                        if item.len() > MAX_TAG_VALUE_LEN {
                            return Err(TagValidationError::ValueTooLong(key.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TagValidationError {
    #[error("Too many tags: {0} (max: {MAX_TAGS})")]
    TooManyTags(usize),
    #[error("Tag key too long: {0}")]
    KeyTooLong(String),
    #[error("Tag value too long for key: {0}")]
    ValueTooLong(String),
}
```

**Tasks:**
- [ ] Create `crates/common/src/types/tag.rs`
- [ ] Add `TagValue` enum with Text, Number, Boolean, TextList variants
- [ ] Implement `Tags` wrapper with validation
- [ ] Add unit tests for validation limits
- [ ] Export from `crates/common/src/lib.rs`

### 1.2 Update DocumentIdentifier

**File:** `crates/common/src/types/document.rs` (new)

```rust
use uuid::Uuid;
use crate::types::tag::Tags;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentIdentifier {
    pub content_hash: [u8; 32],
    pub category_uid: Option<String>,
    pub source_path: String,
    pub instance_id: Uuid,
    #[serde(default, skip_serializing_if = "Tags::is_empty")]
    pub tags: Tags,
}

impl DocumentIdentifier {
    pub fn new(content: &[u8], source_path: String) -> Self {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(content);
        let content_hash: [u8; 32] = hasher.finalize().into();

        Self {
            content_hash,
            category_uid: None,
            source_path,
            instance_id: Uuid::now_v7(),
            tags: Tags::default(),
        }
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category_uid = Some(category.into());
        self
    }

    pub fn with_tags(mut self, tags: Tags) -> Self {
        self.tags = tags;
        self
    }
}
```

**Tasks:**
- [ ] Create `crates/common/src/types/document.rs`
- [ ] Add `DocumentIdentifier` struct with all fields
- [ ] Builder pattern for optional fields
- [ ] Add `uuid` v7 support to Cargo.toml

### 1.3 Add DeleteState Enum

**File:** `crates/common/src/types/lifecycle.rs` (new)

```rust
use std::time::SystemTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DeleteState {
    Active,
    MarkedForDeletion { detected_at: SystemTime },
    ConfirmedMissing { confirmed_at: SystemTime },
    HardDeleteScheduled { scheduled_for: SystemTime },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectManifest {
    pub key: String,
    pub etag: String,
    pub content_hash: [u8; 32],
    pub last_seen_epoch: u64,
    pub missing_count: u8,
    pub doc_id: DocumentIdentifier,
    pub delete_state: DeleteState,
}
```

**Tasks:**
- [ ] Create `crates/common/src/types/lifecycle.rs`
- [ ] Add `DeleteState` enum
- [ ] Add `ObjectManifest` struct
- [ ] Add constants for thresholds (DELETION_THRESHOLD = 3)

---

## Phase 2: RocksDB Tag Indexing (Week 2)

### 2.1 Create Tag Index Column Family

**File:** `crates/storage/src/tag_index.rs` (new)

```rust
use roaring::RoaringBitmap;
use rocksdb::{ColumnFamily, DB};

pub struct TagIndex {
    db: Arc<DB>,
    cf_name: &'static str,
}

impl TagIndex {
    pub const CF_NAME: &'static str = "tag_index";

    /// Build index key for a tag
    fn index_key(tag_type: &str, key: &str, value: &str) -> Vec<u8> {
        format!("tag:{}:{}:{}", tag_type, key, value).into_bytes()
    }

    /// Add vector ID to tag index
    pub fn add(&self, id: InternalId, tags: &Tags) -> Result<()> {
        let cf = self.db.cf_handle(Self::CF_NAME)?;
        let mut batch = WriteBatch::new();

        for (key, value) in &tags.0 {
            let index_keys = Self::tag_to_index_keys(key, value);
            for idx_key in index_keys {
                let mut bitmap = self.get_bitmap(&idx_key)?;
                bitmap.insert(id.0 as u32);
                batch.put_cf(&cf, &idx_key, bitmap.serialize()?);
            }
        }

        self.db.write(batch)?;
        Ok(())
    }

    /// Remove vector ID from tag index
    pub fn remove(&self, id: InternalId, tags: &Tags) -> Result<()> {
        // Similar to add but with bitmap.remove()
    }

    /// Query vectors matching tag filter
    pub fn query(&self, filter: &TagFilter) -> Result<RoaringBitmap> {
        match filter {
            TagFilter::And(filters) => {
                let mut result = RoaringBitmap::full(); // Start with all
                for f in filters {
                    result &= self.query(f)?;
                }
                Ok(result)
            }
            TagFilter::Or(filters) => {
                let mut result = RoaringBitmap::new();
                for f in filters {
                    result |= self.query(f)?;
                }
                Ok(result)
            }
            TagFilter::Not(filter) => {
                let inner = self.query(filter)?;
                Ok(!inner) // Complement
            }
            TagFilter::Condition(cond) => {
                self.query_condition(cond)
            }
        }
    }

    fn tag_to_index_keys(key: &str, value: &TagValue) -> Vec<Vec<u8>> {
        match value {
            TagValue::Text(s) => vec![Self::index_key("txt", key, s)],
            TagValue::Number(n) => vec![Self::index_key("num", key, &n.to_string())],
            TagValue::Boolean(b) => vec![Self::index_key("bool", key, &b.to_string())],
            TagValue::TextList(list) => {
                list.iter().map(|s| Self::index_key("lst", key, s)).collect()
            }
        }
    }
}
```

**Tasks:**
- [ ] Create `crates/storage/src/tag_index.rs`
- [ ] Implement `TagIndex` with RoaringBitmap storage
- [ ] Add column family creation in DB initialization
- [ ] Implement `add`, `remove`, `query` methods
- [ ] Add `TagFilter` enum for composable queries
- [ ] Add roaring crate to Cargo.toml
- [ ] Unit tests for bitmap operations

### 2.2 Integrate with Vector Metadata

**File:** `crates/storage/src/metadata.rs` (modify)

```rust
// Add tags to VectorMetadata
pub struct VectorMetadata {
    pub doc_id: DocumentIdentifier,
    pub version: u64,
    pub tombstone: bool,
    pub ingested_at: SystemTime,
}
```

**Tasks:**
- [ ] Update `VectorMetadata` to include `DocumentIdentifier`
- [ ] Migrate existing metadata format
- [ ] Add version field for reindexing
- [ ] Update serialization/deserialization

---

## Phase 3: Manifest & Scheduler (Week 3)

### 3.1 Object Manifest Store

**File:** `crates/ingestion-orchestrator/src/manifest.rs` (new)

```rust
pub struct ManifestStore {
    db: Arc<RocksDB>,
}

impl ManifestStore {
    pub fn get(&self, key: &str) -> Result<Option<ObjectManifest>> {
        // Lookup by MinIO object key
    }

    pub fn upsert(&self, manifest: &ObjectManifest) -> Result<()> {
        // Insert or update manifest entry
    }

    pub fn increment_missing(&self, key: &str) -> Result<u8> {
        // Increment missing_count, return new value
    }

    pub fn list_confirmed_deletes(&self) -> Result<Vec<ObjectManifest>> {
        // List all with delete_state == ConfirmedMissing
    }

    pub fn iter_by_key(&self) -> impl Iterator<Item = ObjectManifest> {
        // Iterate all manifests sorted by key (for streaming diff)
    }
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/manifest.rs`
- [ ] Implement `ManifestStore` with RocksDB backend
- [ ] Add methods for CRUD operations
- [ ] Add iterator for streaming comparison
- [ ] Separate column family for manifest data

### 3.2 Ingestion Scheduler

**File:** `crates/ingestion-orchestrator/src/scheduler.rs` (new)

```rust
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

pub struct IngestionScheduler {
    config: SchedulerConfig,
    run_lock: Arc<Mutex<()>>,
    manifest: Arc<ManifestStore>,
    checkpoint: Arc<CheckpointStore>,
}

impl IngestionScheduler {
    pub async fn run(
        &self,
        minio: MinioClient,
        akidb: AkiDbClient,
    ) -> Result<()> {
        let mut ticker = interval(Duration::from_secs(
            self.config.interval_hours * 3600
        ));

        loop {
            ticker.tick().await;

            // Add jitter
            let jitter = rand::thread_rng().gen_range(
                Duration::ZERO..Duration::from_secs(self.config.jitter_minutes * 60)
            );
            tokio::time::sleep(jitter).await;

            // Try to acquire lock
            let guard = match self.run_lock.try_lock() {
                Ok(g) => g,
                Err(_) => {
                    tracing::warn!("Skipping sync - previous run still active");
                    metrics::counter!("ingestion_sync_skipped").increment(1);
                    continue;
                }
            };

            if let Err(e) = self.execute_sync(&minio, &akidb).await {
                tracing::error!(?e, "Sync run failed");
                metrics::counter!("ingestion_sync_failed").increment(1);
            } else {
                metrics::counter!("ingestion_sync_success").increment(1);
            }

            drop(guard);
        }
    }

    async fn execute_sync(
        &self,
        minio: &MinioClient,
        akidb: &AkiDbClient,
    ) -> Result<()> {
        let run_id = Uuid::now_v7();
        let checkpoint = self.checkpoint.load_or_create(run_id).await?;

        // Detect changes
        let changes = self.detect_changes(minio, &checkpoint).await?;

        for change in changes {
            // Check backpressure
            while akidb.current_p95_latency() > Duration::from_millis(40) {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            match change {
                Change::New(obj) => self.ingest_new(obj, akidb).await?,
                Change::Updated(obj) => self.reindex(obj, akidb).await?,
                Change::Missing(obj) => self.handle_missing(obj).await?,
                Change::ConfirmedDelete(obj) => self.soft_delete(obj, akidb).await?,
            }

            self.checkpoint.save(&checkpoint).await?;
        }

        Ok(())
    }
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/scheduler.rs`
- [ ] Implement `IngestionScheduler` with tokio timer
- [ ] Add jitter and mutex for overlap prevention
- [ ] Implement `execute_sync` with change detection
- [ ] Add backpressure integration
- [ ] Add Prometheus metrics

### 3.3 Change Detection

**File:** `crates/ingestion-orchestrator/src/differ.rs` (new)

```rust
pub struct MinIODiffer {
    manifest: Arc<ManifestStore>,
}

impl MinIODiffer {
    pub async fn detect_changes(
        &self,
        minio: &MinioClient,
        bucket: &str,
    ) -> Result<Vec<Change>> {
        let mut changes = Vec::new();

        // Stream MinIO listing
        let minio_objects = minio.list_objects_v2(bucket).await?;

        // Stream manifest entries
        let manifest_iter = self.manifest.iter_by_key();

        // Merge-join comparison (both sorted by key)
        let mut minio_iter = minio_objects.into_iter().peekable();
        let mut manifest_iter = manifest_iter.peekable();

        loop {
            match (minio_iter.peek(), manifest_iter.peek()) {
                (Some(m), Some(man)) => {
                    match m.key.cmp(&man.key) {
                        Ordering::Less => {
                            // In MinIO, not in manifest -> NEW
                            changes.push(Change::New(minio_iter.next().unwrap()));
                        }
                        Ordering::Greater => {
                            // In manifest, not in MinIO -> MISSING
                            changes.push(Change::Missing(manifest_iter.next().unwrap()));
                        }
                        Ordering::Equal => {
                            // In both
                            let m = minio_iter.next().unwrap();
                            let man = manifest_iter.next().unwrap();
                            if m.etag != man.etag {
                                changes.push(Change::Updated(m));
                            }
                            // Same ETag -> no change, skip
                        }
                    }
                }
                (Some(_), None) => {
                    // Remaining in MinIO -> all NEW
                    changes.push(Change::New(minio_iter.next().unwrap()));
                }
                (None, Some(_)) => {
                    // Remaining in manifest -> all MISSING
                    changes.push(Change::Missing(manifest_iter.next().unwrap()));
                }
                (None, None) => break,
            }
        }

        Ok(changes)
    }
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/differ.rs`
- [ ] Implement streaming merge-join comparison
- [ ] Handle NEW, UPDATED, MISSING states
- [ ] Add tests with mock MinIO client

---

## Phase 4: Lifecycle Operations (Week 4)

### 4.1 Soft Delete Implementation

**File:** `crates/ingestion-orchestrator/src/lifecycle.rs` (new)

```rust
pub struct LifecycleManager {
    manifest: Arc<ManifestStore>,
    akidb: Arc<AkiDbClient>,
    config: LifecycleConfig,
}

impl LifecycleManager {
    pub async fn handle_missing(&self, manifest: &ObjectManifest) -> Result<()> {
        let new_count = self.manifest.increment_missing(&manifest.key).await?;

        if new_count >= self.config.deletion_threshold {
            // Transition to ConfirmedMissing
            self.transition_to_confirmed(&manifest.key).await?;

            // Set tombstone in AkiDB
            self.akidb.set_tombstone(&manifest.doc_id).await?;

            tracing::info!(
                key = %manifest.key,
                "Document confirmed deleted, tombstone set"
            );
        }

        Ok(())
    }

    pub async fn process_hard_deletes(&self) -> Result<usize> {
        let candidates = self.manifest.list_hard_delete_candidates(
            self.config.hard_delete_delay
        ).await?;

        let mut count = 0;
        for manifest in candidates {
            // Remove from AkiDB (physical deletion)
            self.akidb.hard_delete(&manifest.doc_id).await?;

            // Remove from manifest
            self.manifest.remove(&manifest.key).await?;

            count += 1;
        }

        Ok(count)
    }
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/lifecycle.rs`
- [ ] Implement `handle_missing` with threshold checking
- [ ] Implement `process_hard_deletes` for compaction
- [ ] Add state transition methods
- [ ] Add metrics for deletion counts

### 4.2 Version-Based Reindexing

**File:** `crates/ingestion-orchestrator/src/reindex.rs` (new)

```rust
pub struct Reindexer {
    minio: Arc<MinioClient>,
    akidb: Arc<AkiDbClient>,
    embedder: Arc<EmbeddingClient>,
}

impl Reindexer {
    pub async fn reindex_category(&self, category_uid: &str) -> Result<ReindexResult> {
        // 1. Get current max version
        let current_version = self.akidb
            .query_max_version(category_uid)
            .await?
            .unwrap_or(0);
        let new_version = current_version + 1;

        // 2. Get source paths for this category
        let source_paths = self.akidb
            .query_source_paths(category_uid)
            .await?;

        let mut inserted = 0;

        // 3. Re-fetch and embed each document
        for path in &source_paths {
            let content = self.minio.get_object(path).await?;
            let embeddings = self.embedder.embed(&content).await?;

            for (i, embedding) in embeddings.iter().enumerate() {
                let doc_id = DocumentIdentifier::new(&content, path.clone())
                    .with_category(category_uid);

                let metadata = VectorMetadata {
                    doc_id,
                    version: new_version,
                    tombstone: false,
                    ingested_at: SystemTime::now(),
                };

                self.akidb.insert(embedding, metadata).await?;
                inserted += 1;
            }
        }

        // 4. Tombstone old version
        let tombstoned = self.akidb
            .tombstone_by_category_version(category_uid, current_version)
            .await?;

        Ok(ReindexResult {
            category_uid: category_uid.to_string(),
            documents: source_paths.len(),
            vectors_inserted: inserted,
            vectors_tombstoned: tombstoned,
            old_version: current_version,
            new_version,
        })
    }
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/reindex.rs`
- [ ] Implement version-based shadow insert
- [ ] Add batch tombstoning by category+version
- [ ] Add progress reporting and checkpointing
- [ ] Integrate with backpressure controller

---

## Phase 5: gRPC API & Tags Update (Week 5)

### 5.1 Proto Definitions

**File:** `proto/ingestion.proto` (new)

```protobuf
syntax = "proto3";
package akidb.ingestion;

message TagValue {
    oneof value {
        string text = 1;
        double number = 2;
        bool boolean = 3;
        TextList text_list = 4;
    }
}

message TextList {
    repeated string values = 1;
}

// Tag filter for search queries
message TagFilter {
    oneof filter_type {
        AndFilter and = 1;
        OrFilter or = 2;
        NotFilter not = 3;
        Condition condition = 4;
    }
}

message AndFilter { repeated TagFilter filters = 1; }
message OrFilter { repeated TagFilter filters = 1; }
message NotFilter { TagFilter filter = 1; }

message Condition {
    string key = 1;
    TagValue value = 2;
    Operator op = 3;

    enum Operator {
        EQ = 0;
        GT = 1;
        LT = 2;
        GTE = 3;
        LTE = 4;
        CONTAINS = 5;
        EXISTS = 6;
    }
}

// Update tags without re-embedding
message UpdateTagsRequest {
    string document_id = 1;  // instance_id or content_hash
    map<string, TagValue> tags = 2;
    bool merge = 3;  // true = merge, false = replace
}

message UpdateTagsResponse {
    bool success = 1;
    string error = 2;
}

// Trigger sync manually
message TriggerSyncRequest {
    bool force = 1;
}

message TriggerSyncResponse {
    string run_id = 1;
    string status = 2;
}

// Get sync status
message GetSyncStatusRequest {}

message SyncStatusResponse {
    string last_run_id = 1;
    string last_run_status = 2;
    int64 last_run_timestamp = 3;
    int64 next_run_timestamp = 4;
    bool is_running = 5;
}

// Reindex by category
message ReindexCategoryRequest {
    string category_uid = 1;
    bool dry_run = 2;
}

message ReindexCategoryResponse {
    int32 documents = 1;
    int32 vectors_inserted = 2;
    int32 vectors_tombstoned = 3;
}

// Delete by category
message DeleteCategoryRequest {
    string category_uid = 1;
    bool hard_delete = 2;
}

message DeleteCategoryResponse {
    int32 vectors_affected = 1;
}

// List categories
message ListCategoriesRequest {
    int32 limit = 1;
    int32 offset = 2;
}

message CategoryInfo {
    string category_uid = 1;
    int64 vector_count = 2;
    int64 document_count = 3;
}

message ListCategoriesResponse {
    repeated CategoryInfo categories = 1;
}

service IngestionService {
    rpc TriggerSync(TriggerSyncRequest) returns (TriggerSyncResponse);
    rpc GetSyncStatus(GetSyncStatusRequest) returns (SyncStatusResponse);
    rpc UpdateTags(UpdateTagsRequest) returns (UpdateTagsResponse);
    rpc ReindexCategory(ReindexCategoryRequest) returns (ReindexCategoryResponse);
    rpc DeleteCategory(DeleteCategoryRequest) returns (DeleteCategoryResponse);
    rpc ListCategories(ListCategoriesRequest) returns (ListCategoriesResponse);
}
```

**Tasks:**
- [ ] Create `proto/ingestion.proto`
- [ ] Add TagValue and TagFilter messages
- [ ] Add all service methods
- [ ] Generate Rust code with tonic-build
- [ ] Add to build.rs

### 5.2 Tag Update Service

**File:** `crates/ingestion-orchestrator/src/grpc/tags.rs` (new)

```rust
impl IngestionService for IngestionServiceImpl {
    async fn update_tags(
        &self,
        request: Request<UpdateTagsRequest>,
    ) -> Result<Response<UpdateTagsResponse>, Status> {
        let req = request.into_inner();

        // Validate tags
        let tags = Tags::from_proto(req.tags)?;
        tags.validate().map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Update metadata (no vector modification)
        let affected = self.storage
            .update_tags(&req.document_id, tags, req.merge)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Update tag index
        self.tag_index
            .update(&req.document_id, &tags)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(UpdateTagsResponse {
            success: true,
            error: String::new(),
        }))
    }
}
```

**Tasks:**
- [ ] Implement `UpdateTags` gRPC handler
- [ ] Implement `TriggerSync` gRPC handler
- [ ] Implement `GetSyncStatus` gRPC handler
- [ ] Implement `ReindexCategory` gRPC handler
- [ ] Implement `DeleteCategory` gRPC handler
- [ ] Implement `ListCategories` gRPC handler

### 5.3 Search with Tag Filtering

**File:** `crates/grpc-server/src/search.rs` (modify)

```rust
// Extend SearchRequest to include TagFilter
impl SearchService for SearchServiceImpl {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();

        // Resolve tag filter to candidate bitset
        let filter_bitset = if let Some(filter) = req.filter {
            Some(self.tag_index.query(&filter).await?)
        } else {
            None
        };

        // Combine with tombstone bitset
        let search_bitset = match filter_bitset {
            Some(fb) => fb & !self.storage.tombstone_bitset(),
            None => !self.storage.tombstone_bitset(),
        };

        // Execute FAISS search with pre-filter
        let results = self.faiss
            .search_with_filter(&req.query_vector, req.top_k, &search_bitset)
            .await?;

        Ok(Response::new(SearchResponse { results }))
    }
}
```

**Tasks:**
- [ ] Add `filter` field to `SearchRequest` proto
- [ ] Implement filter resolution in search handler
- [ ] Integrate filter bitset with FAISS pre-filter
- [ ] Add tests for filtered search

---

## Phase 6: Observability & Polish (Week 6)

### 6.1 OpenTelemetry Integration

**File:** `crates/ingestion-orchestrator/src/telemetry.rs` (new)

```rust
use opentelemetry::trace::{Tracer, SpanKind};

pub fn create_sync_span(run_id: Uuid) -> Span {
    tracer().span_builder("ingestion_sync")
        .with_kind(SpanKind::Internal)
        .with_attributes(vec![
            KeyValue::new("run_id", run_id.to_string()),
        ])
        .start(&tracer())
}

pub fn create_ingest_span(instance_id: Uuid) -> Span {
    tracer().span_builder("ingest_document")
        .with_kind(SpanKind::Internal)
        .with_attributes(vec![
            KeyValue::new("instance_id", instance_id.to_string()),
        ])
        .start(&tracer())
}
```

**Tasks:**
- [ ] Create `crates/ingestion-orchestrator/src/telemetry.rs`
- [ ] Add spans for sync, ingest, reindex operations
- [ ] Add instance_id as trace context
- [ ] Configure sampling rate

### 6.2 Prometheus Metrics

```rust
// Add these metrics
lazy_static! {
    pub static ref SYNC_RUNS: Counter = register_counter!(
        "ingestion_sync_runs_total",
        "Total sync runs",
        &["status"]
    ).unwrap();

    pub static ref SYNC_DURATION: Histogram = register_histogram!(
        "ingestion_sync_duration_seconds",
        "Duration of sync runs"
    ).unwrap();

    pub static ref FILES_PROCESSED: Counter = register_counter!(
        "ingestion_files_processed_total",
        "Files processed",
        &["action"]
    ).unwrap();

    pub static ref VECTORS_TOMBSTONED: Counter = register_counter!(
        "ingestion_vectors_tombstoned_total",
        "Vectors soft-deleted"
    ).unwrap();

    pub static ref TAG_UPDATES: Counter = register_counter!(
        "ingestion_tag_updates_total",
        "Tag update operations"
    ).unwrap();

    pub static ref TAG_FILTER_HITS: Counter = register_counter!(
        "search_tag_filter_hits_total",
        "Tag filter applications in search"
    ).unwrap();
}
```

**Tasks:**
- [ ] Add metrics for sync runs, duration
- [ ] Add metrics for file processing by action
- [ ] Add metrics for tag operations
- [ ] Add metrics for filtered search
- [ ] Expose via existing `/metrics` endpoint

### 6.3 Configuration Documentation

**File:** `docs/configuration/ingestion.md` (new)

**Tasks:**
- [ ] Document all configuration options
- [ ] Add examples for common use cases
- [ ] Document tag naming conventions
- [ ] Add troubleshooting guide

### 6.4 Integration Tests

**Tasks:**
- [ ] End-to-end sync cycle test
- [ ] Tag update and filtered search test
- [ ] Reindex with version transition test
- [ ] Soft delete and recovery test
- [ ] Backpressure pause/resume test

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `roaring` | 0.10 | RoaringBitmap for tag indexes |
| `uuid` | 1.12 (with v7 feature) | UUIDv7 for instance_id |
| `sha2` | 0.10 | Content hashing |
| `opentelemetry` | 0.22 | Distributed tracing |

---

## Testing Strategy

### Unit Tests
- [ ] TagValue validation
- [ ] DocumentIdentifier construction
- [ ] DeleteState transitions
- [ ] Manifest CRUD operations
- [ ] Tag index operations
- [ ] Change detection logic

### Integration Tests
- [ ] Scheduler with mock MinIO
- [ ] Full sync cycle
- [ ] Tag update without re-embed
- [ ] Filtered search accuracy
- [ ] Version-based reindex

### Performance Tests
- [ ] Sync 100k files in <30 minutes
- [ ] Tag lookup <1ms P99
- [ ] Filtered search <50ms P99 (1M vectors)
- [ ] Tag update <10ms

---

## Rollout Checklist

### Pre-Release
- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] Performance benchmarks met
- [ ] Documentation complete
- [ ] Security review passed

### Deployment
- [ ] RocksDB migration for new column families
- [ ] Proto schema deployed
- [ ] Configuration updated
- [ ] Monitoring dashboards created

### Post-Release
- [ ] Monitor sync success rate
- [ ] Monitor tag operation latency
- [ ] Monitor filtered search performance
- [ ] Collect user feedback

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Large bucket sync takes >1 hour | Mutex prevents overlap; checkpoint enables resumption |
| Tag index bloats memory | LRU cache with configurable size; disk-backed bitmaps |
| Reindex causes query latency spike | Backpressure pauses ingestion at 80% SLO |
| Migration fails | Rollback procedure; feature flags for gradual enablement |

---

*Implementation Plan v1.8 - Scheduled Ingestion, Lifecycle, and Tags*
