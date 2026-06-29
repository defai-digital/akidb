//! Version-Based Reindexer
//!
//! Handles zero-downtime reindexing of documents by:
//! - Creating new vectors with incremented version
//! - Tombstoning old version after new vectors are indexed
//! - Supporting category-based reindexing
//!
//! This enables:
//! - Re-embedding with updated models
//! - Reconfiguring chunking parameters
//! - Recovering from corrupted index segments

use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error, info};
use uuid::Uuid;

use crate::manifest::ManifestStore;
use crate::{IngestionError, Result};

/// Configuration for reindexing operations
#[derive(Debug, Clone)]
pub struct ReindexConfig {
    /// Maximum documents to process in a single batch
    pub batch_size: usize,
    /// Whether to checkpoint progress for resumption
    pub enable_checkpoint: bool,
    /// Maximum concurrent embedding requests
    pub max_concurrent_embeds: usize,
    /// Whether to pause on backpressure
    pub respect_backpressure: bool,
}

impl Default for ReindexConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            enable_checkpoint: true,
            max_concurrent_embeds: 4,
            respect_backpressure: true,
        }
    }
}

/// Result of a reindex operation
#[derive(Debug, Clone)]
pub struct ReindexResult {
    /// Unique ID for this reindex run
    pub run_id: Uuid,
    /// Category that was reindexed (if applicable)
    pub category_uid: Option<String>,
    /// Number of documents processed
    pub documents_processed: u64,
    /// Number of vectors inserted (new version)
    pub vectors_inserted: u64,
    /// Number of vectors tombstoned (old version)
    pub vectors_tombstoned: u64,
    /// Old version number
    pub old_version: u64,
    /// New version number
    pub new_version: u64,
    /// Duration of the operation
    pub duration_ms: u64,
    /// Whether the operation completed successfully
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

impl Default for ReindexResult {
    fn default() -> Self {
        Self {
            run_id: Uuid::now_v7(),
            category_uid: None,
            documents_processed: 0,
            vectors_inserted: 0,
            vectors_tombstoned: 0,
            old_version: 0,
            new_version: 0,
            duration_ms: 0,
            success: false,
            error: None,
        }
    }
}

/// Checkpoint for resumable reindexing
#[derive(Debug, Clone, Default)]
pub struct ReindexCheckpoint {
    pub run_id: Uuid,
    pub category_uid: Option<String>,
    pub old_version: u64,
    pub new_version: u64,
    pub last_processed_key: Option<String>,
    pub documents_processed: u64,
    pub vectors_inserted: u64,
}

/// Document to reindex
#[derive(Debug, Clone)]
pub struct DocumentToReindex {
    pub source_path: String,
    pub content_hash: [u8; 32],
    pub category_uid: Option<String>,
    pub current_version: u64,
}

/// Backend hook used to tombstone vectors after a new document version is indexed.
pub trait VersionTombstoner: Send + Sync {
    fn tombstone_version(&self, category_uid: &str, version: u64) -> Result<u64>;
}

/// Reindexer for version-based document reindexing
///
/// This struct provides methods for reindexing documents with new embeddings
/// while maintaining query availability through version-based shadow writes.
pub struct Reindexer {
    manifest: Arc<ManifestStore>,
    config: ReindexConfig,
    tombstoner: Option<Arc<dyn VersionTombstoner>>,
}

impl Reindexer {
    /// Create a new reindexer
    pub fn new(manifest: Arc<ManifestStore>, config: ReindexConfig) -> Self {
        Self {
            manifest,
            config,
            tombstoner: None,
        }
    }

    /// Attach the backend hook used to tombstone old vector versions.
    pub fn with_tombstoner(mut self, tombstoner: Arc<dyn VersionTombstoner>) -> Self {
        self.tombstoner = Some(tombstoner);
        self
    }

    /// Plan a reindex operation for a category
    ///
    /// Returns a list of documents that would be reindexed
    pub fn plan_reindex_category(&self, category_uid: &str) -> Result<ReindexPlan> {
        let manifests = self.manifest.list_all()?;

        let documents: Vec<DocumentToReindex> = manifests
            .into_iter()
            .filter(|m| {
                m.doc_id.category_uid.as_deref() == Some(category_uid)
                    && m.delete_state.is_active()
            })
            .map(|m| DocumentToReindex {
                source_path: m.key.clone(),
                content_hash: m.content_hash,
                category_uid: m.doc_id.category_uid.clone(),
                current_version: m.version,
            })
            .collect();

        // Find max current version
        let current_max_version = documents
            .iter()
            .map(|d| d.current_version)
            .max()
            .unwrap_or(0);

        let new_version = current_max_version.checked_add(1).ok_or_else(|| {
            IngestionError::Manifest("Reindex version space exhausted".to_string())
        })?;

        Ok(ReindexPlan {
            category_uid: category_uid.to_string(),
            documents,
            current_version: current_max_version,
            new_version,
        })
    }

    /// Execute a reindex operation with a callback for processing each document
    ///
    /// The callback should:
    /// 1. Fetch document content from storage
    /// 2. Generate new embeddings
    /// 3. Insert vectors with the new version
    /// 4. Return the number of vectors inserted
    pub async fn execute_reindex<F, Fut>(
        &self,
        plan: &ReindexPlan,
        process_document: F,
    ) -> Result<ReindexResult>
    where
        F: Fn(DocumentToReindex, u64) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<u64>> + Send,
    {
        let run_id = Uuid::now_v7();
        let start = Instant::now();

        info!(
            %run_id,
            category = %plan.category_uid,
            document_count = plan.documents.len(),
            old_version = plan.current_version,
            new_version = plan.new_version,
            "Starting reindex operation"
        );

        let mut result = ReindexResult {
            run_id,
            category_uid: Some(plan.category_uid.clone()),
            old_version: plan.current_version,
            new_version: plan.new_version,
            ..Default::default()
        };

        // Process documents in batches
        let checkpoint_interval = if self.config.enable_checkpoint {
            Some(self.config.batch_size).filter(|batch_size| *batch_size > 0)
        } else {
            None
        };
        for (i, doc) in plan.documents.iter().enumerate() {
            match process_document(doc.clone(), plan.new_version).await {
                Ok(vectors_inserted) => {
                    let update_result =
                        self.update_manifest_version(&doc.source_path, plan.new_version);
                    if let Err(e) = update_result {
                        error!(
                            %run_id,
                            path = %doc.source_path,
                            error = ?e,
                            "Failed to update manifest version after reindex"
                        );
                        result.error = Some(format!(
                            "Failed to update manifest for {}: {}",
                            doc.source_path, e
                        ));
                        result.duration_ms = start.elapsed().as_millis() as u64;
                        return Ok(result);
                    }

                    result.documents_processed += 1;
                    result.vectors_inserted += vectors_inserted;

                    if checkpoint_interval.is_some_and(|batch_size| (i + 1) % batch_size == 0) {
                        debug!(
                            %run_id,
                            processed = result.documents_processed,
                            "Checkpoint saved"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        %run_id,
                        path = %doc.source_path,
                        error = ?e,
                        "Failed to reindex document"
                    );
                    result.error = Some(format!("Failed at {}: {}", doc.source_path, e));
                    result.duration_ms = start.elapsed().as_millis() as u64;
                    return Ok(result);
                }
            }
        }

        result.success = true;
        result.duration_ms = start.elapsed().as_millis() as u64;

        info!(
            %run_id,
            documents = result.documents_processed,
            vectors = result.vectors_inserted,
            duration_ms = result.duration_ms,
            "Reindex operation completed successfully"
        );

        Ok(result)
    }

    fn update_manifest_version(&self, source_path: &str, new_version: u64) -> Result<()> {
        let mut manifest = self.manifest.get(source_path)?.ok_or_else(|| {
            IngestionError::Manifest(format!("Manifest not found: {}", source_path))
        })?;
        manifest.version = new_version;
        self.manifest.upsert(&manifest)
    }

    /// Tombstone vectors with a specific version for a category
    ///
    /// Called after new vectors are successfully indexed to remove old versions
    ///
    pub fn tombstone_old_version(&self, category_uid: &str, version: u64) -> Result<u64> {
        let Some(tombstoner) = &self.tombstoner else {
            return Err(IngestionError::Storage(
                "Cannot tombstone old reindex version: no vector tombstoner configured"
                    .to_string(),
            ));
        };

        let tombstoned = tombstoner.tombstone_version(category_uid, version)?;
        info!(
            category = %category_uid,
            version = version,
            tombstoned = tombstoned,
            "Tombstoned old reindex vector version"
        );
        Ok(tombstoned)
    }

    /// Get the current version for a category
    pub fn get_category_version(&self, category_uid: &str) -> Result<Option<u64>> {
        let manifests = self.manifest.list_all()?;

        let max_version = manifests
            .into_iter()
            .filter(|m| {
                m.doc_id.category_uid.as_deref() == Some(category_uid)
                    && m.delete_state.is_active()
            })
            .map(|m| m.version)
            .max();

        Ok(max_version)
    }

    /// Resume a previously interrupted reindex operation
    pub async fn resume_reindex<F, Fut>(
        &self,
        checkpoint: &ReindexCheckpoint,
        process_document: F,
    ) -> Result<ReindexResult>
    where
        F: Fn(DocumentToReindex, u64) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<u64>> + Send,
    {
        let category = checkpoint.category_uid.as_deref().unwrap_or("");
        let plan = self.plan_reindex_category(category)?;

        // Skip already processed documents
        let remaining: Vec<_> = if let Some(ref last_key) = checkpoint.last_processed_key {
            if !plan.documents.iter().any(|d| &d.source_path == last_key) {
                return Err(IngestionError::Scheduler(format!(
                    "Cannot resume reindex: checkpoint key '{}' is not in the current plan",
                    last_key
                )));
            }

            plan.documents
                .into_iter()
                .skip_while(|d| &d.source_path != last_key)
                .skip(1) // Skip the last processed one
                .collect()
        } else {
            plan.documents
        };

        let remaining_plan = ReindexPlan {
            category_uid: plan.category_uid,
            documents: remaining,
            current_version: checkpoint.old_version,
            new_version: checkpoint.new_version,
        };

        info!(
            run_id = %checkpoint.run_id,
            remaining = remaining_plan.documents.len(),
            "Resuming reindex operation"
        );

        self.execute_reindex(&remaining_plan, process_document).await
    }
}

/// Plan for a reindex operation
#[derive(Debug, Clone)]
pub struct ReindexPlan {
    pub category_uid: String,
    pub documents: Vec<DocumentToReindex>,
    pub current_version: u64,
    pub new_version: u64,
}

impl ReindexPlan {
    /// Check if the plan is empty (no documents to reindex)
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Get estimated document count
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestStore;
    use akidb_common::types::{DocumentIdentifier, ObjectManifest};
    use std::sync::Mutex;
    use tempfile::tempdir;

    struct MockTombstoner {
        calls: Mutex<Vec<(String, u64)>>,
        tombstoned: u64,
    }

    impl MockTombstoner {
        fn new(tombstoned: u64) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                tombstoned,
            }
        }
    }

    impl VersionTombstoner for MockTombstoner {
        fn tombstone_version(&self, category_uid: &str, version: u64) -> Result<u64> {
            self.calls
                .lock()
                .unwrap()
                .push((category_uid.to_string(), version));
            Ok(self.tombstoned)
        }
    }

    fn create_test_store() -> Arc<ManifestStore> {
        let dir = tempdir().unwrap();
        Arc::new(ManifestStore::open(dir.path()).unwrap())
    }

    fn create_categorized_manifest(key: &str, category: &str) -> ObjectManifest {
        let doc_id = DocumentIdentifier::new(b"test content", key.to_string())
            .with_category(category);
        ObjectManifest::new(key.to_string(), "etag123".to_string(), doc_id)
    }

    #[test]
    fn test_plan_reindex_category() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        // Add documents to category
        store.upsert(&create_categorized_manifest("doc1.pdf", "category-a")).unwrap();
        store.upsert(&create_categorized_manifest("doc2.pdf", "category-a")).unwrap();
        store.upsert(&create_categorized_manifest("doc3.pdf", "category-b")).unwrap();

        let plan = reindexer.plan_reindex_category("category-a").unwrap();

        assert_eq!(plan.category_uid, "category-a");
        assert_eq!(plan.documents.len(), 2);
        assert_eq!(plan.new_version, 1); // 0 + 1
    }

    #[test]
    fn test_plan_reindex_rejects_version_exhaustion() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        let mut manifest = create_categorized_manifest("doc1.pdf", "category-a");
        manifest.version = u64::MAX;
        store.upsert(&manifest).unwrap();

        let result = reindexer.plan_reindex_category("category-a");

        assert!(matches!(
            result,
            Err(IngestionError::Manifest(message)) if message.contains("exhausted")
        ));
    }

    #[test]
    fn test_plan_excludes_deleted() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        // Add active document
        store.upsert(&create_categorized_manifest("active.pdf", "cat")).unwrap();

        // Add deleted document
        let mut deleted = create_categorized_manifest("deleted.pdf", "cat");
        deleted.transition_to_confirmed();
        store.upsert(&deleted).unwrap();

        let plan = reindexer.plan_reindex_category("cat").unwrap();

        assert_eq!(plan.documents.len(), 1);
        assert_eq!(plan.documents[0].source_path, "active.pdf");
    }

    #[tokio::test]
    async fn test_execute_reindex() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        // Add documents
        store.upsert(&create_categorized_manifest("doc1.pdf", "test-cat")).unwrap();
        store.upsert(&create_categorized_manifest("doc2.pdf", "test-cat")).unwrap();

        let plan = reindexer.plan_reindex_category("test-cat").unwrap();

        // Mock processor that returns 5 vectors per document
        let result = reindexer
            .execute_reindex(&plan, |_doc, _version| async { Ok(5) })
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.documents_processed, 2);
        assert_eq!(result.vectors_inserted, 10);
        assert_eq!(result.new_version, 1);
    }

    #[tokio::test]
    async fn test_execute_reindex_updates_manifest_versions() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        store
            .upsert(&create_categorized_manifest("doc1.pdf", "test-cat"))
            .unwrap();
        store
            .upsert(&create_categorized_manifest("doc2.pdf", "test-cat"))
            .unwrap();

        let plan = reindexer.plan_reindex_category("test-cat").unwrap();
        let result = reindexer
            .execute_reindex(&plan, |_doc, _version| async { Ok(3) })
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(
            store.get("doc1.pdf").unwrap().unwrap().version,
            plan.new_version
        );
        assert_eq!(
            store.get("doc2.pdf").unwrap().unwrap().version,
            plan.new_version
        );
        assert_eq!(
            reindexer.get_category_version("test-cat").unwrap(),
            Some(plan.new_version)
        );
    }

    #[tokio::test]
    async fn test_execute_reindex_handles_error() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        // Add documents
        store.upsert(&create_categorized_manifest("doc1.pdf", "test-cat")).unwrap();
        store.upsert(&create_categorized_manifest("doc2.pdf", "test-cat")).unwrap();

        let plan = reindexer.plan_reindex_category("test-cat").unwrap();

        // Mock processor that fails on second document
        let counter = std::sync::atomic::AtomicU64::new(0);
        let result = reindexer
            .execute_reindex(&plan, |_doc, _version| {
                let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if count > 0 {
                        Err(IngestionError::Other("Test error".to_string()))
                    } else {
                        Ok(5)
                    }
                }
            })
            .await
            .unwrap();

        assert!(!result.success);
        assert_eq!(result.documents_processed, 1);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn test_execute_reindex_zero_batch_size_does_not_panic() {
        let store = create_test_store();
        let reindexer = Reindexer::new(
            Arc::clone(&store),
            ReindexConfig {
                batch_size: 0,
                ..Default::default()
            },
        );

        store.upsert(&create_categorized_manifest("doc1.pdf", "test-cat")).unwrap();
        let plan = reindexer.plan_reindex_category("test-cat").unwrap();

        let result = reindexer
            .execute_reindex(&plan, |_doc, _version| async { Ok(1) })
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.documents_processed, 1);
    }

    #[test]
    fn test_tombstone_old_version_requires_configured_tombstoner() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        let result = reindexer.tombstone_old_version("test-cat", 1);

        assert!(
            matches!(result, Err(IngestionError::Storage(message)) if message.contains("no vector tombstoner"))
        );
    }

    #[test]
    fn test_tombstone_old_version_delegates_to_tombstoner() {
        let store = create_test_store();
        let tombstoner = Arc::new(MockTombstoner::new(7));
        let reindexer =
            Reindexer::new(Arc::clone(&store), ReindexConfig::default())
                .with_tombstoner(tombstoner.clone());

        let tombstoned = reindexer.tombstone_old_version("test-cat", 3).unwrap();

        assert_eq!(tombstoned, 7);
        assert_eq!(
            *tombstoner.calls.lock().unwrap(),
            vec![("test-cat".to_string(), 3)]
        );
    }

    #[tokio::test]
    async fn test_resume_reindex_rejects_stale_checkpoint_key() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        store.upsert(&create_categorized_manifest("doc1.pdf", "test-cat")).unwrap();

        let checkpoint = ReindexCheckpoint {
            category_uid: Some("test-cat".to_string()),
            last_processed_key: Some("missing.pdf".to_string()),
            old_version: 0,
            new_version: 1,
            ..Default::default()
        };

        let result = reindexer
            .resume_reindex(&checkpoint, |_doc, _version| async { Ok(1) })
            .await;

        assert!(
            matches!(result, Err(IngestionError::Scheduler(message)) if message.contains("checkpoint key"))
        );
    }

    #[test]
    fn test_get_category_version() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        // No documents yet
        assert!(reindexer.get_category_version("test").unwrap().is_none());

        // Add document
        store.upsert(&create_categorized_manifest("doc.pdf", "test")).unwrap();

        // Version should be 0 (initial)
        assert_eq!(reindexer.get_category_version("test").unwrap(), Some(0));
    }

    #[test]
    fn test_get_category_version_ignores_deleted_documents() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        let mut deleted = create_categorized_manifest("deleted.pdf", "test");
        deleted.version = 99;
        deleted.transition_to_confirmed();
        store.upsert(&deleted).unwrap();

        assert_eq!(reindexer.get_category_version("test").unwrap(), None);

        let mut active = create_categorized_manifest("active.pdf", "test");
        active.version = 7;
        store.upsert(&active).unwrap();

        assert_eq!(reindexer.get_category_version("test").unwrap(), Some(7));
    }

    #[test]
    fn test_reindex_plan_empty() {
        let store = create_test_store();
        let reindexer = Reindexer::new(Arc::clone(&store), ReindexConfig::default());

        let plan = reindexer.plan_reindex_category("nonexistent").unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.document_count(), 0);
    }
}
