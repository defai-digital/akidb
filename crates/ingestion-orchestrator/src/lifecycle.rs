//! Document Lifecycle Manager
//!
//! Handles soft delete and hard delete operations for documents
//! that are missing from MinIO during scheduled syncs.
//!
//! Features:
//! - Missing count threshold checking
//! - Transition to ConfirmedMissing state
//! - Scheduled hard delete processing
//! - Metrics for deletion tracking

use std::sync::Arc;

use tracing::{debug, error, info, warn};

use akidb_common::types::{DeleteState, DEFAULT_HARD_DELETE_DELAY_DAYS, DELETION_THRESHOLD};
use crate::manifest::ManifestStore;
use crate::Result;

/// Configuration for lifecycle management
#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    /// Number of consecutive misses before confirming deletion
    pub deletion_threshold: u8,
    /// Number of days to wait before hard delete
    pub hard_delete_delay_days: u32,
    /// Whether to enable hard delete processing
    pub hard_delete_enabled: bool,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            deletion_threshold: DELETION_THRESHOLD,
            hard_delete_delay_days: DEFAULT_HARD_DELETE_DELAY_DAYS,
            hard_delete_enabled: true,
        }
    }
}

/// Result of processing lifecycle operations
#[derive(Debug, Clone, Default)]
pub struct LifecycleResult {
    /// Number of documents transitioned to ConfirmedMissing
    pub confirmed_count: u64,
    /// Number of documents hard deleted
    pub hard_deleted_count: u64,
    /// Documents that failed processing
    pub failed_count: u64,
}

/// Document lifecycle manager
pub struct LifecycleManager {
    manifest: Arc<ManifestStore>,
    config: LifecycleConfig,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(manifest: Arc<ManifestStore>, config: LifecycleConfig) -> Self {
        Self { manifest, config }
    }

    /// Handle a missing document
    ///
    /// Increments the missing count and transitions to ConfirmedMissing
    /// if the threshold is reached.
    pub fn handle_missing(&self, object_key: &str) -> Result<MissingResult> {
        let new_count = self
            .manifest
            .increment_missing_with_threshold(object_key, self.config.deletion_threshold)?;

        if new_count >= self.config.deletion_threshold {
            // Transition to ConfirmedMissing
            if let Some(mut manifest) = self.manifest.get(object_key)? {
                manifest.transition_to_confirmed();
                self.manifest.upsert(&manifest)?;

                info!(
                    key = %object_key,
                    missing_count = new_count,
                    "Document confirmed as deleted"
                );

                return Ok(MissingResult::Confirmed);
            }
        }

        debug!(
            key = %object_key,
            missing_count = new_count,
            threshold = self.config.deletion_threshold,
            "Document missing count incremented"
        );

        Ok(MissingResult::Incremented(new_count))
    }

    /// Schedule a document for hard delete
    ///
    /// Transitions from ConfirmedMissing to HardDeleteScheduled
    pub fn schedule_hard_delete(&self, object_key: &str) -> Result<bool> {
        if let Some(mut manifest) = self.manifest.get(object_key)? {
            if !matches!(manifest.delete_state, DeleteState::ConfirmedMissing { .. }) {
                warn!(
                    key = %object_key,
                    state = ?manifest.delete_state,
                    "Document is not confirmed missing; hard delete not scheduled"
                );
                return Ok(false);
            }

            let delay_days = self.config.hard_delete_delay_days;
            manifest.schedule_hard_delete(delay_days);
            self.manifest.upsert(&manifest)?;

            info!(
                key = %object_key,
                delay_days = delay_days,
                "Document scheduled for hard delete"
            );

            return Ok(true);
        }

        warn!(key = %object_key, "Document not found for hard delete scheduling");
        Ok(false)
    }

    /// Process all confirmed deletes that are ready for hard delete
    ///
    /// Returns the number of documents hard deleted
    pub fn process_hard_deletes(&self) -> Result<u64> {
        if !self.config.hard_delete_enabled {
            debug!("Hard delete processing is disabled");
            return Ok(0);
        }

        let candidates = self.manifest.list_hard_delete_ready()?;
        let mut count = 0u64;

        for manifest in candidates {
            match self.hard_delete_document(&manifest.key) {
                Ok(true) => count += 1,
                Ok(false) => {
                    warn!(key = %manifest.key, "Document already removed");
                }
                Err(e) => {
                    error!(key = %manifest.key, error = ?e, "Failed to hard delete document");
                }
            }
        }

        if count > 0 {
            info!(count = count, "Hard deleted documents");
        }

        Ok(count)
    }

    /// Hard delete a specific document
    fn hard_delete_document(&self, object_key: &str) -> Result<bool> {
        // In a full implementation, this would:
        // 1. Remove vectors from FAISS index
        // 2. Remove metadata from RocksDB
        // 3. Remove from tag index
        // 4. Remove manifest entry

        // For now, we just remove the manifest entry
        self.manifest.delete(object_key)?;

        debug!(key = %object_key, "Document hard deleted");
        Ok(true)
    }

    /// Recover a document from soft delete
    ///
    /// Resets the missing count and transitions back to Active state
    pub fn recover_document(&self, object_key: &str, etag: &str, epoch: u64) -> Result<bool> {
        if let Some(mut manifest) = self.manifest.get(object_key)? {
            // Only recover if not hard deleted
            if matches!(manifest.delete_state, DeleteState::HardDeleteScheduled { .. }) {
                warn!(
                    key = %object_key,
                    "Cannot recover document scheduled for hard delete"
                );
                return Ok(false);
            }

            // Reset to active state
            manifest.mark_seen(epoch);
            manifest.etag = etag.to_string();
            self.manifest.upsert(&manifest)?;

            info!(
                key = %object_key,
                "Document recovered from soft delete"
            );

            return Ok(true);
        }

        Ok(false)
    }

    /// Get lifecycle statistics
    pub fn stats(&self) -> Result<LifecycleStats> {
        let manifest_stats = self.manifest.stats()?;

        Ok(LifecycleStats {
            total_documents: manifest_stats.total,
            active_documents: manifest_stats.active,
            marked_for_deletion: manifest_stats.marked,
            confirmed_missing: manifest_stats.confirmed,
            scheduled_for_hard_delete: manifest_stats.scheduled,
            current_epoch: manifest_stats.epoch,
        })
    }
}

/// Result of handling a missing document
#[derive(Debug, Clone, PartialEq)]
pub enum MissingResult {
    /// Missing count was incremented but threshold not reached
    Incremented(u8),
    /// Document confirmed as deleted (threshold reached)
    Confirmed,
}

/// Lifecycle statistics
#[derive(Debug, Clone, Default)]
pub struct LifecycleStats {
    pub total_documents: u64,
    pub active_documents: u64,
    pub marked_for_deletion: u64,
    pub confirmed_missing: u64,
    pub scheduled_for_hard_delete: u64,
    pub current_epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ManifestStore;
    use akidb_common::types::{DocumentIdentifier, ObjectManifest};
    use tempfile::tempdir;

    fn create_test_store() -> Arc<ManifestStore> {
        let dir = tempdir().unwrap();
        Arc::new(ManifestStore::open(dir.path()).unwrap())
    }

    fn create_test_manifest(key: &str) -> ObjectManifest {
        let doc_id = DocumentIdentifier::new(b"test content", key.to_string());
        ObjectManifest::new(key.to_string(), "etag123".to_string(), doc_id)
    }

    #[test]
    fn test_handle_missing_increments_count() {
        let store = create_test_store();
        let manager = LifecycleManager::new(Arc::clone(&store), LifecycleConfig::default());

        // Add a document
        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        // First miss
        let result = manager.handle_missing("test/file.pdf").unwrap();
        assert_eq!(result, MissingResult::Incremented(1));

        // Verify count
        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert_eq!(m.missing_count, 1);
    }

    #[test]
    fn test_handle_missing_confirms_at_threshold() {
        let store = create_test_store();
        let config = LifecycleConfig {
            deletion_threshold: 3,
            ..Default::default()
        };
        let manager = LifecycleManager::new(Arc::clone(&store), config);

        // Add a document
        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        // Miss 3 times
        assert_eq!(
            manager.handle_missing("test/file.pdf").unwrap(),
            MissingResult::Incremented(1)
        );
        assert_eq!(
            manager.handle_missing("test/file.pdf").unwrap(),
            MissingResult::Incremented(2)
        );
        assert_eq!(
            manager.handle_missing("test/file.pdf").unwrap(),
            MissingResult::Confirmed
        );

        // Verify state
        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert!(matches!(m.delete_state, DeleteState::ConfirmedMissing { .. }));
    }

    #[test]
    fn test_handle_missing_respects_threshold_above_default() {
        let store = create_test_store();
        let config = LifecycleConfig {
            deletion_threshold: 5,
            ..Default::default()
        };
        let manager = LifecycleManager::new(Arc::clone(&store), config);

        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        for expected_count in 1..=4 {
            assert_eq!(
                manager.handle_missing("test/file.pdf").unwrap(),
                MissingResult::Incremented(expected_count)
            );
            let m = store.get("test/file.pdf").unwrap().unwrap();
            assert!(matches!(
                m.delete_state,
                DeleteState::MarkedForDeletion { .. }
            ));
        }

        assert_eq!(
            manager.handle_missing("test/file.pdf").unwrap(),
            MissingResult::Confirmed
        );
        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert!(matches!(m.delete_state, DeleteState::ConfirmedMissing { .. }));
    }

    #[test]
    fn test_recover_document() {
        let store = create_test_store();
        let manager = LifecycleManager::new(Arc::clone(&store), LifecycleConfig::default());

        // Add and mark as missing
        let mut manifest = create_test_manifest("test/file.pdf");
        manifest.increment_missing();
        manifest.increment_missing();
        store.upsert(&manifest).unwrap();

        // Recover
        let recovered = manager.recover_document("test/file.pdf", "new-etag", 5).unwrap();
        assert!(recovered);

        // Verify recovery
        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert_eq!(m.missing_count, 0);
        assert_eq!(m.delete_state, DeleteState::Active);
        assert_eq!(m.etag, "new-etag");
        assert_eq!(m.last_seen_epoch, 5);
    }

    #[test]
    fn test_lifecycle_stats() {
        let store = create_test_store();
        let manager = LifecycleManager::new(Arc::clone(&store), LifecycleConfig::default());

        // Add some documents
        store.upsert(&create_test_manifest("a.pdf")).unwrap();
        store.upsert(&create_test_manifest("b.pdf")).unwrap();
        store.upsert(&create_test_manifest("c.pdf")).unwrap();

        // Mark one as missing
        manager.handle_missing("b.pdf").unwrap();

        // Confirm delete another
        manager.handle_missing("c.pdf").unwrap();
        manager.handle_missing("c.pdf").unwrap();
        manager.handle_missing("c.pdf").unwrap();

        let stats = manager.stats().unwrap();
        assert_eq!(stats.total_documents, 3);
        assert_eq!(stats.active_documents, 1);
        assert_eq!(stats.marked_for_deletion, 1);
        assert_eq!(stats.confirmed_missing, 1);
    }

    #[test]
    fn test_schedule_hard_delete() {
        let store = create_test_store();
        let manager = LifecycleManager::new(Arc::clone(&store), LifecycleConfig::default());

        // Add and confirm delete
        let mut manifest = create_test_manifest("test/file.pdf");
        manifest.transition_to_confirmed();
        store.upsert(&manifest).unwrap();

        // Schedule hard delete
        let scheduled = manager.schedule_hard_delete("test/file.pdf").unwrap();
        assert!(scheduled);

        // Verify state
        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert!(matches!(m.delete_state, DeleteState::HardDeleteScheduled { .. }));
    }

    #[test]
    fn test_schedule_hard_delete_rejects_active_document() {
        let store = create_test_store();
        let manager = LifecycleManager::new(Arc::clone(&store), LifecycleConfig::default());

        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        let scheduled = manager.schedule_hard_delete("test/file.pdf").unwrap();
        assert!(!scheduled);

        let m = store.get("test/file.pdf").unwrap().unwrap();
        assert_eq!(m.delete_state, DeleteState::Active);
    }
}
