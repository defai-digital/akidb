//! Document lifecycle management types.
//!
//! This module provides types for managing the lifecycle of documents in AkiDB:
//! - Soft delete with confirmation threshold
//! - Hard delete scheduling
//! - Object manifest for MinIO synchronization

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::document::DocumentIdentifier;

/// Number of consecutive misses before confirming deletion
pub const DELETION_THRESHOLD: u8 = 3;

/// Default retention period in days before hard delete
pub const DEFAULT_HARD_DELETE_DELAY_DAYS: u32 = 7;

/// State machine for document deletion lifecycle.
///
/// Documents go through a multi-phase deletion process to prevent
/// accidental data loss:
///
/// ```text
/// Active -> MarkedForDeletion -> ConfirmedMissing -> HardDeleteScheduled
///   ^              |                                        |
///   |______________|  (if file reappears in MinIO)          |
///                                                           v
///                                                    [Permanently Deleted]
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state")]
pub enum DeleteState {
    /// Document is active and searchable
    Active,

    /// Source file missing from MinIO, awaiting confirmation
    /// Requires consecutive misses to transition to ConfirmedMissing
    MarkedForDeletion {
        /// When the deletion was first detected
        detected_at: DateTime<Utc>,
    },

    /// Deletion confirmed after threshold consecutive misses
    /// Vector is tombstoned and excluded from search
    ConfirmedMissing {
        /// When the deletion was confirmed
        confirmed_at: DateTime<Utc>,
    },

    /// Scheduled for permanent deletion after retention period
    HardDeleteScheduled {
        /// When the hard delete will occur
        scheduled_for: DateTime<Utc>,
    },
}

impl DeleteState {
    /// Create a new MarkedForDeletion state with current timestamp
    pub fn mark_for_deletion() -> Self {
        Self::MarkedForDeletion {
            detected_at: Utc::now(),
        }
    }

    /// Transition to ConfirmedMissing state
    pub fn confirm_missing() -> Self {
        Self::ConfirmedMissing {
            confirmed_at: Utc::now(),
        }
    }

    /// Schedule hard delete after the given number of days
    pub fn schedule_hard_delete(delay_days: u32) -> Self {
        let scheduled_for = Utc::now() + chrono::Duration::days(delay_days as i64);
        Self::HardDeleteScheduled { scheduled_for }
    }

    /// Check if the document is in an active, searchable state
    pub fn is_active(&self) -> bool {
        matches!(self, DeleteState::Active)
    }

    /// Check if the document should be excluded from search results
    pub fn is_tombstoned(&self) -> bool {
        matches!(
            self,
            DeleteState::ConfirmedMissing { .. } | DeleteState::HardDeleteScheduled { .. }
        )
    }

    /// Check if the document is ready for hard delete
    pub fn is_ready_for_hard_delete(&self) -> bool {
        match self {
            DeleteState::HardDeleteScheduled { scheduled_for } => Utc::now() >= *scheduled_for,
            _ => false,
        }
    }

    /// Get the timestamp when this state was entered, if available
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            DeleteState::Active => None,
            DeleteState::MarkedForDeletion { detected_at } => Some(*detected_at),
            DeleteState::ConfirmedMissing { confirmed_at } => Some(*confirmed_at),
            DeleteState::HardDeleteScheduled { scheduled_for } => Some(*scheduled_for),
        }
    }
}

impl Default for DeleteState {
    fn default() -> Self {
        Self::Active
    }
}

/// Manifest entry for tracking MinIO objects.
///
/// The manifest maintains a record of all objects in the source MinIO bucket,
/// enabling efficient change detection through streaming ETag comparison.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectManifest {
    /// MinIO object key (path)
    pub key: String,

    /// ETag from MinIO (usually MD5 for single-part uploads)
    pub etag: String,

    /// SHA-256 content hash for deduplication
    pub content_hash: [u8; 32],

    /// Sync cycle counter when last seen
    pub last_seen_epoch: u64,

    /// Number of consecutive sync cycles where object was missing
    pub missing_count: u8,

    /// Document identifier for the ingested vectors
    pub doc_id: DocumentIdentifier,

    /// Current deletion state
    pub delete_state: DeleteState,

    /// Version number for reindexing (increments on each reindex)
    #[serde(default)]
    pub version: u64,
}

impl ObjectManifest {
    /// Create a new manifest entry for a newly discovered object
    pub fn new(key: String, etag: String, doc_id: DocumentIdentifier) -> Self {
        Self {
            key,
            etag,
            content_hash: doc_id.content_hash,
            last_seen_epoch: 0,
            missing_count: 0,
            doc_id,
            delete_state: DeleteState::Active,
            version: 0,
        }
    }

    /// Update the manifest with new ETag (object was modified)
    pub fn update_etag(&mut self, etag: String, epoch: u64) {
        self.etag = etag;
        self.last_seen_epoch = epoch;
        self.missing_count = 0;
        // Reset to active if it was previously marked
        if matches!(self.delete_state, DeleteState::MarkedForDeletion { .. }) {
            self.delete_state = DeleteState::Active;
        }
    }

    /// Mark as seen in the current sync cycle
    pub fn mark_seen(&mut self, epoch: u64) {
        self.last_seen_epoch = epoch;
        self.missing_count = 0;
        // Reset to active if it was previously marked
        if matches!(self.delete_state, DeleteState::MarkedForDeletion { .. }) {
            self.delete_state = DeleteState::Active;
        }
    }

    /// Increment missing count and potentially transition delete state
    ///
    /// Returns true if the document should now be tombstoned
    ///
    /// BUG-011 FIX: Only increments count in Active or MarkedForDeletion states
    pub fn increment_missing(&mut self) -> bool {
        match &self.delete_state {
            DeleteState::Active => {
                self.missing_count = self.missing_count.saturating_add(1);
                self.delete_state = DeleteState::mark_for_deletion();
                false
            }
            DeleteState::MarkedForDeletion { .. } => {
                self.missing_count = self.missing_count.saturating_add(1);
                if self.missing_count >= DELETION_THRESHOLD {
                    self.delete_state = DeleteState::confirm_missing();
                    true
                } else {
                    false
                }
            }
            // BUG-011 FIX: Don't increment count in ConfirmedMissing or HardDeleteScheduled
            DeleteState::ConfirmedMissing { .. } | DeleteState::HardDeleteScheduled { .. } => {
                // Already confirmed/scheduled - no-op
                false
            }
        }
    }

    /// Schedule hard delete with the default retention period
    pub fn schedule_hard_delete(&mut self, delay_days: u32) {
        if matches!(self.delete_state, DeleteState::ConfirmedMissing { .. }) {
            self.delete_state = DeleteState::schedule_hard_delete(delay_days);
        }
    }

    /// Transition to ConfirmedMissing state
    pub fn transition_to_confirmed(&mut self) {
        self.delete_state = DeleteState::confirm_missing();
    }

    /// Increment version for reindexing
    pub fn increment_version(&mut self) {
        self.version += 1;
    }

    /// Check if this manifest entry should be included in search
    pub fn is_searchable(&self) -> bool {
        self.delete_state.is_active()
    }

    /// Get content hash as hex string
    pub fn content_hash_hex(&self) -> String {
        self.content_hash
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

/// Change type detected during MinIO sync
#[derive(Clone, Debug, PartialEq)]
pub enum ChangeType {
    /// New object discovered in MinIO
    New,
    /// Object ETag changed (content modified)
    Updated,
    /// Object missing from MinIO
    Missing,
    /// Deletion confirmed after threshold misses
    ConfirmedDelete,
}

/// Result of a sync operation
#[derive(Clone, Debug, Default)]
pub struct SyncResult {
    /// Number of new files ingested
    pub new_count: u64,
    /// Number of files updated (re-indexed)
    pub updated_count: u64,
    /// Number of files marked for deletion
    pub marked_count: u64,
    /// Number of files confirmed deleted (tombstoned)
    pub confirmed_count: u64,
    /// Number of hard deletes executed
    pub hard_deleted_count: u64,
    /// Number of files skipped (unchanged)
    pub skipped_count: u64,
    /// Any errors encountered
    pub errors: Vec<String>,
}

impl SyncResult {
    /// Create a new empty sync result
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the sync completed without errors
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Total number of files processed
    pub fn total_processed(&self) -> u64 {
        self.new_count
            + self.updated_count
            + self.marked_count
            + self.confirmed_count
            + self.hard_deleted_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_state_transitions() {
        let mut state = DeleteState::Active;
        assert!(state.is_active());
        assert!(!state.is_tombstoned());

        state = DeleteState::mark_for_deletion();
        assert!(!state.is_active());
        assert!(!state.is_tombstoned());

        state = DeleteState::confirm_missing();
        assert!(!state.is_active());
        assert!(state.is_tombstoned());

        state = DeleteState::schedule_hard_delete(7);
        assert!(state.is_tombstoned());
        assert!(!state.is_ready_for_hard_delete());
    }

    #[test]
    fn test_delete_state_ready_for_hard_delete() {
        // Schedule for immediate deletion (0 days in the past)
        let state = DeleteState::HardDeleteScheduled {
            scheduled_for: Utc::now() - chrono::Duration::hours(1),
        };
        assert!(state.is_ready_for_hard_delete());
    }

    #[test]
    fn test_object_manifest_missing_count() {
        let doc_id = DocumentIdentifier::new(b"content", "path".to_string());
        let mut manifest = ObjectManifest::new("key".to_string(), "etag".to_string(), doc_id);

        // First miss - should transition to MarkedForDeletion
        assert!(!manifest.increment_missing());
        assert_eq!(manifest.missing_count, 1);
        assert!(matches!(
            manifest.delete_state,
            DeleteState::MarkedForDeletion { .. }
        ));

        // Second miss - still MarkedForDeletion
        assert!(!manifest.increment_missing());
        assert_eq!(manifest.missing_count, 2);

        // Third miss - should transition to ConfirmedMissing
        assert!(manifest.increment_missing());
        assert_eq!(manifest.missing_count, 3);
        assert!(matches!(
            manifest.delete_state,
            DeleteState::ConfirmedMissing { .. }
        ));
    }

    #[test]
    fn test_increment_missing_stops_at_confirmed() {
        // BUG-011 test: missing_count should not increment past ConfirmedMissing
        let doc_id = DocumentIdentifier::new(b"content", "path".to_string());
        let mut manifest = ObjectManifest::new("key".to_string(), "etag".to_string(), doc_id);

        // Get to ConfirmedMissing state (3 misses)
        manifest.increment_missing();
        manifest.increment_missing();
        manifest.increment_missing();

        assert_eq!(manifest.missing_count, 3);
        assert!(matches!(
            manifest.delete_state,
            DeleteState::ConfirmedMissing { .. }
        ));

        // Additional increment should not change count
        assert!(!manifest.increment_missing());
        assert_eq!(manifest.missing_count, 3); // Should stay at 3, not 4
    }

    #[test]
    fn test_manifest_recovery() {
        let doc_id = DocumentIdentifier::new(b"content", "path".to_string());
        let mut manifest = ObjectManifest::new("key".to_string(), "etag".to_string(), doc_id);

        // Mark for deletion
        manifest.increment_missing();
        assert!(matches!(
            manifest.delete_state,
            DeleteState::MarkedForDeletion { .. }
        ));

        // File reappears
        manifest.mark_seen(1);
        assert!(matches!(manifest.delete_state, DeleteState::Active));
        assert_eq!(manifest.missing_count, 0);
    }

    #[test]
    fn test_sync_result() {
        let mut result = SyncResult::new();
        result.new_count = 5;
        result.updated_count = 3;
        result.skipped_count = 100;

        assert!(result.is_success());
        assert_eq!(result.total_processed(), 8);

        result.errors.push("Error 1".to_string());
        assert!(!result.is_success());
    }

    #[test]
    fn test_serde_delete_state() {
        let states = vec![
            DeleteState::Active,
            DeleteState::mark_for_deletion(),
            DeleteState::confirm_missing(),
            DeleteState::schedule_hard_delete(7),
        ];

        for state in states {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: DeleteState = serde_json::from_str(&json).unwrap();
            // Compare by is_active/is_tombstoned since timestamps may differ
            assert_eq!(state.is_active(), parsed.is_active());
            assert_eq!(state.is_tombstoned(), parsed.is_tombstoned());
        }
    }
}
