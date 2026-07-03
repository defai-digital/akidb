//! Snapshot state machine with persistent state
//!
//! This module provides a crash-safe state machine for snapshot operations.
//! State is persisted to RocksDB, allowing recovery after coordinator restart.

use crate::{AkiDbError, Result, StorageBackend};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Snapshot operation state
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum SnapshotState {
    /// No operation in progress
    #[default]
    Idle,
    /// Compressing local data
    Compressing {
        progress: f64,
        started_at: u64,
    },
    /// Uploading to remote storage
    Uploading {
        chunks_completed: u64,
        total_chunks: u64,
        bytes_uploaded: u64,
        total_bytes: u64,
        started_at: u64,
    },
    /// Verifying upload integrity
    Verifying {
        started_at: u64,
    },
    /// Completing (atomic rename)
    Completing {
        started_at: u64,
    },
    /// Operation failed
    Failed {
        error: String,
        retry_count: u32,
        last_attempt: u64,
        original_state: Box<SnapshotState>,
    },
    /// Operation completed successfully
    Completed {
        completed_at: u64,
    },
}

impl SnapshotState {
    /// Check if this state represents an in-progress operation
    pub fn is_in_progress(&self) -> bool {
        matches!(
            self,
            SnapshotState::Compressing { .. }
                | SnapshotState::Uploading { .. }
                | SnapshotState::Verifying { .. }
                | SnapshotState::Completing { .. }
        )
    }

    /// Check if this state is resumable
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            SnapshotState::Uploading { .. }
                | SnapshotState::Failed { .. }
        )
    }

    /// Get progress as percentage (0.0 - 1.0)
    pub fn progress(&self) -> f64 {
        match self {
            SnapshotState::Idle => 0.0,
            SnapshotState::Compressing { progress, .. } => progress * 0.2, // 0-20%
            SnapshotState::Uploading {
                chunks_completed,
                total_chunks,
                ..
            } => {
                if *total_chunks == 0 {
                    0.2
                } else {
                    0.2 + (*chunks_completed as f64 / *total_chunks as f64) * 0.6 // 20-80%
                }
            }
            SnapshotState::Verifying { .. } => 0.85,  // 85%
            SnapshotState::Completing { .. } => 0.95, // 95%
            SnapshotState::Failed { original_state, .. } => original_state.progress(),
            SnapshotState::Completed { .. } => 1.0,
        }
    }

    /// Get state name for logging/metrics
    pub fn name(&self) -> &'static str {
        match self {
            SnapshotState::Idle => "idle",
            SnapshotState::Compressing { .. } => "compressing",
            SnapshotState::Uploading { .. } => "uploading",
            SnapshotState::Verifying { .. } => "verifying",
            SnapshotState::Completing { .. } => "completing",
            SnapshotState::Failed { .. } => "failed",
            SnapshotState::Completed { .. } => "completed",
        }
    }
}

/// Persistent snapshot state record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStateRecord {
    /// Unique snapshot operation ID
    pub operation_id: String,
    /// Snapshot ID being created
    pub snapshot_id: String,
    /// Collection name
    pub collection: String,
    /// Shard ID (if applicable)
    pub shard_id: Option<String>,
    /// Current state
    pub state: SnapshotState,
    /// Operation started timestamp
    pub started_at: u64,
    /// Last state update timestamp
    pub updated_at: u64,
    /// Upload checkpoint data (for resumable uploads)
    pub upload_checkpoint: Option<UploadCheckpoint>,
}

impl SnapshotStateRecord {
    /// Create a new state record for a snapshot operation
    pub fn new(snapshot_id: String, collection: String, shard_id: Option<String>) -> Self {
        let now = current_timestamp();
        Self {
            operation_id: uuid::Uuid::new_v4().to_string(),
            snapshot_id,
            collection,
            shard_id,
            state: SnapshotState::Idle,
            started_at: now,
            updated_at: now,
            upload_checkpoint: None,
        }
    }

    /// Get elapsed time since operation started
    pub fn elapsed_secs(&self) -> u64 {
        current_timestamp().saturating_sub(self.started_at)
    }
}

/// Checkpoint data for resumable uploads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadCheckpoint {
    /// S3 multipart upload ID
    pub upload_id: String,
    /// Completed parts (part_number, etag)
    pub completed_parts: Vec<(u32, String)>,
    /// Next part number to upload
    pub next_part: u32,
    /// Bytes uploaded so far
    pub bytes_uploaded: u64,
    /// Total bytes to upload
    pub total_bytes: u64,
    /// Remote object key
    pub object_key: String,
    /// Local file path (for reading remaining data)
    pub local_path: String,
}

/// State machine manager for snapshot operations
pub struct SnapshotStateMachine<S: StorageBackend> {
    storage: Arc<S>,
    /// Key prefix for state records
    key_prefix: String,
    /// Maximum retry count
    max_retries: u32,
    /// Retry backoff schedule (seconds)
    retry_backoff: Vec<u64>,
}

impl<S: StorageBackend> SnapshotStateMachine<S> {
    /// Create a new state machine with the given storage backend
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            key_prefix: "snapshot_state:".to_string(),
            max_retries: 3,
            retry_backoff: vec![60, 300, 900], // 1min, 5min, 15min
        }
    }

    /// Configure maximum retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Configure retry backoff schedule
    pub fn with_retry_backoff(mut self, backoff: Vec<u64>) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Get the storage key for a state record
    fn state_key(&self, operation_id: &str) -> Vec<u8> {
        format!("{}{}", self.key_prefix, operation_id).into_bytes()
    }

    /// Start a new snapshot operation
    pub fn start_operation(
        &self,
        snapshot_id: String,
        collection: String,
        shard_id: Option<String>,
    ) -> Result<SnapshotStateRecord> {
        let record = SnapshotStateRecord::new(snapshot_id, collection, shard_id);
        self.save_state(&record)?;
        info!(
            operation_id = %record.operation_id,
            snapshot_id = %record.snapshot_id,
            "Started snapshot operation"
        );
        Ok(record)
    }

    /// Transition to compressing state
    pub fn transition_to_compressing(&self, record: &mut SnapshotStateRecord) -> Result<()> {
        let now = current_timestamp();
        record.state = SnapshotState::Compressing {
            progress: 0.0,
            started_at: now,
        };
        record.updated_at = now;
        self.save_state(record)?;
        debug!(
            operation_id = %record.operation_id,
            "Transitioned to compressing state"
        );
        Ok(())
    }

    /// Update compression progress
    pub fn update_compression_progress(
        &self,
        record: &mut SnapshotStateRecord,
        progress: f64,
    ) -> Result<()> {
        if let SnapshotState::Compressing { started_at, .. } = record.state {
            record.state = SnapshotState::Compressing {
                progress: progress.clamp(0.0, 1.0),
                started_at,
            };
            record.updated_at = current_timestamp();
            self.save_state(record)?;
        }
        Ok(())
    }

    /// Transition to uploading state
    ///
    /// # Arguments
    /// * `record` - The state record to update
    /// * `total_chunks` - Total number of chunks to upload
    /// * `total_bytes` - Total bytes to upload
    /// * `checkpoint` - Upload checkpoint (used for resume progress)
    ///
    /// # FIX BUG-HUNT-001
    /// Previously hardcoded `bytes_uploaded: 0` and `chunks_completed: 0`, which caused
    /// incorrect progress display after resuming an interrupted upload. Now uses the
    /// checkpoint values to preserve resume progress.
    pub fn transition_to_uploading(
        &self,
        record: &mut SnapshotStateRecord,
        total_chunks: u64,
        total_bytes: u64,
        checkpoint: UploadCheckpoint,
    ) -> Result<()> {
        let now = current_timestamp();
        // FIX BUG-HUNT-001: Use checkpoint values for resume progress instead of hardcoded 0
        let chunks_completed = checkpoint.completed_parts.len() as u64;
        let bytes_uploaded = checkpoint.bytes_uploaded;
        record.state = SnapshotState::Uploading {
            chunks_completed,
            total_chunks,
            bytes_uploaded,
            total_bytes,
            started_at: now,
        };
        record.upload_checkpoint = Some(checkpoint);
        record.updated_at = now;
        self.save_state(record)?;
        debug!(
            operation_id = %record.operation_id,
            total_chunks,
            total_bytes,
            "Transitioned to uploading state"
        );
        Ok(())
    }

    /// Update upload progress
    pub fn update_upload_progress(
        &self,
        record: &mut SnapshotStateRecord,
        chunks_completed: u64,
        bytes_uploaded: u64,
        checkpoint: UploadCheckpoint,
    ) -> Result<()> {
        if let SnapshotState::Uploading {
            total_chunks,
            total_bytes,
            started_at,
            ..
        } = record.state
        {
            record.state = SnapshotState::Uploading {
                chunks_completed,
                total_chunks,
                bytes_uploaded,
                total_bytes,
                started_at,
            };
            record.upload_checkpoint = Some(checkpoint);
            record.updated_at = current_timestamp();
            self.save_state(record)?;
            debug!(
                operation_id = %record.operation_id,
                chunks_completed,
                total_chunks,
                bytes_uploaded,
                total_bytes,
                "Updated upload progress"
            );
        }
        Ok(())
    }

    /// Transition to verifying state
    pub fn transition_to_verifying(&self, record: &mut SnapshotStateRecord) -> Result<()> {
        let now = current_timestamp();
        record.state = SnapshotState::Verifying { started_at: now };
        record.updated_at = now;
        self.save_state(record)?;
        debug!(
            operation_id = %record.operation_id,
            "Transitioned to verifying state"
        );
        Ok(())
    }

    /// Transition to completing state
    pub fn transition_to_completing(&self, record: &mut SnapshotStateRecord) -> Result<()> {
        let now = current_timestamp();
        record.state = SnapshotState::Completing { started_at: now };
        record.updated_at = now;
        self.save_state(record)?;
        debug!(
            operation_id = %record.operation_id,
            "Transitioned to completing state"
        );
        Ok(())
    }

    /// Mark operation as completed
    pub fn complete_operation(&self, record: &mut SnapshotStateRecord) -> Result<()> {
        let now = current_timestamp();
        record.state = SnapshotState::Completed { completed_at: now };
        record.upload_checkpoint = None; // Clear checkpoint data
        record.updated_at = now;
        self.save_state(record)?;
        info!(
            operation_id = %record.operation_id,
            snapshot_id = %record.snapshot_id,
            elapsed_secs = record.elapsed_secs(),
            "Snapshot operation completed successfully"
        );
        Ok(())
    }

    /// Mark operation as failed
    pub fn fail_operation(&self, record: &mut SnapshotStateRecord, error: String) -> Result<()> {
        let now = current_timestamp();
        let (retry_count, original_state) = match &record.state {
            SnapshotState::Failed {
                retry_count,
                original_state,
                ..
            } => (retry_count.saturating_add(1), original_state.clone()),
            other => (0, Box::new(other.clone())),
        };

        record.state = SnapshotState::Failed {
            error: error.clone(),
            retry_count,
            last_attempt: now,
            original_state,
        };
        record.updated_at = now;
        self.save_state(record)?;

        if retry_count < self.max_retries {
            warn!(
                operation_id = %record.operation_id,
                error = %error,
                retry_count,
                max_retries = self.max_retries,
                "Snapshot operation failed, will retry"
            );
        } else {
            error!(
                operation_id = %record.operation_id,
                error = %error,
                retry_count,
                "Snapshot operation failed permanently (max retries exceeded)"
            );
        }
        Ok(())
    }

    /// Check if operation should be retried
    pub fn should_retry(&self, record: &SnapshotStateRecord) -> bool {
        if let SnapshotState::Failed {
            retry_count,
            last_attempt,
            ..
        } = &record.state
        {
            if *retry_count >= self.max_retries {
                return false;
            }
            // Check if enough time has passed since last attempt
            let backoff_secs = self.retry_backoff_secs(*retry_count);
            let elapsed = current_timestamp().saturating_sub(*last_attempt);
            elapsed >= backoff_secs
        } else {
            false
        }
    }

    /// Get time until next retry is allowed (in seconds)
    pub fn time_until_retry(&self, record: &SnapshotStateRecord) -> Option<u64> {
        if let SnapshotState::Failed {
            retry_count,
            last_attempt,
            ..
        } = &record.state
        {
            if *retry_count >= self.max_retries {
                return None;
            }
            let backoff_secs = self.retry_backoff_secs(*retry_count);
            let elapsed = current_timestamp().saturating_sub(*last_attempt);
            if elapsed >= backoff_secs {
                Some(0)
            } else {
                Some(backoff_secs - elapsed)
            }
        } else {
            None
        }
    }

    fn retry_backoff_secs(&self, retry_count: u32) -> u64 {
        let backoff_idx = usize::try_from(retry_count).unwrap_or(usize::MAX);
        self.retry_backoff
            .get(backoff_idx.min(self.retry_backoff.len().saturating_sub(1)))
            .copied()
            .unwrap_or(60)
    }

    /// Reset a failed operation for retry
    pub fn reset_for_retry(&self, record: &mut SnapshotStateRecord) -> Result<()> {
        if let SnapshotState::Failed { original_state, .. } = &record.state {
            record.state = *original_state.clone();
            record.updated_at = current_timestamp();
            self.save_state(record)?;
            info!(
                operation_id = %record.operation_id,
                new_state = record.state.name(),
                "Reset operation for retry"
            );
        }
        Ok(())
    }

    /// Load state record by operation ID
    pub fn load_state(&self, operation_id: &str) -> Result<Option<SnapshotStateRecord>> {
        let key = self.state_key(operation_id);
        match self.storage.get(&key)? {
            Some(data) => {
                let record: SnapshotStateRecord = serde_json::from_slice(&data).map_err(|e| {
                    AkiDbError::StorageError(format!("Failed to deserialize state: {}", e))
                })?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Save state record
    pub fn save_state(&self, record: &SnapshotStateRecord) -> Result<()> {
        let key = self.state_key(&record.operation_id);
        let data = serde_json::to_vec(record).map_err(|e| {
            AkiDbError::StorageError(format!("Failed to serialize state: {}", e))
        })?;
        self.storage.put(&key, &data)
    }

    /// Delete state record
    pub fn delete_state(&self, operation_id: &str) -> Result<()> {
        let key = self.state_key(operation_id);
        self.storage.delete(&key)
    }

    /// List all in-progress operations
    pub fn list_in_progress(&self) -> Result<Vec<SnapshotStateRecord>> {
        let records = self.list_all()?;
        Ok(records
            .into_iter()
            .filter(|r| r.state.is_in_progress())
            .collect())
    }

    /// List all failed operations that can be retried
    pub fn list_retryable(&self) -> Result<Vec<SnapshotStateRecord>> {
        let records = self.list_all()?;
        Ok(records
            .into_iter()
            .filter(|r| self.should_retry(r))
            .collect())
    }

    /// List all state records
    pub fn list_all(&self) -> Result<Vec<SnapshotStateRecord>> {
        let prefix = self.key_prefix.as_bytes();
        let entries = self.storage.scan_prefix(prefix)?;
        let mut records = Vec::new();
        for (_, data) in entries {
            match serde_json::from_slice::<SnapshotStateRecord>(&data) {
                Ok(record) => records.push(record),
                Err(e) => {
                    warn!("Failed to deserialize state record: {}", e);
                }
            }
        }
        // Sort by started_at descending
        records.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(records)
    }

    /// Clean up old completed/failed records
    pub fn cleanup_old_records(&self, max_age_secs: u64) -> Result<u32> {
        let now = current_timestamp();
        let records = self.list_all()?;
        let mut cleaned = 0;

        for record in records {
            let age = now.saturating_sub(record.updated_at);
            let should_cleanup = match &record.state {
                SnapshotState::Completed { .. } => age > max_age_secs,
                SnapshotState::Failed { retry_count, .. } => {
                    *retry_count >= self.max_retries && age > max_age_secs
                }
                _ => false,
            };

            if should_cleanup {
                self.delete_state(&record.operation_id)?;
                cleaned += 1;
                debug!(
                    operation_id = %record.operation_id,
                    age_secs = age,
                    "Cleaned up old state record"
                );
            }
        }

        if cleaned > 0 {
            info!(cleaned, "Cleaned up old snapshot state records");
        }
        Ok(cleaned)
    }

    /// Recover in-progress operations after restart
    pub fn recover_operations(&self) -> Result<Vec<SnapshotStateRecord>> {
        let in_progress = self.list_in_progress()?;
        let retryable = self.list_retryable()?;

        let mut to_recover = Vec::new();
        to_recover.extend(in_progress);
        to_recover.extend(retryable);

        // Deduplicate by operation_id
        to_recover.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        to_recover.dedup_by(|a, b| a.operation_id == b.operation_id);

        if !to_recover.is_empty() {
            info!(
                count = to_recover.len(),
                "Found snapshot operations to recover"
            );
        }

        Ok(to_recover)
    }
}

/// Get current timestamp in seconds since UNIX epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksDbBackend;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Arc<RocksDbBackend>) {
        let dir = TempDir::new().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();
        (dir, Arc::new(backend))
    }

    #[test]
    fn test_state_machine_lifecycle() {
        let (_dir, storage) = create_test_storage();
        let sm = SnapshotStateMachine::new(storage);

        // Start operation
        let mut record = sm
            .start_operation("snap-1".to_string(), "test-collection".to_string(), None)
            .unwrap();
        assert_eq!(record.state, SnapshotState::Idle);

        // Transition to compressing
        sm.transition_to_compressing(&mut record).unwrap();
        assert!(matches!(record.state, SnapshotState::Compressing { .. }));

        // Update compression progress
        sm.update_compression_progress(&mut record, 0.5).unwrap();
        if let SnapshotState::Compressing { progress, .. } = record.state {
            assert!((progress - 0.5).abs() < 0.001);
        }

        // Transition to uploading
        let checkpoint = UploadCheckpoint {
            upload_id: "test-upload".to_string(),
            completed_parts: vec![],
            next_part: 1,
            bytes_uploaded: 0,
            total_bytes: 1000,
            object_key: "test-key".to_string(),
            local_path: "/tmp/test".to_string(),
        };
        sm.transition_to_uploading(&mut record, 10, 1000, checkpoint)
            .unwrap();
        assert!(matches!(record.state, SnapshotState::Uploading { .. }));

        // Complete
        sm.transition_to_verifying(&mut record).unwrap();
        sm.transition_to_completing(&mut record).unwrap();
        sm.complete_operation(&mut record).unwrap();
        assert!(matches!(record.state, SnapshotState::Completed { .. }));
    }

    #[test]
    fn test_failure_and_retry() {
        let (_dir, storage) = create_test_storage();
        let sm = SnapshotStateMachine::new(storage).with_max_retries(3);

        let mut record = sm
            .start_operation("snap-2".to_string(), "test-collection".to_string(), None)
            .unwrap();
        sm.transition_to_compressing(&mut record).unwrap();

        // Fail the operation
        sm.fail_operation(&mut record, "Test error".to_string())
            .unwrap();
        assert!(matches!(record.state, SnapshotState::Failed { .. }));

        if let SnapshotState::Failed { retry_count, .. } = record.state {
            assert_eq!(retry_count, 0);
        }

        // Should be retryable (but backoff not elapsed)
        // Note: In test, time hasn't passed so should_retry would be false
    }

    #[test]
    fn test_empty_retry_backoff_uses_default_delay() {
        let (_dir, storage) = create_test_storage();
        let sm = SnapshotStateMachine::new(storage)
            .with_max_retries(3)
            .with_retry_backoff(vec![]);
        let mut record =
            SnapshotStateRecord::new("snap-empty-backoff".to_string(), "test".to_string(), None);
        record.state = SnapshotState::Failed {
            error: "retryable".to_string(),
            retry_count: 0,
            last_attempt: current_timestamp().saturating_sub(60),
            original_state: Box::new(SnapshotState::Uploading {
                chunks_completed: 0,
                total_chunks: 1,
                bytes_uploaded: 0,
                total_bytes: 1,
                started_at: 0,
            }),
        };

        assert!(sm.should_retry(&record));
        assert_eq!(sm.time_until_retry(&record), Some(0));
    }

    #[test]
    fn test_retry_count_saturates_on_repeated_failure() {
        let (_dir, storage) = create_test_storage();
        let sm = SnapshotStateMachine::new(storage);
        let mut record =
            SnapshotStateRecord::new("snap-retry-overflow".to_string(), "test".to_string(), None);
        record.state = SnapshotState::Failed {
            error: "previous".to_string(),
            retry_count: u32::MAX,
            last_attempt: 0,
            original_state: Box::new(SnapshotState::Compressing {
                progress: 0.0,
                started_at: 0,
            }),
        };

        sm.fail_operation(&mut record, "again".to_string()).unwrap();

        assert!(matches!(
            record.state,
            SnapshotState::Failed {
                retry_count: u32::MAX,
                ..
            }
        ));
    }

    #[test]
    fn test_persistence() {
        let (_dir, storage) = create_test_storage();
        let sm = SnapshotStateMachine::new(storage.clone());

        // Create and save a record
        let mut record = sm
            .start_operation("snap-3".to_string(), "test-collection".to_string(), None)
            .unwrap();
        sm.transition_to_compressing(&mut record).unwrap();

        // Load it back
        let loaded = sm.load_state(&record.operation_id).unwrap().unwrap();
        assert_eq!(loaded.snapshot_id, "snap-3");
        assert!(matches!(loaded.state, SnapshotState::Compressing { .. }));
    }

    #[test]
    fn test_progress_calculation() {
        assert!((SnapshotState::Idle.progress() - 0.0).abs() < 0.001);

        let compressing = SnapshotState::Compressing {
            progress: 0.5,
            started_at: 0,
        };
        assert!((compressing.progress() - 0.1).abs() < 0.001); // 0.5 * 0.2 = 0.1

        let uploading = SnapshotState::Uploading {
            chunks_completed: 5,
            total_chunks: 10,
            bytes_uploaded: 500,
            total_bytes: 1000,
            started_at: 0,
        };
        assert!((uploading.progress() - 0.5).abs() < 0.001); // 0.2 + 0.5 * 0.6 = 0.5

        let completed = SnapshotState::Completed { completed_at: 0 };
        assert!((completed.progress() - 1.0).abs() < 0.001);
    }
}
