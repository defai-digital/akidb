//! Persistent rebuild state for crash recovery
//!
//! This module provides RocksDB-backed persistence for rebuild operations,
//! enabling recovery after coordinator restarts.

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Persistent rebuild state that can be stored in RocksDB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PersistentRebuildPhase {
    /// No rebuild in progress
    Idle,
    /// Preparing for rebuild
    Preparing {
        wal_lsn: u64,
        started_at: u64,
    },
    /// Scanning vectors from current index
    Scanning {
        vectors_scanned: u64,
        total_vectors: u64,
        started_at: u64,
    },
    /// Building new index
    Building {
        vectors_built: u64,
        total_vectors: u64,
        temp_index_path: String,
        started_at: u64,
    },
    /// Replaying WAL entries to shadow index
    Replaying {
        entries_replayed: u64,
        total_entries: Option<u64>,
        started_at: u64,
    },
    /// Validating shadow index
    Validating {
        samples_checked: u64,
        total_samples: u64,
        started_at: u64,
    },
    /// Swapping indices
    Swapping {
        started_at: u64,
    },
    /// Cleaning up old index
    Cleaning {
        old_index_path: String,
        started_at: u64,
    },
    /// Rebuild failed
    Failed {
        error: String,
        phase_when_failed: Box<PersistentRebuildPhase>,
        retry_count: u32,
        failed_at: u64,
    },
    /// Rebuild completed
    Completed {
        completed_at: u64,
        vectors_rebuilt: u64,
        duration_secs: u64,
    },
}

impl PersistentRebuildPhase {
    /// Get the phase name for logging/metrics
    pub fn name(&self) -> &'static str {
        match self {
            PersistentRebuildPhase::Idle => "idle",
            PersistentRebuildPhase::Preparing { .. } => "preparing",
            PersistentRebuildPhase::Scanning { .. } => "scanning",
            PersistentRebuildPhase::Building { .. } => "building",
            PersistentRebuildPhase::Replaying { .. } => "replaying",
            PersistentRebuildPhase::Validating { .. } => "validating",
            PersistentRebuildPhase::Swapping { .. } => "swapping",
            PersistentRebuildPhase::Cleaning { .. } => "cleaning",
            PersistentRebuildPhase::Failed { .. } => "failed",
            PersistentRebuildPhase::Completed { .. } => "completed",
        }
    }

    /// Check if this phase is in progress (can be resumed)
    pub fn is_in_progress(&self) -> bool {
        matches!(
            self,
            PersistentRebuildPhase::Preparing { .. }
                | PersistentRebuildPhase::Scanning { .. }
                | PersistentRebuildPhase::Building { .. }
                | PersistentRebuildPhase::Replaying { .. }
                | PersistentRebuildPhase::Validating { .. }
                | PersistentRebuildPhase::Swapping { .. }
                | PersistentRebuildPhase::Cleaning { .. }
        )
    }

    /// Check if this phase is resumable
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            PersistentRebuildPhase::Scanning { .. }
                | PersistentRebuildPhase::Building { .. }
                | PersistentRebuildPhase::Replaying { .. }
                | PersistentRebuildPhase::Failed { .. }
        )
    }

    /// Get progress as percentage (0.0 - 1.0)
    pub fn progress(&self) -> f64 {
        match self {
            PersistentRebuildPhase::Idle => 0.0,
            PersistentRebuildPhase::Preparing { .. } => 0.05,
            PersistentRebuildPhase::Scanning {
                vectors_scanned,
                total_vectors,
                ..
            } => {
                if *total_vectors == 0 {
                    0.05
                } else {
                    0.05 + (*vectors_scanned as f64 / *total_vectors as f64) * 0.15
                }
            }
            PersistentRebuildPhase::Building {
                vectors_built,
                total_vectors,
                ..
            } => {
                if *total_vectors == 0 {
                    0.20
                } else {
                    0.20 + (*vectors_built as f64 / *total_vectors as f64) * 0.50
                }
            }
            PersistentRebuildPhase::Replaying {
                entries_replayed,
                total_entries,
                ..
            } => {
                if let Some(total) = total_entries {
                    if *total == 0 {
                        0.75
                    } else {
                        0.70 + (*entries_replayed as f64 / *total as f64) * 0.10
                    }
                } else {
                    0.75
                }
            }
            PersistentRebuildPhase::Validating {
                samples_checked,
                total_samples,
                ..
            } => {
                if *total_samples == 0 {
                    0.85
                } else {
                    0.80 + (*samples_checked as f64 / *total_samples as f64) * 0.10
                }
            }
            PersistentRebuildPhase::Swapping { .. } => 0.92,
            PersistentRebuildPhase::Cleaning { .. } => 0.97,
            PersistentRebuildPhase::Completed { .. } => 1.0,
            PersistentRebuildPhase::Failed {
                phase_when_failed, ..
            } => phase_when_failed.progress(),
        }
    }
}

impl Default for PersistentRebuildPhase {
    fn default() -> Self {
        PersistentRebuildPhase::Idle
    }
}

/// Complete rebuild state record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildStateRecord {
    /// Unique rebuild operation ID
    pub operation_id: String,
    /// Shard ID
    pub shard_id: String,
    /// Current phase
    pub phase: PersistentRebuildPhase,
    /// WAL LSN at rebuild start
    pub wal_start_lsn: u64,
    /// Operation started timestamp
    pub started_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
    /// Checkpoint data for resumable operations
    pub checkpoint: Option<RebuildCheckpoint>,
    /// Configuration used for this rebuild
    pub config: RebuildPersistentConfig,
}

impl RebuildStateRecord {
    /// Create a new rebuild state record
    pub fn new(shard_id: String, config: RebuildPersistentConfig) -> Self {
        let now = current_timestamp();
        Self {
            operation_id: uuid::Uuid::new_v4().to_string(),
            shard_id,
            phase: PersistentRebuildPhase::Idle,
            wal_start_lsn: 0,
            started_at: now,
            updated_at: now,
            checkpoint: None,
            config,
        }
    }

    /// Get elapsed time since rebuild started
    pub fn elapsed_secs(&self) -> u64 {
        current_timestamp().saturating_sub(self.started_at)
    }
}

/// Checkpoint data for resumable rebuilds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildCheckpoint {
    /// Last processed internal ID
    pub last_processed_id: i64,
    /// Temporary index path
    pub temp_index_path: String,
    /// Vectors exported so far
    pub vectors_exported: u64,
    /// Total vectors to export
    pub total_vectors: u64,
    /// WAL entries replayed
    pub wal_entries_replayed: u64,
    /// Exported vector data (optional, for small rebuilds)
    pub exported_vectors_path: Option<String>,
}

/// Persistent configuration for rebuilds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildPersistentConfig {
    /// Tombstone threshold that triggered rebuild
    pub tombstone_threshold: f64,
    /// Checkpoint interval (vectors)
    pub checkpoint_interval: u64,
    /// Validation sample size
    pub validation_samples: u64,
    /// Maximum rebuild time before timeout
    pub timeout_secs: u64,
}

impl Default for RebuildPersistentConfig {
    fn default() -> Self {
        Self {
            tombstone_threshold: 0.10,
            checkpoint_interval: 100_000,
            validation_samples: 1000,
            timeout_secs: 3600, // 1 hour
        }
    }
}

/// Trait for rebuild state persistence backend
pub trait RebuildStatePersistence: Send + Sync {
    /// Save rebuild state
    fn save_state(&self, record: &RebuildStateRecord) -> Result<(), String>;

    /// Load rebuild state by operation ID
    fn load_state(&self, operation_id: &str) -> Result<Option<RebuildStateRecord>, String>;

    /// Load rebuild state by shard ID (returns most recent)
    fn load_by_shard(&self, shard_id: &str) -> Result<Option<RebuildStateRecord>, String>;

    /// Delete rebuild state
    fn delete_state(&self, operation_id: &str) -> Result<(), String>;

    /// List all rebuild states
    fn list_all(&self) -> Result<Vec<RebuildStateRecord>, String>;
}

/// In-memory implementation for testing
pub struct InMemoryRebuildPersistence {
    states: parking_lot::RwLock<std::collections::HashMap<String, RebuildStateRecord>>,
}

impl InMemoryRebuildPersistence {
    pub fn new() -> Self {
        Self {
            states: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryRebuildPersistence {
    fn default() -> Self {
        Self::new()
    }
}

impl RebuildStatePersistence for InMemoryRebuildPersistence {
    fn save_state(&self, record: &RebuildStateRecord) -> Result<(), String> {
        let mut states = self.states.write();
        states.insert(record.operation_id.clone(), record.clone());
        Ok(())
    }

    fn load_state(&self, operation_id: &str) -> Result<Option<RebuildStateRecord>, String> {
        let states = self.states.read();
        Ok(states.get(operation_id).cloned())
    }

    fn load_by_shard(&self, shard_id: &str) -> Result<Option<RebuildStateRecord>, String> {
        let states = self.states.read();
        let mut matching: Vec<_> = states
            .values()
            .filter(|r| r.shard_id == shard_id)
            .cloned()
            .collect();
        matching.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(matching.into_iter().next())
    }

    fn delete_state(&self, operation_id: &str) -> Result<(), String> {
        let mut states = self.states.write();
        states.remove(operation_id);
        Ok(())
    }

    fn list_all(&self) -> Result<Vec<RebuildStateRecord>, String> {
        let states = self.states.read();
        let mut records: Vec<_> = states.values().cloned().collect();
        records.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(records)
    }
}

/// Persistent rebuild state machine
pub struct PersistentRebuildStateMachine<P: RebuildStatePersistence> {
    persistence: P,
    max_retries: u32,
}

impl<P: RebuildStatePersistence> PersistentRebuildStateMachine<P> {
    /// Create a new persistent rebuild state machine
    pub fn new(persistence: P) -> Self {
        Self {
            persistence,
            max_retries: 3,
        }
    }

    /// Configure maximum retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Start a new rebuild operation
    pub fn start_operation(
        &self,
        shard_id: String,
        wal_lsn: u64,
        config: RebuildPersistentConfig,
    ) -> Result<RebuildStateRecord, String> {
        // Check for existing in-progress rebuild for this shard
        if let Some(existing) = self.persistence.load_by_shard(&shard_id)? {
            if existing.phase.is_in_progress() {
                return Err(format!(
                    "Rebuild already in progress for shard {} (operation {})",
                    shard_id, existing.operation_id
                ));
            }
        }

        let mut record = RebuildStateRecord::new(shard_id.clone(), config);
        record.wal_start_lsn = wal_lsn;
        record.phase = PersistentRebuildPhase::Preparing {
            wal_lsn,
            started_at: current_timestamp(),
        };

        self.persistence.save_state(&record)?;
        info!(
            operation_id = %record.operation_id,
            shard_id = %shard_id,
            wal_lsn,
            "Started rebuild operation"
        );
        Ok(record)
    }

    /// Transition to scanning phase
    pub fn transition_to_scanning(
        &self,
        record: &mut RebuildStateRecord,
        total_vectors: u64,
    ) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Scanning {
            vectors_scanned: 0,
            total_vectors,
            started_at: current_timestamp(),
        };
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, total_vectors, "Transitioned to scanning");
        Ok(())
    }

    /// Update scanning progress
    pub fn update_scanning_progress(
        &self,
        record: &mut RebuildStateRecord,
        vectors_scanned: u64,
        checkpoint: Option<RebuildCheckpoint>,
    ) -> Result<(), String> {
        if let PersistentRebuildPhase::Scanning {
            total_vectors,
            started_at,
            ..
        } = record.phase
        {
            record.phase = PersistentRebuildPhase::Scanning {
                vectors_scanned,
                total_vectors,
                started_at,
            };
            record.checkpoint = checkpoint;
            record.updated_at = current_timestamp();

            // Only save periodically to reduce I/O
            if vectors_scanned % record.config.checkpoint_interval == 0 {
                self.persistence.save_state(record)?;
            }
        }
        Ok(())
    }

    /// Transition to building phase
    pub fn transition_to_building(
        &self,
        record: &mut RebuildStateRecord,
        total_vectors: u64,
        temp_index_path: String,
    ) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Building {
            vectors_built: 0,
            total_vectors,
            temp_index_path,
            started_at: current_timestamp(),
        };
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, total_vectors, "Transitioned to building");
        Ok(())
    }

    /// Update building progress
    pub fn update_building_progress(
        &self,
        record: &mut RebuildStateRecord,
        vectors_built: u64,
        checkpoint: Option<RebuildCheckpoint>,
    ) -> Result<(), String> {
        if let PersistentRebuildPhase::Building {
            total_vectors,
            ref temp_index_path,
            started_at,
            ..
        } = record.phase
        {
            record.phase = PersistentRebuildPhase::Building {
                vectors_built,
                total_vectors,
                temp_index_path: temp_index_path.clone(),
                started_at,
            };
            record.checkpoint = checkpoint;
            record.updated_at = current_timestamp();

            if vectors_built % record.config.checkpoint_interval == 0 {
                self.persistence.save_state(record)?;
            }
        }
        Ok(())
    }

    /// Transition to replaying phase
    pub fn transition_to_replaying(
        &self,
        record: &mut RebuildStateRecord,
        total_entries: Option<u64>,
    ) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Replaying {
            entries_replayed: 0,
            total_entries,
            started_at: current_timestamp(),
        };
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, ?total_entries, "Transitioned to replaying");
        Ok(())
    }

    /// Update replay progress
    pub fn update_replay_progress(
        &self,
        record: &mut RebuildStateRecord,
        entries_replayed: u64,
    ) -> Result<(), String> {
        if let PersistentRebuildPhase::Replaying {
            total_entries,
            started_at,
            ..
        } = record.phase
        {
            record.phase = PersistentRebuildPhase::Replaying {
                entries_replayed,
                total_entries,
                started_at,
            };
            record.updated_at = current_timestamp();
            // Save less frequently for WAL replay
            if entries_replayed % 10000 == 0 {
                self.persistence.save_state(record)?;
            }
        }
        Ok(())
    }

    /// Transition to validating phase
    pub fn transition_to_validating(
        &self,
        record: &mut RebuildStateRecord,
        total_samples: u64,
    ) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Validating {
            samples_checked: 0,
            total_samples,
            started_at: current_timestamp(),
        };
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, total_samples, "Transitioned to validating");
        Ok(())
    }

    /// Update validation progress
    pub fn update_validation_progress(
        &self,
        record: &mut RebuildStateRecord,
        samples_checked: u64,
    ) -> Result<(), String> {
        if let PersistentRebuildPhase::Validating {
            total_samples,
            started_at,
            ..
        } = record.phase
        {
            record.phase = PersistentRebuildPhase::Validating {
                samples_checked,
                total_samples,
                started_at,
            };
            record.updated_at = current_timestamp();
        }
        Ok(())
    }

    /// Transition to swapping phase
    pub fn transition_to_swapping(&self, record: &mut RebuildStateRecord) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Swapping {
            started_at: current_timestamp(),
        };
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, "Transitioned to swapping");
        Ok(())
    }

    /// Transition to cleaning phase
    pub fn transition_to_cleaning(
        &self,
        record: &mut RebuildStateRecord,
        old_index_path: String,
    ) -> Result<(), String> {
        record.phase = PersistentRebuildPhase::Cleaning {
            old_index_path,
            started_at: current_timestamp(),
        };
        record.checkpoint = None; // Clear checkpoint, no longer needed
        record.updated_at = current_timestamp();
        self.persistence.save_state(record)?;
        debug!(operation_id = %record.operation_id, "Transitioned to cleaning");
        Ok(())
    }

    /// Complete the rebuild operation
    pub fn complete_operation(
        &self,
        record: &mut RebuildStateRecord,
        vectors_rebuilt: u64,
    ) -> Result<(), String> {
        let now = current_timestamp();
        let duration_secs = now.saturating_sub(record.started_at);

        record.phase = PersistentRebuildPhase::Completed {
            completed_at: now,
            vectors_rebuilt,
            duration_secs,
        };
        record.checkpoint = None;
        record.updated_at = now;
        self.persistence.save_state(record)?;

        info!(
            operation_id = %record.operation_id,
            shard_id = %record.shard_id,
            vectors_rebuilt,
            duration_secs,
            "Rebuild completed successfully"
        );
        Ok(())
    }

    /// Mark operation as failed
    pub fn fail_operation(
        &self,
        record: &mut RebuildStateRecord,
        error: String,
    ) -> Result<(), String> {
        let now = current_timestamp();
        let (retry_count, phase_when_failed) = match &record.phase {
            PersistentRebuildPhase::Failed {
                retry_count,
                phase_when_failed,
                ..
            } => (*retry_count + 1, phase_when_failed.clone()),
            other => (0, Box::new(other.clone())),
        };

        record.phase = PersistentRebuildPhase::Failed {
            error: error.clone(),
            phase_when_failed,
            retry_count,
            failed_at: now,
        };
        record.updated_at = now;
        self.persistence.save_state(record)?;

        if retry_count < self.max_retries {
            warn!(
                operation_id = %record.operation_id,
                error = %error,
                retry_count,
                max_retries = self.max_retries,
                "Rebuild failed, will retry"
            );
        } else {
            error!(
                operation_id = %record.operation_id,
                error = %error,
                retry_count,
                "Rebuild failed permanently"
            );
        }
        Ok(())
    }

    /// Check if operation should be retried
    pub fn should_retry(&self, record: &RebuildStateRecord) -> bool {
        if let PersistentRebuildPhase::Failed { retry_count, .. } = &record.phase {
            *retry_count < self.max_retries
        } else {
            false
        }
    }

    /// Reset a failed operation for retry
    pub fn reset_for_retry(&self, record: &mut RebuildStateRecord) -> Result<(), String> {
        if let PersistentRebuildPhase::Failed {
            phase_when_failed, ..
        } = &record.phase
        {
            record.phase = *phase_when_failed.clone();
            record.updated_at = current_timestamp();
            self.persistence.save_state(record)?;
            info!(
                operation_id = %record.operation_id,
                new_phase = record.phase.name(),
                "Reset rebuild for retry"
            );
        }
        Ok(())
    }

    /// Load state for a shard
    pub fn load_shard_state(&self, shard_id: &str) -> Result<Option<RebuildStateRecord>, String> {
        self.persistence.load_by_shard(shard_id)
    }

    /// List all in-progress rebuilds
    pub fn list_in_progress(&self) -> Result<Vec<RebuildStateRecord>, String> {
        let all = self.persistence.list_all()?;
        Ok(all.into_iter().filter(|r| r.phase.is_in_progress()).collect())
    }

    /// Recover operations after restart
    pub fn recover_operations(&self) -> Result<Vec<RebuildStateRecord>, String> {
        let all = self.persistence.list_all()?;
        let recoverable: Vec<_> = all
            .into_iter()
            .filter(|r| r.phase.is_in_progress() || (r.phase.is_resumable() && self.should_retry(r)))
            .collect();

        if !recoverable.is_empty() {
            info!(count = recoverable.len(), "Found rebuild operations to recover");
        }

        Ok(recoverable)
    }

    /// Clean up old completed/failed records
    pub fn cleanup_old_records(&self, max_age_secs: u64) -> Result<u32, String> {
        let now = current_timestamp();
        let all = self.persistence.list_all()?;
        let mut cleaned = 0;

        for record in all {
            let age = now.saturating_sub(record.updated_at);
            let should_cleanup = match &record.phase {
                PersistentRebuildPhase::Completed { .. } => age > max_age_secs,
                PersistentRebuildPhase::Failed { retry_count, .. } => {
                    *retry_count >= self.max_retries && age > max_age_secs
                }
                _ => false,
            };

            if should_cleanup {
                self.persistence.delete_state(&record.operation_id)?;
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            info!(cleaned, "Cleaned up old rebuild state records");
        }
        Ok(cleaned)
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

    #[test]
    fn test_phase_progress() {
        assert!((PersistentRebuildPhase::Idle.progress() - 0.0).abs() < 0.001);

        let scanning = PersistentRebuildPhase::Scanning {
            vectors_scanned: 500,
            total_vectors: 1000,
            started_at: 0,
        };
        let progress = scanning.progress();
        assert!(progress > 0.05 && progress < 0.20);

        let building = PersistentRebuildPhase::Building {
            vectors_built: 500,
            total_vectors: 1000,
            temp_index_path: "/tmp/test".to_string(),
            started_at: 0,
        };
        let progress = building.progress();
        assert!(progress > 0.40 && progress < 0.50);

        let completed = PersistentRebuildPhase::Completed {
            completed_at: 0,
            vectors_rebuilt: 1000,
            duration_secs: 60,
        };
        assert!((completed.progress() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_state_machine_lifecycle() {
        let persistence = InMemoryRebuildPersistence::new();
        let sm = PersistentRebuildStateMachine::new(persistence);

        // Start operation
        let mut record = sm
            .start_operation("shard-1".to_string(), 100, RebuildPersistentConfig::default())
            .unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Preparing { .. }));

        // Transition through phases
        sm.transition_to_scanning(&mut record, 10000).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Scanning { .. }));

        sm.transition_to_building(&mut record, 10000, "/tmp/index".to_string())
            .unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Building { .. }));

        sm.transition_to_replaying(&mut record, Some(100)).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Replaying { .. }));

        sm.transition_to_validating(&mut record, 1000).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Validating { .. }));

        sm.transition_to_swapping(&mut record).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Swapping { .. }));

        sm.transition_to_cleaning(&mut record, "/old/index".to_string())
            .unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Cleaning { .. }));

        sm.complete_operation(&mut record, 10000).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Completed { .. }));
    }

    #[test]
    fn test_failure_and_retry() {
        let persistence = InMemoryRebuildPersistence::new();
        let sm = PersistentRebuildStateMachine::new(persistence).with_max_retries(3);

        let mut record = sm
            .start_operation("shard-1".to_string(), 100, RebuildPersistentConfig::default())
            .unwrap();
        sm.transition_to_building(&mut record, 10000, "/tmp/index".to_string())
            .unwrap();

        // Fail the operation
        sm.fail_operation(&mut record, "Test error".to_string())
            .unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Failed { .. }));
        assert!(sm.should_retry(&record));

        // Reset for retry
        sm.reset_for_retry(&mut record).unwrap();
        assert!(matches!(record.phase, PersistentRebuildPhase::Building { .. }));
    }

    #[test]
    fn test_cannot_start_rebuild_twice() {
        let persistence = InMemoryRebuildPersistence::new();
        let sm = PersistentRebuildStateMachine::new(persistence);

        let _record = sm
            .start_operation("shard-1".to_string(), 100, RebuildPersistentConfig::default())
            .unwrap();

        // Second attempt should fail
        let result = sm.start_operation("shard-1".to_string(), 200, RebuildPersistentConfig::default());
        assert!(result.is_err());
    }
}
