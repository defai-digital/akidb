//! Index rebuild manager for zero-downtime index rebuilds
//!
//! This module implements the dual-index swap mechanism that allows
//! rebuilding the index without service interruption.
//!
//! ## Rebuild Process
//!
//! 1. **PRE-REBUILD**: Record WAL position, allocate shadow index
//! 2. **DURING REBUILD**: Reads served by old index, writes go to both
//! 3. **POST-REBUILD**: Replay WAL, validate, atomic pointer swap
//! 4. **CLEANUP**: Deallocate old index, clear WAL
//!
//! ## Guards (AutomatosX Principles)
//!
//! State transitions are protected by guards that verify preconditions:
//! - G1: Idle → Preparing: No active rebuild
//! - G2: Preparing → Building: Shadow index allocated
//! - G3: Building → Swapping: Shadow index valid and non-empty
//! - G4: Swapping → Idle: No data loss (vector count maintained)

use crate::{InternalId, Result, SearchParams, SearchResult, TombstoneBitset, VectorId, VectorIndex};
use akidb_common::metrics::{record_rebuild_phase_duration, set_rebuild_state};
use akidb_common::AkiDbError;
use akidb_invariants::{critical_invariant, debug_invariant};
use parking_lot::RwLock;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Rebuild state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildState {
    /// No rebuild in progress
    Idle,
    /// Preparing for rebuild (recording WAL position)
    Preparing,
    /// Building shadow index
    Building,
    /// Replaying WAL entries
    Replaying,
    /// Validating shadow index
    Validating,
    /// Swapping indices
    Swapping,
    /// Cleaning up old index
    Cleaning,
}

impl std::fmt::Display for RebuildState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebuildState::Idle => write!(f, "idle"),
            RebuildState::Preparing => write!(f, "preparing"),
            RebuildState::Building => write!(f, "building"),
            RebuildState::Replaying => write!(f, "replaying"),
            RebuildState::Validating => write!(f, "validating"),
            RebuildState::Swapping => write!(f, "swapping"),
            RebuildState::Cleaning => write!(f, "cleaning"),
        }
    }
}

/// Rebuild progress metrics
#[derive(Debug, Clone)]
pub struct RebuildProgress {
    pub state: RebuildState,
    pub vectors_processed: u64,
    pub vectors_total: u64,
    pub wal_entries_replayed: u64,
    pub started_at: Option<Instant>,
    pub phase_started_at: Option<Instant>,
}

impl Default for RebuildProgress {
    fn default() -> Self {
        Self {
            state: RebuildState::Idle,
            vectors_processed: 0,
            vectors_total: 0,
            wal_entries_replayed: 0,
            started_at: None,
            phase_started_at: None,
        }
    }
}

impl RebuildProgress {
    /// Get rebuild progress as a percentage (0.0 - 1.0)
    pub fn progress_percent(&self) -> f64 {
        if self.vectors_total == 0 {
            return 0.0;
        }
        self.vectors_processed as f64 / self.vectors_total as f64
    }

    /// Get elapsed time since rebuild started
    pub fn elapsed(&self) -> Option<Duration> {
        self.started_at.map(|start| start.elapsed())
    }

    /// Get estimated time remaining
    pub fn estimated_remaining(&self) -> Option<Duration> {
        let elapsed = self.elapsed()?;
        let progress = self.progress_percent();
        if progress <= 0.0 {
            return None;
        }
        let total_estimated = elapsed.as_secs_f64() / progress;
        let remaining = total_estimated - elapsed.as_secs_f64();
        Some(Duration::from_secs_f64(remaining.max(0.0)))
    }
}

/// Configuration for rebuild manager
#[derive(Debug, Clone)]
pub struct RebuildConfig {
    /// Tombstone ratio threshold to trigger automatic rebuild (default: 0.10)
    pub compaction_threshold: f64,
    /// Maximum memory budget for shadow index (bytes)
    pub max_shadow_memory: usize,
    /// Batch size for processing vectors during rebuild
    pub rebuild_batch_size: usize,
    /// Timeout for rebuild operation
    pub rebuild_timeout: Duration,
    /// Minimum interval between automatic rebuilds
    pub min_rebuild_interval: Duration,
    /// Whether to validate shadow index before swap
    pub validate_before_swap: bool,
    /// Number of random vectors to validate
    pub validation_sample_size: usize,
}

impl Default for RebuildConfig {
    fn default() -> Self {
        Self {
            compaction_threshold: 0.10, // 10% tombstones
            max_shadow_memory: 8 * 1024 * 1024 * 1024, // 8GB
            rebuild_batch_size: 10_000,
            rebuild_timeout: Duration::from_secs(600), // 10 minutes
            min_rebuild_interval: Duration::from_secs(3600), // 1 hour
            validate_before_swap: true,
            validation_sample_size: 1000,
        }
    }
}

/// Rebuild manager for zero-downtime index rebuilds
///
/// This manages the dual-index swap mechanism:
/// - Primary index handles all reads
/// - Shadow index is built in background
/// - Atomic swap when shadow is ready
pub struct RebuildManager<I: VectorIndex> {
    /// Primary (active) index
    primary: Arc<RwLock<Arc<I>>>,
    /// Shadow index (being built)
    shadow: Arc<RwLock<Option<Arc<I>>>>,
    /// Configuration
    config: RebuildConfig,
    /// Current rebuild state
    state: Arc<RwLock<RebuildState>>,
    /// Rebuild progress
    progress: Arc<RwLock<RebuildProgress>>,
    /// WAL position at rebuild start
    wal_start_lsn: AtomicU64,
    /// Last rebuild timestamp
    last_rebuild: Arc<RwLock<Option<Instant>>>,
    /// Rebuild in progress flag
    rebuilding: AtomicBool,
}

impl<I: VectorIndex + 'static> RebuildManager<I> {
    /// Create a new rebuild manager with the given primary index
    pub fn new(primary: Arc<I>, config: RebuildConfig) -> Self {
        Self {
            primary: Arc::new(RwLock::new(primary)),
            shadow: Arc::new(RwLock::new(None)),
            config,
            state: Arc::new(RwLock::new(RebuildState::Idle)),
            progress: Arc::new(RwLock::new(RebuildProgress::default())),
            wal_start_lsn: AtomicU64::new(0),
            last_rebuild: Arc::new(RwLock::new(None)),
            rebuilding: AtomicBool::new(false),
        }
    }

    /// Get the current primary index
    pub fn primary(&self) -> Arc<I> {
        self.primary.read().clone()
    }

    /// Get current rebuild state
    pub fn state(&self) -> RebuildState {
        *self.state.read()
    }

    /// Get rebuild progress
    pub fn progress(&self) -> RebuildProgress {
        self.progress.read().clone()
    }

    /// Check if rebuild is in progress
    pub fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(Ordering::Acquire)
    }

    /// Check if compaction is needed based on tombstone ratio
    pub fn needs_compaction(&self, tombstones: &TombstoneBitset) -> bool {
        // Check minimum interval since last rebuild
        if let Some(last) = *self.last_rebuild.read() {
            if last.elapsed() < self.config.min_rebuild_interval {
                return false;
            }
        }

        tombstones.needs_compaction(self.config.compaction_threshold)
    }

    /// Start a rebuild operation
    ///
    /// Returns the WAL LSN at rebuild start for replay tracking
    pub fn start_rebuild(&self, current_wal_lsn: u64) -> Result<u64> {
        // Check if already rebuilding
        if self.rebuilding.swap(true, Ordering::AcqRel) {
            return Err(AkiDbError::RebuildInProgress);
        }

        // Update state
        let mut state = self.state.write();
        *state = RebuildState::Preparing;
        set_rebuild_state("preparing", true);

        // Record WAL position
        self.wal_start_lsn.store(current_wal_lsn, Ordering::Release);

        // Initialize progress
        let mut progress = self.progress.write();
        *progress = RebuildProgress {
            state: RebuildState::Preparing,
            vectors_processed: 0,
            vectors_total: 0,
            wal_entries_replayed: 0,
            started_at: Some(Instant::now()),
            phase_started_at: Some(Instant::now()),
        };

        info!(
            wal_lsn = current_wal_lsn,
            "Started rebuild operation"
        );

        Ok(current_wal_lsn)
    }

    /// Set the shadow index for swap
    pub fn set_shadow(&self, shadow_index: Arc<I>) {
        let mut shadow = self.shadow.write();
        *shadow = Some(shadow_index);

        // Record phase duration and transition state
        let mut progress = self.progress.write();
        if let Some(phase_start) = progress.phase_started_at {
            record_rebuild_phase_duration("preparing", phase_start.elapsed().as_secs_f64());
        }
        set_rebuild_state("preparing", false);
        set_rebuild_state("building", true);

        // Update state
        let mut state = self.state.write();
        *state = RebuildState::Building;

        progress.state = RebuildState::Building;
        progress.phase_started_at = Some(Instant::now());
    }

    /// Update rebuild progress
    pub fn update_progress(&self, vectors_processed: u64, vectors_total: u64) {
        let mut progress = self.progress.write();
        progress.vectors_processed = vectors_processed;
        progress.vectors_total = vectors_total;
    }

    /// Transition to WAL replay phase
    pub fn start_wal_replay(&self) {
        // Record building phase duration
        let mut progress = self.progress.write();
        if let Some(phase_start) = progress.phase_started_at {
            record_rebuild_phase_duration("building", phase_start.elapsed().as_secs_f64());
        }
        set_rebuild_state("building", false);
        set_rebuild_state("replaying", true);

        let mut state = self.state.write();
        *state = RebuildState::Replaying;

        progress.state = RebuildState::Replaying;
        progress.phase_started_at = Some(Instant::now());

        info!("Transitioning to WAL replay phase");
    }

    /// Update WAL replay progress
    pub fn update_wal_replay_progress(&self, entries_replayed: u64) {
        let mut progress = self.progress.write();
        progress.wal_entries_replayed = entries_replayed;
    }

    /// Get the WAL LSN at rebuild start
    pub fn wal_start_lsn(&self) -> u64 {
        self.wal_start_lsn.load(Ordering::Acquire)
    }

    /// Perform atomic swap of primary and shadow indices
    ///
    /// This is the critical section that makes the rebuild zero-downtime.
    ///
    /// ## Guards
    /// - G3: Shadow index must exist and be valid
    /// - G4: New index must have at least as many active vectors as old (minus tombstones)
    pub fn swap_indices(&self) -> Result<()> {
        // GUARD G3: Validate shadow exists
        let shadow = {
            let shadow_guard = self.shadow.read();
            shadow_guard.clone().ok_or_else(|| {
                AkiDbError::InvalidParameter("No shadow index to swap".to_string())
            })?
        };

        // GUARD G3: Validate shadow is healthy (has vectors)
        let shadow_stats = shadow.stats();
        debug_invariant!(
            shadow_stats.total_vectors > 0 || self.primary().stats().total_vectors == 0,
            "Shadow index has no vectors but primary has {}",
            self.primary().stats().total_vectors
        );

        // Record replaying phase duration and update state
        {
            let mut progress = self.progress.write();
            if let Some(phase_start) = progress.phase_started_at {
                record_rebuild_phase_duration("replaying", phase_start.elapsed().as_secs_f64());
            }
            set_rebuild_state("replaying", false);
            set_rebuild_state("swapping", true);
            progress.state = RebuildState::Swapping;
            progress.phase_started_at = Some(Instant::now());
        }
        {
            let mut state = self.state.write();
            *state = RebuildState::Swapping;
        }

        // Capture old stats for guard verification
        let old_stats = self.primary().stats();
        let old_active_vectors = old_stats.active_vectors;

        // Atomic swap
        {
            let mut primary = self.primary.write();
            let old_primary = std::mem::replace(&mut *primary, shadow);

            let new_stats = primary.stats();

            info!(
                old_vectors = old_primary.stats().total_vectors,
                new_vectors = new_stats.total_vectors,
                "Swapped indices"
            );

            // GUARD G4: Critical check - no data loss during rebuild
            // New index should have at least as many active vectors as old index had
            // (minus any tombstoned vectors that were compacted)
            critical_invariant!(
                new_stats.active_vectors >= old_active_vectors.saturating_sub(old_stats.tombstoned_vectors),
                "rebuild_data_loss",
                "Data loss detected during rebuild: new index has {} active vectors, expected at least {} (old: {} - tombstones: {})",
                new_stats.active_vectors,
                old_active_vectors.saturating_sub(old_stats.tombstoned_vectors),
                old_active_vectors,
                old_stats.tombstoned_vectors
            );
        }

        // Clear shadow
        {
            let mut shadow_guard = self.shadow.write();
            *shadow_guard = None;
        }

        Ok(())
    }

    /// Complete the rebuild operation
    pub fn complete_rebuild(&self) -> Result<()> {
        // Record swapping phase duration and transition to cleaning
        {
            let mut progress = self.progress.write();
            if let Some(phase_start) = progress.phase_started_at {
                record_rebuild_phase_duration("swapping", phase_start.elapsed().as_secs_f64());
            }
            set_rebuild_state("swapping", false);
            set_rebuild_state("cleaning", true);
            progress.state = RebuildState::Cleaning;
            progress.phase_started_at = Some(Instant::now());
        }
        {
            let mut state = self.state.write();
            *state = RebuildState::Cleaning;
        }

        // Record completion time
        {
            let mut last_rebuild = self.last_rebuild.write();
            *last_rebuild = Some(Instant::now());
        }

        // Get final stats
        let elapsed = self.progress.read().elapsed();
        let vectors_processed = self.progress.read().vectors_processed;
        let cleaning_start = self.progress.read().phase_started_at;

        // Record cleaning phase duration
        if let Some(phase_start) = cleaning_start {
            record_rebuild_phase_duration("cleaning", phase_start.elapsed().as_secs_f64());
        }
        set_rebuild_state("cleaning", false);

        // Reset state
        {
            let mut state = self.state.write();
            *state = RebuildState::Idle;
        }
        {
            let mut progress = self.progress.write();
            progress.state = RebuildState::Idle;
        }
        self.rebuilding.store(false, Ordering::Release);

        info!(
            elapsed_secs = elapsed.map(|d| d.as_secs_f64()),
            vectors_processed,
            "Rebuild completed successfully"
        );

        Ok(())
    }

    /// Abort the current rebuild
    pub fn abort_rebuild(&self, reason: &str) {
        warn!(reason, "Aborting rebuild");

        // Clear all rebuild state metrics
        set_rebuild_state("preparing", false);
        set_rebuild_state("building", false);
        set_rebuild_state("replaying", false);
        set_rebuild_state("swapping", false);
        set_rebuild_state("cleaning", false);

        // Clear shadow
        {
            let mut shadow = self.shadow.write();
            *shadow = None;
        }

        // Reset state
        {
            let mut state = self.state.write();
            *state = RebuildState::Idle;
        }
        {
            let mut progress = self.progress.write();
            *progress = RebuildProgress::default();
        }
        self.rebuilding.store(false, Ordering::Release);
    }

    /// Get configuration
    pub fn config(&self) -> &RebuildConfig {
        &self.config
    }
}

// Implement VectorIndex for RebuildManager to allow transparent usage
impl<I: VectorIndex + 'static> VectorIndex for RebuildManager<I> {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        // Insert into primary
        let result = self.primary().insert(id, vector)?;

        // If rebuilding, also insert into shadow
        // FIX BUG-001: Acquire shadow lock first to avoid TOCTOU race
        // The lock acquisition is atomic - if shadow exists, we write to it
        // FIX BUG-104: Fail the operation if shadow insert fails to prevent data loss
        // After the swap, data that succeeded on primary but failed on shadow would be lost.
        // FIX BUG-HUNT-201: Use catch_unwind to handle panics from shadow insert.
        // If shadow.insert() panics (not just returns Err), the rollback code would never
        // execute, causing data loss after the swap completes.
        {
            let shadow_guard = self.shadow.read();
            if let Some(shadow) = shadow_guard.as_ref() {
                // FIX BUG-HUNT-201: Wrap shadow insert in catch_unwind to catch panics
                let shadow_result = panic::catch_unwind(AssertUnwindSafe(|| {
                    shadow.insert(id, vector)
                }));

                let shadow_err = match shadow_result {
                    Ok(Ok(_)) => None, // Shadow insert succeeded
                    Ok(Err(e)) => Some(format!("Shadow insert error: {}", e)),
                    Err(panic_info) => {
                        // Extract panic message if possible
                        let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        Some(format!("Shadow insert PANICKED: {}", panic_msg))
                    }
                };

                if let Some(e) = shadow_err {
                    // FIX BUG-H031: Handle rollback failure explicitly instead of ignoring with `let _ =`
                    // If rollback fails, we're in an inconsistent state and must report it
                    warn!(error = %e, "Shadow insert failed, attempting rollback of primary insert");
                    if let Err(rollback_err) = self.primary().delete(result) {
                        error!(
                            shadow_error = %e,
                            rollback_error = %rollback_err,
                            internal_id = %result.0,
                            "CRITICAL: Shadow insert failed AND rollback failed. Index is in inconsistent state."
                        );
                        return Err(crate::AkiDbError::IndexError(format!(
                            "Shadow insert failed ({}) AND rollback failed ({}). Index may be inconsistent - manual intervention required.",
                            e, rollback_err
                        )));
                    }
                    return Err(crate::AkiDbError::IndexError(format!(
                        "Shadow insert failed during rebuild: {}. Primary insert rolled back to prevent data loss after swap.",
                        e
                    )));
                }
            }
        }

        Ok(result)
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        // Insert into primary
        let results = self.primary().insert_batch(vectors)?;

        // If rebuilding, also insert into shadow
        // FIX BUG-001: Acquire shadow lock first to avoid TOCTOU race
        // FIX BUG-104: Fail the operation if shadow insert fails to prevent data loss
        // FIX BUG-HUNT-201: Use catch_unwind to handle panics from shadow insert.
        {
            let shadow_guard = self.shadow.read();
            if let Some(shadow) = shadow_guard.as_ref() {
                // FIX BUG-HUNT-201: Wrap shadow insert in catch_unwind to catch panics
                let shadow_result = panic::catch_unwind(AssertUnwindSafe(|| {
                    shadow.insert_batch(vectors)
                }));

                let shadow_err = match shadow_result {
                    Ok(Ok(_)) => None, // Shadow insert succeeded
                    Ok(Err(e)) => Some(format!("Shadow batch insert error: {}", e)),
                    Err(panic_info) => {
                        let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        Some(format!("Shadow batch insert PANICKED: {}", panic_msg))
                    }
                };

                if let Some(e) = shadow_err {
                    // FIX BUG-H031: Handle rollback failures explicitly
                    warn!(error = %e, "Shadow batch insert failed, rolling back {} primary inserts", results.len());
                    let mut rollback_failures = Vec::new();
                    for internal_id in &results {
                        if let Err(rollback_err) = self.primary().delete(*internal_id) {
                            rollback_failures.push((*internal_id, rollback_err));
                        }
                    }
                    if !rollback_failures.is_empty() {
                        let failed_ids: Vec<_> = rollback_failures.iter().map(|(id, _)| id.0).collect();
                        error!(
                            shadow_error = %e,
                            failed_rollback_count = rollback_failures.len(),
                            failed_ids = ?failed_ids,
                            "CRITICAL: Shadow batch insert failed AND some rollbacks failed. Index is in inconsistent state."
                        );
                        return Err(crate::AkiDbError::IndexError(format!(
                            "Shadow batch insert failed ({}) AND {} rollbacks failed. Index may be inconsistent - manual intervention required. Failed IDs: {:?}",
                            e, rollback_failures.len(), failed_ids
                        )));
                    }
                    return Err(crate::AkiDbError::IndexError(format!(
                        "Shadow batch insert failed during rebuild: {}. Primary inserts rolled back to prevent data loss after swap.",
                        e
                    )));
                }
            }
        }

        Ok(results)
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        // Always search primary (reads are not affected by rebuild)
        self.primary().search(query, params)
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        self.primary().search_batch(queries, params)
    }

    fn delete(&self, id: InternalId) -> Result<()> {
        // FIX BUG-H010: Make delete atomic across primary and shadow
        // If shadow delete fails, we must NOT proceed with primary delete
        // to prevent "data resurrection" after rebuild swap.
        //
        // Strategy: Check if shadow exists and try shadow delete first
        // If shadow delete fails, abort the operation entirely
        // Only delete from primary if shadow succeeds (or no shadow exists)

        // First, check if we're rebuilding and need to delete from shadow
        {
            let shadow_guard = self.shadow.read();
            if let Some(shadow) = shadow_guard.as_ref() {
                // FIX BUG-H010: Shadow delete MUST succeed for atomic operation
                if let Err(e) = shadow.delete(id) {
                    return Err(crate::AkiDbError::IndexError(format!(
                        "Shadow delete failed during rebuild: {}. Delete aborted to prevent data resurrection after swap.",
                        e
                    )));
                }
            }
        }

        // Shadow delete succeeded (or no shadow), now delete from primary
        self.primary().delete(id)?;

        Ok(())
    }

    fn get_vector(&self, id: InternalId) -> Result<Option<Vec<f32>>> {
        self.primary().get_vector(id)
    }

    fn stats(&self) -> crate::IndexStats {
        let mut stats = self.primary().stats();
        stats.rebuild_in_progress = self.is_rebuilding();
        stats
    }

    fn is_deleted(&self, internal_id: InternalId) -> bool {
        self.primary().is_deleted(internal_id)
    }

    fn dimensions(&self) -> usize {
        self.primary().dimensions()
    }

    fn is_ready(&self) -> bool {
        self.primary().is_ready()
    }

    fn train(&self, training_data: &[f32]) -> Result<()> {
        // Train primary
        self.primary().train(training_data)?;

        // FIX BUG-076: Acquire shadow lock first to avoid TOCTOU race
        // Previous code checked is_rebuilding() before acquiring lock, which could
        // race with abort_rebuild() or complete_rebuild() clearing the shadow.
        // Now we follow the same pattern as insert/delete - the lock acquisition
        // is atomic, and we check if shadow exists inside the lock.
        {
            let shadow_guard = self.shadow.read();
            if let Some(shadow) = shadow_guard.as_ref() {
                if let Err(e) = shadow.train(training_data) {
                    warn!(error = %e, "Failed to train shadow index");
                }
            }
        }

        Ok(())
    }

    fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(Ordering::Acquire)
    }

    fn trigger_rebuild(&self) -> Result<()> {
        // This just marks intent - actual rebuild is orchestrated externally
        if self.is_rebuilding() {
            return Err(AkiDbError::RebuildInProgress);
        }
        info!("Rebuild triggered");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockIndex;

    fn create_test_manager() -> RebuildManager<MockIndex> {
        let primary = Arc::new(MockIndex::new(128, 10000));
        RebuildManager::new(primary, RebuildConfig::default())
    }

    #[test]
    fn test_rebuild_state_transitions() {
        let manager = create_test_manager();

        assert_eq!(manager.state(), RebuildState::Idle);
        assert!(!manager.is_rebuilding());

        // Start rebuild
        let lsn = manager.start_rebuild(100).unwrap();
        assert_eq!(lsn, 100);
        assert_eq!(manager.state(), RebuildState::Preparing);
        assert!(manager.is_rebuilding());

        // Set shadow
        let shadow = Arc::new(MockIndex::new(128, 10000));
        manager.set_shadow(shadow);
        assert_eq!(manager.state(), RebuildState::Building);

        // Start WAL replay
        manager.start_wal_replay();
        assert_eq!(manager.state(), RebuildState::Replaying);

        // Swap
        manager.swap_indices().unwrap();
        assert_eq!(manager.state(), RebuildState::Swapping);

        // Complete
        manager.complete_rebuild().unwrap();
        assert_eq!(manager.state(), RebuildState::Idle);
        assert!(!manager.is_rebuilding());
    }

    #[test]
    fn test_cannot_start_rebuild_twice() {
        let manager = create_test_manager();

        manager.start_rebuild(100).unwrap();

        // Should fail
        let result = manager.start_rebuild(200);
        assert!(matches!(result, Err(AkiDbError::RebuildInProgress)));
    }

    #[test]
    fn test_operations_during_rebuild() {
        let manager = create_test_manager();

        // Insert before rebuild
        let _id1 = manager
            .insert(&VectorId::new("vec-1"), &vec![1.0; 128])
            .unwrap();

        // Start rebuild with shadow
        manager.start_rebuild(100).unwrap();
        let shadow = Arc::new(MockIndex::new(128, 10000));
        manager.set_shadow(shadow);

        // Insert during rebuild (should go to both)
        let _id2 = manager
            .insert(&VectorId::new("vec-2"), &vec![2.0; 128])
            .unwrap();

        // Search should work (uses primary)
        let results = manager
            .search(&vec![1.0; 128], &SearchParams::new(10))
            .unwrap();
        assert!(!results.is_empty());

        // Complete rebuild
        manager.swap_indices().unwrap();
        manager.complete_rebuild().unwrap();

        // Stats should reflect rebuild state
        let stats = manager.stats();
        assert!(!stats.rebuild_in_progress);
    }

    #[test]
    fn test_abort_rebuild() {
        let manager = create_test_manager();

        manager.start_rebuild(100).unwrap();
        let shadow = Arc::new(MockIndex::new(128, 10000));
        manager.set_shadow(shadow);

        // Abort
        manager.abort_rebuild("test abort");

        assert_eq!(manager.state(), RebuildState::Idle);
        assert!(!manager.is_rebuilding());
    }

    #[test]
    fn test_progress_tracking() {
        let manager = create_test_manager();

        manager.start_rebuild(100).unwrap();

        // Update progress
        manager.update_progress(5000, 10000);

        let progress = manager.progress();
        assert_eq!(progress.vectors_processed, 5000);
        assert_eq!(progress.vectors_total, 10000);
        assert!((progress.progress_percent() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_needs_compaction() {
        let manager = create_test_manager();
        let tombstones = TombstoneBitset::new(1000);

        // No tombstones - no compaction needed
        assert!(!manager.needs_compaction(&tombstones));

        // Mark 15% as deleted
        for i in 0..150 {
            tombstones.mark_deleted(InternalId(i)).unwrap();
        }

        // Should need compaction now (> 10% threshold)
        assert!(manager.needs_compaction(&tombstones));
    }
}
