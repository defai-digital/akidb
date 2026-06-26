//! Typestate pattern for rebuild state machine
//!
//! This module provides compile-time guarantees for rebuild state transitions
//! using Rust's type system. Invalid state transitions become compile errors.
//!
//! ## States
//!
//! ```text
//! Idle → Preparing → Building → Replaying → Validating → Swapping → Cleaning → Idle
//! ```
//!
//! ## Guards (Checked at Runtime)
//!
//! - G1: Idle → Preparing: No active rebuild
//! - G2: Preparing → Building: Shadow index allocated
//! - G3: Building → Replaying: Shadow index valid
//! - G4: Swapping → Cleaning: No data loss
//!
//! ## Example
//!
//! ```rust,ignore
//! // Type system enforces valid transitions
//! let fsm = RebuildFsm::<Idle>::new(index);
//!
//! // Start rebuild - returns Preparing state
//! let fsm = fsm.start_rebuild(wal_lsn)?;
//!
//! // Set shadow - returns Building state
//! let fsm = fsm.set_shadow(shadow_index);
//!
//! // Invalid: Can't swap from Building state
//! // fsm.swap(); // COMPILE ERROR!
//!
//! // Must go through all states
//! let fsm = fsm.complete_build();
//! let fsm = fsm.complete_replay();
//! let fsm = fsm.complete_validation();
//! let fsm = fsm.swap()?;
//! let fsm = fsm.cleanup();
//! // Now back to Idle
//! ```

use crate::{AkiDbError, Result, VectorIndex};
use akidb_common::metrics::{record_rebuild_phase_duration, set_rebuild_state};
use akidb_invariants::{critical_invariant, debug_invariant};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

// ============================================================================
// State Types (Zero-Sized)
// ============================================================================

/// Idle state - no rebuild in progress
pub struct Idle;

/// Preparing state - recording WAL position
pub struct Preparing {
    /// WAL LSN at rebuild start
    pub wal_lsn: u64,
    /// When this phase started
    pub started_at: Instant,
}

/// Building state - shadow index being built
pub struct Building<I: VectorIndex> {
    /// WAL LSN at rebuild start
    pub wal_lsn: u64,
    /// Shadow index being built
    pub shadow: Arc<I>,
    /// When this phase started
    pub started_at: Instant,
    /// When rebuild started overall
    pub rebuild_started_at: Instant,
}

/// Replaying state - WAL entries being replayed
pub struct Replaying<I: VectorIndex> {
    /// WAL LSN at rebuild start
    pub wal_lsn: u64,
    /// Shadow index
    pub shadow: Arc<I>,
    /// When this phase started
    pub started_at: Instant,
    /// When rebuild started overall
    pub rebuild_started_at: Instant,
    /// Entries replayed
    pub entries_replayed: u64,
}

/// Validating state - shadow index being validated
pub struct Validating<I: VectorIndex> {
    /// Shadow index
    pub shadow: Arc<I>,
    /// When this phase started
    pub started_at: Instant,
    /// When rebuild started overall
    pub rebuild_started_at: Instant,
}

/// Swapping state - indices being swapped
pub struct Swapping<I: VectorIndex> {
    /// Shadow index (to become primary)
    pub shadow: Arc<I>,
    /// Old primary stats for verification
    pub old_active_vectors: u64,
    /// Old tombstone count
    pub old_tombstones: u64,
    /// When this phase started
    pub started_at: Instant,
    /// When rebuild started overall
    pub rebuild_started_at: Instant,
}

/// Cleaning state - old index being cleaned up
pub struct Cleaning {
    /// When this phase started
    pub started_at: Instant,
    /// When rebuild started overall
    pub rebuild_started_at: Instant,
}

// ============================================================================
// Rebuild FSM with Typestate
// ============================================================================

/// Type-safe rebuild finite state machine
///
/// The state `S` determines which methods are available.
/// Invalid state transitions are compile-time errors.
pub struct RebuildFsm<S, I: VectorIndex> {
    /// Primary index
    primary: Arc<I>,
    /// Current state (contains state-specific data)
    state: S,
    /// Phantom data for type parameter
    _marker: PhantomData<I>,
}

// ============================================================================
// Idle State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Idle, I> {
    /// Create a new FSM in idle state
    pub fn new(primary: Arc<I>) -> Self {
        set_rebuild_state("idle", true);
        Self {
            primary,
            state: Idle,
            _marker: PhantomData,
        }
    }

    /// Start a rebuild operation
    ///
    /// ## Guard G1
    /// - No active rebuild (enforced by type system)
    ///
    /// ## Returns
    /// FSM in Preparing state
    pub fn start_rebuild(self, wal_lsn: u64) -> RebuildFsm<Preparing, I> {
        set_rebuild_state("idle", false);
        set_rebuild_state("preparing", true);

        info!(wal_lsn, "Rebuild started - transitioning to Preparing");

        RebuildFsm {
            primary: self.primary,
            state: Preparing {
                wal_lsn,
                started_at: Instant::now(),
            },
            _marker: PhantomData,
        }
    }

    /// Get reference to primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }
}

// ============================================================================
// Preparing State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Preparing, I> {
    /// Set the shadow index and transition to Building
    ///
    /// ## Guard G2
    /// - Shadow index must be provided (enforced by signature)
    pub fn set_shadow(self, shadow: Arc<I>) -> RebuildFsm<Building<I>, I> {
        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("preparing", duration.as_secs_f64());
        set_rebuild_state("preparing", false);
        set_rebuild_state("building", true);

        info!(
            duration_ms = duration.as_millis(),
            "Transitioning to Building"
        );

        RebuildFsm {
            primary: self.primary,
            state: Building {
                wal_lsn: self.state.wal_lsn,
                shadow,
                started_at: Instant::now(),
                rebuild_started_at: self.state.started_at,
            },
            _marker: PhantomData,
        }
    }

    /// Abort rebuild and return to Idle
    pub fn abort(self, reason: &str) -> RebuildFsm<Idle, I> {
        warn!(reason, "Rebuild aborted in Preparing state");
        set_rebuild_state("preparing", false);
        set_rebuild_state("idle", true);

        RebuildFsm {
            primary: self.primary,
            state: Idle,
            _marker: PhantomData,
        }
    }

    /// Get WAL LSN at rebuild start
    pub fn wal_lsn(&self) -> u64 {
        self.state.wal_lsn
    }

    /// Get reference to primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }
}

// ============================================================================
// Building State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Building<I>, I> {
    /// Complete building and transition to Replaying
    ///
    /// ## Guard G3
    /// - Shadow index must have vectors (checked at runtime)
    pub fn complete_build(self) -> Result<RebuildFsm<Replaying<I>, I>> {
        let shadow_stats = self.state.shadow.stats();

        // Guard G3: Shadow must be valid
        debug_invariant!(
            shadow_stats.total_vectors > 0 || self.primary.stats().total_vectors == 0,
            "Shadow index has no vectors but primary has {}",
            self.primary.stats().total_vectors
        );

        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("building", duration.as_secs_f64());
        set_rebuild_state("building", false);
        set_rebuild_state("replaying", true);

        info!(
            duration_ms = duration.as_millis(),
            shadow_vectors = shadow_stats.total_vectors,
            "Transitioning to Replaying"
        );

        Ok(RebuildFsm {
            primary: self.primary,
            state: Replaying {
                wal_lsn: self.state.wal_lsn,
                shadow: self.state.shadow,
                started_at: Instant::now(),
                rebuild_started_at: self.state.rebuild_started_at,
                entries_replayed: 0,
            },
            _marker: PhantomData,
        })
    }

    /// Abort rebuild and return to Idle
    pub fn abort(self, reason: &str) -> RebuildFsm<Idle, I> {
        warn!(reason, "Rebuild aborted in Building state");
        set_rebuild_state("building", false);
        set_rebuild_state("idle", true);

        RebuildFsm {
            primary: self.primary,
            state: Idle,
            _marker: PhantomData,
        }
    }

    /// Get reference to shadow index for inserting vectors
    pub fn shadow(&self) -> &Arc<I> {
        &self.state.shadow
    }

    /// Get reference to primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }

    /// Get WAL LSN at rebuild start
    pub fn wal_lsn(&self) -> u64 {
        self.state.wal_lsn
    }
}

// ============================================================================
// Replaying State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Replaying<I>, I> {
    /// Update replay progress
    pub fn update_progress(&mut self, entries_replayed: u64) {
        self.state.entries_replayed = entries_replayed;
    }

    /// Complete replay and transition to Validating
    pub fn complete_replay(self) -> RebuildFsm<Validating<I>, I> {
        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("replaying", duration.as_secs_f64());
        set_rebuild_state("replaying", false);
        set_rebuild_state("validating", true);

        info!(
            duration_ms = duration.as_millis(),
            entries_replayed = self.state.entries_replayed,
            "Transitioning to Validating"
        );

        RebuildFsm {
            primary: self.primary,
            state: Validating {
                shadow: self.state.shadow,
                started_at: Instant::now(),
                rebuild_started_at: self.state.rebuild_started_at,
            },
            _marker: PhantomData,
        }
    }

    /// Abort rebuild and return to Idle
    pub fn abort(self, reason: &str) -> RebuildFsm<Idle, I> {
        warn!(reason, "Rebuild aborted in Replaying state");
        set_rebuild_state("replaying", false);
        set_rebuild_state("idle", true);

        RebuildFsm {
            primary: self.primary,
            state: Idle,
            _marker: PhantomData,
        }
    }

    /// Get reference to shadow index
    pub fn shadow(&self) -> &Arc<I> {
        &self.state.shadow
    }

    /// Get reference to primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }
}

// ============================================================================
// Validating State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Validating<I>, I> {
    /// Complete validation and transition to Swapping
    ///
    /// Captures old primary stats for data loss detection
    pub fn complete_validation(self) -> RebuildFsm<Swapping<I>, I> {
        let primary_stats = self.primary.stats();

        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("validating", duration.as_secs_f64());
        set_rebuild_state("validating", false);
        set_rebuild_state("swapping", true);

        info!(
            duration_ms = duration.as_millis(),
            "Transitioning to Swapping"
        );

        RebuildFsm {
            primary: self.primary,
            state: Swapping {
                shadow: self.state.shadow,
                old_active_vectors: primary_stats.active_vectors,
                old_tombstones: primary_stats.tombstoned_vectors,
                started_at: Instant::now(),
                rebuild_started_at: self.state.rebuild_started_at,
            },
            _marker: PhantomData,
        }
    }

    /// Abort rebuild and return to Idle
    pub fn abort(self, reason: &str) -> RebuildFsm<Idle, I> {
        warn!(reason, "Rebuild aborted in Validating state");
        set_rebuild_state("validating", false);
        set_rebuild_state("idle", true);

        RebuildFsm {
            primary: self.primary,
            state: Idle,
            _marker: PhantomData,
        }
    }

    /// Get reference to shadow index for validation
    pub fn shadow(&self) -> &Arc<I> {
        &self.state.shadow
    }

    /// Get reference to primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }
}

// ============================================================================
// Swapping State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Swapping<I>, I> {
    /// Perform the atomic swap and transition to Cleaning
    ///
    /// ## Guard G4
    /// - New index must have at least as many active vectors as old (minus tombstones)
    ///
    /// ## Returns
    /// - Ok: FSM in Cleaning state with new primary
    /// - Err: Data loss detected, returns to Idle
    pub fn swap(self) -> Result<RebuildFsm<Cleaning, I>> {
        let new_stats = self.state.shadow.stats();
        let expected_min = self.state.old_active_vectors.saturating_sub(self.state.old_tombstones);

        // Guard G4: No data loss
        critical_invariant!(
            new_stats.active_vectors >= expected_min,
            "rebuild_data_loss",
            "Data loss detected: new index has {} active vectors, expected at least {} (old: {} - tombstones: {})",
            new_stats.active_vectors,
            expected_min,
            self.state.old_active_vectors,
            self.state.old_tombstones
        );

        if new_stats.active_vectors < expected_min {
            warn!(
                new_vectors = new_stats.active_vectors,
                expected_min,
                "Data loss detected during swap - aborting"
            );
            set_rebuild_state("swapping", false);
            set_rebuild_state("idle", true);
            return Err(AkiDbError::IndexError(format!(
                "Data loss detected: new index has {} vectors, expected at least {}",
                new_stats.active_vectors, expected_min
            )));
        }

        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("swapping", duration.as_secs_f64());
        set_rebuild_state("swapping", false);
        set_rebuild_state("cleaning", true);

        info!(
            duration_ms = duration.as_millis(),
            old_vectors = self.state.old_active_vectors,
            new_vectors = new_stats.active_vectors,
            "Swap complete - transitioning to Cleaning"
        );

        // The swap itself - shadow becomes the new primary
        Ok(RebuildFsm {
            primary: self.state.shadow,
            state: Cleaning {
                started_at: Instant::now(),
                rebuild_started_at: self.state.rebuild_started_at,
            },
            _marker: PhantomData,
        })
    }

    /// Abort rebuild and return to Idle (keeps old primary)
    pub fn abort(self, reason: &str) -> RebuildFsm<Idle, I> {
        warn!(reason, "Rebuild aborted in Swapping state");
        set_rebuild_state("swapping", false);
        set_rebuild_state("idle", true);

        RebuildFsm {
            primary: self.primary,
            state: Idle,
            _marker: PhantomData,
        }
    }
}

// ============================================================================
// Cleaning State Implementation
// ============================================================================

impl<I: VectorIndex + 'static> RebuildFsm<Cleaning, I> {
    /// Complete cleanup and return to Idle
    ///
    /// Returns the new primary index
    pub fn complete(self) -> (RebuildFsm<Idle, I>, std::time::Duration) {
        let duration = self.state.started_at.elapsed();
        record_rebuild_phase_duration("cleaning", duration.as_secs_f64());

        let total_duration = self.state.rebuild_started_at.elapsed();
        set_rebuild_state("cleaning", false);
        set_rebuild_state("idle", true);

        info!(
            cleaning_duration_ms = duration.as_millis(),
            total_duration_ms = total_duration.as_millis(),
            "Rebuild complete"
        );

        (
            RebuildFsm {
                primary: self.primary,
                state: Idle,
                _marker: PhantomData,
            },
            total_duration,
        )
    }

    /// Get reference to new primary index
    pub fn primary(&self) -> &Arc<I> {
        &self.primary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockIndex;

    fn create_mock_index() -> Arc<MockIndex> {
        Arc::new(MockIndex::new(128, 10000))
    }

    #[test]
    fn test_typestate_valid_transitions() {
        let primary = create_mock_index();

        // Insert some vectors into primary
        primary
            .insert(&crate::VectorId::new("vec-1"), &vec![1.0; 128])
            .unwrap();

        // Start in Idle
        let fsm = RebuildFsm::<Idle, MockIndex>::new(primary);

        // Idle → Preparing
        let fsm = fsm.start_rebuild(100);
        assert_eq!(fsm.wal_lsn(), 100);

        // Preparing → Building
        let shadow = create_mock_index();
        shadow
            .insert(&crate::VectorId::new("vec-1"), &vec![1.0; 128])
            .unwrap();
        let fsm = fsm.set_shadow(shadow);

        // Building → Replaying
        let fsm = fsm.complete_build().unwrap();

        // Replaying → Validating
        let fsm = fsm.complete_replay();

        // Validating → Swapping
        let fsm = fsm.complete_validation();

        // Swapping → Cleaning
        let fsm = fsm.swap().unwrap();

        // Cleaning → Idle
        let (fsm, duration) = fsm.complete();
        // Duration might be 0 in fast tests, just verify it's not negative
        // (Duration is always non-negative by construction)
        assert!(duration.as_nanos() >= 0);

        // Back to Idle, can start new rebuild
        let _fsm = fsm.start_rebuild(200);
    }

    #[test]
    fn test_abort_from_preparing() {
        let primary = create_mock_index();
        let fsm = RebuildFsm::<Idle, MockIndex>::new(primary);
        let fsm = fsm.start_rebuild(100);
        let fsm = fsm.abort("test abort");
        // Back to Idle
        let _fsm = fsm.start_rebuild(200);
    }

    #[test]
    fn test_abort_from_building() {
        let primary = create_mock_index();
        let fsm = RebuildFsm::<Idle, MockIndex>::new(primary);
        let fsm = fsm.start_rebuild(100);
        let shadow = create_mock_index();
        let fsm = fsm.set_shadow(shadow);
        let fsm = fsm.abort("test abort");
        // Back to Idle
        let _fsm = fsm.start_rebuild(200);
    }

    #[test]
    fn test_data_loss_detection() {
        let primary = create_mock_index();

        // Insert vectors into primary
        for i in 0..10 {
            primary
                .insert(&crate::VectorId::new(format!("vec-{}", i)), &vec![1.0; 128])
                .unwrap();
        }

        let fsm = RebuildFsm::<Idle, MockIndex>::new(primary);
        let fsm = fsm.start_rebuild(100);

        // Create shadow with fewer vectors (simulating data loss)
        let shadow = create_mock_index();
        shadow
            .insert(&crate::VectorId::new("vec-0"), &vec![1.0; 128])
            .unwrap();

        let fsm = fsm.set_shadow(shadow);
        let fsm = fsm.complete_build().unwrap();
        let fsm = fsm.complete_replay();
        let fsm = fsm.complete_validation();

        // Swap should fail due to data loss
        let result = fsm.swap();
        assert!(result.is_err());
    }
}
