//! Index rebuild manager for zero-downtime index rebuilds
//!
//! This module implements the dual-index swap mechanism that allows
//! rebuilding the index without service interruption, with crash-safe
//! persistence and checkpoint recovery.
//!
//! ## Rebuild Process
//!
//! 1. **PRE-REBUILD**: Record WAL position, allocate shadow index
//! 2. **SCANNING**: Export vectors from current index (with checkpoints)
//! 3. **BUILDING**: Build new index from exported vectors
//! 4. **REPLAY**: Replay WAL entries since rebuild started
//! 5. **VALIDATE**: Validate shadow index with random samples
//! 6. **SWAP**: Atomic pointer swap
//! 7. **CLEANUP**: Deallocate old index, clear checkpoints
//!
//! ## Crash Recovery
//!
//! All state is persisted to RocksDB. On coordinator restart:
//! 1. Load in-progress rebuild states
//! 2. Resume from last checkpoint
//! 3. Continue from scanning/building phase
//!
//! ## Resource Awareness
//!
//! Rebuilds can be deferred or paused when:
//! - P95 latency exceeds threshold
//! - System is under high load
//! - Manual pause requested

pub mod checkpoint;
pub mod manager;
pub mod persistent_state;
pub mod typestate;

// Re-export from manager (original rebuild types)
pub use manager::{RebuildConfig, RebuildManager, RebuildProgress, RebuildState};

// Re-export persistent state types
pub use persistent_state::{
    InMemoryRebuildPersistence, PersistentRebuildPhase, PersistentRebuildStateMachine,
    RebuildCheckpoint, RebuildPersistentConfig, RebuildStatePersistence, RebuildStateRecord,
};

// Re-export checkpoint types
pub use checkpoint::{CheckpointConfig, CheckpointManager, ResourceAwareScheduler};

// Re-export typestate FSM (compile-time state machine)
pub use typestate::{
    Building, Cleaning, Idle, Preparing, RebuildFsm, Replaying, Swapping, Validating,
};
