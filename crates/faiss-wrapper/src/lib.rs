//! AkiDB FAISS Wrapper - Vector index abstraction
//!
//! This crate provides a trait-based abstraction over FAISS vector indexing,
//! allowing for CPU and mock implementations on macOS Apple Silicon.

pub mod index;
pub mod rebuild;
pub mod tombstone;

#[cfg(feature = "cpu")]
pub mod cpu;

// Mock is always available for testing
pub mod mock;

pub use index::{IndexStats, SearchParams, VectorIndex};
pub use rebuild::{
    // Original rebuild types
    RebuildConfig, RebuildManager, RebuildProgress, RebuildState,
    // Persistent state types
    InMemoryRebuildPersistence, PersistentRebuildPhase, PersistentRebuildStateMachine,
    RebuildCheckpoint, RebuildPersistentConfig, RebuildStatePersistence, RebuildStateRecord,
    // Checkpoint types
    CheckpointConfig, CheckpointManager, ResourceAwareScheduler,
};
pub use tombstone::TombstoneBitset;

// Re-export the supported index implementation based on features.
#[cfg(all(feature = "cpu", not(feature = "gpu")))]
pub use cpu::CpuIndex;

// Mock is always available for testing
pub use mock::{MockIndex, MockIndexConfig};

/// Re-export common types
pub use akidb_common::{AkiDbError, InternalId, Result, SearchResult, Vector, VectorId};
