//! AkiDB Vector Index - HNSW-based vector index abstraction
//!
//! This crate provides a trait-based vector index abstraction with a real
//! HNSW implementation via usearch and a mock implementation for tests.

pub mod hnsw;
pub mod index;
pub mod rebuild;
pub mod tombstone;

// Mock is available for testing
pub mod mock;

pub use hnsw::{HnswConfig, HnswIndex};
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

// Mock is available for testing
pub use mock::{MockIndex, MockIndexConfig};

/// Re-export common types
pub use akidb_common::{AkiDbError, InternalId, Result, SearchResult, Vector, VectorId};
