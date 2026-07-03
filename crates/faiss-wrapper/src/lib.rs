//! AkiDB Vector Index - HNSW-based vector index abstraction
//!
//! This crate provides a trait-based vector index abstraction with a real
//! HNSW implementation via usearch and a mock implementation for tests.

mod hnsw;
mod index;
pub mod rebuild;
mod tombstone;

// Mock is available for testing
mod mock;

pub use hnsw::{HnswConfig, HnswIndex};
pub use index::{IndexStats, SearchFilter, SearchParams, VectorIndex, VectorIndexAsync};
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

pub(crate) fn allocate_internal_id(next_id: &std::sync::atomic::AtomicI64) -> Result<i64> {
    next_id
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |current| current.checked_add(1).filter(|next| *next >= 0),
        )
        .map_err(|_| AkiDbError::InvalidParameter("internal vector id space exhausted".to_string()))
}

pub(crate) fn validate_finite_vector_values(vector: &[f32], operation: &str) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(AkiDbError::InvalidParameter(format!(
            "{operation} vector contains NaN or Infinity values"
        )));
    }
    Ok(())
}
