//! AkiDB FAISS Wrapper - Vector index abstraction
//!
//! This crate provides a trait-based abstraction over FAISS vector indexing,
//! allowing for CPU, GPU, cuVS, and mock implementations.

pub mod index;
pub mod rebuild;
pub mod tombstone;

#[cfg(feature = "cpu")]
pub mod cpu;

#[cfg(feature = "gpu")]
pub mod ffi;

#[cfg(feature = "gpu")]
pub mod gpu;

// cuVS integration (Phase 4) - behind feature flag
#[cfg(feature = "cuvs")]
pub mod cuvs;

// Mock and cuVS mock are always available for testing
pub mod mock;

// cuVS module without actual FFI bindings (for testing/development)
#[cfg(not(feature = "cuvs"))]
pub mod cuvs;

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

// Re-export the appropriate index implementation based on features
#[cfg(feature = "gpu")]
pub use gpu::{GpuIndex, GpuIndexConfig};

#[cfg(all(feature = "cpu", not(feature = "gpu")))]
pub use cpu::CpuIndex;

// Mock is always available for testing
pub use mock::{MockIndex, MockIndexConfig};

// cuVS exports
pub use cuvs::{
    CuvsAlgorithm, CuvsConfig, CuvsGateResult, CuvsIndex, CuvsStats, RollbackManager,
    RollbackStatus, ShadowModeResult, ShadowModeValidator, ShadowValidationStats,
};

/// Re-export common types
pub use akidb_common::{AkiDbError, InternalId, Result, SearchResult, Vector, VectorId};
