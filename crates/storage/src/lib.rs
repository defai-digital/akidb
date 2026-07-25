//! AkiDB Storage - Persistence layer abstraction
//!
//! Provides storage backends for vector metadata, ID mappings, WAL,
//! snapshot storage for S3/MinIO integration, and tag indexing.

mod backend;
pub mod generation_bundle;
mod id_mapping;
pub mod serving_state;
pub mod snapshot;
mod tag_index;
mod wal;

pub use backend::{BatchOperation, RocksDbBackend, StorageBackend};
pub use generation_bundle::{
    consume_knowledge_bundle, consume_knowledge_bundle_with_limits, KnowledgeBundleReadError,
    KnowledgeBundleReadLimits, KnowledgeBundleSummary,
};
pub use id_mapping::{IdMapping, IdMappingEntry};
pub use serving_state::{
    ApplyMutationOutcome, GenerationServingState, LocalGenerationState, ServingStateError,
    ServingStateRecord, ServingStateStore, StageGenerationOutcome, SERVING_STATE_SCHEMA_VERSION,
};
pub use snapshot::{
    // Backend types
    LocalSnapshotBackend, S3SnapshotBackend, SnapshotBackend, SnapshotFile, SnapshotManager,
    SnapshotMetadata,
    // State machine types
    SnapshotState, SnapshotStateMachine, SnapshotStateRecord, UploadCheckpoint,
    // Resumable upload types
    CompletedPart, ResumableUploadConfig, ResumableUploader, SnapshotUploadExecutor,
    // Cleanup types
    cleanup_orphaned_uploads, CleanupConfig, CleanupResult, SnapshotCleanup,
};
pub use tag_index::{TagCondition, TagFilter, TagIndex, TagIndexStats, TagOperator};
pub use wal::{WalEntry, WriteAheadLog};

pub use akidb_common::{AkiDbError, Result};
