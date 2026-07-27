//! AkiDB Storage - Persistence layer abstraction
//!
//! Provides storage backends for vector metadata, ID mappings, WAL,
//! snapshot storage for S3/MinIO integration, and tag indexing.

mod backend;
pub mod generation_bundle;
pub mod generation_layout;
mod id_mapping;
pub mod memory;
pub mod serving_state;
pub mod snapshot;
mod tag_index;
mod wal;

pub use backend::{BatchOperation, RocksDbBackend, StorageBackend};
pub use generation_bundle::{
    consume_knowledge_bundle, consume_knowledge_bundle_with_limits, KnowledgeBundleReadError,
    KnowledgeBundleReadLimits, KnowledgeBundleSummary,
};
pub use generation_layout::{
    BundleInstallOutcome, GenerationBuildJournal, GenerationBuildPhase, GenerationGcEntry,
    GenerationGcEvidence, GenerationLayoutError, GenerationPointer, GenerationPointerSet,
    GenerationPrepareOutcome, GenerationRevisionMarker, GenerationStore, MaterializationEvidence,
    PreparedGeneration, PreparedGenerationRevision, ReadyGeneration, ReadyGenerationMarker,
    ReplicaVolumeClaimOutcome, ReplicaVolumeOwner, GENERATION_LAYOUT_SCHEMA_VERSION,
};
pub use id_mapping::{IdMapping, IdMappingEntry};
pub use memory::{
    ActiveProjectionSet, ClaimedMemoryCompilerJob, CommitMemoryOutcome, CommitMemoryReceipt,
    CommitMemoryRequest, CommitProposalRequest, ExecuteMemoryDeletionReceipt,
    ExecuteMemoryDeletionRequest, ForgetMemoryRequest, MemoryAccessGrant, MemoryAccessIssuer,
    MemoryAccessProof, MemoryAccessVerifier, MemoryDerivationInput, MemoryEvidenceInput,
    MemoryExportRecord, MemoryHistoryView, MemoryLedger, MemoryLedgerError, MemoryRecallSnapshot,
    MemoryRecallSnapshotDraft, MemoryVersionView, ObserveMemoryReceipt, ObserveMemoryRequest,
    PlanMemoryDeletionRequest, ProjectionApplyOutcome, ProjectionDataOperation,
    ReinforceMemoryRequest,
};
pub use serving_state::{
    ApplyMutationOutcome, GenerationServingState, LocalGenerationState, ServingStateError,
    ServingStateRecord, ServingStateStore, StageGenerationOutcome, SERVING_STATE_SCHEMA_VERSION,
};
pub use snapshot::{
    // Cleanup types
    cleanup_orphaned_uploads,
    CleanupConfig,
    CleanupResult,
    // Resumable upload types
    CompletedPart,
    // Backend types
    LocalSnapshotBackend,
    ResumableUploadConfig,
    ResumableUploader,
    S3SnapshotBackend,
    SnapshotBackend,
    SnapshotCleanup,
    SnapshotFile,
    SnapshotManager,
    SnapshotMetadata,
    // State machine types
    SnapshotState,
    SnapshotStateMachine,
    SnapshotStateRecord,
    SnapshotUploadExecutor,
    UploadCheckpoint,
};
pub use tag_index::{TagCondition, TagFilter, TagIndex, TagIndexStats, TagOperator};
pub use wal::{WalEntry, WriteAheadLog};

pub use akidb_common::{AkiDbError, Result};
