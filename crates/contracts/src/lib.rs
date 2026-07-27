//! AkiDB Contracts - Boundary validation and type-safe newtypes
//!
//! This crate provides:
//! - Explicit validation contracts at system boundaries
//! - Type-safe newtypes that encode invariants at compile time
//! - Contract violation error types
//!
//! # Philosophy
//!
//! Contracts are enforced at system boundaries (gRPC, WAL, storage) to catch
//! invalid data early. Inside the system, we rely on Rust's type system and
//! newtypes to maintain invariants without runtime cost.
//!
//! # Example
//!
//! ```rust
//! use akidb_contracts::{WalContract, CollectionKey, ContractViolation};
//!
//! // Validate at boundary
//! let vector = vec![1.0f32; 100];
//! assert!(WalContract::validate_vector(&vector).is_ok());
//!
//! // Use newtype for guaranteed-correct keys
//! let key = CollectionKey::new("my_collection", "vec-123");
//! // key.as_bytes() is guaranteed to be properly encoded
//! ```

pub mod collection_key;
pub mod error;
pub mod knowledge;
pub mod knowledge_bundle;
pub mod memory;
pub mod memory_ax;
pub mod memory_compiler;
pub mod memory_consolidation;
pub mod memory_evidence_graph;
pub mod memory_trajectory;
pub mod wal;

pub use collection_key::CollectionKey;
pub use error::{ContractViolation, ContractViolationKind};
pub use knowledge::{
    ImmutableObjectReference, KnowledgeBundleCompression, KnowledgeBundleFormat,
    KnowledgeGenerationManifest, KnowledgeMutation, KnowledgeOperation, KnowledgeScope,
    ReplicaCheckpoint, ReplicaState, KNOWLEDGE_SCHEMA_VERSION, MAX_SAFE_JSON_INTEGER,
};
pub use knowledge_bundle::{
    KnowledgeAssertionState, KnowledgeBundleEdge, KnowledgeBundleEntry, KnowledgeBundleHeader,
    KnowledgeBundleNode, KnowledgeBundleRecord, KnowledgeEdgeKind, KnowledgeMutationPayload,
    KnowledgeNodeKind,
};
pub use memory::{
    canonical_content_sha256, memory_deletion_plan_sha256, DecisionAuthority, DerivationRecord,
    EpistemicFormation, EvidenceRecord, IdempotencyRecord, MemoryAssertion,
    MemoryAssertionIdentity, MemoryContent, MemoryDeletionExecution, MemoryDeletionPlan,
    MemoryDeletionSelector, MemoryDeletionTargetKind, MemoryDeletionTombstone, MemoryHead,
    MemoryKind, MemoryMutation, MemoryObservation, MemoryOperation, MemoryReinforcement,
    MemoryReinforcementOutcome, MemoryRelation, MemoryRelationKind, MemoryScope,
    MemoryTemporalQuery, MemoryVersion, PolicyDecisionOutcome, PolicyDecisionRecord,
    ProjectionCheckpoint, ProjectionOutboxEntry, ProjectionSetManifest, ProjectionStatus,
    Sensitivity, SourceAssurance, VersionLifecycle, VersionState, VisibilityReceipt,
    MAX_MEMORY_ACTIVE_HEADS, MAX_MEMORY_DELETION_TARGETS, MAX_MEMORY_EVIDENCE, MAX_MEMORY_ID_BYTES,
    MAX_MEMORY_SCOPE_BYTES, MAX_MEMORY_TEXT_BYTES, MEMORY_IDENTITY_HASH_VERSION,
    MEMORY_SCHEMA_VERSION,
};
pub use memory_ax::{
    ax_fabric_candidate_batch_sha256, ax_studio_debug_record_sha256,
    ax_studio_timeline_entry_sha256, ax_studio_timeline_page_sha256, compiler_input_sha256,
    AxFabricMemoryCandidateBatch, AxMemoryContractError, AxMemoryContractResult,
    AxStudioMemoryDebugRecord, AxStudioMemoryTimelineEntry, AxStudioMemoryTimelinePage,
    AX_MEMORY_EXCHANGE_CONTRACT_VERSION, MAX_AX_STUDIO_DEBUG_ITEMS, MAX_AX_STUDIO_TIMELINE_ENTRIES,
};
pub use memory_compiler::{
    memory_compiler_job_sha256, verify_compiler_conformance, CompilerCandidate, CompilerHead,
    CompilerObservation, CompilerProposalRelation, CompilerProposalRelationKind, MemoryCommitPlan,
    MemoryCompiler, MemoryCompilerError, MemoryCompilerInput, MemoryCompilerJob,
    MemoryCompilerJobFailure, MemoryCompilerJobState, MemoryCompilerJobStatus,
    MemoryCompilerResult, ReferenceTextCompiler, MAX_MEMORY_COMPILER_JOB_ATTEMPTS,
    MEMORY_COMPILER_CONTRACT_VERSION, REFERENCE_TEXT_COMPILER_ARTIFACT_ID,
};
pub use memory_consolidation::{
    verify_consolidation_conformance, ConsolidationAction, ConsolidationVersion,
    MemoryConsolidationError, MemoryConsolidationExecutor, MemoryConsolidationInput,
    MemoryConsolidationPlan, MemoryConsolidationResult, ReferenceConsolidationExecutor,
    MEMORY_CONSOLIDATION_CONTRACT_VERSION, REFERENCE_CONSOLIDATION_ARTIFACT_ID,
};
pub use memory_evidence_graph::{
    evidence_graph_input_sha256, evidence_graph_traversal_sha256,
    verify_evidence_graph_conformance, EvidenceGraphEdge, EvidenceGraphEdgeKind, EvidenceGraphNode,
    EvidenceGraphNodeKind, MemoryEvidenceGraphBounds, MemoryEvidenceGraphError,
    MemoryEvidenceGraphInput, MemoryEvidenceGraphResult, MemoryEvidenceGraphTraversal,
    ReferenceEvidenceGraphProjection, MAX_EVIDENCE_GRAPH_DEPTH, MAX_EVIDENCE_GRAPH_INPUT_EDGES,
    MAX_EVIDENCE_GRAPH_INPUT_NODES, MAX_EVIDENCE_GRAPH_RESULT_NODES, MAX_EVIDENCE_GRAPH_ROOTS,
    MEMORY_EVIDENCE_GRAPH_CONTRACT_VERSION, REFERENCE_EVIDENCE_GRAPH_ARTIFACT_ID,
};
pub use memory_trajectory::{
    verify_trajectory_conformance, MemoryTrajectoryCompiler, MemoryTrajectoryError,
    MemoryTrajectoryInput, MemoryTrajectoryPlan, MemoryTrajectoryResult,
    ReferenceTrajectoryCompiler, TrajectoryEvent, TrajectoryEventOutcome,
    TrajectoryProcedureCandidate, MEMORY_TRAJECTORY_CONTRACT_VERSION,
    REFERENCE_TRAJECTORY_COMPILER_ARTIFACT_ID,
};
pub use wal::WalContract;
