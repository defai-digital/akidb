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
pub use wal::WalContract;
