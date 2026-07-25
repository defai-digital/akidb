//! Common types for AkiDB
//!
//! This module contains all shared types used across AkiDB components:
//! - Core vector and search types
//! - Tag types for document metadata
//! - Document identifier types
//! - Lifecycle management types

pub mod document;
pub mod lifecycle;
pub mod tag;

// Re-export all types for convenient access
pub use document::{DocumentIdentifier, VectorMetadata};
pub use lifecycle::{
    ChangeType, DeleteState, ObjectManifest, SyncResult, DEFAULT_HARD_DELETE_DELAY_DAYS,
    DELETION_THRESHOLD,
};
pub use tag::{
    TagValidationError, TagValue, Tags, MAX_TAGS, MAX_TAG_KEY_LEN, MAX_TAG_VALUE_LEN,
};

// ============================================================================
// Core vector types (originally in types.rs)
// ============================================================================

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// External vector ID (user-facing)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorId(pub String);

impl VectorId {
    /// Create a new VectorId
    ///
    /// Note: For user input, prefer `try_new()` which validates the ID.
    /// This method accepts any string including empty strings for backwards compatibility.
    ///
    /// FIX BUG-H017: Added debug assertion to catch empty IDs during development
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        debug_assert!(!id.is_empty(), "VectorId should not be empty - use try_new() for validated construction");
        Self(id)
    }

    /// FIX BUG-H017: Create a validated VectorId
    ///
    /// Returns None if the ID is empty or exceeds the maximum length of 1024 bytes.
    /// Use this when accepting user input.
    pub fn try_new(id: impl Into<String>) -> Option<Self> {
        let id = id.into();
        if id.is_empty() || id.len() > 1024 {
            None
        } else {
            Some(Self(id))
        }
    }

    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Check if the VectorId is valid (non-empty and within length limit)
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty() && self.0.len() <= 1024
    }
}

impl std::fmt::Display for VectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for VectorId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for VectorId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Internal FAISS index ID
///
/// Note: FAISS uses i64 for IDs, but valid IDs should be non-negative.
/// Use `try_new()` or `as_index()` for safe validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InternalId(pub i64);

impl InternalId {
    /// Create a new InternalId without validation
    /// Prefer `try_new()` when the source is untrusted
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    /// FIX BUG-060: Create a validated InternalId, rejecting negative values
    /// Returns None if the ID is negative
    pub fn try_new(id: i64) -> Option<Self> {
        if id >= 0 {
            Some(Self(id))
        } else {
            None
        }
    }

    /// FIX BUG-060: Safely convert to usize for array indexing
    /// Returns None if the ID is negative or exceeds usize::MAX
    pub fn as_index(&self) -> Option<usize> {
        if self.0 >= 0 {
            usize::try_from(self.0).ok()
        } else {
            None
        }
    }

    /// Check if this ID is valid (non-negative)
    pub fn is_valid(&self) -> bool {
        self.0 >= 0
    }

    /// Check if this ID fits in a u32 for RoaringBitmap usage
    /// Returns true if 0 <= id <= u32::MAX
    pub fn fits_in_u32(&self) -> bool {
        self.0 >= 0 && self.0 <= u32::MAX as i64
    }

    /// Convert to u32 for bitmap operations, returning None if out of range
    pub fn as_u32(&self) -> Option<u32> {
        if self.fits_in_u32() {
            Some(self.0 as u32)
        } else {
            None
        }
    }
}

/// Vector with its embedding data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vector {
    pub id: VectorId,
    pub embedding: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

impl Vector {
    pub fn new(id: impl Into<VectorId>, embedding: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            embedding,
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn dimensions(&self) -> usize {
        self.embedding.len()
    }
}

/// Search result from vector similarity search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: VectorId,
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}

impl SearchResult {
    pub fn new(id: VectorId, score: f32) -> Self {
        Self {
            id,
            score,
            metadata: None,
        }
    }
}

/// Delete operation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteStatus {
    /// Vector existed and was deleted
    Deleted,
    /// Vector ID did not exist (no-op, success)
    NotFound,
    /// Vector was already deleted (no-op, success)
    AlreadyDeleted,
}

/// Update operation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateStatus {
    /// Existing vector was updated
    Updated,
    /// New vector was created (upsert)
    Created,
}

/// Visibility guarantee for operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisibilityInfo {
    pub delete_visibility: VisibilityGuarantee,
    pub insert_visibility: VisibilityGuarantee,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VisibilityGuarantee {
    /// Visible immediately in same request
    Immediate,
    /// Visible within specified milliseconds
    WithinMs(u64),
}

impl Default for VisibilityInfo {
    fn default() -> Self {
        Self {
            delete_visibility: VisibilityGuarantee::Immediate,
            insert_visibility: VisibilityGuarantee::WithinMs(100),
        }
    }
}

/// SLO compliance information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloInfo {
    pub latency_us: u64,
    pub within_slo: bool,
    pub degraded_mode: bool,
    pub warning: Option<String>,
}

/// Collection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: String,
    pub dimensions: usize,
    pub metric: DistanceMetric,
    pub index_config: IndexConfig,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    #[default]
    Cosine,
    L2,
    InnerProduct,
}

/// Index configuration for FAISS IVF-Flat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Number of clusters (nlist)
    pub nlist: u32,
    /// Number of probes for search (nprobe)
    pub nprobe: u32,
    /// Use GPU acceleration. Unsupported in the active CPU-portable build.
    pub use_gpu: bool,
    /// GPU memory fraction (0.0 - 1.0)
    pub gpu_memory_fraction: f32,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            nlist: 4096,
            nprobe: 32,
            use_gpu: false,
            gpu_memory_fraction: 0.6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_id_generation() {
        let id1 = VectorId::generate();
        let id2 = VectorId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_vector_dimensions() {
        let v = Vector::new("test", vec![1.0, 2.0, 3.0]);
        assert_eq!(v.dimensions(), 3);
    }
}
