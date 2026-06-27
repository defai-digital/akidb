//! Vector index trait and types

use crate::{InternalId, Result, SearchResult, VectorId};
use std::sync::Arc;

/// Search parameters for vector queries
#[derive(Clone)]
pub struct SearchParams {
    /// Number of results to return
    pub top_k: usize,
    /// Number of probes for IVF index
    pub nprobe: u32,
    /// Optional filter function
    pub filter: Option<Arc<dyn Fn(&VectorId) -> bool + Send + Sync>>,
}

impl std::fmt::Debug for SearchParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchParams")
            .field("top_k", &self.top_k)
            .field("nprobe", &self.nprobe)
            .field("filter", &self.filter.is_some())
            .finish()
    }
}

impl Default for SearchParams {
    fn default() -> Self {
        Self {
            top_k: 10,
            nprobe: 32,
            filter: None,
        }
    }
}

impl SearchParams {
    /// Create new search parameters
    ///
    /// # Panics
    /// Panics if top_k is 0 (FIX BUG-092)
    ///
    /// # Note
    /// For fallible construction, use `try_new()` instead.
    pub fn new(top_k: usize) -> Self {
        // FIX BUG-092: Validate top_k to prevent empty result confusion
        assert!(top_k > 0, "top_k must be > 0, got 0");
        Self {
            top_k,
            ..Default::default()
        }
    }

    /// Create new search parameters with validation
    ///
    /// FIX BUG-HUNT-602: Fallible constructor that returns Result instead of panicking.
    /// Use this when top_k comes from untrusted input or when panicking is unacceptable.
    ///
    /// # Errors
    /// Returns `AkiDbError::InvalidParameter` if top_k is 0.
    pub fn try_new(top_k: usize) -> Result<Self> {
        if top_k == 0 {
            return Err(crate::AkiDbError::InvalidParameter(
                "top_k must be > 0, got 0".to_string(),
            ));
        }
        Ok(Self {
            top_k,
            ..Default::default()
        })
    }

    pub fn with_nprobe(mut self, nprobe: u32) -> Self {
        self.nprobe = nprobe;
        self
    }

    /// Attach a predicate over external `VectorId`s. Only candidates for which
    /// the predicate returns `true` are kept. Used to apply metadata filtering
    /// during search.
    pub fn with_filter(mut self, filter: Arc<dyn Fn(&VectorId) -> bool + Send + Sync>) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// Statistics about the index
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Total vectors in index (including tombstoned)
    pub total_vectors: u64,
    /// Active vectors (excluding tombstoned)
    pub active_vectors: u64,
    /// Tombstoned vectors
    pub tombstoned_vectors: u64,
    /// Index dimensions
    pub dimensions: usize,
    /// Memory usage in bytes
    pub memory_bytes: u64,
    /// GPU memory usage in bytes (if applicable)
    pub gpu_memory_bytes: Option<u64>,
    /// Whether GPU is being used
    pub using_gpu: bool,
    /// Whether rebuild is in progress
    pub rebuild_in_progress: bool,
}

/// Trait for vector index implementations
///
/// This trait abstracts over different FAISS backends (CPU, GPU) and allows
/// for mock implementations in tests.
#[allow(async_fn_in_trait)]
pub trait VectorIndex: Send + Sync {
    /// Insert a vector into the index
    ///
    /// Returns the internal ID assigned to this vector
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId>;

    /// Insert multiple vectors in batch
    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>>;

    /// Search for similar vectors
    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>>;

    /// Search for similar vectors in batch
    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>>;

    /// Mark a vector as deleted (tombstone)
    fn delete(&self, internal_id: InternalId) -> Result<()>;

    /// Check if a vector is tombstoned
    fn is_deleted(&self, internal_id: InternalId) -> bool;

    /// Get the vector by internal ID (for validation)
    fn get_vector(&self, internal_id: InternalId) -> Result<Option<Vec<f32>>>;

    /// Get index statistics
    fn stats(&self) -> IndexStats;

    /// Get the dimensions of vectors in this index
    fn dimensions(&self) -> usize;

    /// Check if the index is trained and ready for queries
    fn is_ready(&self) -> bool;

    /// Train the index (for IVF indexes)
    fn train(&self, training_data: &[f32]) -> Result<()>;

    /// Trigger a rebuild of the index
    fn trigger_rebuild(&self) -> Result<()>;

    /// Check if rebuild is in progress
    fn is_rebuilding(&self) -> bool;
}

/// Extension trait for async operations
#[allow(async_fn_in_trait)]
pub trait VectorIndexAsync: VectorIndex {
    /// Async search operation
    async fn search_async(&self, query: &[f32], params: &SearchParams)
        -> Result<Vec<SearchResult>>;

    /// Async batch search
    async fn search_batch_async(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_params_default() {
        let params = SearchParams::default();
        assert_eq!(params.top_k, 10);
        assert_eq!(params.nprobe, 32);
    }

    #[test]
    fn test_search_params_builder() {
        let params = SearchParams::new(50).with_nprobe(64);
        assert_eq!(params.top_k, 50);
        assert_eq!(params.nprobe, 64);
    }
}
