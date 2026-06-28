//! Mock vector index for testing
//!
//! This implementation uses in-memory storage and brute-force search,
//! suitable for unit tests on machines without GPU.

use crate::{
    index::{IndexStats, SearchParams, VectorIndex},
    tombstone::TombstoneBitset,
    validate_finite_vector_values, AkiDbError, InternalId, Result, SearchResult, VectorId,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tracing::warn;

/// Configuration for MockIndex
#[derive(Debug, Clone)]
pub struct MockIndexConfig {
    /// Vector dimensions
    pub dimensions: usize,
    /// Initial capacity for vectors
    pub capacity: u64,
}

impl Default for MockIndexConfig {
    fn default() -> Self {
        Self {
            dimensions: 768,
            capacity: 100_000,
        }
    }
}

impl MockIndexConfig {
    /// Create a new config with specified dimensions
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            ..Default::default()
        }
    }

    /// Set capacity
    pub fn with_capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity;
        self
    }
}

/// Mock vector index for testing
pub struct MockIndex {
    /// Vector storage: internal_id -> (external_id, vector)
    vectors: RwLock<HashMap<i64, (VectorId, Vec<f32>)>>,
    /// External to internal ID mapping
    id_mapping: RwLock<HashMap<String, i64>>,
    /// Tombstone bitset
    tombstones: TombstoneBitset,
    /// Next internal ID
    next_id: AtomicI64,
    /// Vector dimensions
    dimensions: usize,
    /// Is trained/ready
    is_trained: AtomicBool,
    /// Is rebuilding
    is_rebuilding: AtomicBool,
}

impl MockIndex {
    /// Create a new mock index
    ///
    /// # Panics
    /// Panics if dimensions is 0 (FIX BUG-072)
    pub fn new(dimensions: usize, capacity: u64) -> Self {
        // FIX BUG-072: Validate dimensions to prevent downstream issues
        // Zero dimensions would cause division by zero in normalization and
        // hide bugs in code that relies on valid vector dimensions
        assert!(dimensions > 0, "MockIndex dimensions must be > 0, got 0");

        Self {
            vectors: RwLock::new(HashMap::new()),
            id_mapping: RwLock::new(HashMap::new()),
            tombstones: TombstoneBitset::new(capacity),
            next_id: AtomicI64::new(0),
            dimensions,
            is_trained: AtomicBool::new(true), // Mock is always ready
            is_rebuilding: AtomicBool::new(false),
        }
    }

    /// Create from config
    pub fn from_config(config: MockIndexConfig) -> Self {
        Self::new(config.dimensions, config.capacity)
    }

    /// Compute cosine similarity between two vectors
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    fn ensure_tombstone_capacity_for(&self, required_capacity: u64) -> Result<()> {
        if self.tombstones.capacity() < required_capacity {
            self.tombstones.resize(required_capacity)?;
        }
        Ok(())
    }
}

impl VectorIndex for MockIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        if vector.len() != self.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }
        validate_finite_vector_values(vector, "Insert")?;

        // Check if ID already exists
        let mut id_mapping = self.id_mapping.write();
        if let Some(&existing_internal_id) = id_mapping.get(id.as_str()) {
            // Upsert: update existing vector
            let mut vectors = self.vectors.write();
            vectors.insert(existing_internal_id, (id.clone(), vector.to_vec()));
            // FIX BUG-024: Clear tombstone if it was deleted
            // This ensures re-inserted vectors become visible in search results
            // FIX BUG-HUNT-203: Log warning instead of silently ignoring errors.
            // If tombstone clear fails, the vector remains invisible despite insert appearing successful.
            if let Err(e) = self
                .tombstones
                .clear_deleted(InternalId(existing_internal_id))
            {
                warn!(
                    internal_id = existing_internal_id,
                    error = %e,
                    "Failed to clear tombstone during upsert - vector may remain invisible"
                );
            }
            return Ok(InternalId(existing_internal_id));
        }

        // New vector
        // FIX BUG-HUNT-005: Use SeqCst instead of Relaxed for proper ordering on
        // weaker memory model architectures, including Apple Silicon ARM64.
        let internal_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.ensure_tombstone_capacity_for(internal_id as u64 + 1)?;

        id_mapping.insert(id.as_str().to_string(), internal_id);
        drop(id_mapping);

        let mut vectors = self.vectors.write();
        vectors.insert(internal_id, (id.clone(), vector.to_vec()));

        Ok(InternalId(internal_id))
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        vectors
            .iter()
            .map(|(id, vec)| self.insert(id, vec))
            .collect()
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        if query.len() != self.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.dimensions,
                actual: query.len(),
            });
        }
        validate_finite_vector_values(query, "Search")?;

        let vectors = self.vectors.read();
        // FIX BUG-062: Removed unused id_mapping lock (was: let id_mapping = self.id_mapping.read();)

        // Compute similarities for all non-deleted vectors
        let mut results: Vec<(VectorId, f32)> = vectors
            .iter()
            .filter(|(&internal_id, _)| !self.tombstones.is_deleted(InternalId(internal_id)))
            .map(|(_, (ext_id, vec))| {
                let score = Self::cosine_similarity(query, vec);
                (ext_id.clone(), score)
            })
            .collect();

        // Apply filter if provided
        if let Some(ref filter) = params.filter {
            results.retain(|(id, _)| filter(id));
        }

        // Sort by score descending with tie-breaking by ID (ascending) for deterministic ordering
        results.sort_by(|a, b| match b.1.partial_cmp(&a.1) {
            Some(std::cmp::Ordering::Equal) | None => a.0.as_str().cmp(b.0.as_str()),
            Some(ord) => ord,
        });

        // Take top_k
        let top_results: Vec<SearchResult> = results
            .into_iter()
            .take(params.top_k)
            .map(|(id, score)| SearchResult::new(id, score))
            .collect();

        Ok(top_results)
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        queries.iter().map(|q| self.search(q, params)).collect()
    }

    fn delete(&self, internal_id: InternalId) -> Result<()> {
        self.tombstones.mark_deleted(internal_id)
    }

    fn is_deleted(&self, internal_id: InternalId) -> bool {
        self.tombstones.is_deleted(internal_id)
    }

    fn get_vector(&self, internal_id: InternalId) -> Result<Option<Vec<f32>>> {
        if self.tombstones.is_deleted(internal_id) {
            return Ok(None);
        }

        let vectors = self.vectors.read();
        Ok(vectors.get(&internal_id.0).map(|(_, v)| v.clone()))
    }

    fn stats(&self) -> IndexStats {
        let vectors = self.vectors.read();
        let total = vectors.len() as u64;
        let deleted = self.tombstones.deleted_count();

        IndexStats {
            total_vectors: total,
            active_vectors: total.saturating_sub(deleted),
            tombstoned_vectors: deleted,
            dimensions: self.dimensions,
            memory_bytes: total * (self.dimensions * 4 + 32) as u64, // Estimate
            gpu_memory_bytes: None,
            using_gpu: false,
            rebuild_in_progress: self.is_rebuilding.load(Ordering::SeqCst),
        }
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn is_ready(&self) -> bool {
        self.is_trained.load(Ordering::SeqCst)
    }

    fn train(&self, _training_data: &[f32]) -> Result<()> {
        // Mock index doesn't need training
        self.is_trained.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn trigger_rebuild(&self) -> Result<()> {
        self.is_rebuilding.store(true, Ordering::SeqCst);
        // In mock, just reset tombstones
        self.tombstones.reset();
        self.is_rebuilding.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_rebuilding(&self) -> bool {
        self.is_rebuilding.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_random_vector(dim: usize) -> Vec<f32> {
        (0..dim).map(|i| (i as f32).sin()).collect()
    }

    #[test]
    fn test_mock_insert_search() {
        let index = MockIndex::new(128, 1000);

        let v1 = create_random_vector(128);
        let v2 = create_random_vector(128);

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        let results = index.search(&v1, &SearchParams::new(10)).unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].id.as_str(), "vec-1");
        assert!(results[0].score > 0.99); // Should find itself
    }

    #[test]
    fn test_mock_delete() {
        let index = MockIndex::new(128, 1000);

        let v1 = create_random_vector(128);
        let id1 = index.insert(&VectorId::new("vec-1"), &v1).unwrap();

        // Should find before delete
        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert_eq!(results.len(), 1);

        // Delete
        index.delete(id1).unwrap();

        // Should not find after delete
        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_mock_delete_after_capacity_growth() {
        let index = MockIndex::new(128, 1);

        let v1 = create_random_vector(128);
        let v2: Vec<f32> = create_random_vector(128)
            .into_iter()
            .map(|value| value * 0.5)
            .collect();

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        let grown_id = index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        index
            .delete(grown_id)
            .expect("delete should work after mock capacity growth");

        let results = index.search(&v2, &SearchParams::new(10)).unwrap();
        assert!(
            results.iter().all(|result| result.id.as_str() != "vec-2"),
            "deleted vector from grown capacity should not be searchable"
        );
    }

    #[test]
    fn test_mock_dimension_mismatch() {
        let index = MockIndex::new(128, 1000);

        let wrong_dim = vec![1.0; 64];
        let result = index.insert(&VectorId::new("vec-1"), &wrong_dim);

        assert!(matches!(result, Err(AkiDbError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_mock_search_rejects_non_finite_query_values() {
        let index = MockIndex::new(2, 1000);
        index.insert(&VectorId::new("vec-1"), &[1.0, 0.0]).unwrap();

        let result = index.search(&[f32::NAN, 0.0], &SearchParams::new(1));

        assert!(matches!(result, Err(AkiDbError::InvalidParameter(_))));
    }

    #[test]
    fn test_mock_insert_rejects_non_finite_vector_values() {
        let index = MockIndex::new(2, 1000);

        let result = index.insert(&VectorId::new("vec-1"), &[1.0, f32::INFINITY]);

        assert!(matches!(result, Err(AkiDbError::InvalidParameter(_))));
        assert!(index
            .search(&[1.0, 0.0], &SearchParams::new(10))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_mock_stats() {
        let index = MockIndex::new(128, 1000);

        for i in 0..100 {
            let v = create_random_vector(128);
            index
                .insert(&VectorId::new(format!("vec-{}", i)), &v)
                .unwrap();
        }

        let stats = index.stats();
        assert_eq!(stats.total_vectors, 100);
        assert_eq!(stats.active_vectors, 100);
        assert!(!stats.using_gpu);
    }
}
