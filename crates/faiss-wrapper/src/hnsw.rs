//! HNSW vector index implementation using usearch
//!
//! This module provides a real HNSW (Hierarchical Navigable Small World) index
//! via the `usearch` crate. It is the sole vector index backend for AkiDB on
//! Mac Apple Silicon.

use crate::{
    index::{IndexStats, SearchParams, VectorIndex},
    tombstone::TombstoneBitset,
    validate_finite_vector_values, AkiDbError, InternalId, Result, SearchResult, VectorId,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tracing::{debug, warn};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

/// Configuration for HNSW index
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Vector dimensions
    pub dimensions: usize,
    /// Initial capacity for vectors
    pub capacity: usize,
    /// HNSW M parameter (max connections per layer, default 16)
    pub m: usize,
    /// ef_construction parameter (default 128)
    pub ef_construction: usize,
    /// ef_search parameter (default 64)
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dimensions: 768,
            capacity: 1_000_000,
            m: 16,
            ef_construction: 128,
            ef_search: 64,
        }
    }
}

impl HnswConfig {
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            ..Default::default()
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }

    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }

    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }
}

/// HNSW vector index backed by usearch
pub struct HnswIndex {
    /// The underlying usearch HNSW index
    index: Index,
    /// External ID mapping: external_id -> internal_id
    id_mapping: RwLock<HashMap<String, i64>>,
    /// Reverse mapping: internal_id -> external_id
    reverse_mapping: RwLock<HashMap<i64, VectorId>>,
    /// Tombstone bitset for soft deletes
    tombstones: TombstoneBitset,
    /// Next internal ID counter
    next_id: AtomicI64,
    /// Vector dimensions
    dimensions: usize,
    /// Index is ready
    is_ready: AtomicBool,
    /// Rebuild in progress
    is_rebuilding: AtomicBool,
    /// ef_search parameter
    ef_search: usize,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(config: HnswConfig) -> Result<Self> {
        if config.dimensions == 0 {
            return Err(AkiDbError::InvalidParameter(
                "HNSW dimensions must be > 0".to_string(),
            ));
        }

        let mut options = IndexOptions::default();
        options.dimensions = config.dimensions;
        options.metric = MetricKind::Cos;
        options.quantization = ScalarKind::F32;
        options.connectivity = config.m;
        options.expansion_add = config.ef_construction;
        options.expansion_search = config.ef_search;

        let index = Index::new(&options).map_err(|e| {
            AkiDbError::InvalidParameter(format!("Failed to create HNSW index: {}", e))
        })?;

        index.reserve(config.capacity).map_err(|e| {
            AkiDbError::InvalidParameter(format!("Failed to reserve HNSW capacity: {}", e))
        })?;

        debug!(
            dimensions = config.dimensions,
            capacity = config.capacity,
            m = config.m,
            ef_construction = config.ef_construction,
            ef_search = config.ef_search,
            "HNSW index created"
        );

        Ok(Self {
            index,
            id_mapping: RwLock::new(HashMap::new()),
            reverse_mapping: RwLock::new(HashMap::new()),
            tombstones: TombstoneBitset::new(config.capacity as u64),
            next_id: AtomicI64::new(0),
            dimensions: config.dimensions,
            is_ready: AtomicBool::new(true),
            is_rebuilding: AtomicBool::new(false),
            ef_search: config.ef_search,
        })
    }

    fn ensure_capacity_for(&self, required_capacity: usize) {
        if required_capacity <= self.index.capacity() {
            if self.tombstones.capacity() < required_capacity as u64 {
                if let Err(e) = self.tombstones.resize(required_capacity as u64) {
                    warn!(error = %e, "Failed to grow HNSW tombstone capacity");
                }
            }
            return;
        }

        let current_capacity = self.index.capacity();
        let new_capacity = (current_capacity * 2).max(required_capacity + 1024);
        if let Err(e) = self.index.reserve(new_capacity) {
            warn!(error = %e, "Failed to grow HNSW index capacity");
            return;
        }
        if let Err(e) = self.tombstones.resize(new_capacity as u64) {
            warn!(error = %e, "Failed to grow HNSW tombstone capacity");
        }
    }
}

impl VectorIndex for HnswIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        if vector.len() != self.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }
        validate_finite_vector_values(vector, "Insert")?;

        // Check for existing vector (upsert)
        let mut id_map = self.id_mapping.write();
        if let Some(&existing_id) = id_map.get(id.as_str()) {
            // Upsert: remove old and re-insert
            let _ = self.index.remove(existing_id as u64);
            if let Err(e) = self.tombstones.clear_deleted(InternalId(existing_id)) {
                warn!(
                    internal_id = existing_id,
                    error = %e,
                    "Failed to clear tombstone during upsert"
                );
            }
            self.index
                .add(existing_id as u64, vector)
                .map_err(|e| AkiDbError::InvalidParameter(format!("HNSW upsert failed: {}", e)))?;
            let mut reverse = self.reverse_mapping.write();
            reverse.insert(existing_id, id.clone());
            return Ok(InternalId(existing_id));
        }

        // New vector
        let internal_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.ensure_capacity_for(internal_id as usize + 1);

        self.index
            .add(internal_id as u64, vector)
            .map_err(|e| AkiDbError::InvalidParameter(format!("HNSW insert failed: {}", e)))?;

        id_map.insert(id.as_str().to_string(), internal_id);
        drop(id_map);

        let mut reverse = self.reverse_mapping.write();
        reverse.insert(internal_id, id.clone());

        Ok(InternalId(internal_id))
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        // Pre-reserve capacity for the batch
        let needed = self.index.size() + vectors.len();
        self.ensure_capacity_for(needed);

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

        // Request extra candidates to account for tombstoned vectors. Filters
        // are evaluated after usearch returns candidates, so filtered searches
        // must scan the available result set before applying the final top_k.
        let tombstoned = self.tombstones.deleted_count() as usize;
        let search_count = if params.filter.is_some() {
            self.index.size().max(params.top_k)
        } else {
            params.top_k + tombstoned + params.top_k / 2
        };

        // Update ef_search if nprobe differs from default
        if params.nprobe as usize != self.ef_search {
            self.index.change_expansion_search(params.nprobe as usize);
        }

        let matches = self
            .index
            .search::<f32>(query, search_count)
            .map_err(|e| AkiDbError::InvalidParameter(format!("HNSW search failed: {}", e)))?;

        let reverse = self.reverse_mapping.read();
        let mut results: Vec<SearchResult> = Vec::with_capacity(params.top_k);

        for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            let internal_id = *key as i64;

            // Skip tombstoned vectors
            if self.tombstones.is_deleted(InternalId(internal_id)) {
                continue;
            }

            // Look up external ID
            if let Some(ext_id) = reverse.get(&internal_id) {
                // Apply filter if provided
                if let Some(ref filter) = params.filter {
                    if !filter(ext_id) {
                        continue;
                    }
                }

                // usearch returns distance; for cosine metric, convert to similarity score
                // Cosine distance = 1 - cosine_similarity, so score = 1 - distance
                let score = 1.0 - distance;
                results.push(SearchResult::new(ext_id.clone(), score));

                if results.len() >= params.top_k {
                    break;
                }
            }
        }

        // Reset ef_search if we changed it
        if params.nprobe as usize != self.ef_search {
            self.index.change_expansion_search(self.ef_search);
        }

        Ok(results)
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        queries.iter().map(|q| self.search(q, params)).collect()
    }

    fn delete(&self, internal_id: InternalId) -> Result<()> {
        self.tombstones.mark_deleted(internal_id)?;
        // Note: We don't remove from usearch immediately to avoid graph disruption.
        // Tombstoned vectors are filtered during search.
        // Physical removal happens during rebuild.
        Ok(())
    }

    fn is_deleted(&self, internal_id: InternalId) -> bool {
        self.tombstones.is_deleted(internal_id)
    }

    fn get_vector(&self, internal_id: InternalId) -> Result<Option<Vec<f32>>> {
        if self.tombstones.is_deleted(internal_id) {
            return Ok(None);
        }

        let mut buffer = vec![0.0f32; self.dimensions];
        let found = self
            .index
            .get::<f32>(internal_id.0 as u64, &mut buffer)
            .map_err(|e| AkiDbError::InvalidParameter(format!("HNSW get failed: {}", e)))?;

        if found > 0 {
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn stats(&self) -> IndexStats {
        let total = self.index.size() as u64;
        let deleted = self.tombstones.deleted_count();

        IndexStats {
            total_vectors: total,
            active_vectors: total.saturating_sub(deleted),
            tombstoned_vectors: deleted,
            dimensions: self.dimensions,
            memory_bytes: total * (self.dimensions * 4 + 64) as u64, // vectors + graph overhead
            gpu_memory_bytes: None,
            using_gpu: false,
            rebuild_in_progress: self.is_rebuilding.load(Ordering::SeqCst),
        }
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }

    fn train(&self, _training_data: &[f32]) -> Result<()> {
        // HNSW doesn't require training - it's a graph-based index
        self.is_ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn trigger_rebuild(&self) -> Result<()> {
        self.is_rebuilding.store(true, Ordering::SeqCst);

        // Physically remove tombstoned vectors from the usearch index
        let reverse = self.reverse_mapping.read();
        let mut removed = 0u64;
        for (&internal_id, _) in reverse.iter() {
            if self.tombstones.is_deleted(InternalId(internal_id)) {
                if let Ok(count) = self.index.remove(internal_id as u64) {
                    removed += count as u64;
                }
            }
        }
        drop(reverse);

        // Clean up reverse mapping
        let mut reverse = self.reverse_mapping.write();
        reverse.retain(|&id, _| !self.tombstones.is_deleted(InternalId(id)));

        // Reset tombstones
        self.tombstones.reset();

        debug!(removed = removed, "HNSW rebuild completed");
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

    fn create_test_config() -> HnswConfig {
        HnswConfig {
            dimensions: 128,
            capacity: 1000,
            m: 16,
            ef_construction: 64,
            ef_search: 32,
        }
    }

    fn create_random_vector(dim: usize, seed: f32) -> Vec<f32> {
        (0..dim).map(|i| ((i as f32 + seed) * 0.1).sin()).collect()
    }

    #[test]
    fn test_hnsw_create() {
        let config = create_test_config();
        let index = HnswIndex::new(config);
        assert!(index.is_ok());
        let index = index.unwrap();
        assert_eq!(index.dimensions(), 128);
        assert!(index.is_ready());
    }

    #[test]
    fn test_hnsw_insert_search() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let v2 = create_random_vector(128, 2.0);

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].id.as_str(), "vec-1");
        // Self-match should have score very close to 1.0
        assert!(results[0].score > 0.99);
    }

    #[test]
    fn test_hnsw_filter_applies_before_top_k_cutoff() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let blocked = create_random_vector(128, 1.0);
        let allowed = create_random_vector(128, 1.1);
        index.insert(&VectorId::new("blocked"), &blocked).unwrap();
        index.insert(&VectorId::new("allowed"), &allowed).unwrap();

        let params = SearchParams::new(1).with_filter(std::sync::Arc::new(|id: &VectorId| {
            id.as_str() == "allowed"
        }));
        let results = index.search(&blocked, &params).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.as_str(), "allowed");
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let wrong_dim = vec![1.0; 64];
        let result = index.insert(&VectorId::new("vec-1"), &wrong_dim);
        assert!(matches!(result, Err(AkiDbError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_hnsw_search_rejects_non_finite_query_values() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        index.insert(&VectorId::new("vec-1"), &v1).unwrap();

        let mut query = v1;
        query[0] = f32::INFINITY;
        let result = index.search(&query, &SearchParams::new(1));

        assert!(matches!(result, Err(AkiDbError::InvalidParameter(_))));
    }

    #[test]
    fn test_hnsw_insert_rejects_non_finite_vector_values() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let mut vector = create_random_vector(128, 1.0);
        vector[0] = f32::NAN;
        let result = index.insert(&VectorId::new("vec-1"), &vector);

        assert!(matches!(result, Err(AkiDbError::InvalidParameter(_))));
        assert_eq!(index.stats().total_vectors, 0);
    }

    #[test]
    fn test_hnsw_delete() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let internal_id = index.insert(&VectorId::new("vec-1"), &v1).unwrap();

        // Should find before delete
        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert_eq!(results.len(), 1);

        // Delete
        index.delete(internal_id).unwrap();

        // Should not find after delete (tombstoned)
        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_batch_insert() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let vectors: Vec<(VectorId, Vec<f32>)> = (0..50)
            .map(|i| {
                (
                    VectorId::new(format!("vec-{}", i)),
                    create_random_vector(128, i as f32),
                )
            })
            .collect();

        let ids = index.insert_batch(&vectors).unwrap();
        assert_eq!(ids.len(), 50);

        let stats = index.stats();
        assert_eq!(stats.total_vectors, 50);
        assert_eq!(stats.active_vectors, 50);
    }

    #[test]
    fn test_hnsw_delete_after_capacity_growth() {
        let config = HnswConfig {
            dimensions: 128,
            capacity: 1,
            m: 16,
            ef_construction: 64,
            ef_search: 32,
        };
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let v2 = create_random_vector(128, 2.0);

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        let grown_id = index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        index
            .delete(grown_id)
            .expect("delete should work after automatic capacity growth");

        let results = index.search(&v2, &SearchParams::new(10)).unwrap();
        assert!(
            results.iter().all(|result| result.id.as_str() != "vec-2"),
            "deleted vector from grown capacity should not be searchable"
        );
    }

    #[test]
    fn test_hnsw_stats() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        for i in 0..20 {
            let v = create_random_vector(128, i as f32);
            index
                .insert(&VectorId::new(format!("vec-{}", i)), &v)
                .unwrap();
        }

        let stats = index.stats();
        assert_eq!(stats.total_vectors, 20);
        assert_eq!(stats.active_vectors, 20);
        assert_eq!(stats.tombstoned_vectors, 0);
        assert!(!stats.using_gpu);
        assert_eq!(stats.dimensions, 128);
    }

    #[test]
    fn test_hnsw_upsert() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let v2 = create_random_vector(128, 2.0);

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        // Upsert with same ID but different vector
        index.insert(&VectorId::new("vec-1"), &v2).unwrap();

        // Should find the updated vector (v2)
        let results = index.search(&v2, &SearchParams::new(2)).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].id.as_str(), "vec-1");
    }

    #[test]
    fn test_hnsw_reinsert_after_rebuild_restores_reverse_mapping() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let v2 = create_random_vector(128, 2.0);
        let id = VectorId::new("vec-1");

        let internal_id = index.insert(&id, &v1).unwrap();
        index.delete(internal_id).unwrap();
        index.trigger_rebuild().unwrap();

        index.insert(&id, &v2).unwrap();

        let results = index.search(&v2, &SearchParams::new(2)).unwrap();
        assert!(
            results.iter().any(|result| result.id.as_str() == "vec-1"),
            "reinserted vector should be visible after rebuild"
        );
    }

    #[test]
    fn test_hnsw_zero_dimensions() {
        let config = HnswConfig {
            dimensions: 0,
            ..Default::default()
        };
        let result = HnswIndex::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_hnsw_search_params_nprobe() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        index.insert(&VectorId::new("vec-1"), &v1).unwrap();

        // Search with custom nprobe (maps to ef_search)
        let params = SearchParams::new(5).with_nprobe(128);
        let results = index.search(&v1, &params).unwrap();
        assert!(!results.is_empty());
    }
}
