//! HNSW vector index implementation using usearch
//!
//! This module provides a real HNSW (Hierarchical Navigable Small World) index
//! via the `usearch` crate. It is the portable CPU vector-index backend used
//! on supported macOS ARM64 and Ubuntu AMD64 targets.

use crate::snapshot;
#[cfg(test)]
use crate::snapshot::{durable_vector_fingerprint, DurableVectorRef};
use crate::{
    allocate_internal_id,
    index::{IndexStats, SearchParams, SearchWindow, VectorIndex},
    tombstone::TombstoneBitset,
    validate_finite_vector_values, AkiDbError, InternalId, Result, SearchResult, VectorId,
};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tracing::{debug, warn};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

const FILTER_CANDIDATE_MULTIPLIER: usize = 2;
const MIN_FILTER_CANDIDATES: usize = 32;
/// Routed I8 hits are rescored with original f32 vectors. The pool has to be
/// wider than `top_k` so a quantization miss can still be corrected.
const I8_RERANK_CANDIDATES: usize = 64;

fn candidate_count(top_k: usize, index_size: usize, tombstoned: usize, filtered: bool) -> usize {
    if index_size == 0 {
        return 0;
    }
    let base = if filtered {
        top_k
            .saturating_mul(FILTER_CANDIDATE_MULTIPLIER)
            .max(MIN_FILTER_CANDIDATES)
    } else {
        top_k.saturating_add(top_k.saturating_add(1) / 2)
    };
    let active = index_size.saturating_sub(tombstoned.min(index_size));
    if active == 0 {
        // Every vector is deleted, so no query can return a result. Asking
        // usearch for the whole index would be a full scan plus a window-sized
        // allocation only to discard every candidate.
        return 0;
    }
    let compensated = (base as u128)
        .saturating_mul(index_size as u128)
        .saturating_add(active.saturating_sub(1) as u128)
        / active as u128;
    usize::try_from(compensated)
        .unwrap_or(usize::MAX)
        .min(index_size)
}

fn next_candidate_count(current: usize, limit: usize) -> usize {
    current
        .saturating_mul(2)
        .max(current.saturating_add(1))
        .min(limit)
}

struct ExpansionSearchReset<'a> {
    index: &'a Index,
    default: usize,
}

impl Drop for ExpansionSearchReset<'_> {
    fn drop(&mut self) {
        self.index.change_expansion_search(self.default);
    }
}

/// Vector storage precision for usearch (GAP-010).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorPrecision {
    #[default]
    F32,
    F16,
    /// Signed 8-bit routing codes. Search rescores candidates with the original f32 vectors.
    I8,
}

impl VectorPrecision {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "f32" | "float32" | "" => Ok(Self::F32),
            "f16" | "float16" | "half" => Ok(Self::F16),
            "i8" | "int8" | "sq8" => Ok(Self::I8),
            other => Err(AkiDbError::InvalidParameter(format!(
                "unsupported vector_precision '{other}'; expected f32, f16, or i8"
            ))),
        }
    }

    pub(crate) fn to_scalar_kind(self) -> ScalarKind {
        match self {
            Self::F32 => ScalarKind::F32,
            Self::F16 => ScalarKind::F16,
            Self::I8 => ScalarKind::I8,
        }
    }
}

/// Distance metric for HNSW (GAP-007).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    #[default]
    Cosine,
    L2,
    InnerProduct,
}

impl DistanceMetric {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cosine" | "cos" | "" => Ok(Self::Cosine),
            "l2" | "euclidean" => Ok(Self::L2),
            "ip" | "inner_product" | "dot" => Ok(Self::InnerProduct),
            other => Err(AkiDbError::InvalidParameter(format!(
                "unsupported metric '{other}'; expected cosine, l2, or ip"
            ))),
        }
    }

    pub(crate) fn to_metric_kind(self) -> MetricKind {
        match self {
            Self::Cosine => MetricKind::Cos,
            Self::L2 => MetricKind::L2sq,
            Self::InnerProduct => MetricKind::IP,
        }
    }
}

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
    /// Storage precision (f32 default; f16 for lower RAM)
    pub precision: VectorPrecision,
    /// Distance metric (cosine default)
    pub metric: DistanceMetric,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dimensions: 768,
            capacity: 1_000_000,
            m: 16,
            ef_construction: 128,
            ef_search: 64,
            precision: VectorPrecision::F32,
            metric: DistanceMetric::Cosine,
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
    /// Distance metric used for score conversion
    metric: DistanceMetric,
    /// Storage precision. I8 keeps original f32 vectors for the final score.
    precision: VectorPrecision,
    /// Original f32 vectors, indexed by internal id. Populated only for I8.
    exact_vectors: RwLock<Vec<Option<Vec<f32>>>>,
    /// Prevents usearch's shared expansion-search setting from changing while
    /// any default search reads it. Default searches share a read lock;
    /// non-default `nprobe` searches take the write lock.
    ef_search_lock: RwLock<()>,
    /// Prevents concurrent insert/delete operations during trigger_rebuild.
    /// Searches do NOT acquire this lock (tombstone filtering remains correct).
    rebuild_lock: Mutex<()>,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(config: HnswConfig) -> Result<Self> {
        if config.dimensions == 0 {
            return Err(AkiDbError::InvalidParameter(
                "HNSW dimensions must be > 0".to_string(),
            ));
        }

        let options = IndexOptions {
            dimensions: config.dimensions,
            metric: config.metric.to_metric_kind(),
            quantization: config.precision.to_scalar_kind(),
            connectivity: config.m,
            expansion_add: config.ef_construction,
            expansion_search: config.ef_search,
            ..Default::default()
        };

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
            precision = ?config.precision,
            metric = ?config.metric,
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
            metric: config.metric,
            precision: config.precision,
            exact_vectors: RwLock::new(Vec::new()),
            ef_search_lock: RwLock::new(()),
            rebuild_lock: Mutex::new(()),
        })
    }

    fn remember_exact(&self, internal_id: i64, vector: &[f32]) {
        if self.precision != VectorPrecision::I8 || internal_id < 0 {
            return;
        }
        let mut exact = self.exact_vectors.write();
        let slot = internal_id as usize;
        if exact.len() <= slot {
            exact.resize(slot + 1, None);
        }
        exact[slot] = Some(vector.to_vec());
    }

    fn forget_exact(&self, internal_id: i64) {
        if self.precision != VectorPrecision::I8 || internal_id < 0 {
            return;
        }
        let mut exact = self.exact_vectors.write();
        if let Some(slot) = exact.get_mut(internal_id as usize) {
            *slot = None;
        }
    }

    /// Exact score in the same units as [`Self::distance_to_score`].
    fn exact_score(&self, query: &[f32], vector: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => {
                let mut dot = 0.0f32;
                let mut query_norm = 0.0f32;
                let mut vector_norm = 0.0f32;
                for (left, right) in query.iter().zip(vector) {
                    dot += left * right;
                    query_norm += left * left;
                    vector_norm += right * right;
                }
                let denom = query_norm.sqrt() * vector_norm.sqrt();
                if denom == 0.0 {
                    0.0
                } else {
                    dot / denom
                }
            }
            DistanceMetric::L2 => {
                let mut sum = 0.0f32;
                for (left, right) in query.iter().zip(vector) {
                    let delta = left - right;
                    sum += delta * delta;
                }
                1.0 / (1.0 + sum)
            }
            DistanceMetric::InnerProduct => query
                .iter()
                .zip(vector)
                .map(|(left, right)| left * right)
                .sum(),
        }
    }

    /// Convert a usearch distance value to a similarity score based on the
    /// configured distance metric.
    ///
    /// - Cosine: usearch returns `1 - cos_sim`, so `score = 1 - distance`
    /// - L2 (squared Euclidean): maps [0, inf) to (0, 1] via `1 / (1 + d)`
    /// - InnerProduct: usearch returns `1 - dot_product`, so `score = 1 - distance`
    fn distance_to_score(&self, distance: f32) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => 1.0 - distance,
            DistanceMetric::L2 => 1.0 / (1.0 + distance),
            DistanceMetric::InnerProduct => 1.0 - distance,
        }
    }

    fn search_candidate_window(
        &self,
        query: &[f32],
        params: &SearchParams,
        mut search_count: usize,
        candidate_limit: usize,
    ) -> Result<SearchWindow> {
        // Only negative decisions are memoized: never reuse an earlier ACL
        // approval after metadata changes. This set is local to one query,
        // including when SearchParams is reused by a batch or another caller.
        let mut rejected = HashSet::new();
        loop {
            let matches = self
                .index
                .search::<f32>(query, search_count)
                .map_err(|error| {
                    AkiDbError::InvalidParameter(format!("HNSW search failed: {error}"))
                })?;

            let reverse = self.reverse_mapping.read();
            let rerank = self.precision == VectorPrecision::I8;
            let mut hits = Vec::with_capacity(params.top_k);
            for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
                let internal_id = *key as i64;

                if self.tombstones.is_deleted(InternalId(internal_id)) {
                    continue;
                }
                if let Some(ext_id) = reverse.get(&internal_id) {
                    if let Some(filter) = &params.filter {
                        if params.cache_filter_rejections && rejected.contains(&internal_id) {
                            continue;
                        }
                        if !filter(ext_id) {
                            if params.cache_filter_rejections {
                                rejected.insert(internal_id);
                            }
                            continue;
                        }
                    }
                    hits.push((
                        internal_id,
                        SearchResult::new(ext_id.clone(), self.distance_to_score(*distance)),
                    ));
                    if !rerank && hits.len() >= params.top_k {
                        break;
                    }
                }
            }
            drop(reverse);
            if rerank {
                let exact = self.exact_vectors.read();
                for (internal_id, result) in &mut hits {
                    if let Some(Some(vector)) = exact.get(*internal_id as usize) {
                        result.score = self.exact_score(query, vector);
                    }
                }
                hits.sort_by(|left, right| right.1.score.total_cmp(&left.1.score));
                hits.truncate(params.top_k);
            }
            let results: Vec<SearchResult> = hits.into_iter().map(|(_, result)| result).collect();

            if results.len() >= params.top_k || search_count >= candidate_limit {
                let candidate_limited = params.filter.is_some()
                    && results.len() < params.top_k
                    && search_count >= params.filter_candidate_limit;
                return Ok(SearchWindow {
                    results,
                    candidate_limited,
                });
            }
            search_count = next_candidate_count(search_count, candidate_limit);
        }
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
        // Acquire rebuild_lock to prevent interleaving with trigger_rebuild
        let _rebuild_guard = self.rebuild_lock.lock();

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
            self.remember_exact(existing_id, vector);
            let mut reverse = self.reverse_mapping.write();
            reverse.insert(existing_id, id.clone());
            return Ok(InternalId(existing_id));
        }

        // New vector
        let internal_id = allocate_internal_id(&self.next_id)?;
        self.ensure_capacity_for(internal_id as usize + 1);

        self.index
            .add(internal_id as u64, vector)
            .map_err(|e| AkiDbError::InvalidParameter(format!("HNSW insert failed: {}", e)))?;
        self.remember_exact(internal_id, vector);

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
        Ok(self.search_window(query, params)?.results)
    }

    fn search_window(&self, query: &[f32], params: &SearchParams) -> Result<SearchWindow> {
        if query.len() != self.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.dimensions,
                actual: query.len(),
            });
        }
        validate_finite_vector_values(query, "Search")?;

        // The service already expands top_k according to its configured
        // post-filter policy. Add one bounded window here rather than asking
        // usearch to return the entire index whenever the mandatory workspace
        // ACL contributes a predicate. Compensate proportionally for
        // tombstones so normal delete churn does not starve the final top_k.
        let tombstoned = usize::try_from(self.tombstones.deleted_count()).unwrap_or(usize::MAX);
        let mut search_count = candidate_count(
            params.top_k,
            self.index.size(),
            tombstoned,
            params.filter.is_some(),
        );
        if search_count == 0 {
            return Ok(SearchWindow {
                results: Vec::new(),
                candidate_limited: false,
            });
        }
        if self.precision == VectorPrecision::I8 {
            let floor = I8_RERANK_CANDIDATES
                .min(self.index.size())
                .max(params.top_k);
            search_count = search_count.max(floor);
        }
        let candidate_limit = if params.filter.is_some() || tombstoned > 0 {
            params
                .filter_candidate_limit
                .max(params.top_k)
                .min(self.index.size())
        } else {
            search_count
        };
        search_count = search_count.min(candidate_limit);

        // Usearch exposes expansion_search as shared mutable index state
        // rather than a per-call option. Every default search therefore holds
        // a shared guard; a custom nprobe search exclusively performs the
        // change-search-restore sequence. This avoids a C++ read/write data
        // race while preserving concurrency at the configured operating point.
        let breadth = params.nprobe.map_or(self.ef_search, |value| value as usize);
        if breadth == self.ef_search {
            let _guard = self.ef_search_lock.read();
            self.search_candidate_window(query, params, search_count, candidate_limit)
        } else {
            let _guard = self.ef_search_lock.write();
            self.index.change_expansion_search(breadth);
            let _reset = ExpansionSearchReset {
                index: &self.index,
                default: self.ef_search,
            };
            self.search_candidate_window(query, params, search_count, candidate_limit)
        }
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        queries.iter().map(|q| self.search(q, params)).collect()
    }

    fn delete(&self, internal_id: InternalId) -> Result<()> {
        // Acquire rebuild_lock to prevent interleaving with trigger_rebuild
        let _rebuild_guard = self.rebuild_lock.lock();
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
        if self.precision == VectorPrecision::I8 {
            let exact = self.exact_vectors.read();
            return Ok(exact
                .get(internal_id.0 as usize)
                .and_then(|slot| slot.clone()));
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
        let stored_bytes = match self.precision {
            VectorPrecision::F32 => self.dimensions * 4,
            VectorPrecision::F16 => self.dimensions * 2,
            VectorPrecision::I8 => self.dimensions * (1 + 4),
        };

        IndexStats {
            total_vectors: total,
            active_vectors: total.saturating_sub(deleted),
            tombstoned_vectors: deleted,
            dimensions: self.dimensions,
            memory_bytes: total * (stored_bytes + 64) as u64,
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

    /// Physically remove tombstoned vectors. Tombstones stay queryable through
    /// the bitset filter until this runs, so callers invoke it from a
    /// maintenance path rather than the mutate hot path (compaction is by
    /// rebuild, never by deleting from the graph per delete).
    fn trigger_rebuild(&self) -> Result<()> {
        self.is_rebuilding.store(true, Ordering::SeqCst);

        // Physically remove tombstoned vectors from the usearch index. Only
        // vectors that are actually removed get their mapping pruned and
        // tombstone cleared below — a failed removal must stay tombstoned
        // and mapped so it is retried on the next rebuild instead of being
        // orphaned (unreachable via any mapping, but never freed in usearch).
        let reverse = self.reverse_mapping.read();
        let mut removed = 0u64;
        let mut successfully_removed = HashSet::new();
        for &internal_id in reverse.keys() {
            if self.tombstones.is_deleted(InternalId(internal_id)) {
                match self.index.remove(internal_id as u64) {
                    Ok(count) => {
                        removed += count as u64;
                        successfully_removed.insert(internal_id);
                    }
                    Err(error) => {
                        warn!(
                            internal_id,
                            %error,
                            "failed to physically remove tombstoned vector; leaving it tombstoned for retry"
                        );
                    }
                }
            }
        }
        drop(reverse);

        // Clean up reverse mapping for successfully-removed IDs, then prune the
        // forward external→internal map so deleted IDs do not force the upsert
        // path forever.
        let mut reverse = self.reverse_mapping.write();
        reverse.retain(|id, _| !successfully_removed.contains(id));
        {
            let mut id_map = self.id_mapping.write();
            id_map.retain(|_, &mut internal_id| reverse.contains_key(&internal_id));
        }
        drop(reverse);

        // Clear tombstones only for the IDs that were actually removed.
        for internal_id in successfully_removed {
            let _ = self.tombstones.clear_deleted(InternalId(internal_id));
            self.forget_exact(internal_id);
        }

        debug!(removed = removed, "HNSW rebuild completed");
        self.is_rebuilding.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn tombstoned_count(&self) -> u64 {
        self.tombstones.deleted_count()
    }

    fn total_count(&self) -> u64 {
        self.index.size() as u64
    }

    fn compact_tombstones(&self) -> Result<u64> {
        let before = self.tombstones.deleted_count();
        self.trigger_rebuild()?;
        let removed = before.saturating_sub(self.tombstones.deleted_count());
        Ok(removed)
    }

    fn is_rebuilding(&self) -> bool {
        self.is_rebuilding.load(Ordering::SeqCst)
    }
}

impl HnswIndex {
    /// Write the graph, id maps, and tombstones so a later process can skip
    /// reinserting every vector. `payload_sha256` must be
    /// [`crate::durable_vector_fingerprint`] of the durable vectors this graph serves.
    pub fn save_snapshot(&self, directory: &std::path::Path, payload_sha256: &str) -> Result<()> {
        let _rebuild_guard = self.rebuild_lock.lock();
        let _search_guard = self.ef_search_lock.write();
        let id_mapping = self.id_mapping.read();
        let reverse_mapping = self.reverse_mapping.read();
        let exact_vectors = self.exact_vectors.read();
        snapshot::save(
            &snapshot::SnapshotSource {
                index: &self.index,
                id_mapping: &id_mapping,
                reverse_mapping: &reverse_mapping,
                tombstones: &self.tombstones,
                next_id: self.next_id.load(Ordering::SeqCst),
                dimensions: self.dimensions,
                ef_search: self.ef_search,
                metric: self.metric,
                exact_vectors: &exact_vectors,
            },
            directory,
            payload_sha256,
        )
    }

    /// Restore a snapshot only when its configuration and payload fingerprint match.
    pub fn load_snapshot(
        directory: &std::path::Path,
        expected: &HnswConfig,
        payload_sha256: &str,
    ) -> Result<Self> {
        let loaded = snapshot::load(directory, expected, payload_sha256)?;
        Ok(Self {
            index: loaded.index,
            id_mapping: RwLock::new(loaded.id_mapping),
            reverse_mapping: RwLock::new(loaded.reverse_mapping),
            tombstones: loaded.tombstones,
            next_id: AtomicI64::new(loaded.next_id),
            dimensions: expected.dimensions,
            is_ready: AtomicBool::new(true),
            is_rebuilding: AtomicBool::new(false),
            ef_search: expected.ef_search,
            metric: expected.metric,
            precision: expected.precision,
            exact_vectors: RwLock::new(loaded.exact_vectors),
            ef_search_lock: RwLock::new(()),
            rebuild_lock: Mutex::new(()),
        })
    }

    /// Report whether this external id is currently mapped to `internal_id`.
    pub fn external_id_maps_to(&self, external_id: &str, internal_id: i64) -> bool {
        self.id_mapping.read().get(external_id).copied() == Some(internal_id)
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
            ..Default::default()
        }
    }

    fn create_random_vector(dim: usize, seed: f32) -> Vec<f32> {
        (0..dim).map(|i| ((i as f32 + seed) * 0.1).sin()).collect()
    }

    #[test]
    fn default_search_uses_configured_breadth_and_shared_guard() {
        let index =
            std::sync::Arc::new(HnswIndex::new(create_test_config().with_ef_search(96)).unwrap());
        let vector = create_random_vector(128, 1.0);
        index.insert(&VectorId::new("row"), &vector).unwrap();
        let observed = index.clone();
        let params = SearchParams::new(1).with_filter(std::sync::Arc::new(move |_| {
            assert_eq!(observed.index.expansion_search(), 96);
            assert!(observed.ef_search_lock.try_read().is_some());
            true
        }));
        assert_eq!(index.search(&vector, &params).unwrap().len(), 1);

        let observed = index.clone();
        let explicit = SearchParams::new(1)
            .with_nprobe(32)
            .with_filter(std::sync::Arc::new(move |_| {
                assert_eq!(observed.index.expansion_search(), 32);
                assert!(observed.ef_search_lock.try_read().is_none());
                true
            }));
        assert_eq!(index.search(&vector, &explicit).unwrap().len(), 1);
        assert_eq!(index.index.expansion_search(), 96);
    }

    #[test]
    fn rejected_candidates_are_evaluated_once_per_search_and_not_cached_between_queries() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        for row in 0..100 {
            index
                .insert(
                    &VectorId::new(row.to_string()),
                    &create_random_vector(128, row as f32),
                )
                .unwrap();
        }
        let calls = std::sync::Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let allow = std::sync::Arc::new(AtomicBool::new(false));
        let observed = calls.clone();
        let allowed = allow.clone();
        let params = SearchParams::new(1)
            .with_filter(std::sync::Arc::new(move |id| {
                *observed.lock().entry(id.to_string()).or_default() += 1;
                allowed.load(Ordering::Relaxed)
            }))
            .with_filter_rejection_cache(true)
            .with_filter_candidate_limit(100);
        let query = create_random_vector(128, 0.0);
        assert!(index.search(&query, &params).unwrap().is_empty());
        assert_eq!(calls.lock().len(), 100);
        assert!(calls.lock().values().all(|count| *count == 1));
        allow.store(true, Ordering::Relaxed);
        assert_eq!(index.search(&query, &params).unwrap().len(), 1);
    }

    #[test]
    fn allowed_candidates_are_rechecked_when_the_window_expands() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        for row in 0..100 {
            index
                .insert(
                    &VectorId::new(row.to_string()),
                    &create_random_vector(128, row as f32),
                )
                .unwrap();
        }
        let seen = std::sync::Arc::new(AtomicBool::new(false));
        let params = SearchParams::new(2)
            .with_filter(std::sync::Arc::new(move |id| {
                id.as_str() == "0" && !seen.swap(true, Ordering::Relaxed)
            }))
            .with_filter_rejection_cache(true)
            .with_filter_candidate_limit(100);
        assert!(index
            .search(&create_random_vector(128, 0.0), &params)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn clustered_tombstones_retry_within_the_candidate_limit() {
        let mut config = HnswConfig::new(2).with_capacity(100).with_ef_search(128);
        config.metric = DistanceMetric::L2;
        let index = HnswIndex::new(config).unwrap();
        for row in 0..100 {
            let id = index
                .insert(&VectorId::new(row.to_string()), &[row as f32, 0.0])
                .unwrap();
            if row < 20 {
                index.delete(id).unwrap();
            }
        }
        let results = index.search(&[0.0, 0.0], &SearchParams::new(5)).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(
            results
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            vec!["20", "21", "22", "23", "24"]
        );
        let bounded = SearchParams::new(5).with_filter_candidate_limit(10);
        assert!(index.search(&[0.0, 0.0], &bounded).unwrap().is_empty());
    }

    #[test]
    fn filter_candidate_cap_is_reported_when_top_k_is_not_filled() {
        let mut config = HnswConfig::new(2).with_capacity(32).with_ef_search(16);
        config.metric = DistanceMetric::L2;
        let index = HnswIndex::new(config).unwrap();
        for row in 0..20 {
            index
                .insert(&VectorId::new(row.to_string()), &[row as f32, 0.0])
                .unwrap();
        }
        let limited = SearchParams::new(5)
            .with_filter(std::sync::Arc::new(|id: &VectorId| {
                id.as_str() == "missing"
            }))
            .with_filter_candidate_limit(4);
        let window = index.search_window(&[0.0, 0.0], &limited).unwrap();
        assert!(window.results.is_empty());
        assert!(window.candidate_limited);

        let filled = SearchParams::new(1)
            .with_filter(std::sync::Arc::new(|_: &VectorId| true))
            .with_filter_candidate_limit(4);
        let window = index.search_window(&[0.0, 0.0], &filled).unwrap();
        assert_eq!(window.results.len(), 1);
        assert!(!window.candidate_limited);
    }

    #[test]
    fn filtered_tombstone_compensation_respects_the_window_cap() {
        let mut config = HnswConfig::new(2).with_capacity(100).with_ef_search(128);
        config.metric = DistanceMetric::L2;
        let index = HnswIndex::new(config).unwrap();
        for row in 0..100 {
            let id = index
                .insert(&VectorId::new(row.to_string()), &[row as f32, 0.0])
                .unwrap();
            if row >= 50 {
                index.delete(id).unwrap();
            }
        }
        let calls = std::sync::Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let observed = calls.clone();
        // The compensated first window would be 64, but the cap is 40.
        let params = SearchParams::new(1)
            .with_filter(std::sync::Arc::new(move |id| {
                *observed.lock().entry(id.to_string()).or_default() += 1;
                id.as_str() == "45"
            }))
            .with_filter_rejection_cache(true)
            .with_filter_candidate_limit(40);
        assert!(index.search(&[0.0, 0.0], &params).unwrap().is_empty());
        assert_eq!(calls.lock().len(), 40);
        assert!(calls.lock().values().all(|count| *count == 1));
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
    fn selective_filter_expands_until_it_finds_a_bounded_match() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        for row in 0..100 {
            index
                .insert(
                    &VectorId::new(format!("row-{row}")),
                    &create_random_vector(128, row as f32),
                )
                .unwrap();
        }
        let query = create_random_vector(128, 0.0);
        let broad = index
            .search(&query, &SearchParams::new(100).with_nprobe(128))
            .unwrap();
        assert!(broad.len() > MIN_FILTER_CANDIDATES);
        let allowed = broad.last().unwrap().id.as_str().to_string();

        let params = SearchParams::new(1)
            .with_filter(std::sync::Arc::new(move |id: &VectorId| {
                id.as_str() == allowed
            }))
            .with_filter_candidate_limit(100);
        let results = index.search(&query, &params).unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn selective_filter_work_is_geometrically_bounded() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        for row in 0..100 {
            index
                .insert(
                    &VectorId::new(format!("row-{row}")),
                    &create_random_vector(128, row as f32),
                )
                .unwrap();
        }
        let predicate_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_calls = std::sync::Arc::clone(&predicate_calls);
        let params = SearchParams::new(1)
            .with_filter(std::sync::Arc::new(move |_id: &VectorId| {
                observed_calls.fetch_add(1, Ordering::Relaxed);
                false
            }))
            .with_filter_candidate_limit(100);

        let results = index
            .search(&create_random_vector(128, 0.0), &params)
            .unwrap();

        assert!(results.is_empty());
        // Without memoization, geometric growth still bounds callback work.
        assert!(predicate_calls.load(Ordering::Relaxed) < 200);
    }

    #[test]
    fn custom_expansion_is_restored_when_a_filter_panics() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        let vector = create_random_vector(128, 1.0);
        index.insert(&VectorId::new("row-1"), &vector).unwrap();
        let params = SearchParams::new(1)
            .with_nprobe(128)
            .with_filter(std::sync::Arc::new(|_id: &VectorId| {
                panic!("intentional predicate panic")
            }));

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = index.search(&vector, &params);
        }));

        assert!(panic.is_err());
        assert_eq!(index.index.expansion_search(), index.ef_search);
        assert_eq!(
            index.search(&vector, &SearchParams::new(1)).unwrap().len(),
            1
        );
    }

    #[test]
    fn rebuild_prunes_forward_id_mapping_for_deleted_ids() {
        let index = HnswIndex::new(create_test_config()).unwrap();
        let vector = create_random_vector(128, 3.0);
        let external = VectorId::new("to-delete");
        let internal = index.insert(&external, &vector).unwrap();

        index.delete(internal).unwrap();
        assert!(index.is_deleted(internal));
        assert!(index.id_mapping.read().contains_key(external.as_str()));

        index.trigger_rebuild().unwrap();

        assert!(!index.is_deleted(internal));
        assert!(
            !index.id_mapping.read().contains_key(external.as_str()),
            "rebuild must drop forward mapping for tombstoned external ids"
        );
        assert!(!index.reverse_mapping.read().contains_key(&internal.0));
        // Fresh insert after rebuild must allocate a new mapping, not upsert a ghost.
        let reinserted = index.insert(&external, &vector).unwrap();
        assert_ne!(reinserted.0, internal.0);
    }

    #[test]
    fn filtered_candidate_window_is_bounded_and_tombstone_aware() {
        assert_eq!(candidate_count(50, 100_000, 0, true), 100);
        assert_eq!(candidate_count(1, 2, 0, true), 2);
        assert_eq!(candidate_count(50, 100_000, 50_000, true), 200);
        assert_eq!(candidate_count(10, 100_000, 0, false), 15);
        // An index whose every vector is deleted cannot answer a query, so no
        // window is requested at all rather than one spanning the whole index.
        assert_eq!(candidate_count(10, 100, 100, true), 0);
        assert_eq!(next_candidate_count(32, 1_000), 64);
        assert_eq!(next_candidate_count(768, 1_000), 1_000);
        assert_eq!(next_candidate_count(1_000, 1_000), 1_000);
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
    fn test_hnsw_insert_rejects_exhausted_internal_ids() {
        let config = create_test_config();
        let index = HnswIndex::new(config).unwrap();
        index.next_id.store(i64::MAX, Ordering::SeqCst);

        let vector = create_random_vector(128, 1.0);
        let result = index.insert(&VectorId::new("vec-1"), &vector);

        assert!(matches!(result, Err(AkiDbError::InvalidParameter(_))));
        assert_eq!(index.next_id.load(Ordering::SeqCst), i64::MAX);
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
            ..Default::default()
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

    #[test]
    fn test_hnsw_l2_score_conversion() {
        let config = HnswConfig {
            dimensions: 128,
            capacity: 1000,
            metric: DistanceMetric::L2,
            ..create_test_config()
        };
        let index = HnswIndex::new(config).unwrap();

        let v1 = create_random_vector(128, 1.0);
        let v2 = create_random_vector(128, 2.0);
        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert!(!results.is_empty());

        // L2 score = 1/(1+d), so all scores must be in (0, 1]
        for result in &results {
            assert!(
                result.score > 0.0 && result.score <= 1.0,
                "L2 score {} out of range (0, 1]",
                result.score
            );
        }

        // Self-match should have score close to 1.0 (distance ~0)
        assert_eq!(results[0].id.as_str(), "vec-1");
        assert!(results[0].score > 0.99);
    }

    #[test]
    fn test_hnsw_ip_score_conversion() {
        let config = HnswConfig {
            dimensions: 128,
            capacity: 1000,
            metric: DistanceMetric::InnerProduct,
            ..create_test_config()
        };
        let index = HnswIndex::new(config).unwrap();

        // Use normalized vectors so dot product is in [-1, 1]
        let mut v1 = create_random_vector(128, 1.0);
        let norm: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in &mut v1 {
            *x /= norm;
        }

        let mut v2 = create_random_vector(128, 2.0);
        let norm: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in &mut v2 {
            *x /= norm;
        }

        index.insert(&VectorId::new("vec-1"), &v1).unwrap();
        index.insert(&VectorId::new("vec-2"), &v2).unwrap();

        let results = index.search(&v1, &SearchParams::new(10)).unwrap();
        assert!(!results.is_empty());

        // IP score = 1 - distance = dot_product, for normalized vectors in [-1, 1]
        for result in &results {
            assert!(
                result.score >= -1.01 && result.score <= 1.01,
                "IP score {} out of expected range [-1, 1]",
                result.score
            );
        }

        // Self-match with normalized vector: dot product with itself = 1.0
        assert_eq!(results[0].id.as_str(), "vec-1");
        assert!(
            results[0].score > 0.99,
            "IP self-match score was {}",
            results[0].score
        );
    }

    #[test]
    fn test_hnsw_concurrent_search_with_different_nprobe() {
        use std::sync::Arc;
        use std::thread;

        let config = create_test_config().with_ef_search(96);
        let index = Arc::new(HnswIndex::new(config).unwrap());

        // Insert some vectors
        for i in 0..20 {
            let v = create_random_vector(128, i as f32);
            index
                .insert(&VectorId::new(format!("vec-{}", i)), &v)
                .unwrap();
        }

        let query = create_random_vector(128, 0.5);
        let mut handles = Vec::new();

        // Spawn threads with different nprobe values to exercise ef_search_lock
        for nprobe in [None, Some(16), Some(32), Some(96), Some(128), Some(256)] {
            let idx = Arc::clone(&index);
            let q = query.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let observed = idx.clone();
                    let params = SearchParams::new(5)
                        .with_optional_nprobe(nprobe)
                        .with_filter(Arc::new(move |_| {
                            assert_eq!(
                                observed.index.expansion_search(),
                                nprobe.unwrap_or(96) as usize
                            );
                            true
                        }));
                    let results = idx.search(&q, &params).unwrap();
                    // Results must be valid and non-empty (we have 20 vectors)
                    assert!(!results.is_empty());
                    assert!(results.len() <= 5);
                    // All scores must be finite
                    for r in &results {
                        assert!(r.score.is_finite());
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("concurrent search thread panicked");
        }
        assert_eq!(index.index.expansion_search(), 96);
    }

    fn fingerprint(records: &[(&str, i64, &[f32])]) -> String {
        let refs: Vec<DurableVectorRef<'_>> = records
            .iter()
            .map(|(external_id, internal_id, vector)| DurableVectorRef {
                external_id,
                internal_id: *internal_id,
                vector,
            })
            .collect();
        durable_vector_fingerprint(&refs)
    }

    #[test]
    fn test_hnsw_save_load_roundtrip_preserves_search_results() {
        let config = HnswConfig::new(4).with_capacity(8).with_ef_search(16);
        let index = HnswIndex::new(config.clone()).unwrap();
        let rows = [
            ("near", vec![1.0, 0.0, 0.0, 0.0]),
            ("mid", vec![0.2, 0.9, 0.0, 0.0]),
            ("far", vec![0.0, 0.0, 1.0, 0.0]),
        ];
        for (id, vector) in &rows {
            index.insert(&VectorId::new(*id), vector).unwrap();
        }
        index.delete(InternalId(1)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("hnsw-snapshot");
        let fingerprint = fingerprint(&[
            ("near", 0, rows[0].1.as_slice()),
            ("mid", 1, rows[1].1.as_slice()),
            ("far", 2, rows[2].1.as_slice()),
        ]);
        index.save_snapshot(&snapshot, &fingerprint).unwrap();

        let loaded = HnswIndex::load_snapshot(&snapshot, &config, &fingerprint).unwrap();
        let params = SearchParams::new(3);
        let original = index.search(&rows[0].1, &params).unwrap();
        let restored = loaded.search(&rows[0].1, &params).unwrap();
        assert_eq!(
            original
                .iter()
                .map(|result| result.id.as_str().to_string())
                .collect::<Vec<_>>(),
            restored
                .iter()
                .map(|result| result.id.as_str().to_string())
                .collect::<Vec<_>>()
        );
        assert!(original
            .iter()
            .zip(restored.iter())
            .all(|(left, right)| (left.score - right.score).abs() < 1e-6));
        assert!(loaded.is_deleted(InternalId(1)));
        assert!(!restored.iter().any(|result| result.id.as_str() == "mid"));
        let inserted = loaded
            .insert(&VectorId::new("fresh"), &[0.0, 0.0, 0.0, 1.0])
            .unwrap();
        assert_eq!(inserted.0, 3);
        assert!(loaded.external_id_maps_to("fresh", 3));
    }

    #[test]
    fn test_hnsw_load_rejects_checksum_mismatch() {
        let config = HnswConfig::new(4).with_capacity(4);
        let index = HnswIndex::new(config.clone()).unwrap();
        let vector = vec![1.0, 0.0, 0.0, 0.0];
        index.insert(&VectorId::new("only"), &vector).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("hnsw-snapshot");
        let fingerprint = fingerprint(&[("only", 0, vector.as_slice())]);
        index.save_snapshot(&snapshot, &fingerprint).unwrap();

        let graph = snapshot.join("graph.usearch");
        let mut bytes = std::fs::read(&graph).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&graph, bytes).unwrap();

        let error = match HnswIndex::load_snapshot(&snapshot, &config, &fingerprint) {
            Ok(_) => panic!("corrupt HNSW snapshot was accepted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_hnsw_load_rejects_config_or_payload_mismatch() {
        let config = HnswConfig::new(4).with_capacity(4);
        let index = HnswIndex::new(config.clone()).unwrap();
        let vector = vec![1.0, 0.0, 0.0, 0.0];
        index.insert(&VectorId::new("only"), &vector).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("hnsw-snapshot");
        let fingerprint = fingerprint(&[("only", 0, vector.as_slice())]);
        index.save_snapshot(&snapshot, &fingerprint).unwrap();

        let wrong_dimensions = HnswConfig::new(8).with_capacity(4);
        let error = match HnswIndex::load_snapshot(&snapshot, &wrong_dimensions, &fingerprint) {
            Ok(_) => panic!("mismatched HNSW configuration was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("configuration"), "{error}");

        let error = match HnswIndex::load_snapshot(&snapshot, &config, "deadbeef") {
            Ok(_) => panic!("mismatched HNSW payload fingerprint was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("fingerprint"), "{error}");
    }

    #[test]
    fn test_vector_precision_parses_i8_aliases() {
        assert_eq!(VectorPrecision::parse("i8").unwrap(), VectorPrecision::I8);
        assert_eq!(VectorPrecision::parse("int8").unwrap(), VectorPrecision::I8);
        assert_eq!(VectorPrecision::parse("sq8").unwrap(), VectorPrecision::I8);
        assert!(VectorPrecision::parse("f8").is_err());
    }

    #[test]
    fn test_vector_precision_error_lists_every_accepted_value() {
        let error = VectorPrecision::parse("f8").unwrap_err().to_string();
        assert!(error.contains("f32"), "{error}");
        assert!(error.contains("f16"), "{error}");
        assert!(error.contains("i8"), "{error}");
    }

    #[test]
    fn test_i8_routing_returns_exact_f32_score() {
        let config = HnswConfig::new(4).with_capacity(8).with_ef_search(16);
        let mut i8_config = config.clone();
        i8_config.precision = VectorPrecision::I8;
        let index = HnswIndex::new(i8_config).unwrap();
        let query = [1.0f32, 0.0, 0.0, 0.0];
        let near = [0.2f32, 0.0, 0.0, 0.05];
        let far = [0.0f32, 1.0, 0.0, 0.0];
        index.insert(&VectorId::new("near"), &near).unwrap();
        index.insert(&VectorId::new("far"), &far).unwrap();

        assert_eq!(
            index.get_vector(InternalId(0)).unwrap().as_deref(),
            Some(near.as_slice())
        );
        let results = index.search(&query, &SearchParams::new(1)).unwrap();
        assert_eq!(results[0].id.as_str(), "near");
        let expected = 0.2 / (0.2f32 * 0.2 + 0.05 * 0.05).sqrt();
        assert!(
            (results[0].score - expected).abs() < 1e-5,
            "score {} differed from exact cosine {expected}",
            results[0].score
        );

        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("hnsw-snapshot");
        let payload = fingerprint(&[("near", 0, near.as_slice()), ("far", 1, far.as_slice())]);
        index.save_snapshot(&snapshot, &payload).unwrap();
        assert!(snapshot.join("exact-f32.bin").is_file());
        let mut restored_config = config.clone();
        restored_config.precision = VectorPrecision::I8;
        let restored = HnswIndex::load_snapshot(&snapshot, &restored_config, &payload).unwrap();
        let restored_results = restored.search(&query, &SearchParams::new(1)).unwrap();
        assert_eq!(restored_results[0].id.as_str(), "near");
        assert!((restored_results[0].score - expected).abs() < 1e-5);
        assert_eq!(
            restored.get_vector(InternalId(0)).unwrap().as_deref(),
            Some(near.as_slice())
        );
    }
}
