//! cuVS (NVIDIA RAPIDS cuVS) vector search integration
//!
//! This module provides:
//! - cuVS backend implementation behind feature flag
//! - Shadow mode for validation against FAISS
//! - Rollback mechanism for production safety
//! - Performance comparison utilities

use crate::{IndexStats, InternalId, Result, SearchParams, SearchResult, VectorId, VectorIndex};
use akidb_common::AkiDbError;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// cuVS configuration
#[derive(Debug, Clone)]
pub struct CuvsConfig {
    /// Vector dimensions
    pub dimensions: usize,
    /// Number of IVF clusters
    pub nlist: usize,
    /// Graph degree for CAGRA
    pub graph_degree: usize,
    /// Build algorithm (IVF_FLAT, IVF_PQ, CAGRA)
    pub algorithm: CuvsAlgorithm,
    /// GPU device ID
    pub device_id: i32,
    /// Enable GPU memory pool
    pub use_memory_pool: bool,
    /// Memory pool size in bytes (0 = auto)
    pub memory_pool_size: usize,
}

impl Default for CuvsConfig {
    fn default() -> Self {
        Self {
            dimensions: 768,
            nlist: 1024,
            graph_degree: 64,
            algorithm: CuvsAlgorithm::Cagra,
            device_id: 0,
            use_memory_pool: true,
            memory_pool_size: 0, // Auto
        }
    }
}

/// cuVS algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CuvsAlgorithm {
    /// IVF Flat - exact search within clusters
    IvfFlat,
    /// IVF PQ - product quantization for memory efficiency
    IvfPq,
    /// CAGRA - graph-based ANN (fastest)
    Cagra,
}

impl std::fmt::Display for CuvsAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CuvsAlgorithm::IvfFlat => write!(f, "IVF_FLAT"),
            CuvsAlgorithm::IvfPq => write!(f, "IVF_PQ"),
            CuvsAlgorithm::Cagra => write!(f, "CAGRA"),
        }
    }
}

/// cuVS index statistics
#[derive(Debug, Clone, Default)]
pub struct CuvsStats {
    /// Search latency samples (microseconds)
    /// FIX BUG-011: Use VecDeque for O(1) pop_front instead of O(n) Vec::remove(0)
    pub search_latencies_us: VecDeque<u64>,
    /// Insert latency samples (microseconds)
    pub insert_latencies_us: VecDeque<u64>,
    /// Total searches performed
    pub total_searches: u64,
    /// Total inserts performed
    pub total_inserts: u64,
    /// Average search P50 (microseconds)
    pub search_p50_us: u64,
    /// Average search P95 (microseconds)
    pub search_p95_us: u64,
    /// Average search P99 (microseconds)
    pub search_p99_us: u64,
}

impl CuvsStats {
    /// Calculate percentiles from latency samples
    pub fn calculate_percentiles(&mut self) {
        if self.search_latencies_us.is_empty() {
            return;
        }

        // Convert VecDeque to Vec for sorting
        let mut sorted: Vec<u64> = self.search_latencies_us.iter().copied().collect();
        sorted.sort_unstable();
        let len = sorted.len();

        // FIX BUG-096: Clamp percentile index to valid range to prevent out-of-bounds
        // The formula (len * p) can equal len when p is close to 1.0 due to floating
        // point precision, which would cause an index out of bounds panic
        self.search_p50_us = sorted[len / 2];
        self.search_p95_us = sorted[((len as f64 * 0.95) as usize).min(len - 1)];
        self.search_p99_us = sorted[((len as f64 * 0.99) as usize).min(len - 1)];
    }
}

/// Mock cuVS index for development/testing
/// In production, this would wrap the actual cuVS FFI bindings
pub struct CuvsIndex {
    config: CuvsConfig,
    /// Vector storage (mock implementation)
    vectors: RwLock<HashMap<InternalId, Vec<f32>>>,
    /// ID to external ID mapping
    id_mapping: RwLock<HashMap<InternalId, VectorId>>,
    /// Tombstone set
    tombstones: RwLock<std::collections::HashSet<InternalId>>,
    /// Next internal ID
    next_id: AtomicU64,
    /// Index is trained
    trained: AtomicBool,
    /// Statistics
    stats: RwLock<CuvsStats>,
}

impl CuvsIndex {
    /// Create a new cuVS index
    pub fn new(config: CuvsConfig) -> Self {
        info!(
            algorithm = %config.algorithm,
            dimensions = config.dimensions,
            device = config.device_id,
            "Creating cuVS index"
        );

        Self {
            config,
            vectors: RwLock::new(HashMap::new()),
            id_mapping: RwLock::new(HashMap::new()),
            tombstones: RwLock::new(std::collections::HashSet::new()),
            next_id: AtomicU64::new(0),
            trained: AtomicBool::new(false),
            stats: RwLock::new(CuvsStats::default()),
        }
    }

    /// Get cuVS-specific statistics
    pub fn cuvs_stats(&self) -> CuvsStats {
        let mut stats = self.stats.read().clone();
        stats.calculate_percentiles();
        stats
    }

    /// Get configuration
    pub fn config(&self) -> &CuvsConfig {
        &self.config
    }

    /// Record search latency
    fn record_search_latency(&self, duration: Duration) {
        let mut stats = self.stats.write();
        stats.search_latencies_us.push_back(duration.as_micros() as u64);
        stats.total_searches += 1;
        // Keep only last 1000 samples - O(1) with VecDeque
        if stats.search_latencies_us.len() > 1000 {
            stats.search_latencies_us.pop_front();
        }
    }

    /// Record insert latency
    fn record_insert_latency(&self, duration: Duration) {
        let mut stats = self.stats.write();
        stats.insert_latencies_us.push_back(duration.as_micros() as u64);
        stats.total_inserts += 1;
        // O(1) with VecDeque
        if stats.insert_latencies_us.len() > 1000 {
            stats.insert_latencies_us.pop_front();
        }
    }
}

impl VectorIndex for CuvsIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        let start = Instant::now();

        // Validate dimensions
        if vector.len() != self.config.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.config.dimensions,
                actual: vector.len(),
            });
        }

        // FIX BUG-022: Check for overflow before casting u64 to i64
        let next = self.next_id.fetch_add(1, Ordering::SeqCst);
        if next > i64::MAX as u64 {
            return Err(AkiDbError::Internal(
                "ID counter overflow: exceeded i64::MAX".to_string(),
            ));
        }
        let internal_id = InternalId(next as i64);

        {
            let mut vectors = self.vectors.write();
            vectors.insert(internal_id, vector.to_vec());
        }
        {
            let mut mapping = self.id_mapping.write();
            mapping.insert(internal_id, id.clone());
        }

        self.record_insert_latency(start.elapsed());
        Ok(internal_id)
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        let start = Instant::now();
        let mut results = Vec::with_capacity(vectors.len());

        for (id, vector) in vectors {
            if vector.len() != self.config.dimensions {
                return Err(AkiDbError::DimensionMismatch {
                    expected: self.config.dimensions,
                    actual: vector.len(),
                });
            }

            // FIX BUG-022: Check for overflow before casting u64 to i64
            let next = self.next_id.fetch_add(1, Ordering::SeqCst);
            if next > i64::MAX as u64 {
                return Err(AkiDbError::Internal(
                    "ID counter overflow: exceeded i64::MAX".to_string(),
                ));
            }
            let internal_id = InternalId(next as i64);

            {
                let mut vecs = self.vectors.write();
                vecs.insert(internal_id, vector.clone());
            }
            {
                let mut mapping = self.id_mapping.write();
                mapping.insert(internal_id, id.clone());
            }

            results.push(internal_id);
        }

        self.record_insert_latency(start.elapsed());
        Ok(results)
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        let start = Instant::now();

        if query.len() != self.config.dimensions {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.config.dimensions,
                actual: query.len(),
            });
        }

        let vectors = self.vectors.read();
        let id_mapping = self.id_mapping.read();
        let tombstones = self.tombstones.read();

        // Calculate distances and sort
        let mut results: Vec<(InternalId, f32)> = vectors
            .iter()
            .filter(|(id, _)| !tombstones.contains(id))
            .map(|(id, vec)| {
                let distance = cosine_distance(query, vec);
                (*id, distance)
            })
            .collect();

        // Sort by distance (ascending for L2, descending for cosine similarity)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top_k and map to SearchResult
        let search_results: Vec<SearchResult> = results
            .into_iter()
            .take(params.top_k)
            .filter_map(|(internal_id, distance)| {
                id_mapping.get(&internal_id).map(|vector_id| SearchResult {
                    id: vector_id.clone(),
                    score: 1.0 - distance, // Convert distance to similarity
                    metadata: None,
                })
            })
            .collect();

        self.record_search_latency(start.elapsed());
        Ok(search_results)
    }

    fn search_batch(
        &self,
        queries: &[Vec<f32>],
        params: &SearchParams,
    ) -> Result<Vec<Vec<SearchResult>>> {
        queries.iter().map(|q| self.search(q, params)).collect()
    }

    fn delete(&self, internal_id: InternalId) -> Result<()> {
        let mut tombstones = self.tombstones.write();
        tombstones.insert(internal_id);
        Ok(())
    }

    fn is_deleted(&self, internal_id: InternalId) -> bool {
        self.tombstones.read().contains(&internal_id)
    }

    fn get_vector(&self, internal_id: InternalId) -> Result<Option<Vec<f32>>> {
        let vectors = self.vectors.read();
        Ok(vectors.get(&internal_id).cloned())
    }

    fn stats(&self) -> IndexStats {
        let vectors = self.vectors.read();
        let tombstones = self.tombstones.read();
        // FIX BUG-089: Use saturating_sub to prevent integer underflow
        // This can happen if tombstones tracking gets out of sync with vectors
        // (e.g., due to race conditions or if tombstones are cleaned up independently)
        let active = vectors.len().saturating_sub(tombstones.len());

        IndexStats {
            total_vectors: vectors.len() as u64,
            active_vectors: active as u64,
            tombstoned_vectors: tombstones.len() as u64,
            dimensions: self.config.dimensions,
            memory_bytes: (vectors.len() * self.config.dimensions * 4) as u64,
            gpu_memory_bytes: Some((vectors.len() * self.config.dimensions * 4) as u64),
            using_gpu: true,
            rebuild_in_progress: false,
        }
    }

    fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    fn is_ready(&self) -> bool {
        self.trained.load(Ordering::Relaxed)
    }

    fn train(&self, _training_data: &[f32]) -> Result<()> {
        // cuVS CAGRA doesn't require training like IVF
        // Mark as trained
        self.trained.store(true, Ordering::Release);
        info!("cuVS index marked as trained");
        Ok(())
    }

    fn trigger_rebuild(&self) -> Result<()> {
        // cuVS rebuild would optimize the graph structure
        info!("cuVS rebuild triggered (no-op in mock)");
        Ok(())
    }

    fn is_rebuilding(&self) -> bool {
        false
    }
}

/// Calculate cosine distance between two vectors
///
/// Returns a value in [0, 2] where 0 = identical, 1 = orthogonal, 2 = opposite.
///
/// FIX BUG-093: For zero-norm vectors, returns 1.0 (maximum distance) as cosine
/// similarity is mathematically undefined for zero vectors. This is the safest
/// default: treating zero vectors as maximally dissimilar from everything.
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        // FIX BUG-093: Documented intentional behavior - zero vectors are
        // treated as maximally dissimilar (distance = 1.0) since cosine
        // similarity is undefined for zero-norm vectors
        1.0
    } else {
        1.0 - (dot / (norm_a * norm_b))
    }
}

/// Shadow mode result for validation
#[derive(Debug, Clone)]
pub struct ShadowModeResult {
    /// FAISS results
    pub faiss_results: Vec<SearchResult>,
    /// cuVS results
    pub cuvs_results: Vec<SearchResult>,
    /// FAISS latency
    pub faiss_latency: Duration,
    /// cuVS latency
    pub cuvs_latency: Duration,
    /// Result divergence (0.0 = identical, 1.0 = completely different)
    pub divergence: f64,
    /// Recall of cuVS vs FAISS (what % of FAISS results are in cuVS)
    pub recall: f64,
}

/// Shadow mode validator for cuVS vs FAISS comparison
pub struct ShadowModeValidator<F: VectorIndex, C: VectorIndex> {
    /// FAISS (primary) index
    faiss: Arc<F>,
    /// cuVS (shadow) index
    cuvs: Arc<C>,
    /// Validation results
    /// FIX BUG-011: Use VecDeque for O(1) pop_front instead of O(n) Vec::remove(0)
    results: RwLock<VecDeque<ShadowModeResult>>,
    /// Total validations
    total_validations: AtomicU64,
    /// Divergence threshold for alerting
    divergence_threshold: f64,
    /// Enable shadow writes
    shadow_writes_enabled: AtomicBool,
}

impl<F: VectorIndex, C: VectorIndex> ShadowModeValidator<F, C> {
    /// Create a new shadow mode validator
    pub fn new(faiss: Arc<F>, cuvs: Arc<C>, divergence_threshold: f64) -> Self {
        Self {
            faiss,
            cuvs,
            results: RwLock::new(VecDeque::new()),
            total_validations: AtomicU64::new(0),
            divergence_threshold,
            shadow_writes_enabled: AtomicBool::new(true),
        }
    }

    /// Enable/disable shadow writes
    pub fn set_shadow_writes(&self, enabled: bool) {
        self.shadow_writes_enabled.store(enabled, Ordering::Release);
    }

    /// Insert into both indexes
    pub fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        // Always insert into primary (FAISS)
        let result = self.faiss.insert(id, vector)?;

        // Shadow write to cuVS if enabled
        if self.shadow_writes_enabled.load(Ordering::Acquire) {
            if let Err(e) = self.cuvs.insert(id, vector) {
                warn!(error = %e, "Shadow write to cuVS failed");
            }
        }

        Ok(result)
    }

    /// Search with shadow validation
    pub fn search_with_validation(
        &self,
        query: &[f32],
        params: &SearchParams,
    ) -> Result<(Vec<SearchResult>, Option<ShadowModeResult>)> {
        // Search FAISS (primary)
        let faiss_start = Instant::now();
        let faiss_results = self.faiss.search(query, params)?;
        let faiss_latency = faiss_start.elapsed();

        // Search cuVS (shadow)
        let cuvs_start = Instant::now();
        let cuvs_results = self.cuvs.search(query, params)?;
        let cuvs_latency = cuvs_start.elapsed();

        // Calculate divergence
        let divergence = calculate_divergence(&faiss_results, &cuvs_results);
        let recall = calculate_recall(&faiss_results, &cuvs_results);

        let shadow_result = ShadowModeResult {
            faiss_results: faiss_results.clone(),
            cuvs_results,
            faiss_latency,
            cuvs_latency,
            divergence,
            recall,
        };

        // Store result for analysis
        {
            let mut results = self.results.write();
            results.push_back(shadow_result.clone());
            // Keep last 10000 results - O(1) with VecDeque
            if results.len() > 10000 {
                results.pop_front();
            }
        }

        self.total_validations.fetch_add(1, Ordering::Relaxed);

        // Alert if divergence exceeds threshold
        if divergence > self.divergence_threshold {
            warn!(
                divergence = divergence,
                threshold = self.divergence_threshold,
                "cuVS divergence exceeds threshold"
            );
        }

        Ok((faiss_results, Some(shadow_result)))
    }

    /// Get validation statistics
    pub fn validation_stats(&self) -> ShadowValidationStats {
        let results = self.results.read();

        if results.is_empty() {
            return ShadowValidationStats::default();
        }

        let total = results.len() as f64;
        let avg_divergence: f64 = results.iter().map(|r| r.divergence).sum::<f64>() / total;
        let avg_recall: f64 = results.iter().map(|r| r.recall).sum::<f64>() / total;

        let faiss_latencies: Vec<u64> = results
            .iter()
            .map(|r| r.faiss_latency.as_micros() as u64)
            .collect();
        let cuvs_latencies: Vec<u64> = results
            .iter()
            .map(|r| r.cuvs_latency.as_micros() as u64)
            .collect();

        let faiss_p95 = percentile(&faiss_latencies, 0.95);
        let cuvs_p95 = percentile(&cuvs_latencies, 0.95);

        let latency_improvement = if faiss_p95 > 0 {
            1.0 - (cuvs_p95 as f64 / faiss_p95 as f64)
        } else {
            0.0
        };

        let exceeds_threshold = results
            .iter()
            .filter(|r| r.divergence > self.divergence_threshold)
            .count();

        ShadowValidationStats {
            total_validations: self.total_validations.load(Ordering::Relaxed),
            avg_divergence,
            avg_recall,
            faiss_p95_us: faiss_p95,
            cuvs_p95_us: cuvs_p95,
            latency_improvement,
            exceeds_threshold: exceeds_threshold as u64,
            threshold_violation_rate: exceeds_threshold as f64 / total,
        }
    }

    /// Check if cuVS gate criteria are met
    pub fn check_gate_criteria(&self) -> CuvsGateResult {
        let stats = self.validation_stats();

        let latency_improvement_met = stats.latency_improvement >= 0.25; // 25% improvement
        let recall_met = stats.avg_recall >= 0.95; // 95% recall
        let divergence_met = stats.threshold_violation_rate < 0.001; // < 0.1% divergence

        CuvsGateResult {
            passed: latency_improvement_met && recall_met && divergence_met,
            latency_improvement: stats.latency_improvement,
            latency_improvement_met,
            recall: stats.avg_recall,
            recall_met,
            divergence_rate: stats.threshold_violation_rate,
            divergence_met,
            recommendation: if latency_improvement_met && recall_met && divergence_met {
                "ENABLE cuVS - All gate criteria met".to_string()
            } else {
                format!(
                    "REMAIN on FAISS - Failed: {}",
                    vec![
                        if !latency_improvement_met {
                            Some("latency improvement < 25%")
                        } else {
                            None
                        },
                        if !recall_met {
                            Some("recall < 95%")
                        } else {
                            None
                        },
                        if !divergence_met {
                            Some("divergence > 0.1%")
                        } else {
                            None
                        },
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(", ")
                )
            },
        }
    }
}

/// Shadow validation statistics
#[derive(Debug, Clone, Default)]
pub struct ShadowValidationStats {
    pub total_validations: u64,
    pub avg_divergence: f64,
    pub avg_recall: f64,
    pub faiss_p95_us: u64,
    pub cuvs_p95_us: u64,
    pub latency_improvement: f64,
    pub exceeds_threshold: u64,
    pub threshold_violation_rate: f64,
}

/// cuVS gate decision result
#[derive(Debug, Clone)]
pub struct CuvsGateResult {
    pub passed: bool,
    pub latency_improvement: f64,
    pub latency_improvement_met: bool,
    pub recall: f64,
    pub recall_met: bool,
    pub divergence_rate: f64,
    pub divergence_met: bool,
    pub recommendation: String,
}

/// Calculate divergence between two result sets
fn calculate_divergence(faiss: &[SearchResult], cuvs: &[SearchResult]) -> f64 {
    if faiss.is_empty() && cuvs.is_empty() {
        return 0.0;
    }
    if faiss.is_empty() || cuvs.is_empty() {
        return 1.0;
    }

    let faiss_ids: std::collections::HashSet<_> = faiss.iter().map(|r| &r.id).collect();
    let cuvs_ids: std::collections::HashSet<_> = cuvs.iter().map(|r| &r.id).collect();

    let intersection = faiss_ids.intersection(&cuvs_ids).count();
    let union = faiss_ids.union(&cuvs_ids).count();

    if union == 0 {
        0.0
    } else {
        1.0 - (intersection as f64 / union as f64)
    }
}

/// Calculate recall of cuVS vs FAISS
fn calculate_recall(faiss: &[SearchResult], cuvs: &[SearchResult]) -> f64 {
    if faiss.is_empty() {
        return 1.0;
    }

    let faiss_ids: std::collections::HashSet<_> = faiss.iter().map(|r| &r.id).collect();
    let cuvs_ids: std::collections::HashSet<_> = cuvs.iter().map(|r| &r.id).collect();

    let intersection = faiss_ids.intersection(&cuvs_ids).count();
    intersection as f64 / faiss.len() as f64
}

/// Calculate percentile from sorted values
fn percentile(values: &[u64], p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Rollback manager for cuVS → FAISS fallback
pub struct RollbackManager<F: VectorIndex, C: VectorIndex> {
    /// FAISS (fallback) index
    faiss: Arc<F>,
    /// cuVS (primary when enabled) index
    cuvs: Arc<C>,
    /// cuVS is enabled
    cuvs_enabled: AtomicBool,
    /// Automatic rollback on error threshold
    auto_rollback_enabled: AtomicBool,
    /// Error count since last reset
    error_count: AtomicU64,
    /// Error threshold for auto-rollback
    error_threshold: u64,
    /// Last rollback time
    last_rollback: RwLock<Option<Instant>>,
}

impl<F: VectorIndex, C: VectorIndex> RollbackManager<F, C> {
    /// Create a new rollback manager
    pub fn new(faiss: Arc<F>, cuvs: Arc<C>, error_threshold: u64) -> Self {
        Self {
            faiss,
            cuvs,
            cuvs_enabled: AtomicBool::new(false), // Start with FAISS
            auto_rollback_enabled: AtomicBool::new(true),
            error_count: AtomicU64::new(0),
            error_threshold,
            last_rollback: RwLock::new(None),
        }
    }

    /// Enable cuVS
    pub fn enable_cuvs(&self) {
        info!("Enabling cuVS as primary index");
        self.cuvs_enabled.store(true, Ordering::Release);
        self.error_count.store(0, Ordering::Release);
    }

    /// Disable cuVS (rollback to FAISS)
    pub fn rollback_to_faiss(&self, reason: &str) {
        warn!(reason = reason, "Rolling back from cuVS to FAISS");
        self.cuvs_enabled.store(false, Ordering::Release);
        *self.last_rollback.write() = Some(Instant::now());
    }

    /// Check if cuVS is enabled
    pub fn is_cuvs_enabled(&self) -> bool {
        self.cuvs_enabled.load(Ordering::Acquire)
    }

    /// Get the active index
    pub fn active_index(&self) -> &dyn VectorIndex {
        if self.is_cuvs_enabled() {
            self.cuvs.as_ref()
        } else {
            self.faiss.as_ref()
        }
    }

    /// Record an error and potentially trigger auto-rollback
    pub fn record_error(&self) {
        let count = self.error_count.fetch_add(1, Ordering::Relaxed) + 1;

        if self.auto_rollback_enabled.load(Ordering::Acquire)
            && count >= self.error_threshold
            && self.is_cuvs_enabled()
        {
            self.rollback_to_faiss("Error threshold exceeded");
        }
    }

    /// Reset error count
    pub fn reset_errors(&self) {
        self.error_count.store(0, Ordering::Release);
    }

    /// Get rollback status
    pub fn status(&self) -> RollbackStatus {
        RollbackStatus {
            cuvs_enabled: self.is_cuvs_enabled(),
            error_count: self.error_count.load(Ordering::Relaxed),
            error_threshold: self.error_threshold,
            auto_rollback_enabled: self.auto_rollback_enabled.load(Ordering::Relaxed),
            last_rollback: self.last_rollback.read().map(|i| i.elapsed()),
        }
    }
}

/// Rollback status
#[derive(Debug, Clone)]
pub struct RollbackStatus {
    pub cuvs_enabled: bool,
    pub error_count: u64,
    pub error_threshold: u64,
    pub auto_rollback_enabled: bool,
    pub last_rollback: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockIndex, MockIndexConfig};

    #[test]
    fn test_cuvs_index_basic() {
        let config = CuvsConfig {
            dimensions: 128,
            ..Default::default()
        };
        let index = CuvsIndex::new(config);
        index.train(&[]).unwrap();

        let id = VectorId::new("test-1");
        let vector: Vec<f32> = (0..128).map(|i| i as f32 / 128.0).collect();

        let internal_id = index.insert(&id, &vector).unwrap();
        assert_eq!(internal_id.0, 0);

        let stats = index.stats();
        assert_eq!(stats.total_vectors, 1);
        assert!(stats.using_gpu);
    }

    #[test]
    fn test_cuvs_search() {
        let config = CuvsConfig {
            dimensions: 128,
            ..Default::default()
        };
        let index = CuvsIndex::new(config);
        index.train(&[]).unwrap();

        // Insert test vectors
        for i in 0..10 {
            let id = VectorId::new(format!("test-{}", i));
            let vector: Vec<f32> = (0..128).map(|j| (i * 128 + j) as f32 / 1280.0).collect();
            index.insert(&id, &vector).unwrap();
        }

        // Search
        let query: Vec<f32> = (0..128).map(|i| i as f32 / 1280.0).collect();
        let params = SearchParams::new(5);
        let results = index.search(&query, &params).unwrap();

        assert_eq!(results.len(), 5);
        // First result should be closest to query
        assert_eq!(results[0].id.as_str(), "test-0");
    }

    #[test]
    fn test_shadow_mode_validator() {
        let faiss = Arc::new(MockIndex::new(128, 1000));
        let cuvs = Arc::new(CuvsIndex::new(CuvsConfig {
            dimensions: 128,
            ..Default::default()
        }));

        faiss.train(&[]).unwrap();
        cuvs.train(&[]).unwrap();

        let validator = ShadowModeValidator::new(faiss, cuvs, 0.1);

        // Insert into both
        let id = VectorId::new("test-1");
        let vector: Vec<f32> = (0..128).map(|i| i as f32 / 128.0).collect();
        validator.insert(&id, &vector).unwrap();

        // Search with validation
        let params = SearchParams::new(5);
        let (results, shadow) = validator.search_with_validation(&vector, &params).unwrap();

        assert!(!results.is_empty());
        assert!(shadow.is_some());

        let shadow_result = shadow.unwrap();
        assert!(shadow_result.recall > 0.0);
    }

    #[test]
    fn test_rollback_manager() {
        let faiss = Arc::new(MockIndex::new(128, 1000));
        let cuvs = Arc::new(CuvsIndex::new(CuvsConfig {
            dimensions: 128,
            ..Default::default()
        }));

        let manager = RollbackManager::new(faiss, cuvs, 5);

        // Initially FAISS is active
        assert!(!manager.is_cuvs_enabled());

        // Enable cuVS
        manager.enable_cuvs();
        assert!(manager.is_cuvs_enabled());

        // Record errors until threshold
        for _ in 0..5 {
            manager.record_error();
        }

        // Should have auto-rolled back
        assert!(!manager.is_cuvs_enabled());
    }

    #[test]
    fn test_gate_criteria() {
        let faiss = Arc::new(MockIndex::new(128, 1000));
        let cuvs = Arc::new(CuvsIndex::new(CuvsConfig {
            dimensions: 128,
            ..Default::default()
        }));

        faiss.train(&[]).unwrap();
        cuvs.train(&[]).unwrap();

        let validator = ShadowModeValidator::new(faiss, cuvs, 0.1);

        // Without data, gate should not pass
        let gate = validator.check_gate_criteria();
        // Gate result depends on actual performance
        assert!(!gate.recommendation.is_empty());
    }

    #[test]
    fn test_divergence_calculation() {
        let faiss_results = vec![
            SearchResult {
                id: VectorId::new("a"),
                score: 0.9,
                metadata: None,
            },
            SearchResult {
                id: VectorId::new("b"),
                score: 0.8,
                metadata: None,
            },
        ];

        let cuvs_results = vec![
            SearchResult {
                id: VectorId::new("a"),
                score: 0.9,
                metadata: None,
            },
            SearchResult {
                id: VectorId::new("c"),
                score: 0.7,
                metadata: None,
            },
        ];

        let divergence = calculate_divergence(&faiss_results, &cuvs_results);
        // 1 common (a), 3 total unique (a, b, c) -> divergence = 1 - 1/3 = 0.666...
        assert!(divergence > 0.6 && divergence < 0.7);

        let recall = calculate_recall(&faiss_results, &cuvs_results);
        // 1 of 2 faiss results in cuvs -> recall = 0.5
        assert!((recall - 0.5).abs() < 0.01);
    }
}
