//! GPU-accelerated FAISS index implementation

#[cfg(feature = "gpu")]
use crate::ffi::{
    self, FaissError, FaissGpuIndex, FaissIndexConfig, FaissIndexStats, FaissSearchParams,
    FaissSearchResult, FaissTombstones,
};

use crate::index::{IndexStats, SearchParams, VectorIndex};
use crate::tombstone::TombstoneBitset;
use akidb_common::{AkiDbError, InternalId, Result, SearchResult, VectorId};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Configuration for GPU index
#[derive(Debug, Clone)]
pub struct GpuIndexConfig {
    /// Vector dimension
    pub dimension: usize,
    /// Number of IVF clusters (nlist)
    pub nlist: usize,
    /// Default number of probes for search
    pub nprobe: u32,
    /// GPU device ID
    pub device_id: i32,
    /// Fraction of GPU memory to use (0.0 - 1.0)
    pub memory_fraction: f32,
    /// Use FP16 for memory efficiency
    pub use_float16: bool,
    /// Maximum vectors before requiring training
    pub training_threshold: usize,
    /// Tombstone ratio that triggers rebuild
    pub rebuild_threshold: f32,
    /// Enable CPU fallback on GPU OOM
    pub fallback_to_cpu: bool,
}

impl Default for GpuIndexConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            nlist: 4096,
            nprobe: 32,
            device_id: 0,
            memory_fraction: 0.6,
            use_float16: false,
            training_threshold: 100_000,
            rebuild_threshold: 0.10,
            fallback_to_cpu: true,
        }
    }
}

/// GPU-accelerated IVF-Flat index with tombstone support
#[cfg(feature = "gpu")]
pub struct GpuIndex {
    /// Raw pointer to the FAISS GPU index
    index: *mut FaissGpuIndex,
    /// Tombstone bitset for soft deletes
    tombstones: *mut FaissTombstones,
    /// Configuration
    config: GpuIndexConfig,
    /// Whether the index is trained
    is_trained: AtomicBool,
    /// Whether a rebuild is in progress
    rebuilding: AtomicBool,
    /// Whether the index is currently on GPU
    on_gpu: AtomicBool,
    /// Total vector count (including deleted)
    total_count: AtomicU64,
    /// Training vectors buffer
    training_buffer: RwLock<Vec<f32>>,
    /// Lock for thread-safe operations
    lock: RwLock<()>,
}

#[cfg(feature = "gpu")]
unsafe impl Send for GpuIndex {}
#[cfg(feature = "gpu")]
unsafe impl Sync for GpuIndex {}

#[cfg(feature = "gpu")]
impl GpuIndex {
    /// Create a new GPU index
    pub fn new(config: GpuIndexConfig) -> Result<Self> {
        let ffi_config = FaissIndexConfig {
            dimension: config.dimension as i32,
            nlist: config.nlist as i32,
            nprobe: config.nprobe as i32,
            gpu_device_id: config.device_id,
            gpu_memory_fraction: config.memory_fraction,
            use_float16: if config.use_float16 { 1 } else { 0 },
            use_precomputed: 0,
        };

        let mut index: *mut FaissGpuIndex = std::ptr::null_mut();
        let mut tombstones: *mut FaissTombstones = std::ptr::null_mut();

        unsafe {
            let err = ffi::faiss_index_create(&ffi_config, &mut index);
            if !err.is_ok() {
                return Err(AkiDbError::Internal(format!(
                    "Failed to create FAISS index: {:?}",
                    err
                )));
            }

            // Create tombstone bitset with initial capacity of 1M vectors
            let err =
                ffi::faiss_tombstones_create(1_000_000, config.device_id, &mut tombstones);
            if !err.is_ok() {
                ffi::faiss_index_free(index);
                return Err(AkiDbError::Internal(
                    "Failed to create tombstone bitset".into(),
                ));
            }
        }

        info!(
            "Created GPU index: dim={}, nlist={}, device={}",
            config.dimension, config.nlist, config.device_id
        );

        Ok(Self {
            index,
            tombstones,
            config,
            is_trained: AtomicBool::new(false),
            rebuilding: AtomicBool::new(false),
            on_gpu: AtomicBool::new(true),
            total_count: AtomicU64::new(0),
            training_buffer: RwLock::new(Vec::new()),
            lock: RwLock::new(()),
        })
    }

    /// Train the index on accumulated vectors
    fn train_if_needed(&self) -> Result<()> {
        if self.is_trained.load(Ordering::Acquire) {
            return Ok(());
        }

        let buffer = self.training_buffer.read();
        let num_vectors = buffer.len() / self.config.dimension;

        if num_vectors < self.config.nlist {
            // Not enough vectors to train - need at least nlist vectors
            debug!(
                "Not enough vectors to train: {} < {}",
                num_vectors, self.config.nlist
            );
            return Ok(());
        }

        drop(buffer);

        // Acquire write lock for training
        let _lock = self.lock.write();

        // Double-check after acquiring lock
        if self.is_trained.load(Ordering::Acquire) {
            return Ok(());
        }

        let buffer = self.training_buffer.read();
        let num_vectors = buffer.len() / self.config.dimension;

        info!("Training index on {} vectors", num_vectors);

        unsafe {
            let err = ffi::faiss_index_train(self.index, buffer.as_ptr(), num_vectors);
            if !err.is_ok() {
                return Err(AkiDbError::Internal(format!(
                    "Failed to train index: {:?}",
                    err
                )));
            }
        }

        self.is_trained.store(true, Ordering::Release);

        // Add buffered vectors to index
        let mut ids = vec![0i64; num_vectors];
        unsafe {
            let err = ffi::faiss_index_add(self.index, buffer.as_ptr(), num_vectors, ids.as_mut_ptr());
            if !err.is_ok() {
                warn!("Failed to add training vectors to index: {:?}", err);
            }
        }

        self.total_count.store(num_vectors as u64, Ordering::Release);

        info!("Index trained successfully");
        Ok(())
    }

    /// Check if rebuild is needed based on tombstone ratio
    fn check_rebuild_needed(&self) -> bool {
        let total = self.total_count.load(Ordering::Acquire);
        if total == 0 {
            return false;
        }

        unsafe {
            let ratio = ffi::faiss_tombstones_ratio(self.tombstones, total as usize);
            ratio >= self.config.rebuild_threshold
        }
    }

    /// Fallback to CPU if GPU runs out of memory
    ///
    /// BUG-HUNT-015: Fixed race condition in GPU fallback.
    /// Previously, multiple threads could call fallback_to_cpu concurrently,
    /// resulting in double-move UB. Now uses write lock + compare_exchange
    /// to ensure exactly one thread performs the fallback.
    fn fallback_to_cpu(&self) -> Result<()> {
        if !self.config.fallback_to_cpu {
            return Err(AkiDbError::GpuOutOfMemory);
        }

        // Quick check without lock - if already on CPU, return early
        if !self.on_gpu.load(Ordering::Acquire) {
            return Ok(()); // Already on CPU
        }

        // BUG-HUNT-015: Acquire write lock to prevent concurrent fallback attempts
        let _lock = self.lock.write();

        // BUG-HUNT-015: Use compare_exchange to ensure only one thread performs the fallback
        // If on_gpu is currently true, atomically set it to false and proceed
        // If another thread already set it to false, we'll get Err and skip the fallback
        match self.on_gpu.compare_exchange(
            true,
            false,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // We won the race, perform the fallback
                warn!("GPU out of memory, falling back to CPU");

                unsafe {
                    let err = ffi::faiss_index_to_cpu(self.index);
                    if !err.is_ok() {
                        // Rollback on failure
                        self.on_gpu.store(true, Ordering::Release);
                        return Err(AkiDbError::Internal("Failed to move index to CPU".into()));
                    }
                }

                Ok(())
            }
            Err(_) => {
                // Another thread already performed the fallback
                debug!("GPU fallback already completed by another thread");
                Ok(())
            }
        }
    }

    /// Get GPU memory info
    pub fn gpu_memory_info(&self) -> (usize, usize) {
        unsafe {
            let available = ffi::faiss_gpu_available_memory(self.config.device_id);
            let total = ffi::faiss_gpu_total_memory(self.config.device_id);
            (available, total)
        }
    }
}

#[cfg(feature = "gpu")]
impl VectorIndex for GpuIndex {
    fn insert(&self, id: &VectorId, vector: &[f32]) -> Result<InternalId> {
        if vector.len() != self.config.dimension {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.config.dimension,
                actual: vector.len(),
            });
        }

        // If not trained yet, buffer the vector
        if !self.is_trained.load(Ordering::Acquire) {
            let mut buffer = self.training_buffer.write();
            buffer.extend_from_slice(vector);

            let num_vectors = buffer.len() / self.config.dimension;
            if num_vectors >= self.config.training_threshold {
                drop(buffer);
                self.train_if_needed()?;
            } else {
                // Return a temporary ID
                return Ok(InternalId((num_vectors - 1) as i64));
            }
        }

        // BUG-HUNT-015: Simplified lock handling for GPU fallback
        // First attempt with read lock
        let mut out_id: i64 = 0;
        let needs_fallback = {
            let _lock = self.lock.read();
            unsafe {
                let err = ffi::faiss_index_add(self.index, vector.as_ptr(), 1, &mut out_id);
                if err == FaissError::OutOfMemory {
                    true // Need fallback
                } else if !err.is_ok() {
                    return Err(AkiDbError::Internal(format!("Insert failed: {:?}", err)));
                } else {
                    false // Success
                }
            }
        };

        // If GPU OOM, fallback and retry (fallback_to_cpu handles its own locking)
        if needs_fallback {
            self.fallback_to_cpu()?;

            // Retry after fallback
            let _lock = self.lock.read();
            unsafe {
                let err = ffi::faiss_index_add(self.index, vector.as_ptr(), 1, &mut out_id);
                if !err.is_ok() {
                    return Err(AkiDbError::Internal(format!("Insert failed after CPU fallback: {:?}", err)));
                }
            }
        }

        self.total_count.fetch_add(1, Ordering::AcqRel);

        debug!("Inserted vector {} with internal ID {}", id, out_id);
        Ok(InternalId(out_id))
    }

    fn insert_batch(&self, vectors: &[(VectorId, Vec<f32>)]) -> Result<Vec<InternalId>> {
        vectors
            .iter()
            .map(|(id, vec)| self.insert(id, vec))
            .collect()
    }

    fn search(&self, query: &[f32], params: &SearchParams) -> Result<Vec<SearchResult>> {
        if query.len() != self.config.dimension {
            return Err(AkiDbError::DimensionMismatch {
                expected: self.config.dimension,
                actual: query.len(),
            });
        }

        if !self.is_trained.load(Ordering::Acquire) {
            // Return empty results if not trained
            return Ok(Vec::new());
        }

        let _lock = self.lock.read();

        // Export tombstone bitset for filtering
        let tombstone_size = (self.total_count.load(Ordering::Acquire) as usize + 7) / 8;
        let mut tombstone_mask = vec![0u8; tombstone_size];

        unsafe {
            ffi::faiss_tombstones_export(
                self.tombstones,
                tombstone_mask.as_mut_ptr(),
                tombstone_size,
            );
        }

        let ffi_params = FaissSearchParams {
            top_k: params.top_k as i32,
            nprobe: params.nprobe as i32,
            tombstone_mask: tombstone_mask.as_ptr(),
            tombstone_size,
        };

        let mut result = FaissSearchResult::default();

        unsafe {
            let err = ffi::faiss_index_search(self.index, query.as_ptr(), 1, &ffi_params, &mut result);
            if !err.is_ok() {
                return Err(AkiDbError::Internal(format!("Search failed: {:?}", err)));
            }

            let mut results = Vec::with_capacity(params.top_k);

            for i in 0..result.count as usize {
                let id = *result.ids.add(i);
                let distance = *result.distances.add(i);

                // Skip invalid results (tombstoned or not found)
                if id < 0 {
                    continue;
                }

                // Convert L2 distance to similarity score
                let score = 1.0 / (1.0 + distance);

                // Convert internal ID to VectorId - the actual mapping is handled by the service layer
                let vector_id = VectorId::new(format!("__internal_{}", id));

                // Apply filter if provided
                if let Some(ref filter) = params.filter {
                    if !filter(&vector_id) {
                        continue;
                    }
                }

                results.push(SearchResult::new(vector_id, score));
            }

            ffi::faiss_search_result_free(&mut result);

            Ok(results)
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
        let _lock = self.lock.read();

        unsafe {
            let err = ffi::faiss_tombstones_set(self.tombstones, internal_id.0);
            if !err.is_ok() {
                return Err(AkiDbError::Internal(format!(
                    "Failed to set tombstone: {:?}",
                    err
                )));
            }
        }

        // Check if rebuild is needed
        if self.check_rebuild_needed() {
            info!("Tombstone ratio exceeded threshold, rebuild recommended");
        }

        Ok(())
    }

    fn is_deleted(&self, internal_id: InternalId) -> bool {
        unsafe { ffi::faiss_tombstones_is_set(self.tombstones, internal_id.0) != 0 }
    }

    fn get_vector(&self, internal_id: InternalId) -> Result<Option<Vec<f32>>> {
        // Check tombstone first
        if self.is_deleted(internal_id) {
            return Ok(None);
        }

        let _lock = self.lock.read();

        let mut vector = vec![0.0f32; self.config.dimension];

        unsafe {
            let err = ffi::faiss_index_get_vector(self.index, internal_id.0, vector.as_mut_ptr());
            match err {
                FaissError::Ok => Ok(Some(vector)),
                FaissError::NotFound => Ok(None),
                _ => Err(AkiDbError::Internal(format!(
                    "Failed to get vector: {:?}",
                    err
                ))),
            }
        }
    }

    fn stats(&self) -> IndexStats {
        let mut ffi_stats = FaissIndexStats::default();

        unsafe {
            ffi::faiss_index_stats(self.index, &mut ffi_stats);
        }

        let deleted = unsafe { ffi::faiss_tombstones_count(self.tombstones) as u64 };

        IndexStats {
            total_vectors: ffi_stats.total_vectors,
            active_vectors: ffi_stats.total_vectors.saturating_sub(deleted),
            tombstoned_vectors: deleted,
            dimensions: self.config.dimension,
            memory_bytes: ffi_stats.memory_usage_bytes,
            gpu_memory_bytes: Some(ffi_stats.memory_usage_bytes),
            using_gpu: ffi_stats.using_gpu != 0,
            rebuild_in_progress: self.rebuilding.load(Ordering::Acquire),
        }
    }

    fn dimensions(&self) -> usize {
        self.config.dimension
    }

    fn is_ready(&self) -> bool {
        self.is_trained.load(Ordering::Acquire)
    }

    fn train(&self, training_data: &[f32]) -> Result<()> {
        let num_vectors = training_data.len() / self.config.dimension;

        if num_vectors < self.config.nlist {
            return Err(AkiDbError::InvalidParameter(format!(
                "Need at least {} vectors to train, got {}",
                self.config.nlist, num_vectors
            )));
        }

        let _lock = self.lock.write();

        unsafe {
            let err = ffi::faiss_index_train(self.index, training_data.as_ptr(), num_vectors);
            if !err.is_ok() {
                return Err(AkiDbError::Internal(format!("Training failed: {:?}", err)));
            }
        }

        self.is_trained.store(true, Ordering::Release);
        info!("Index trained on {} vectors", num_vectors);

        Ok(())
    }

    fn trigger_rebuild(&self) -> Result<()> {
        self.rebuilding.store(true, Ordering::Release);
        // TODO: Implement actual rebuild logic
        warn!("Rebuild triggered but not yet implemented");
        self.rebuilding.store(false, Ordering::Release);
        Ok(())
    }

    fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(Ordering::Acquire)
    }
}

#[cfg(feature = "gpu")]
impl Drop for GpuIndex {
    fn drop(&mut self) {
        unsafe {
            if !self.tombstones.is_null() {
                ffi::faiss_tombstones_free(self.tombstones);
            }
            if !self.index.is_null() {
                ffi::faiss_index_free(self.index);
            }
        }
    }
}

// Re-export CpuIndex as GpuIndex when gpu feature is disabled
#[cfg(not(feature = "gpu"))]
pub use crate::cpu::CpuIndex as GpuIndex;

#[cfg(not(feature = "gpu"))]
pub use crate::mock::MockIndexConfig as GpuIndexConfig;
