//! FFI bindings to the FAISS C++ wrapper

#![allow(non_camel_case_types)]
#![allow(dead_code)]

use std::os::raw::{c_char, c_float, c_int, c_void};

/// Opaque handle to GPU index
pub type FaissGpuIndex = c_void;

/// Opaque handle to tombstone bitset
pub type FaissTombstones = c_void;

/// Error codes from FAISS wrapper
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaissError {
    Ok = 0,
    InvalidParam = 1,
    OutOfMemory = 2,
    GpuError = 3,
    NotFound = 4,
    DimensionMismatch = 5,
    IndexNotTrained = 6,
    Internal = 99,
}

impl FaissError {
    pub fn is_ok(self) -> bool {
        self == FaissError::Ok
    }
}

/// Index configuration
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FaissIndexConfig {
    pub dimension: i32,
    pub nlist: i32,
    pub nprobe: i32,
    pub gpu_device_id: i32,
    pub gpu_memory_fraction: f32,
    pub use_float16: i32,
    pub use_precomputed: i32,
}

impl Default for FaissIndexConfig {
    fn default() -> Self {
        Self {
            dimension: 768,
            nlist: 4096,
            nprobe: 32,
            gpu_device_id: 0,
            gpu_memory_fraction: 0.6,
            use_float16: 0,
            use_precomputed: 0,
        }
    }
}

/// Search parameters
#[repr(C)]
#[derive(Debug)]
pub struct FaissSearchParams {
    pub top_k: i32,
    pub nprobe: i32,
    pub tombstone_mask: *const u8,
    pub tombstone_size: usize,
}

impl Default for FaissSearchParams {
    fn default() -> Self {
        Self {
            top_k: 10,
            nprobe: 32,
            tombstone_mask: std::ptr::null(),
            tombstone_size: 0,
        }
    }
}

/// Search result
#[repr(C)]
#[derive(Debug)]
pub struct FaissSearchResult {
    pub ids: *mut i64,
    pub distances: *mut f32,
    pub count: i32,
}

impl Default for FaissSearchResult {
    fn default() -> Self {
        Self {
            ids: std::ptr::null_mut(),
            distances: std::ptr::null_mut(),
            count: 0,
        }
    }
}

/// Index statistics
#[repr(C)]
#[derive(Debug, Default)]
pub struct FaissIndexStats {
    pub total_vectors: u64,
    pub active_vectors: u64,
    pub deleted_vectors: u64,
    pub memory_usage_bytes: u64,
    pub is_trained: i32,
    pub using_gpu: i32,
}

#[cfg(feature = "gpu")]
extern "C" {
    // Index Management
    pub fn faiss_index_create(
        config: *const FaissIndexConfig,
        out_index: *mut *mut FaissGpuIndex,
    ) -> FaissError;

    pub fn faiss_index_free(index: *mut FaissGpuIndex);

    pub fn faiss_index_train(
        index: *mut FaissGpuIndex,
        vectors: *const c_float,
        num_vectors: usize,
    ) -> FaissError;

    pub fn faiss_index_is_trained(index: *const FaissGpuIndex) -> i32;

    // Vector Operations
    pub fn faiss_index_add(
        index: *mut FaissGpuIndex,
        vectors: *const c_float,
        num_vectors: usize,
        out_ids: *mut i64,
    ) -> FaissError;

    pub fn faiss_index_add_with_ids(
        index: *mut FaissGpuIndex,
        vectors: *const c_float,
        ids: *const i64,
        num_vectors: usize,
    ) -> FaissError;

    pub fn faiss_index_search(
        index: *mut FaissGpuIndex,
        queries: *const c_float,
        num_queries: usize,
        params: *const FaissSearchParams,
        out_results: *mut FaissSearchResult,
    ) -> FaissError;

    pub fn faiss_index_get_vector(
        index: *mut FaissGpuIndex,
        id: i64,
        out_vector: *mut c_float,
    ) -> FaissError;

    pub fn faiss_index_size(index: *const FaissGpuIndex) -> usize;

    pub fn faiss_index_stats(
        index: *const FaissGpuIndex,
        out_stats: *mut FaissIndexStats,
    ) -> FaissError;

    // Tombstone Management
    pub fn faiss_tombstones_create(
        capacity: usize,
        gpu_device_id: i32,
        out_tombstones: *mut *mut FaissTombstones,
    ) -> FaissError;

    pub fn faiss_tombstones_free(tombstones: *mut FaissTombstones);

    pub fn faiss_tombstones_set(tombstones: *mut FaissTombstones, id: i64) -> FaissError;

    pub fn faiss_tombstones_is_set(tombstones: *const FaissTombstones, id: i64) -> i32;

    pub fn faiss_tombstones_count(tombstones: *const FaissTombstones) -> usize;

    pub fn faiss_tombstones_ratio(tombstones: *const FaissTombstones, total_vectors: usize) -> f32;

    pub fn faiss_tombstones_clear(tombstones: *mut FaissTombstones, id: i64) -> FaissError;

    pub fn faiss_tombstones_export(
        tombstones: *const FaissTombstones,
        out_buffer: *mut u8,
        buffer_size: usize,
    ) -> FaissError;

    // GPU Memory Management
    pub fn faiss_gpu_available_memory(device_id: i32) -> usize;
    pub fn faiss_gpu_total_memory(device_id: i32) -> usize;
    pub fn faiss_index_to_cpu(index: *mut FaissGpuIndex) -> FaissError;
    pub fn faiss_index_to_gpu(index: *mut FaissGpuIndex, device_id: i32) -> FaissError;

    // Utility
    pub fn faiss_search_result_free(result: *mut FaissSearchResult);
    pub fn faiss_error_message(error: FaissError) -> *const c_char;
}
