#ifndef FAISS_WRAPPER_H
#define FAISS_WRAPPER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle types
typedef struct FaissGpuIndex FaissGpuIndex;
typedef struct FaissTombstones FaissTombstones;

// Error codes
typedef enum {
    FAISS_OK = 0,
    FAISS_ERR_INVALID_PARAM = 1,
    FAISS_ERR_OUT_OF_MEMORY = 2,
    FAISS_ERR_GPU_ERROR = 3,
    FAISS_ERR_NOT_FOUND = 4,
    FAISS_ERR_DIMENSION_MISMATCH = 5,
    FAISS_ERR_INDEX_NOT_TRAINED = 6,
    FAISS_ERR_INTERNAL = 99,
} FaissError;

// Index configuration
typedef struct {
    int32_t dimension;
    int32_t nlist;           // Number of IVF clusters
    int32_t nprobe;          // Default probes for search
    int32_t gpu_device_id;
    float gpu_memory_fraction;
    int32_t use_float16;     // Use FP16 for memory efficiency
    int32_t use_precomputed; // Precomputed codes for faster search
} FaissIndexConfig;

// Search parameters
typedef struct {
    int32_t top_k;
    int32_t nprobe;
    const uint8_t* tombstone_mask;  // Optional: filter deleted vectors
    size_t tombstone_size;
} FaissSearchParams;

// Search result
typedef struct {
    int64_t* ids;
    float* distances;
    int32_t count;
} FaissSearchResult;

// Index statistics
typedef struct {
    uint64_t total_vectors;
    uint64_t active_vectors;
    uint64_t deleted_vectors;
    uint64_t memory_usage_bytes;
    int32_t is_trained;
    int32_t using_gpu;
} FaissIndexStats;

// ============================================================================
// Index Management
// ============================================================================

// Create a new GPU IVF-Flat index
FaissError faiss_index_create(
    const FaissIndexConfig* config,
    FaissGpuIndex** out_index
);

// Free an index
void faiss_index_free(FaissGpuIndex* index);

// Train the index on a set of vectors
FaissError faiss_index_train(
    FaissGpuIndex* index,
    const float* vectors,
    size_t num_vectors
);

// Check if index is trained
int32_t faiss_index_is_trained(const FaissGpuIndex* index);

// ============================================================================
// Vector Operations
// ============================================================================

// Add vectors to the index (returns assigned IDs)
FaissError faiss_index_add(
    FaissGpuIndex* index,
    const float* vectors,
    size_t num_vectors,
    int64_t* out_ids
);

// Add vectors with specific IDs
FaissError faiss_index_add_with_ids(
    FaissGpuIndex* index,
    const float* vectors,
    const int64_t* ids,
    size_t num_vectors
);

// Search for similar vectors
FaissError faiss_index_search(
    FaissGpuIndex* index,
    const float* queries,
    size_t num_queries,
    const FaissSearchParams* params,
    FaissSearchResult* out_results
);

// Get a vector by ID (returns 0 if found, error otherwise)
FaissError faiss_index_get_vector(
    FaissGpuIndex* index,
    int64_t id,
    float* out_vector
);

// Get current number of vectors
size_t faiss_index_size(const FaissGpuIndex* index);

// Get index statistics
FaissError faiss_index_stats(
    const FaissGpuIndex* index,
    FaissIndexStats* out_stats
);

// ============================================================================
// Tombstone Management (GPU-accelerated bitset)
// ============================================================================

// Create a tombstone bitset
FaissError faiss_tombstones_create(
    size_t capacity,
    int32_t gpu_device_id,
    FaissTombstones** out_tombstones
);

// Free tombstones
void faiss_tombstones_free(FaissTombstones* tombstones);

// Mark a vector as deleted
FaissError faiss_tombstones_set(
    FaissTombstones* tombstones,
    int64_t id
);

// Check if a vector is deleted
int32_t faiss_tombstones_is_set(
    const FaissTombstones* tombstones,
    int64_t id
);

// Get count of deleted vectors
size_t faiss_tombstones_count(const FaissTombstones* tombstones);

// Get tombstone ratio (deleted / total)
float faiss_tombstones_ratio(
    const FaissTombstones* tombstones,
    size_t total_vectors
);

// Clear a tombstone (undelete)
FaissError faiss_tombstones_clear(
    FaissTombstones* tombstones,
    int64_t id
);

// Export tombstone bitset for search filtering
FaissError faiss_tombstones_export(
    const FaissTombstones* tombstones,
    uint8_t* out_buffer,
    size_t buffer_size
);

// ============================================================================
// GPU Memory Management
// ============================================================================

// Get available GPU memory in bytes
size_t faiss_gpu_available_memory(int32_t device_id);

// Get total GPU memory in bytes
size_t faiss_gpu_total_memory(int32_t device_id);

// Transfer index to CPU (for fallback)
FaissError faiss_index_to_cpu(FaissGpuIndex* index);

// Transfer index back to GPU
FaissError faiss_index_to_gpu(FaissGpuIndex* index, int32_t device_id);

// ============================================================================
// Utility
// ============================================================================

// Free search result memory
void faiss_search_result_free(FaissSearchResult* result);

// Get error message for error code
const char* faiss_error_message(FaissError error);

#ifdef __cplusplus
}
#endif

#endif // FAISS_WRAPPER_H
