#include "faiss_wrapper.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/utils/DeviceUtils.h>

#include <cuda_runtime.h>

#include <memory>
#include <vector>
#include <mutex>
#include <cstring>
#include <unordered_set>

// ============================================================================
// Internal Structures
// ============================================================================

struct FaissGpuIndex {
    std::unique_ptr<faiss::gpu::StandardGpuResources> resources;
    std::unique_ptr<faiss::gpu::GpuIndexIVFFlat> gpu_index;
    std::unique_ptr<faiss::IndexIVFFlat> cpu_index;
    std::unique_ptr<faiss::IndexFlatL2> quantizer;

    int32_t dimension;
    int32_t nlist;
    int32_t default_nprobe;
    int32_t gpu_device_id;
    bool on_gpu;
    bool is_trained;

    int64_t next_id;
    std::mutex mutex;

    // ID to position mapping for reconstruction
    std::vector<int64_t> id_map;
    std::unordered_set<int64_t> id_set;
};

struct FaissTombstones {
    std::vector<uint8_t> bitset;
    size_t capacity;
    size_t count;
    int32_t gpu_device_id;
    std::mutex mutex;

    // GPU-side bitset (optional, for GPU-accelerated filtering)
    uint8_t* gpu_bitset;
    bool gpu_allocated;
};

// ============================================================================
// Error Messages
// ============================================================================

static const char* error_messages[] = {
    "Success",           // FAISS_OK = 0
    "Invalid parameter", // FAISS_ERR_INVALID_PARAM = 1
    "Out of memory",     // FAISS_ERR_OUT_OF_MEMORY = 2
    "GPU error",         // FAISS_ERR_GPU_ERROR = 3
    "Not found",         // FAISS_ERR_NOT_FOUND = 4
    "Dimension mismatch",// FAISS_ERR_DIMENSION_MISMATCH = 5
    "Index not trained", // FAISS_ERR_INDEX_NOT_TRAINED = 6
    "Internal error"     // FAISS_ERR_INTERNAL = 7
};

const char* faiss_error_message(FaissError error) {
    if (error >= 0 && error < sizeof(error_messages) / sizeof(error_messages[0])) {
        return error_messages[error];
    }
    return "Unknown error";
}

// ============================================================================
// Index Management
// ============================================================================

FaissError faiss_index_create(
    const FaissIndexConfig* config,
    FaissGpuIndex** out_index
) {
    if (!config || !out_index || config->dimension <= 0 || config->nlist <= 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    try {
        auto index = new FaissGpuIndex();
        index->dimension = config->dimension;
        index->nlist = config->nlist;
        index->default_nprobe = config->nprobe > 0 ? config->nprobe : 32;
        index->gpu_device_id = config->gpu_device_id;
        index->on_gpu = false;
        index->is_trained = false;
        index->next_id = 0;

        // Create GPU resources
        index->resources = std::make_unique<faiss::gpu::StandardGpuResources>();

        // Set memory fraction if specified
        // BUG-HUNT-010: Fixed cudaMemGetInfo nullptr UB - must pass valid pointers
        if (config->gpu_memory_fraction > 0 && config->gpu_memory_fraction <= 1.0f) {
            size_t free_mem = 0, total_mem = 0;
            cudaSetDevice(config->gpu_device_id);
            cudaError_t err = cudaMemGetInfo(&free_mem, &total_mem);
            if (err == cudaSuccess && total_mem > 0) {
                size_t max_mem = static_cast<size_t>(total_mem * config->gpu_memory_fraction);
                index->resources->setTempMemory(max_mem / 4);  // Temp memory is ~25% of allocation
            }
        }

        // Create quantizer (flat L2 index for centroids)
        index->quantizer = std::make_unique<faiss::IndexFlatL2>(config->dimension);

        // Create CPU IVF index first
        index->cpu_index = std::make_unique<faiss::IndexIVFFlat>(
            index->quantizer.get(),
            config->dimension,
            config->nlist
        );
        index->cpu_index->nprobe = index->default_nprobe;

        *out_index = index;
        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_INTERNAL;
    }
}

void faiss_index_free(FaissGpuIndex* index) {
    if (index) {
        delete index;
    }
}

FaissError faiss_index_train(
    FaissGpuIndex* index,
    const float* vectors,
    size_t num_vectors
) {
    if (!index || !vectors || num_vectors == 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    try {
        // Train the CPU index first
        index->cpu_index->train(num_vectors, vectors);
        index->is_trained = true;

        // Move to GPU using GpuIndexIVFFlatConfig (FAISS 1.8.0+ API)
        faiss::gpu::GpuIndexIVFFlatConfig config;
        config.interleavedLayout = true;

        index->gpu_index = std::make_unique<faiss::gpu::GpuIndexIVFFlat>(
            index->resources.get(),
            index->cpu_index.get(),
            config
        );
        // Set nprobe directly (FAISS 1.8.0+ API)
        index->gpu_index->nprobe = index->default_nprobe;
        index->on_gpu = true;

        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_INTERNAL;
    }
}

int32_t faiss_index_is_trained(const FaissGpuIndex* index) {
    return index ? index->is_trained : 0;
}

// ============================================================================
// Vector Operations
// ============================================================================

FaissError faiss_index_add(
    FaissGpuIndex* index,
    const float* vectors,
    size_t num_vectors,
    int64_t* out_ids
) {
    if (!index || !vectors || num_vectors == 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    if (!index->is_trained) {
        return FAISS_ERR_INDEX_NOT_TRAINED;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    try {
        // Generate sequential IDs
        std::vector<int64_t> ids(num_vectors);
        for (size_t i = 0; i < num_vectors; i++) {
            ids[i] = index->next_id++;
            index->id_map.push_back(ids[i]);
            index->id_set.insert(ids[i]);
        }

        if (index->on_gpu && index->gpu_index) {
            index->gpu_index->add_with_ids(num_vectors, vectors, ids.data());
        } else {
            index->cpu_index->add_with_ids(num_vectors, vectors, ids.data());
        }

        if (out_ids) {
            std::memcpy(out_ids, ids.data(), num_vectors * sizeof(int64_t));
        }

        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_INTERNAL;
    }
}

FaissError faiss_index_add_with_ids(
    FaissGpuIndex* index,
    const float* vectors,
    const int64_t* ids,
    size_t num_vectors
) {
    if (!index || !vectors || !ids || num_vectors == 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    if (!index->is_trained) {
        return FAISS_ERR_INDEX_NOT_TRAINED;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    try {
        for (size_t i = 0; i < num_vectors; i++) {
            index->id_map.push_back(ids[i]);
            index->id_set.insert(ids[i]);
            if (ids[i] >= index->next_id) {
                index->next_id = ids[i] + 1;
            }
        }

        if (index->on_gpu && index->gpu_index) {
            index->gpu_index->add_with_ids(num_vectors, vectors, ids);
        } else {
            index->cpu_index->add_with_ids(num_vectors, vectors, ids);
        }

        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_INTERNAL;
    }
}

FaissError faiss_index_search(
    FaissGpuIndex* index,
    const float* queries,
    size_t num_queries,
    const FaissSearchParams* params,
    FaissSearchResult* out_results
) {
    if (!index || !queries || num_queries == 0 || !params || !out_results) {
        return FAISS_ERR_INVALID_PARAM;
    }

    if (!index->is_trained) {
        return FAISS_ERR_INDEX_NOT_TRAINED;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    // BUG-HUNT-011: Validate k and check for integer overflow before allocation
    int32_t k = params->top_k;
    if (k <= 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    // Check for integer overflow: num_queries * k must not exceed SIZE_MAX
    if (num_queries > SIZE_MAX / static_cast<size_t>(k)) {
        return FAISS_ERR_INVALID_PARAM;  // Would overflow
    }
    size_t result_count = num_queries * static_cast<size_t>(k);

    // BUG-HUNT-012: Use unique_ptr for exception-safe memory management
    std::unique_ptr<int64_t[]> ids_buffer(new int64_t[result_count]);
    std::unique_ptr<float[]> distances_buffer(new float[result_count]);

    try {
        int32_t nprobe = params->nprobe > 0 ? params->nprobe : index->default_nprobe;

        if (index->on_gpu && index->gpu_index) {
            // Set nprobe directly (FAISS 1.8.0+ API)
            index->gpu_index->nprobe = nprobe;
            index->gpu_index->search(num_queries, queries, k,
                                     distances_buffer.get(), ids_buffer.get());
        } else {
            index->cpu_index->nprobe = nprobe;
            index->cpu_index->search(num_queries, queries, k,
                                    distances_buffer.get(), ids_buffer.get());
        }

        // Filter tombstones if provided
        if (params->tombstone_mask && params->tombstone_size > 0) {
            for (size_t q = 0; q < num_queries; q++) {
                size_t base_idx = q * k;

                for (int32_t i = 0; i < k; i++) {
                    int64_t id = ids_buffer[base_idx + i];
                    if (id >= 0 && static_cast<size_t>(id) < params->tombstone_size * 8) {
                        size_t byte_idx = id / 8;
                        size_t bit_idx = id % 8;
                        if (params->tombstone_mask[byte_idx] & (1 << bit_idx)) {
                            // This ID is tombstoned, mark as invalid
                            ids_buffer[base_idx + i] = -1;
                            distances_buffer[base_idx + i] = std::numeric_limits<float>::max();
                        }
                    }
                }
            }
        }

        // Success - transfer ownership to output (no memory leak on exception path)
        out_results->ids = ids_buffer.release();
        out_results->distances = distances_buffer.release();
        out_results->count = k;

        return FAISS_OK;

    } catch (const std::exception& e) {
        // BUG-HUNT-012: unique_ptr automatically frees memory on exception
        return FAISS_ERR_INTERNAL;
    }
}

FaissError faiss_index_get_vector(
    FaissGpuIndex* index,
    int64_t id,
    float* out_vector
) {
    if (!index || !out_vector || id < 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    if (index->id_set.find(id) == index->id_set.end()) {
        return FAISS_ERR_NOT_FOUND;
    }

    try {
        if (index->on_gpu && index->gpu_index) {
            // Need to copy from GPU - reconstruct by ID
            index->gpu_index->reconstruct(id, out_vector);
        } else {
            index->cpu_index->reconstruct(id, out_vector);
        }
        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_NOT_FOUND;
    }
}

size_t faiss_index_size(const FaissGpuIndex* index) {
    if (!index) return 0;

    if (index->on_gpu && index->gpu_index) {
        return index->gpu_index->ntotal;
    } else if (index->cpu_index) {
        return index->cpu_index->ntotal;
    }
    return 0;
}

FaissError faiss_index_stats(
    const FaissGpuIndex* index,
    FaissIndexStats* out_stats
) {
    if (!index || !out_stats) {
        return FAISS_ERR_INVALID_PARAM;
    }

    out_stats->total_vectors = faiss_index_size(index);
    out_stats->active_vectors = out_stats->total_vectors;  // Will be adjusted by tombstones
    out_stats->deleted_vectors = 0;
    out_stats->is_trained = index->is_trained;
    out_stats->using_gpu = index->on_gpu;

    // Estimate memory usage
    size_t vec_size = index->dimension * sizeof(float);
    out_stats->memory_usage_bytes = out_stats->total_vectors * vec_size;

    return FAISS_OK;
}

// ============================================================================
// Tombstone Management
// ============================================================================

FaissError faiss_tombstones_create(
    size_t capacity,
    int32_t gpu_device_id,
    FaissTombstones** out_tombstones
) {
    if (!out_tombstones || capacity == 0) {
        return FAISS_ERR_INVALID_PARAM;
    }

    try {
        auto tombstones = new FaissTombstones();
        size_t byte_size = (capacity + 7) / 8;
        tombstones->bitset.resize(byte_size, 0);
        tombstones->capacity = capacity;
        tombstones->count = 0;
        tombstones->gpu_device_id = gpu_device_id;
        tombstones->gpu_bitset = nullptr;
        tombstones->gpu_allocated = false;

        // Optionally allocate GPU bitset for GPU-accelerated filtering
        if (gpu_device_id >= 0) {
            cudaError_t err = cudaMalloc(&tombstones->gpu_bitset, byte_size);
            if (err == cudaSuccess) {
                cudaMemset(tombstones->gpu_bitset, 0, byte_size);
                tombstones->gpu_allocated = true;
            }
        }

        *out_tombstones = tombstones;
        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_OUT_OF_MEMORY;
    }
}

void faiss_tombstones_free(FaissTombstones* tombstones) {
    if (tombstones) {
        if (tombstones->gpu_allocated && tombstones->gpu_bitset) {
            cudaFree(tombstones->gpu_bitset);
        }
        delete tombstones;
    }
}

FaissError faiss_tombstones_set(FaissTombstones* tombstones, int64_t id) {
    if (!tombstones || id < 0 || static_cast<size_t>(id) >= tombstones->capacity) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(tombstones->mutex);

    size_t byte_idx = id / 8;
    size_t bit_idx = id % 8;

    if (!(tombstones->bitset[byte_idx] & (1 << bit_idx))) {
        tombstones->bitset[byte_idx] |= (1 << bit_idx);
        tombstones->count++;

        // Sync to GPU if allocated
        if (tombstones->gpu_allocated && tombstones->gpu_bitset) {
            cudaMemcpy(tombstones->gpu_bitset + byte_idx,
                      &tombstones->bitset[byte_idx], 1,
                      cudaMemcpyHostToDevice);
        }
    }

    return FAISS_OK;
}

int32_t faiss_tombstones_is_set(const FaissTombstones* tombstones, int64_t id) {
    if (!tombstones || id < 0 || static_cast<size_t>(id) >= tombstones->capacity) {
        return 0;
    }

    size_t byte_idx = id / 8;
    size_t bit_idx = id % 8;

    return (tombstones->bitset[byte_idx] & (1 << bit_idx)) ? 1 : 0;
}

size_t faiss_tombstones_count(const FaissTombstones* tombstones) {
    return tombstones ? tombstones->count : 0;
}

float faiss_tombstones_ratio(const FaissTombstones* tombstones, size_t total_vectors) {
    if (!tombstones || total_vectors == 0) {
        return 0.0f;
    }
    return static_cast<float>(tombstones->count) / static_cast<float>(total_vectors);
}

FaissError faiss_tombstones_clear(FaissTombstones* tombstones, int64_t id) {
    if (!tombstones || id < 0 || static_cast<size_t>(id) >= tombstones->capacity) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(tombstones->mutex);

    size_t byte_idx = id / 8;
    size_t bit_idx = id % 8;

    if (tombstones->bitset[byte_idx] & (1 << bit_idx)) {
        tombstones->bitset[byte_idx] &= ~(1 << bit_idx);
        tombstones->count--;

        if (tombstones->gpu_allocated && tombstones->gpu_bitset) {
            cudaMemcpy(tombstones->gpu_bitset + byte_idx,
                      &tombstones->bitset[byte_idx], 1,
                      cudaMemcpyHostToDevice);
        }
    }

    return FAISS_OK;
}

FaissError faiss_tombstones_export(
    const FaissTombstones* tombstones,
    uint8_t* out_buffer,
    size_t buffer_size
) {
    if (!tombstones || !out_buffer) {
        return FAISS_ERR_INVALID_PARAM;
    }

    size_t copy_size = std::min(buffer_size, tombstones->bitset.size());
    std::memcpy(out_buffer, tombstones->bitset.data(), copy_size);

    return FAISS_OK;
}

// ============================================================================
// GPU Memory Management
// ============================================================================

size_t faiss_gpu_available_memory(int32_t device_id) {
    size_t free_mem = 0, total_mem = 0;
    cudaSetDevice(device_id);
    cudaMemGetInfo(&free_mem, &total_mem);
    return free_mem;
}

size_t faiss_gpu_total_memory(int32_t device_id) {
    size_t free_mem = 0, total_mem = 0;
    cudaSetDevice(device_id);
    cudaMemGetInfo(&free_mem, &total_mem);
    return total_mem;
}

FaissError faiss_index_to_cpu(FaissGpuIndex* index) {
    if (!index) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    if (!index->on_gpu) {
        return FAISS_OK;  // Already on CPU
    }

    try {
        // Copy data back to CPU index using copyTo (FAISS 1.8.0+ recommended API)
        if (index->gpu_index) {
            // Create new CPU index to receive the data
            auto new_cpu_index = std::make_unique<faiss::IndexIVFFlat>(
                index->quantizer.get(),
                index->dimension,
                index->nlist
            );
            index->gpu_index->copyTo(new_cpu_index.get());
            index->cpu_index = std::move(new_cpu_index);
            index->gpu_index.reset();
        }
        index->on_gpu = false;
        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_INTERNAL;
    }
}

FaissError faiss_index_to_gpu(FaissGpuIndex* index, int32_t device_id) {
    if (!index) {
        return FAISS_ERR_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(index->mutex);

    if (index->on_gpu) {
        return FAISS_OK;  // Already on GPU
    }

    try {
        faiss::gpu::GpuIndexIVFFlatConfig config;
        config.interleavedLayout = true;
        index->gpu_index = std::make_unique<faiss::gpu::GpuIndexIVFFlat>(
            index->resources.get(),
            index->cpu_index.get(),
            config
        );
        // Set nprobe directly (FAISS 1.8.0+ API)
        index->gpu_index->nprobe = index->default_nprobe;
        index->on_gpu = true;
        index->gpu_device_id = device_id;
        return FAISS_OK;

    } catch (const std::exception& e) {
        return FAISS_ERR_GPU_ERROR;
    }
}

// ============================================================================
// Utility
// ============================================================================

void faiss_search_result_free(FaissSearchResult* result) {
    if (result) {
        delete[] result->ids;
        delete[] result->distances;
        result->ids = nullptr;
        result->distances = nullptr;
        result->count = 0;
    }
}
