#!/bin/bash
# FAISS Benchmark Script for Thor
# Run this to establish performance baseline

set -e

echo "=========================================="
echo "FAISS GPU Benchmark"
echo "=========================================="

# Configuration
DIMENSIONS=768
VECTORS=1000000
NLIST=4096
NPROBE=32
TOP_K=10
QUERIES=1000

# Create benchmark Python script
cat > /tmp/faiss_benchmark.py << 'EOF'
import faiss
import numpy as np
import time
import sys

# Parameters from environment or defaults
D = int(sys.argv[1]) if len(sys.argv) > 1 else 768
N = int(sys.argv[2]) if len(sys.argv) > 2 else 1000000
NLIST = int(sys.argv[3]) if len(sys.argv) > 3 else 4096
NPROBE = int(sys.argv[4]) if len(sys.argv) > 4 else 32
TOP_K = int(sys.argv[5]) if len(sys.argv) > 5 else 10
NUM_QUERIES = int(sys.argv[6]) if len(sys.argv) > 6 else 1000

print(f"Configuration:")
print(f"  Dimensions (D): {D}")
print(f"  Vectors (N): {N:,}")
print(f"  nlist: {NLIST}")
print(f"  nprobe: {NPROBE}")
print(f"  top_k: {TOP_K}")
print(f"  queries: {NUM_QUERIES}")
print()

# Check GPU
ngpus = faiss.get_num_gpus()
print(f"GPUs available: {ngpus}")
if ngpus == 0:
    print("WARNING: No GPU available, using CPU")
    use_gpu = False
else:
    use_gpu = True
print()

# Generate random data
print("Generating random vectors...")
np.random.seed(42)
xb = np.random.random((N, D)).astype('float32')
xb /= np.linalg.norm(xb, axis=1, keepdims=True)  # Normalize for cosine

xq = np.random.random((NUM_QUERIES, D)).astype('float32')
xq /= np.linalg.norm(xq, axis=1, keepdims=True)

# Build index
print(f"Building IVF{NLIST},Flat index...")
start = time.time()

quantizer = faiss.IndexFlatIP(D)  # Inner product for cosine
index_cpu = faiss.IndexIVFFlat(quantizer, D, NLIST, faiss.METRIC_INNER_PRODUCT)

# Train
print("Training index...")
index_cpu.train(xb)
train_time = time.time() - start
print(f"  Training time: {train_time:.2f}s")

# Add vectors
print("Adding vectors...")
start = time.time()
index_cpu.add(xb)
add_time = time.time() - start
print(f"  Add time: {add_time:.2f}s")
print(f"  Add throughput: {N/add_time:,.0f} vectors/sec")

# Move to GPU if available
if use_gpu:
    print("Moving index to GPU...")
    start = time.time()
    res = faiss.StandardGpuResources()
    index = faiss.index_cpu_to_gpu(res, 0, index_cpu)
    gpu_time = time.time() - start
    print(f"  GPU transfer time: {gpu_time:.2f}s")
else:
    index = index_cpu

# Set search parameters
index.nprobe = NPROBE

# Warmup
print("Warmup...")
_, _ = index.search(xq[:10], TOP_K)

# Benchmark search
print(f"\nBenchmarking search ({NUM_QUERIES} queries)...")
latencies = []

for i in range(NUM_QUERIES):
    start = time.time()
    _, _ = index.search(xq[i:i+1], TOP_K)
    latencies.append((time.time() - start) * 1000)  # Convert to ms

latencies = np.array(latencies)

print(f"\nSearch Latency Results:")
print(f"  P50: {np.percentile(latencies, 50):.2f} ms")
print(f"  P95: {np.percentile(latencies, 95):.2f} ms")
print(f"  P99: {np.percentile(latencies, 99):.2f} ms")
print(f"  Mean: {np.mean(latencies):.2f} ms")
print(f"  Min: {np.min(latencies):.2f} ms")
print(f"  Max: {np.max(latencies):.2f} ms")

# Batch benchmark
print(f"\nBatch search (batch=32)...")
start = time.time()
for i in range(0, NUM_QUERIES, 32):
    batch = xq[i:i+32]
    _, _ = index.search(batch, TOP_K)
batch_time = time.time() - start
print(f"  Total time: {batch_time:.2f}s")
print(f"  Per-query (amortized): {batch_time/NUM_QUERIES*1000:.2f} ms")
print(f"  QPS: {NUM_QUERIES/batch_time:.0f}")

# Memory usage
if use_gpu:
    import subprocess
    result = subprocess.run(['nvidia-smi', '--query-gpu=memory.used', '--format=csv,noheader,nounits'],
                          capture_output=True, text=True)
    gpu_mem = result.stdout.strip()
    print(f"\nGPU Memory Used: {gpu_mem} MB")

print("\n========================================")
print("SLO Assessment (Reference Config):")
print("========================================")
p95 = np.percentile(latencies, 95)
if p95 < 10:
    print(f"  PASS: P95 ({p95:.2f}ms) < 10ms target")
else:
    print(f"  FAIL: P95 ({p95:.2f}ms) >= 10ms target")

print("\nBenchmark complete!")
EOF

# Run benchmark
python3 /tmp/faiss_benchmark.py $DIMENSIONS $VECTORS $NLIST $NPROBE $TOP_K $QUERIES

# Cleanup
rm /tmp/faiss_benchmark.py
