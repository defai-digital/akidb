#!/bin/bash
# Build AkiDB on Thor with GPU support
# Run this after FAISS GPU installation completes

set -euo pipefail

echo "=== Building AkiDB on Thor ==="

# Set environment
export PATH="$HOME/.cargo/bin:$PATH"
export CUDA_HOME=/usr/local/cuda
export FAISS_PATH=/opt/faiss
export LD_LIBRARY_PATH=$FAISS_PATH/lib:$CUDA_HOME/lib64:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=$FAISS_PATH/lib/pkgconfig:$PKG_CONFIG_PATH

# Verify FAISS installation
if [ ! -f "$FAISS_PATH/lib/libfaiss.so" ]; then
    echo "Error: FAISS not found at $FAISS_PATH"
    echo "Please run install-faiss-gpu.sh first"
    exit 1
fi

echo "FAISS found at $FAISS_PATH"

cd ~/akidb

# Build with NVIDIA GPU feature. This requires FAISS GPU and CUDA on the host.
echo "Building with NVIDIA GPU feature..."
cargo build --release -p akidb-server --no-default-features --features gpu 2>&1 | tail -40

# Check if build succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo "=== Build complete! ==="
    echo "Binary at: ~/akidb/target/release/akidb-server"
    ls -la ~/akidb/target/release/akidb-* 2>/dev/null || echo "No akidb binaries found"
else
    echo "Build failed!"
    exit 1
fi
