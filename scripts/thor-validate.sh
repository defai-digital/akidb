#!/bin/bash
# Thor Hardware Validation Script
# Run this on Jetson Thor to validate hardware and software stack

set -e

echo "=========================================="
echo "AkiDB Thor Hardware Validation"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
info() { echo -e "[INFO] $1"; }

# 1. Check system info
echo "=== System Information ==="
info "Hostname: $(hostname)"
info "Kernel: $(uname -r)"
info "Architecture: $(uname -m)"

if [ "$(uname -m)" != "aarch64" ]; then
    warn "Not running on ARM64 architecture"
fi

# 2. Check NVIDIA driver and GPU
echo ""
echo "=== GPU Validation ==="

if command -v nvidia-smi &> /dev/null; then
    nvidia-smi --query-gpu=name,driver_version,memory.total,memory.free --format=csv
    pass "NVIDIA driver installed"
else
    fail "nvidia-smi not found. NVIDIA driver not installed."
fi

# 3. Check CUDA
echo ""
echo "=== CUDA Validation ==="

if [ -d "/usr/local/cuda" ]; then
    CUDA_VERSION=$(cat /usr/local/cuda/version.txt 2>/dev/null || nvcc --version | grep "release" | awk '{print $5}' | tr -d ',')
    info "CUDA Version: $CUDA_VERSION"
    pass "CUDA installed at /usr/local/cuda"
else
    fail "CUDA not found at /usr/local/cuda"
fi

if command -v nvcc &> /dev/null; then
    nvcc --version | head -4
    pass "nvcc compiler available"
else
    warn "nvcc not in PATH"
fi

# 4. Check JetPack version (Thor specific)
echo ""
echo "=== JetPack Validation ==="

if [ -f "/etc/nv_tegra_release" ]; then
    cat /etc/nv_tegra_release
    pass "NVIDIA Tegra release file found"
else
    warn "Tegra release file not found (may not be Jetson device)"
fi

# Check L4T version
if command -v dpkg &> /dev/null; then
    L4T_VERSION=$(dpkg -l | grep nvidia-l4t-core | awk '{print $3}' || echo "not found")
    info "L4T Version: $L4T_VERSION"
fi

# 5. Check memory
echo ""
echo "=== Memory Validation ==="

TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
info "Total Memory: ${TOTAL_MEM}GB"

if [ "$TOTAL_MEM" -lt 16 ]; then
    warn "Less than 16GB RAM. Recommended: 32GB+ for production"
elif [ "$TOTAL_MEM" -ge 32 ]; then
    pass "Memory: ${TOTAL_MEM}GB (meets recommendation)"
else
    info "Memory: ${TOTAL_MEM}GB (acceptable for development)"
fi

# 6. Check storage
echo ""
echo "=== Storage Validation ==="

ROOT_FREE=$(df -BG / | awk 'NR==2 {print $4}' | tr -d 'G')
info "Root partition free: ${ROOT_FREE}GB"

if [ "$ROOT_FREE" -lt 50 ]; then
    warn "Less than 50GB free. Consider adding storage for index data."
else
    pass "Storage: ${ROOT_FREE}GB free"
fi

# 7. Check network
echo ""
echo "=== Network Validation ==="

# Check for 10Gbps interface (common on Thor)
for iface in /sys/class/net/*; do
    iface_name=$(basename $iface)
    if [ -f "$iface/speed" ]; then
        speed=$(cat "$iface/speed" 2>/dev/null || echo "unknown")
        if [ "$speed" != "unknown" ] && [ "$speed" -ge 10000 ]; then
            pass "High-speed interface found: $iface_name (${speed}Mbps)"
        fi
    fi
done

# 8. Check Rust installation
echo ""
echo "=== Rust Validation ==="

if command -v rustc &> /dev/null; then
    RUST_VERSION=$(rustc --version)
    info "Rust: $RUST_VERSION"
    pass "Rust installed"
else
    warn "Rust not installed. Run: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
fi

# 9. Check Python (for TensorRT)
echo ""
echo "=== Python/TensorRT Validation ==="

if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    info "Python: $PYTHON_VERSION"

    # Check TensorRT
    if python3 -c "import tensorrt" 2>/dev/null; then
        TRT_VERSION=$(python3 -c "import tensorrt; print(tensorrt.__version__)")
        info "TensorRT: $TRT_VERSION"
        pass "TensorRT installed"
    else
        warn "TensorRT Python bindings not found"
    fi
else
    warn "Python3 not found"
fi

# 10. FAISS test
echo ""
echo "=== FAISS Validation ==="

# Try to import FAISS in Python
if python3 -c "import faiss; print(f'FAISS version: {faiss.__version__}')" 2>/dev/null; then
    # Check for GPU support
    if python3 -c "import faiss; assert faiss.get_num_gpus() > 0" 2>/dev/null; then
        GPU_COUNT=$(python3 -c "import faiss; print(faiss.get_num_gpus())")
        pass "FAISS GPU support available ($GPU_COUNT GPUs)"
    else
        warn "FAISS installed but no GPU support detected"
    fi
else
    warn "FAISS Python not installed (optional for Rust build)"
fi

# Summary
echo ""
echo "=========================================="
echo "Validation Summary"
echo "=========================================="

echo ""
echo "Next steps:"
echo "1. If any [FAIL] items, resolve before proceeding"
echo "2. Run: ./scripts/faiss-benchmark.sh for performance baseline"
echo "3. Run: ./scripts/minio-setup.sh to configure MinIO"
echo ""
