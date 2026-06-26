#!/bin/bash
# AkiDB Thor Edition - GPU Mode Setup Script
# Configures NVIDIA container toolkit and Docker for GPU passthrough
# Run on each Thor node (thor-01, thor-02)

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if running on Jetson
check_jetson() {
    if [ ! -f /etc/nv_tegra_release ]; then
        log_error "This script is designed for NVIDIA Jetson devices"
        exit 1
    fi
    log_info "Detected Jetson device: $(cat /etc/nv_tegra_release | head -1)"
}

# Check CUDA installation
check_cuda() {
    if ! command -v nvcc &> /dev/null; then
        log_warn "CUDA toolkit (nvcc) not found in PATH"
        log_info "Checking default CUDA locations..."

        if [ -d /usr/local/cuda ]; then
            export PATH=/usr/local/cuda/bin:$PATH
            export LD_LIBRARY_PATH=/usr/local/cuda/lib64:${LD_LIBRARY_PATH:-}
            log_info "CUDA found at /usr/local/cuda"
        fi
    fi

    if command -v nvcc &> /dev/null; then
        CUDA_VERSION=$(nvcc --version | grep "release" | sed 's/.*release //' | sed 's/,.*//')
        log_info "CUDA version: $CUDA_VERSION"
    fi
}

# Check nvidia-smi
check_nvidia_smi() {
    if ! command -v nvidia-smi &> /dev/null; then
        log_error "nvidia-smi not found"
        exit 1
    fi

    log_info "GPU Status:"
    nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
}

# Install NVIDIA Container Toolkit
install_nvidia_container_toolkit() {
    log_info "Checking NVIDIA Container Toolkit..."

    if command -v nvidia-container-runtime &> /dev/null; then
        log_info "NVIDIA Container Runtime already installed"
        return
    fi

    log_info "Installing NVIDIA Container Toolkit..."

    # For Jetson (L4T), the toolkit is typically pre-installed
    # But we can ensure it's configured

    # Add NVIDIA container toolkit repository (if needed)
    if [ ! -f /etc/apt/sources.list.d/nvidia-container-toolkit.list ]; then
        curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
            sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

        curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
            sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
            sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

        sudo apt-get update
    fi

    sudo apt-get install -y nvidia-container-toolkit

    log_info "NVIDIA Container Toolkit installed"
}

# Configure Docker for GPU
configure_docker_gpu() {
    log_info "Configuring Docker for GPU support..."

    # Create or update daemon.json
    DAEMON_JSON="/etc/docker/daemon.json"

    if [ -f "$DAEMON_JSON" ]; then
        log_info "Backing up existing Docker daemon config..."
        sudo cp "$DAEMON_JSON" "${DAEMON_JSON}.bak"
    fi

    # Configure nvidia-container-runtime as default
    sudo nvidia-ctk runtime configure --runtime=docker

    # Ensure the config includes nvidia as default runtime
    sudo tee "$DAEMON_JSON" > /dev/null <<EOF
{
    "default-runtime": "nvidia",
    "runtimes": {
        "nvidia": {
            "path": "nvidia-container-runtime",
            "runtimeArgs": []
        }
    },
    "exec-opts": ["native.cgroupdriver=systemd"],
    "log-driver": "json-file",
    "log-opts": {
        "max-size": "100m",
        "max-file": "3"
    },
    "storage-driver": "overlay2"
}
EOF

    log_info "Docker daemon configured with NVIDIA runtime as default"
}

# Restart Docker
restart_docker() {
    log_info "Restarting Docker daemon..."
    sudo systemctl daemon-reload
    sudo systemctl restart docker

    # Wait for Docker to be ready
    sleep 5

    if ! docker info &> /dev/null; then
        log_error "Docker failed to start"
        exit 1
    fi

    log_info "Docker restarted successfully"
}

# Test GPU in Docker
test_gpu_docker() {
    log_info "Testing GPU access in Docker..."

    # Try to run nvidia-smi in a container
    if docker run --rm nvidia/cuda:12.0-base nvidia-smi &> /dev/null; then
        log_info "GPU access in Docker: SUCCESS"
    else
        # For Jetson, use L4T base image
        log_info "Trying Jetson-specific test..."
        if docker run --rm --runtime nvidia nvcr.io/nvidia/l4t-base:r36.2.0 nvidia-smi &> /dev/null; then
            log_info "GPU access in Docker (L4T): SUCCESS"
        else
            log_warn "GPU Docker test failed - may need to pull appropriate base image"
        fi
    fi
}

# Configure tegrastats for memory monitoring
setup_tegrastats() {
    log_info "Checking tegrastats availability..."

    if command -v tegrastats &> /dev/null; then
        log_info "tegrastats available at: $(which tegrastats)"

        # Test tegrastats
        timeout 2 tegrastats --interval 1000 || true
    else
        log_warn "tegrastats not found - memory monitoring may not work"
    fi
}

# Set up environment variables
setup_environment() {
    log_info "Setting up environment variables..."

    # Create profile script for CUDA paths
    sudo tee /etc/profile.d/akidb-cuda.sh > /dev/null <<EOF
# AkiDB CUDA Environment
export PATH=/usr/local/cuda/bin:\$PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:\${LD_LIBRARY_PATH:-}
export CUDA_HOME=/usr/local/cuda
EOF

    log_info "Environment variables configured in /etc/profile.d/akidb-cuda.sh"
}

# Print summary
print_summary() {
    echo ""
    echo "=========================================="
    echo "AkiDB Thor GPU Setup Complete"
    echo "=========================================="
    echo ""
    nvidia-smi --query-gpu=name,memory.total,memory.free,temperature.gpu --format=csv
    echo ""
    echo "Docker runtime: $(docker info 2>/dev/null | grep 'Default Runtime' || echo 'nvidia (default)')"
    echo ""
    log_info "GPU mode is ready for AkiDB workloads"
    echo ""
    echo "Next steps:"
    echo "  1. Run 'source /etc/profile.d/akidb-cuda.sh' or re-login"
    echo "  2. Test with: docker run --rm --gpus all nvidia/cuda:12.0-base nvidia-smi"
    echo "  3. Deploy AkiDB stack: cd deploy/compose && docker-compose up -d"
}

# Main
main() {
    log_info "Starting AkiDB Thor GPU Setup..."
    echo ""

    check_jetson
    check_cuda
    check_nvidia_smi
    install_nvidia_container_toolkit
    configure_docker_gpu
    restart_docker
    test_gpu_docker
    setup_tegrastats
    setup_environment
    print_summary
}

# Run main
main "$@"
