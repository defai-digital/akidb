#!/bin/bash
# Deploy AkiDB to Thor Cluster
# Usage: ./scripts/deploy-to-thor.sh [validate|build|deploy|all]

set -e

# Thor hosts
THOR_01="devop@192.168.1.61"
THOR_02="devop@192.168.1.62"
THOR_HOSTS=("$THOR_01" "$THOR_02")

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "[INFO] $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check SSH connectivity
check_connectivity() {
    log_info "Checking SSH connectivity..."
    for host in "${THOR_HOSTS[@]}"; do
        if ssh -o ConnectTimeout=5 -o BatchMode=yes "$host" "echo connected" &>/dev/null; then
            log_success "Connected to $host"
        else
            log_error "Cannot connect to $host"
            log_info "Try: ssh-copy-id $host"
            exit 1
        fi
    done
}

# Run validation on Thor nodes
validate_thor() {
    log_info "Running validation on Thor nodes..."
    for host in "${THOR_HOSTS[@]}"; do
        log_info "Validating $host..."
        scp scripts/thor-validate.sh "$host:/tmp/"
        ssh "$host" "chmod +x /tmp/thor-validate.sh && /tmp/thor-validate.sh"
    done
}

# Cross-compile for ARM64
build_arm64() {
    log_info "Building for aarch64-unknown-linux-gnu..."

    # Check if cross is installed
    if ! command -v cross &> /dev/null; then
        log_info "Installing cross..."
        cargo install cross
    fi

    # Build using cross (Docker-based cross compilation)
    cross build --release --target aarch64-unknown-linux-gnu --features cpu

    log_success "Build complete: target/aarch64-unknown-linux-gnu/release/"
}

# Deploy binaries to Thor nodes
deploy_binaries() {
    log_info "Deploying binaries to Thor nodes..."

    BINARY_DIR="target/aarch64-unknown-linux-gnu/release"

    for host in "${THOR_HOSTS[@]}"; do
        log_info "Deploying to $host..."

        # Create directories
        ssh "$host" "sudo mkdir -p /opt/akidb/bin /opt/akidb/config /opt/akidb/data"

        # Copy binaries (if they exist)
        for bin in akidb-server akidb-coordinator; do
            if [ -f "$BINARY_DIR/$bin" ]; then
                scp "$BINARY_DIR/$bin" "$host:/tmp/"
                ssh "$host" "sudo mv /tmp/$bin /opt/akidb/bin/ && sudo chmod +x /opt/akidb/bin/$bin"
                log_success "Deployed $bin to $host"
            fi
        done

        # Copy config
        scp config/default.toml "$host:/tmp/"
        ssh "$host" "sudo mv /tmp/default.toml /opt/akidb/config/"

        log_success "Deployment to $host complete"
    done
}

# Setup MinIO on Thor nodes
setup_minio() {
    log_info "Setting up MinIO on Thor nodes..."
    for host in "${THOR_HOSTS[@]}"; do
        log_info "Setting up MinIO on $host..."
        scp scripts/minio-setup.sh "$host:/tmp/"
        ssh "$host" "chmod +x /tmp/minio-setup.sh && sudo /tmp/minio-setup.sh"
    done
}

# Run FAISS benchmark
benchmark() {
    log_info "Running FAISS benchmark on Thor nodes..."
    for host in "${THOR_HOSTS[@]}"; do
        log_info "Benchmarking $host..."
        scp scripts/faiss-benchmark.sh "$host:/tmp/"
        ssh "$host" "chmod +x /tmp/faiss-benchmark.sh && /tmp/faiss-benchmark.sh"
    done
}

# Main
case "${1:-all}" in
    check)
        check_connectivity
        ;;
    validate)
        check_connectivity
        validate_thor
        ;;
    build)
        build_arm64
        ;;
    deploy)
        check_connectivity
        deploy_binaries
        ;;
    minio)
        check_connectivity
        setup_minio
        ;;
    benchmark)
        check_connectivity
        benchmark
        ;;
    all)
        check_connectivity
        validate_thor
        build_arm64
        deploy_binaries
        ;;
    *)
        echo "Usage: $0 [check|validate|build|deploy|minio|benchmark|all]"
        echo ""
        echo "Commands:"
        echo "  check     - Check SSH connectivity"
        echo "  validate  - Run hardware validation"
        echo "  build     - Cross-compile for ARM64"
        echo "  deploy    - Deploy binaries to Thor nodes"
        echo "  minio     - Setup MinIO cluster"
        echo "  benchmark - Run FAISS benchmark"
        echo "  all       - Run all steps"
        exit 1
        ;;
esac

log_success "Done!"
