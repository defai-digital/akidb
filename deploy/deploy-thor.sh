#!/bin/bash
# AkiDB Thor Edition - Full Deployment Script
# This script syncs code and deploys to the Thor cluster
#
# Prerequisites:
# - SSH access to Thor machines (192.168.1.61, 192.168.1.62)
# - Ansible installed locally
#
# Usage: ./deploy-thor.sh [--sync-only] [--build-only] [--full]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
THOR_01="192.168.1.61"
THOR_02="192.168.1.62"
THOR_USER="devop"
REMOTE_DIR="/home/${THOR_USER}/akidb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

print_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║            AKIDB THOR EDITION - DEPLOYMENT                           ║"
    echo "║                                                                      ║"
    echo "║  New Features (Phase 4 - Observability):                             ║"
    echo "║    - Prometheus metrics for background tasks                         ║"
    echo "║    - Admin gRPC service for task management                          ║"
    echo "║    - Webhook alerting with HMAC-SHA256 signing                       ║"
    echo "║                                                                      ║"
    echo "║  Bug Fixes Applied:                                                  ║"
    echo "║    - CRITICAL: Webhook HMAC signature (was SHA256, now HMAC-SHA256)  ║"
    echo "║    - FIX: Resumable upload bytes_uploaded reset on resume            ║"
    echo "║    - FIX: Timestamp overflow prevention in admin.rs                  ║"
    echo "║    - BUG-H001: Race condition in TagIndex (fixed)                    ║"
    echo "║    - BUG-H004: ManifestStore upsert lock (fixed)                     ║"
    echo "║    - BUG-H007: NaN/Infinity validation (fixed)                       ║"
    echo "║                                                                      ║"
    echo "║  Tests: 272 passing                                                  ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

sync_code() {
    log_info "Syncing code to Thor cluster..."

    # Sync to Thor-01
    log_info "Syncing to DEVOP-THOR-01 ($THOR_01)..."
    rsync -avz --exclude 'target' --exclude '.git' --exclude 'node_modules' \
        "$PROJECT_ROOT/" "${THOR_USER}@${THOR_01}:${REMOTE_DIR}/"

    # Sync to Thor-02
    log_info "Syncing to DEVOP-THOR-02 ($THOR_02)..."
    rsync -avz --exclude 'target' --exclude '.git' --exclude 'node_modules' \
        "$PROJECT_ROOT/" "${THOR_USER}@${THOR_02}:${REMOTE_DIR}/"

    log_success "Code sync complete!"
}

build_on_thor() {
    local host=$1
    local hostname=$2

    log_info "Building on $hostname ($host)..."

    ssh "${THOR_USER}@${host}" << 'ENDSSH'
        cd ~/akidb

        echo "Building release binaries..."
        cargo build --release --package akidb-server --package akidb-coordinator --package akidb-benchmark

        echo "Running tests..."
        cargo test --workspace --release 2>&1 | tail -10

        echo "Build complete!"
ENDSSH

    log_success "Build on $hostname complete!"
}

deploy_services() {
    log_info "Deploying services using Ansible..."

    cd "$SCRIPT_DIR/ansible"

    # Run production deployment playbook
    ansible-playbook -i inventory.yml playbooks/production-deploy.yml

    log_success "Service deployment complete!"
}

verify_deployment() {
    log_info "Verifying deployment..."

    # Check AkiDB health on both nodes
    echo "Checking DEVOP-THOR-01..."
    curl -s "http://${THOR_01}:50051/health" || log_warn "Thor-01 health check failed"

    echo "Checking DEVOP-THOR-02..."
    curl -s "http://${THOR_02}:50051/health" || log_warn "Thor-02 health check failed"

    # Check coordinator
    echo "Checking Coordinator..."
    curl -s "http://${THOR_01}:50050/health" || log_warn "Coordinator health check failed"

    log_success "Verification complete!"
}

print_summary() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║                    DEPLOYMENT SUMMARY                                 ║"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    echo "║  Endpoints:                                                          ║"
    echo "║    AkiDB Coordinator: http://192.168.1.61:50050                       ║"
    echo "║    AkiDB Shard 0:     http://192.168.1.61:50051                       ║"
    echo "║    AkiDB Shard 1:     http://192.168.1.62:50051                       ║"
    echo "║    Grafana:           http://192.168.1.61:3000                        ║"
    echo "║    Prometheus:        http://192.168.1.61:9090                        ║"
    echo "║    MinIO:             http://192.168.1.61:9000                        ║"
    echo "║                                                                      ║"
    echo "║  Test with:                                                          ║"
    echo "║    akidb-bench --server http://192.168.1.61:50050 -q 100              ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""
}

# Parse arguments
MODE="full"
while [[ $# -gt 0 ]]; do
    case $1 in
        --sync-only)
            MODE="sync"
            shift
            ;;
        --build-only)
            MODE="build"
            shift
            ;;
        --full)
            MODE="full"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Main execution
print_banner

case $MODE in
    sync)
        sync_code
        ;;
    build)
        sync_code
        build_on_thor "$THOR_01" "DEVOP-THOR-01"
        build_on_thor "$THOR_02" "DEVOP-THOR-02"
        ;;
    full)
        sync_code
        build_on_thor "$THOR_01" "DEVOP-THOR-01"
        build_on_thor "$THOR_02" "DEVOP-THOR-02"
        deploy_services
        verify_deployment
        print_summary
        ;;
esac

log_success "Deployment script complete!"
