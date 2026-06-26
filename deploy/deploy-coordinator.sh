#!/bin/bash
# AkiDB Coordinator Deployment Script
# Run this on Thor-01 (192.168.1.61)

set -e

AKIDB_DIR="${HOME}/akidb"
INSTALL_DIR="/usr/local/bin"

echo "========================================="
echo "  AkiDB Coordinator Deployment"
echo "========================================="

# Check if we're on the right architecture
ARCH=$(uname -m)
if [[ "$ARCH" != "aarch64" ]]; then
    echo "Warning: Expected aarch64, got $ARCH"
fi

# Update or clone the repository
if [[ -d "$AKIDB_DIR" ]]; then
    echo "Updating existing repository..."
    cd "$AKIDB_DIR"
    git pull origin main 2>/dev/null || echo "Git pull skipped (not a git repo or no remote)"
else
    echo "Please copy the akidb source code to $AKIDB_DIR"
    exit 1
fi

# Build the coordinator
echo ""
echo "Building coordinator (release mode)..."
cd "$AKIDB_DIR"
cargo build --release --package akidb-coordinator

# Build the benchmark tool
echo ""
echo "Building benchmark tool..."
cargo build --release --package akidb-benchmark

# Stop existing service if running
echo ""
echo "Stopping existing coordinator service..."
sudo systemctl stop akidb-coordinator 2>/dev/null || true

# Install binaries
echo ""
echo "Installing binaries to $INSTALL_DIR..."
sudo cp target/release/akidb-coordinator "$INSTALL_DIR/"
sudo cp target/release/akidb-bench "$INSTALL_DIR/"
sudo chmod +x "$INSTALL_DIR/akidb-coordinator" "$INSTALL_DIR/akidb-bench"

# Create/update systemd service
echo ""
echo "Installing systemd service..."
sudo tee /etc/systemd/system/akidb-coordinator.service > /dev/null << 'EOF'
[Unit]
Description=AkiDB Coordinator Service
After=network.target akidb.service
Wants=akidb.service

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/akidb-coordinator \
    --listen 0.0.0.0:50050 \
    --shards 192.168.1.61:50051,192.168.1.62:50051 \
    --pool-size 4 \
    --timeout 5000 \
    --log-level info
Restart=always
RestartSec=5
Environment=RUST_BACKTRACE=1

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start service
echo ""
echo "Starting coordinator service..."
sudo systemctl daemon-reload
sudo systemctl enable akidb-coordinator
sudo systemctl start akidb-coordinator

# Check status
echo ""
echo "Service status:"
sudo systemctl status akidb-coordinator --no-pager

echo ""
echo "========================================="
echo "  Deployment Complete!"
echo "========================================="
echo ""
echo "Coordinator: http://$(hostname -I | awk '{print $1}'):50050"
echo "Metrics:     http://$(hostname -I | awk '{print $1}'):9090/metrics"
echo ""
echo "Test with:"
echo "  akidb-bench --server http://localhost:50050 --num-queries 100"
