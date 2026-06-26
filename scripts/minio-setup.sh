#!/bin/bash
# MinIO Setup Script for AkiDB
# Sets up MinIO for distributed storage

set -e

echo "=========================================="
echo "MinIO Setup for AkiDB"
echo "=========================================="

# Configuration
MINIO_ROOT_USER=${MINIO_ROOT_USER:-"akidb-admin"}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-"akidb-secret-key"}
MINIO_DATA_DIR=${MINIO_DATA_DIR:-"/data/minio"}
MINIO_PORT=${MINIO_PORT:-9000}
MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-9001}
BUCKET_NAME=${BUCKET_NAME:-"akidb-snapshots"}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

# Install MinIO if not present
if ! command -v minio &> /dev/null; then
    echo "Installing MinIO..."

    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        MINIO_URL="https://dl.min.io/server/minio/release/linux-arm64/minio"
    else
        MINIO_URL="https://dl.min.io/server/minio/release/linux-amd64/minio"
    fi

    wget -O /usr/local/bin/minio "$MINIO_URL"
    chmod +x /usr/local/bin/minio
    echo "MinIO installed."
fi

# Install MinIO client if not present
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO client..."

    ARCH=$(uname -m)
    if [ "$ARCH" = "aarch64" ]; then
        MC_URL="https://dl.min.io/client/mc/release/linux-arm64/mc"
    else
        MC_URL="https://dl.min.io/client/mc/release/linux-amd64/mc"
    fi

    wget -O /usr/local/bin/mc "$MC_URL"
    chmod +x /usr/local/bin/mc
    echo "MinIO client installed."
fi

# Create data directory
echo "Creating data directory: $MINIO_DATA_DIR"
mkdir -p "$MINIO_DATA_DIR"

# Create systemd service file
echo "Creating systemd service..."
cat > /etc/systemd/system/minio.service << EOF
[Unit]
Description=MinIO Object Storage
Documentation=https://docs.min.io
Wants=network-online.target
After=network-online.target

[Service]
User=root
Group=root
Environment="MINIO_ROOT_USER=$MINIO_ROOT_USER"
Environment="MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD"
ExecStart=/usr/local/bin/minio server $MINIO_DATA_DIR --address ":$MINIO_PORT" --console-address ":$MINIO_CONSOLE_PORT"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start MinIO
systemctl daemon-reload
systemctl enable minio
systemctl start minio

echo "Waiting for MinIO to start..."
sleep 5

# Configure MinIO client
mc alias set akidb http://localhost:$MINIO_PORT $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# Create bucket
echo "Creating bucket: $BUCKET_NAME"
mc mb akidb/$BUCKET_NAME --ignore-existing

# Set bucket policy (private)
mc anonymous set none akidb/$BUCKET_NAME

echo ""
echo "=========================================="
echo "MinIO Setup Complete"
echo "=========================================="
echo ""
echo "MinIO Server: http://localhost:$MINIO_PORT"
echo "MinIO Console: http://localhost:$MINIO_CONSOLE_PORT"
echo "Bucket: $BUCKET_NAME"
echo ""
echo "Credentials:"
echo "  Access Key: $MINIO_ROOT_USER"
echo "  Secret Key: $MINIO_ROOT_PASSWORD"
echo ""
echo "To test:"
echo "  mc ls akidb/$BUCKET_NAME"
echo ""
