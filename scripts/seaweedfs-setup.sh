#!/bin/bash
# SeaweedFS Setup Script for AkiDB
# Sets up SeaweedFS for distributed storage

set -e

echo "=========================================="
echo "SeaweedFS Setup for AkiDB"
echo "=========================================="

# Configuration
SEAWEEDFS_VERSION=${SEAWEEDFS_VERSION:-"4.47"}
SEAWEEDFS_IDENTITY=${SEAWEEDFS_IDENTITY:-"akidb-admin"}
SEAWEEDFS_ACCESS_KEY=${SEAWEEDFS_ACCESS_KEY:-"akidb-admin"}
SEAWEEDFS_SECRET_KEY=${SEAWEEDFS_SECRET_KEY:-"akidb-secret-key"}
SEAWEEDFS_DATA_DIR=${SEAWEEDFS_DATA_DIR:-"/data/seaweedfs"}
SEAWEEDFS_CONFIG_DIR=${SEAWEEDFS_CONFIG_DIR:-"/etc/seaweedfs"}
SEAWEEDFS_S3_PORT=${SEAWEEDFS_S3_PORT:-8333}
SEAWEEDFS_MASTER_PORT=${SEAWEEDFS_MASTER_PORT:-9333}
SEAWEEDFS_VOLUME_PORT=${SEAWEEDFS_VOLUME_PORT:-8080}
SEAWEEDFS_FILER_PORT=${SEAWEEDFS_FILER_PORT:-8888}
SEAWEEDFS_METRICS_PORT=${SEAWEEDFS_METRICS_PORT:-9327}
SEAWEEDFS_SHA256=${SEAWEEDFS_SHA256:-""}
BUCKET_NAME=${BUCKET_NAME:-"akidb-snapshots"}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    SEAWEEDFS_PLATFORM="linux_arm64"
else
    SEAWEEDFS_PLATFORM="linux_amd64"
fi
SEAWEEDFS_ARCHIVE_URL="https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/${SEAWEEDFS_PLATFORM}.tar.gz"

# Upstream publishes only an .md5 next to each release asset: there is no
# upstream SHA256 and no signature, so an operator-supplied digest is the only
# integrity check available and is therefore mandatory.
if [ -z "$SEAWEEDFS_SHA256" ]; then
    echo "SEAWEEDFS_SHA256 is required."
    echo "Download ${SEAWEEDFS_ARCHIVE_URL}, compute its SHA256, then re-run with SEAWEEDFS_SHA256=<digest>."
    exit 1
fi

# Install SeaweedFS if not present
if ! command -v weed &> /dev/null; then
    echo "Installing SeaweedFS ${SEAWEEDFS_VERSION} (${SEAWEEDFS_PLATFORM})..."

    SEAWEEDFS_TMP_DIR="$(mktemp -d)"
    SEAWEEDFS_ARCHIVE="$SEAWEEDFS_TMP_DIR/seaweedfs.tar.gz"
    wget -O "$SEAWEEDFS_ARCHIVE" "$SEAWEEDFS_ARCHIVE_URL"
    echo "${SEAWEEDFS_SHA256}  ${SEAWEEDFS_ARCHIVE}" | sha256sum --check --strict -
    tar -xzf "$SEAWEEDFS_ARCHIVE" -C "$SEAWEEDFS_TMP_DIR" weed
    install -m 0755 "$SEAWEEDFS_TMP_DIR/weed" /usr/local/bin/weed
    rm -rf "$SEAWEEDFS_TMP_DIR"
    echo "SeaweedFS installed."
fi

# Create data directory
echo "Creating data directory: $SEAWEEDFS_DATA_DIR"
mkdir -p "$SEAWEEDFS_DATA_DIR"

# Write the S3 gateway credentials file. Without it the gateway allows
# anonymous access to every operation, so it is always written.
echo "Writing S3 credentials file: $SEAWEEDFS_CONFIG_DIR/s3.json"
mkdir -p "$SEAWEEDFS_CONFIG_DIR"
cat > "$SEAWEEDFS_CONFIG_DIR/s3.json" << EOF
{
  "identities": [
    {
      "name": "$SEAWEEDFS_IDENTITY",
      "credentials": [
        {
          "accessKey": "$SEAWEEDFS_ACCESS_KEY",
          "secretKey": "$SEAWEEDFS_SECRET_KEY"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF
chmod 0600 "$SEAWEEDFS_CONFIG_DIR/s3.json"

# Create systemd service file
echo "Creating systemd service..."
cat > /etc/systemd/system/seaweedfs.service << EOF
[Unit]
Description=SeaweedFS Object Storage
Documentation=https://github.com/seaweedfs/seaweedfs
Wants=network-online.target
After=network-online.target

[Service]
User=root
Group=root
ExecStart=/usr/local/bin/weed server -filer -s3 -dir=$SEAWEEDFS_DATA_DIR -ip.bind=0.0.0.0 -master.port=$SEAWEEDFS_MASTER_PORT -volume.port=$SEAWEEDFS_VOLUME_PORT -filer.port=$SEAWEEDFS_FILER_PORT -s3.port=$SEAWEEDFS_S3_PORT -s3.config=$SEAWEEDFS_CONFIG_DIR/s3.json -volume.max=0 -master.volumeSizeLimitMB=1024 -metricsPort=$SEAWEEDFS_METRICS_PORT
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start SeaweedFS
systemctl daemon-reload
systemctl enable seaweedfs
systemctl start seaweedfs

echo "Waiting for SeaweedFS to start..."
sleep 5

# Create bucket
echo "Creating bucket: $BUCKET_NAME"
echo "s3.bucket.create -name $BUCKET_NAME" | weed shell -master=localhost:$SEAWEEDFS_MASTER_PORT

echo ""
echo "=========================================="
echo "SeaweedFS Setup Complete"
echo "=========================================="
echo ""
echo "SeaweedFS S3 API: http://localhost:$SEAWEEDFS_S3_PORT"
echo "SeaweedFS Master: http://localhost:$SEAWEEDFS_MASTER_PORT"
echo "Bucket: $BUCKET_NAME"
echo ""
echo "Credentials:"
echo "  Access Key: $SEAWEEDFS_ACCESS_KEY"
echo "  Secret Key: $SEAWEEDFS_SECRET_KEY"
echo ""
echo "To test:"
echo "  echo 's3.bucket.list' | weed shell -master=localhost:$SEAWEEDFS_MASTER_PORT"
echo ""
