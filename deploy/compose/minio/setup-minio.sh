#!/bin/sh
# MinIO Setup Script for AkiDB
# Creates bucket and configures NATS notifications

set -e

echo "Waiting for MinIO to be ready..."
sleep 5

# Read credentials from secret files (ADR-021 compliant)
if [ -f "/run/secrets/minio_root_user" ]; then
    MINIO_ROOT_USER=$(cat /run/secrets/minio_root_user)
fi
if [ -f "/run/secrets/minio_root_password" ]; then
    MINIO_ROOT_PASSWORD=$(cat /run/secrets/minio_root_password)
fi
# Fallback: support _FILE environment variables
if [ -n "${MINIO_ROOT_USER_FILE}" ] && [ -f "${MINIO_ROOT_USER_FILE}" ]; then
    MINIO_ROOT_USER=$(cat "${MINIO_ROOT_USER_FILE}")
fi
if [ -n "${MINIO_ROOT_PASSWORD_FILE}" ] && [ -f "${MINIO_ROOT_PASSWORD_FILE}" ]; then
    MINIO_ROOT_PASSWORD=$(cat "${MINIO_ROOT_PASSWORD_FILE}")
fi

# Configure mc alias
mc alias set akidb http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Create the documents bucket
echo "Creating akidb-documents bucket..."
mc mb --ignore-existing akidb/akidb-documents

# Set bucket policy to allow uploads
echo "Setting bucket policy..."
mc anonymous set upload akidb/akidb-documents

# Configure NATS notification target
echo "Configuring NATS notifications..."
mc admin config set akidb notify_nats:primary \
  address="nats-1:4222" \
  subject="minio.uploads" \
  jetstream="on" \
  streaming_async="on"

# Restart MinIO to apply notification config
mc admin service restart akidb

# Wait for restart
sleep 5

# Add event notification for put/delete events
echo "Adding bucket notification..."
mc event add akidb/akidb-documents arn:minio:sqs::primary:nats \
  --event put,delete \
  --suffix ".pdf,.docx,.doc,.csv,.json,.xml,.html,.xlsx,.txt"

echo "MinIO setup complete!"
mc admin info akidb
