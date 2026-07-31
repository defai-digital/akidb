#!/bin/sh
# MinIO Setup Script for AkiDB
# Creates bucket and configures NATS notifications

set -eu

echo "Waiting for MinIO to be ready..."

read_secret() {
    secret_path="$1"
    secret_name="$2"
    if [ ! -r "$secret_path" ]; then
        echo "Cannot read ${secret_name} secret: ${secret_path}" >&2
        exit 1
    fi
    secret_value=$(cat "$secret_path")
    if [ -z "$secret_value" ]; then
        echo "${secret_name} secret is empty" >&2
        exit 1
    fi
    printf '%s' "$secret_value"
}

MINIO_ROOT_USER=$(read_secret \
  "${MINIO_ROOT_USER_FILE:-/run/secrets/minio_root_user}" \
  "MinIO root user")
MINIO_ROOT_PASSWORD=$(read_secret \
  "${MINIO_ROOT_PASSWORD_FILE:-/run/secrets/minio_root_password}" \
  "MinIO root password")

# Configure mc alias
mc alias set akidb http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# Create the documents bucket
echo "Creating akidb-documents bucket..."
mc mb --ignore-existing akidb/akidb-documents

# Keep object access private; uploads go through authenticated clients.
echo "Setting private bucket policy..."
mc anonymous set none akidb/akidb-documents

# Add upload notifications. Deletion lifecycle events use a separate contract
# and must not be sent through the document-upload consumer.
echo "Adding bucket notification..."
mc event add \
  --ignore-existing \
  --event put \
  akidb/akidb-documents \
  arn:minio:sqs::PRIMARY:nats

echo "MinIO setup complete!"
mc admin info akidb
