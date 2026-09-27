#!/bin/sh
# Render the SeaweedFS S3 gateway credentials file from the mounted secrets.
#
# The S3 gateway runs in anonymous allow-all mode when no configuration file
# is supplied, so this generator is a hard gate: it fails closed when a secret
# is missing, empty, or contains a character that would break the JSON. The
# `seaweedfs` service waits for this container to exit successfully.

set -eu

ACCESS_KEY_FILE="${SEAWEEDFS_ACCESS_KEY_FILE:-/run/secrets/seaweedfs_access_key}"
SECRET_KEY_FILE="${SEAWEEDFS_SECRET_KEY_FILE:-/run/secrets/seaweedfs_secret_key}"
S3_CONFIG="${SEAWEEDFS_S3_CONFIG:-/etc/seaweedfs/s3.json}"
IDENTITY_NAME="${SEAWEEDFS_IDENTITY:-akidb-admin}"

echo "Rendering ${S3_CONFIG} for identity ${IDENTITY_NAME}..."

read_secret() {
    secret_path="$1"
    secret_name="$2"
    if [ ! -r "$secret_path" ]; then
        echo "ERROR: cannot read the ${secret_name} secret: ${secret_path}" >&2
        exit 1
    fi
    secret_value="$(cat "$secret_path")"
    if [ -z "$secret_value" ]; then
        echo "ERROR: the ${secret_name} secret is empty: ${secret_path}" >&2
        exit 1
    fi
    case "$secret_value" in
        *'"'*)
            echo "ERROR: the ${secret_name} secret contains a double quote" >&2
            exit 1
            ;;
        *\\*)
            echo "ERROR: the ${secret_name} secret contains a backslash" >&2
            exit 1
            ;;
    esac
    if [ "$(printf '%s' "$secret_value" | wc -l)" -ne 0 ]; then
        echo "ERROR: the ${secret_name} secret spans multiple lines" >&2
        exit 1
    fi
    printf '%s' "$secret_value"
}

ACCESS_KEY="$(read_secret "$ACCESS_KEY_FILE" "SeaweedFS access key")"
SECRET_KEY="$(read_secret "$SECRET_KEY_FILE" "SeaweedFS secret key")"

S3_CONFIG_DIR="$(dirname "$S3_CONFIG")"
mkdir -p "$S3_CONFIG_DIR"

umask 027
cat > "$S3_CONFIG" <<EOF
{
  "identities": [
    {
      "name": "${IDENTITY_NAME}",
      "credentials": [
        {
          "accessKey": "${ACCESS_KEY}",
          "secretKey": "${SECRET_KEY}"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF
chmod 0640 "$S3_CONFIG"

# The gateway runs as uid/gid 1000 (the image's `seaweed` user) and must be
# able to read the rendered file.
chown 1000:1000 "$S3_CONFIG"
chown 1000:1000 "$S3_CONFIG_DIR"

echo "SeaweedFS S3 credentials file is ready."
