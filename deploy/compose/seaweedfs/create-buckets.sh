#!/bin/sh
# Create the SeaweedFS S3 buckets this stack needs.
#
# `weed shell` reports only the status of the last command in a piped script,
# so every bucket gets its own invocation. `s3.bucket.create` is not idempotent
# upstream: re-creating an existing bucket fails with
# "bucket <name> already exists", which is the steady state on a later
# `docker compose up`. That one failure is accepted; every other failure is
# propagated so a broken gateway cannot look like a successful setup.

set -eu

MASTER="${SEAWEEDFS_MASTER:-seaweedfs:9333}"
BUCKETS="${SEAWEEDFS_BUCKETS:-akidb-documents}"

for bucket in $BUCKETS; do
    echo "Ensuring bucket: ${bucket}"
    if output="$(echo "s3.bucket.create -name ${bucket}" | weed shell -master="${MASTER}" 2>&1)"; then
        continue
    fi
    case "$output" in
        *"already exists"*)
            echo "Bucket ${bucket} already exists."
            ;;
        *)
            echo "$output" >&2
            exit 1
            ;;
    esac
done

echo "SeaweedFS bucket setup complete."
