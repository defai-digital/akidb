#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" || "$(uname -m)" != "arm64" ]]; then
  echo "package-macos-arm64.sh must run on macOS Apple Silicon" >&2
  exit 1
fi

release_id="${1:-$(git rev-parse HEAD)}"
output_dir="${2:-dist}"
build_jobs="${AKIDB_BUILD_JOBS:-2}"

if [[ ! "$release_id" =~ ^[A-Za-z0-9._-]{7,64}$ ]]; then
  echo "release id must be 7-64 characters from A-Z, a-z, 0-9, dot, underscore, or dash" >&2
  exit 1
fi
if [[ ! "$build_jobs" =~ ^[1-9][0-9]*$ ]]; then
  echo "AKIDB_BUILD_JOBS must be a positive integer" >&2
  exit 1
fi

export AKIDB_GIT_COMMIT="${AKIDB_GIT_COMMIT:-$(git rev-parse HEAD)}"

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
staging_dir="$(mktemp -d "${TMPDIR:-/tmp}/akidb-package.XXXXXXXX")"
trap 'rm -rf "$staging_dir"' EXIT
chmod 0755 "$staging_dir"

if [[ "${AKIDB_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --locked --release \
    --jobs "$build_jobs" \
    -p akidb-server \
    -p akidb-coordinator \
    -p akidb-cli \
    -p akidb-benchmark
fi

install -d "$staging_dir/bin"
for binary in \
  akidb \
  akidb-server \
  akidb-coordinator \
  akidb-bench \
  akidb-ann-bench \
  akidb-graph-bench
do
  install -m 0755 "target/release/$binary" "$staging_dir/bin/$binary"
done
install -m 0644 LICENSE "$staging_dir/LICENSE"
install -m 0644 README.md "$staging_dir/README.md"

cargo_version="$(
  awk '
    $0 == "[workspace.package]" {
      in_workspace_package = 1
      next
    }
    /^\[/ {
      in_workspace_package = 0
    }
    in_workspace_package && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' Cargo.toml
)"
if [[ -z "$cargo_version" ]]; then
  echo "workspace package version is missing from Cargo.toml" >&2
  exit 1
fi

source_epoch="${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}"
archive_name="akidb-macos-arm64-${release_id}.tar.gz"
archive_path="$output_dir/$archive_name"

printf '%s\n' \
  '{' \
  "  \"release_id\": \"$release_id\"," \
  "  \"version\": \"$cargo_version\"," \
  '  "target": "aarch64-apple-darwin",' \
  '  "akidb_server_features": [],' \
  "  \"build_jobs\": $build_jobs," \
  "  \"source_date_epoch\": $source_epoch" \
  '}' >"$staging_dir/manifest.json"

# COPYFILE_DISABLE prevents AppleDouble metadata from entering the archive.
COPYFILE_DISABLE=1 tar -czf "$archive_path" -C "$staging_dir" .
(
  cd "$output_dir"
  shasum -a 256 "$archive_name" >"${archive_name}.sha256"
)

printf 'artifact=%s\n' "$archive_path"
printf 'checksum=%s.sha256\n' "$archive_path"
