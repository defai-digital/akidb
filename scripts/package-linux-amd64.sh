#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "package-linux-amd64.sh must run on Linux x86_64" >&2
  exit 1
fi

release_id="${1:-$(git rev-parse HEAD)}"
output_dir="${2:-dist}"

if [[ ! "$release_id" =~ ^[A-Za-z0-9._-]{7,64}$ ]]; then
  echo "release id must be 7-64 characters from A-Z, a-z, 0-9, dot, underscore, or dash" >&2
  exit 1
fi

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
staging_dir="$(mktemp -d "${TMPDIR:-/tmp}/akidb-package.XXXXXXXX")"
trap 'rm -rf "$staging_dir"' EXIT
chmod 0755 "$staging_dir"

if [[ "${AKIDB_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --locked --release \
    -p akidb-coordinator \
    -p akidb-cli \
    -p akidb-benchmark
  cargo build --locked --release \
    -p akidb-server \
    --features generation-s3
fi

install -d "$staging_dir/bin"
for binary in akidb akidb-server akidb-coordinator akidb-bench; do
  install -m 0755 "target/release/$binary" "$staging_dir/bin/$binary"
done

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
archive_name="akidb-linux-amd64-${release_id}.tar.gz"
archive_path="$output_dir/$archive_name"

printf '%s\n' \
  '{' \
  "  \"release_id\": \"$release_id\"," \
  "  \"version\": \"$cargo_version\"," \
  '  "target": "x86_64-unknown-linux-gnu",' \
  '  "akidb_server_features": ["generation-s3"],' \
  "  \"source_date_epoch\": $source_epoch" \
  '}' >"$staging_dir/manifest.json"

tar \
  --sort=name \
  "--mtime=@${source_epoch}" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -czf "$archive_path" \
  -C "$staging_dir" \
  .

(
  cd "$output_dir"
  sha256sum "$archive_name" >"${archive_name}.sha256"
)

printf 'artifact=%s\n' "$archive_path"
printf 'checksum=%s.sha256\n' "$archive_path"
