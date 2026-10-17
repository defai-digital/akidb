#!/usr/bin/env bash
# Include only tracked public operator assets, never ignored local design data.
set -euo pipefail

staging_dir="${1:?usage: package-public-assets.sh STAGING_DIRECTORY}"
while IFS= read -r -d '' path; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "public package asset must be a regular file: $path" >&2
    exit 1
  fi
  install -d "$staging_dir/$(dirname "$path")"
  install -m 0644 "$path" "$staging_dir/$path"
done < <(git ls-files -z -- docs config contracts/fixtures/adoption CONTRIBUTING.md scripts/qa_adoption.py)

for required in docs/development/quickstart.md docs/development/fabric-integration.md \
  contracts/fixtures/adoption/documents/runbook.md CONTRIBUTING.md scripts/qa_adoption.py; do
  [[ -f "$staging_dir/$required" ]] || { echo "missing public asset: $required" >&2; exit 1; }
done
