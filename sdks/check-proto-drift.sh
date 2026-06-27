#!/usr/bin/env bash
# Fail if either SDK's vendored proto has drifted from the canonical engine proto.
# Run from anywhere; intended for CI and pre-release checks.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CANON="$ROOT/crates/grpc-server/proto/akidb.proto"

if [[ ! -f "$CANON" ]]; then
  echo "canonical proto not found at $CANON" >&2
  exit 2
fi

fail=0
for copy in \
  "$ROOT/sdks/python/proto/akidb.proto" \
  "$ROOT/sdks/typescript/proto/akidb.proto"; do
  if diff -q "$CANON" "$copy" >/dev/null 2>&1; then
    echo "OK    $copy"
  else
    echo "DRIFT $copy differs from canonical proto:" >&2
    diff "$CANON" "$copy" || true
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo "" >&2
  echo "Proto drift detected. Re-vendor the proto and regenerate stubs:" >&2
  echo "  cp $CANON \$SDK/proto/akidb.proto   # and regenerate (see SDK README)" >&2
fi
exit "$fail"
