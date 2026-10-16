#!/bin/sh
# Offline test of scripts/install.sh and scripts/render-homebrew-formula.sh
# against a fake release served from file:// URLs.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/akidb-install-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT INT TERM

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) PLATFORM=macos-arm64 ;;
  Linux-x86_64 | Linux-amd64) PLATFORM=linux-amd64 ;;
  *) echo "skip: unsupported test platform"; exit 0 ;;
esac

mkdir -p "$WORK/api/releases/tags" "$WORK/stage/bin" "$WORK/dl"
printf '#!/bin/sh\necho "akidb 9.9.9"\n' >"$WORK/stage/bin/akidb"
chmod +x "$WORK/stage/bin/akidb"
for p in macos-arm64 linux-amd64; do
  tar -czf "$WORK/dl/akidb-$p-abc1234.tar.gz" -C "$WORK/stage" .
  (cd "$WORK/dl" && {
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "akidb-$p-abc1234.tar.gz"
    else
      shasum -a 256 "akidb-$p-abc1234.tar.gz"
    fi
  } >"akidb-$p-abc1234.tar.gz.sha256")
done
cat >"$WORK/api/releases/tags/v9.9.9" <<JSON
{
  "tag_name": "v9.9.9",
  "assets": [
    {"browser_download_url": "file://$WORK/dl/akidb-linux-amd64-abc1234.tar.gz"},
    {"browser_download_url": "file://$WORK/dl/akidb-linux-amd64-abc1234.tar.gz.sha256"},
    {"browser_download_url": "file://$WORK/dl/akidb-macos-arm64-abc1234.tar.gz"},
    {"browser_download_url": "file://$WORK/dl/akidb-macos-arm64-abc1234.tar.gz.sha256"}
  ]
}
JSON
cp "$WORK/api/releases/tags/v9.9.9" "$WORK/api/releases/latest"

export AKIDB_API_URL="file://$WORK/api"
check() { echo "ok - $1"; }
die() { echo "FAIL - $1" >&2; exit 1; }

# 1. Latest install links a working binary.
AKIDB_INSTALL_DIR="$WORK/p1" sh "$ROOT/scripts/install.sh" >/dev/null
[ "$("$WORK/p1/bin/akidb" --version)" = "akidb 9.9.9" ] || die "installed binary does not run"
[ -d "$WORK/p1/versions/v9.9.9" ] || die "versioned directory missing"
check "installs the latest release and links bin/akidb"

# 2. A bare version number resolves to the v-prefixed tag.
AKIDB_VERSION=9.9.9 AKIDB_INSTALL_DIR="$WORK/p2" sh "$ROOT/scripts/install.sh" >/dev/null
[ -x "$WORK/p2/bin/akidb" ] || die "bare version did not install"
check "accepts 9.9.9 as v9.9.9"

# 3. A tampered checksum installs nothing.
echo "deadbeef  x" >"$WORK/dl/akidb-$PLATFORM-abc1234.tar.gz.sha256"
if AKIDB_INSTALL_DIR="$WORK/p3" sh "$ROOT/scripts/install.sh" >/dev/null 2>&1; then
  die "a checksum mismatch was accepted"
fi
[ ! -e "$WORK/p3/versions/v9.9.9" ] || die "mismatch left files behind"
check "refuses a checksum mismatch"

# 4. An unknown version fails cleanly.
if AKIDB_VERSION=v0.0.1 AKIDB_INSTALL_DIR="$WORK/p4" sh "$ROOT/scripts/install.sh" >/dev/null 2>&1; then
  die "an unknown version was accepted"
fi
check "refuses an unknown version"

# 5. Re-installing replaces the same version.
(cd "$WORK/dl" && {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "akidb-$PLATFORM-abc1234.tar.gz"
  else
    shasum -a 256 "akidb-$PLATFORM-abc1234.tar.gz"
  fi
} >"akidb-$PLATFORM-abc1234.tar.gz.sha256")
AKIDB_INSTALL_DIR="$WORK/p1" sh "$ROOT/scripts/install.sh" >/dev/null
[ "$("$WORK/p1/bin/akidb" --version)" = "akidb 9.9.9" ] || die "reinstall broke the binary"
check "reinstalling the same version works"

# 6. The Homebrew formula pins both archives and is valid Ruby.
sh "$ROOT/scripts/render-homebrew-formula.sh" v9.9.9 >"$WORK/akidb.rb"
grep -q 'version "9.9.9"' "$WORK/akidb.rb" || die "formula version missing"
[ "$(grep -c 'sha256 "[0-9a-f]\{64\}"' "$WORK/akidb.rb")" = 2 ] || die "formula must pin two checksums"
if command -v ruby >/dev/null 2>&1; then
  ruby -c "$WORK/akidb.rb" >/dev/null || die "formula is not valid Ruby"
fi
if sh "$ROOT/scripts/render-homebrew-formula.sh" 9.9.9 >/dev/null 2>&1; then
  die "an untagged version was accepted"
fi
check "renders a pinned, valid Homebrew formula"
echo "all install script checks passed"
