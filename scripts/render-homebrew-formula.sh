#!/bin/sh
# Print a Homebrew formula for a published AkiDB release.
#
#   scripts/render-homebrew-formula.sh v2.0.0 > Formula/akidb.rb
#
# Run it after the release workflow has published the archives, then commit the
# output to the tap repository. It reads the archive URLs from the release and
# the SHA-256 values from the published .sha256 files, so the formula pins
# exactly what the release workflow built and attested.
#
# Environment:
#   AKIDB_REPO     GitHub owner/name (default: defai-digital/akidb)
#   AKIDB_API_URL  release API base, for mirrors and tests
set -eu

REPO="${AKIDB_REPO:-defai-digital/akidb}"
API="${AKIDB_API_URL:-https://api.github.com/repos/${REPO}}"
TAG="${1:-}"

fail() {
  echo "render-homebrew-formula: $*" >&2
  exit 1
}

[ -n "$TAG" ] || fail "usage: $0 <release-tag>, for example v2.0.0"
case "$TAG" in
  v[0-9]*) ;;
  *) fail "release tag must look like v2.0.0" ;;
esac
command -v curl >/dev/null 2>&1 || fail "curl is required"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/akidb-formula.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT INT TERM
curl -fsSL "${API}/releases/tags/${TAG}" -o "$WORK/release.json" ||
  fail "could not read release ${TAG}"

archive_url() {
  grep -o "\"browser_download_url\": *\"[^\"]*/akidb-$1-[A-Za-z0-9._-]*\\.tar\\.gz\"" "$WORK/release.json" |
    head -n 1 | sed 's/.*: *"\(.*\)"/\1/'
}

checksum() {
  curl -fsSL "$1.sha256" | cut -d' ' -f1 | head -n 1
}

MAC_URL="$(archive_url macos-arm64)"
LINUX_URL="$(archive_url linux-amd64)"
[ -n "$MAC_URL" ] || fail "release ${TAG} has no macos-arm64 archive"
[ -n "$LINUX_URL" ] || fail "release ${TAG} has no linux-amd64 archive"
MAC_SHA="$(checksum "$MAC_URL")"
LINUX_SHA="$(checksum "$LINUX_URL")"
for value in "$MAC_SHA" "$LINUX_SHA"; do
  case "$value" in
    *[!0-9a-f]* | "") fail "a published checksum is not a SHA-256 hex digest" ;;
  esac
  [ "${#value}" -eq 64 ] || fail "a published checksum is not 64 hex characters"
done

cat <<RUBY
class Akidb < Formula
  desc "Local-first retrieval for private AI that returns cited, token-budgeted context for agents"
  homepage "https://github.com/${REPO}"
  version "${TAG#v}"
  license "Apache-2.0"

  # Supported platforms match the release workflow: macOS Apple Silicon and
  # Linux AMD64. Other platforms should build from source.
  on_macos do
    on_arm do
      url "${MAC_URL}"
      sha256 "${MAC_SHA}"
    end
  end

  on_linux do
    on_intel do
      url "${LINUX_URL}"
      sha256 "${LINUX_SHA}"
    end
  end

  def install
    bin.install "bin/akidb"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/akidb --version")
  end
end
RUBY
