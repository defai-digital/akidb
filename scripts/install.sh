#!/bin/sh
# Install a prebuilt AkiDB release without a Rust toolchain.
#
#   curl -fsSL https://raw.githubusercontent.com/defai-digital/akidb/main/scripts/install.sh | sh
#
# Supported: macOS Apple Silicon and Linux AMD64 (Ubuntu 24.04+), the platforms
# the release workflow builds and tests. The archive's SHA-256 is always
# verified before anything is extracted or run.
#
# Environment:
#   AKIDB_VERSION      release tag to install (default: latest), e.g. v2.0.0
#   AKIDB_INSTALL_DIR  install prefix (default: $HOME/.akidb)
#   AKIDB_REPO         GitHub owner/name (default: defai-digital/akidb)
#   AKIDB_API_URL      release API base, for mirrors and tests
set -eu

REPO="${AKIDB_REPO:-defai-digital/akidb}"
API="${AKIDB_API_URL:-https://api.github.com/repos/${REPO}}"
VERSION="${AKIDB_VERSION:-latest}"
PREFIX="${AKIDB_INSTALL_DIR:-${HOME:-}/.akidb}"

fail() {
  echo "akidb install: $*" >&2
  exit 1
}

[ -n "${HOME:-}" ] || [ -n "${AKIDB_INSTALL_DIR:-}" ] || fail "HOME is not set; set AKIDB_INSTALL_DIR"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) PLATFORM="macos-arm64" ;;
  Linux-x86_64 | Linux-amd64) PLATFORM="linux-amd64" ;;
  *)
    fail "unsupported platform $(uname -s)-$(uname -m). Supported: macOS Apple Silicon and Linux AMD64. Build from source instead: https://github.com/${REPO}#build"
    ;;
esac

if command -v sha256sum >/dev/null 2>&1; then
  sha256_of() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  fail "sha256sum or shasum is required to verify the download"
fi

case "$VERSION" in
  latest) RELEASE_URL="${API}/releases/latest" ;;
  v*) RELEASE_URL="${API}/releases/tags/${VERSION}" ;;
  *) RELEASE_URL="${API}/releases/tags/v${VERSION}" ;;
esac

WORK="$(mktemp -d "${TMPDIR:-/tmp}/akidb-install.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT INT TERM

echo "Looking up the ${VERSION} release for ${PLATFORM}..."
curl -fsSL "$RELEASE_URL" -o "$WORK/release.json" ||
  fail "could not read the release at ${RELEASE_URL}"

TAG="$(grep -o '"tag_name": *"[^"]*"' "$WORK/release.json" | head -n 1 | sed 's/.*: *"\(.*\)"/\1/')"
[ -n "$TAG" ] || fail "the release response has no tag_name"
case "$TAG" in
  *[!A-Za-z0-9._-]*) fail "unexpected release tag '${TAG}'" ;;
esac

ARCHIVE_URL="$(grep -o "\"browser_download_url\": *\"[^\"]*/akidb-${PLATFORM}-[A-Za-z0-9._-]*\\.tar\\.gz\"" "$WORK/release.json" |
  head -n 1 | sed 's/.*: *"\(.*\)"/\1/')"
[ -n "$ARCHIVE_URL" ] || fail "release ${TAG} has no ${PLATFORM} archive"
ARCHIVE_NAME="${ARCHIVE_URL##*/}"

echo "Downloading ${ARCHIVE_NAME}..."
curl -fsSL "$ARCHIVE_URL" -o "$WORK/$ARCHIVE_NAME" || fail "download failed: ${ARCHIVE_URL}"
curl -fsSL "${ARCHIVE_URL}.sha256" -o "$WORK/expected.sha256" ||
  fail "checksum download failed: ${ARCHIVE_URL}.sha256"

EXPECTED="$(cut -d' ' -f1 <"$WORK/expected.sha256" | head -n 1)"
ACTUAL="$(sha256_of "$WORK/$ARCHIVE_NAME")"
[ -n "$EXPECTED" ] && [ "$EXPECTED" = "$ACTUAL" ] ||
  fail "SHA-256 mismatch for ${ARCHIVE_NAME} (expected '${EXPECTED}', got '${ACTUAL}'); nothing was installed"
echo "SHA-256 verified."

# Refuse archives that could write outside the target directory.
if tar -tzf "$WORK/$ARCHIVE_NAME" | grep -E '(^/|(^|/)\.\.(/|$))' >/dev/null 2>&1; then
  fail "archive contains absolute or parent-relative paths; nothing was installed"
fi

VERSIONS="${PREFIX}/versions"
TARGET="${VERSIONS}/${TAG}"
mkdir -p "$VERSIONS" "${PREFIX}/bin"
STAGE="$(mktemp -d "${VERSIONS}/.${TAG}.XXXXXX")"
tar -xzf "$WORK/$ARCHIVE_NAME" -C "$STAGE"
[ -x "$STAGE/bin/akidb" ] || {
  rm -rf "$STAGE"
  fail "archive does not contain bin/akidb"
}
"$STAGE/bin/akidb" --version >/dev/null 2>&1 || {
  rm -rf "$STAGE"
  fail "the downloaded akidb binary does not run on this machine"
}

rm -rf "$TARGET"
mv "$STAGE" "$TARGET"
chmod 755 "$TARGET"
ln -sfn "../versions/${TAG}/bin/akidb" "${PREFIX}/bin/akidb"

echo "Installed AkiDB ${TAG} to ${TARGET}"
echo "Linked ${PREFIX}/bin/akidb"
case ":${PATH:-}:" in
  *":${PREFIX}/bin:"*) ;;
  *) echo "Add it to your PATH:  export PATH=\"${PREFIX}/bin:\$PATH\"" ;;
esac
echo "Next: akidb --help"
