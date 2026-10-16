# Installing AkiDB releases

Release archives are built, tested, and attested by
`.github/workflows/release.yml` for the two supported targets: macOS 26 Apple
Silicon and Ubuntu 24.04+ AMD64. Other platforms build from source.

## Install script

```bash
curl -fsSL https://raw.githubusercontent.com/defai-digital/akidb/main/scripts/install.sh | sh
```

`scripts/install.sh` looks up the release through the GitHub API, downloads the
archive for the current platform and its `.sha256`, refuses a mismatch, rejects
archives with absolute or parent-relative paths, checks that the binary runs,
and then moves it into `$AKIDB_INSTALL_DIR/versions/<tag>` (default
`~/.akidb/versions/<tag>`). `bin/akidb` is a symlink to the active version, so
re-running the script upgrades and keeping older versions allows rollback by
re-pointing the link.

| Variable | Meaning |
| --- | --- |
| `AKIDB_VERSION` | Release tag, for example `v2.0.0` (default: latest) |
| `AKIDB_INSTALL_DIR` | Install prefix (default: `~/.akidb`) |
| `AKIDB_REPO` | GitHub `owner/name` (default: `defai-digital/akidb`) |
| `AKIDB_API_URL` | Release API base, for mirrors and tests |

For an air-gapped host, download the archive and `.sha256` on another machine,
verify with `shasum -a 256 -c`, copy both over, and extract with
`tar -xzf`. The archive's `bin/akidb` is the only binary most users need; the
server, MCP server, and CLI are subcommands of it.

## Homebrew formula

`scripts/render-homebrew-formula.sh` prints a formula for a published release,
with both archive URLs and their published SHA-256 values:

```bash
scripts/render-homebrew-formula.sh v2.0.0 > Formula/akidb.rb
```

Run it after the release workflow finishes, then commit the result to the
Homebrew tap repository. A formula is only as available as that tap; until the
tap is updated, `brew install` will not find the new release.

## Tests

`scripts/tests/test_install.sh` exercises both scripts offline against a fake
release served from `file://` URLs, including checksum-mismatch and
unknown-version refusals. CI runs it together with ShellCheck.
