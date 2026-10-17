# First cited context pack

This tutorial describes the current source checkout. `start`, `index`, and
`pack` require a release newer than v2.0.0; `doctor`, exclusions and local source
verification require a build containing those additions. Do not assume the
latest published binary matches this checkout. Check `akidb --help`.

## Prepare

Use macOS 26 Apple Silicon or Ubuntu 24.04+ AMD64. Follow the native build
prerequisites in [Platform Support](../platform/SUPPORT.md), then:

```bash
cargo build --release -p akidb-cli
export PATH="$PWD/target/release:$PATH"
```

Start your local OpenAI-compatible embedding service. For an existing Ollama
installation, start `ollama serve` and run `ollama pull nomic-embed-text`.
The default profile expects that model's 768-dimensional embeddings. For another
model, pass matching `--embedding-url`, `--embedding-model` and `--dimensions`
to `start`, `doctor`, and `index`. The pack query uses the server's embedding
configuration. Model download time is separate from time to the first pack.

## Index and retrieve

In terminal one:

```bash
akidb start
```

In terminal two, from this checkout:

```bash
akidb doctor
akidb index contracts/fixtures/adoption/documents --dry-run
akidb index contracts/fixtures/adoption/documents
akidb pack "Cedar service health endpoint" --retrieval-mode bm25 \
  --verify-sources-under contracts/fixtures/adoption/documents --json
```

Expect a passage from `runbook.md` containing port `7410`, a source URI and byte
offsets, and `source_verification: "verified"`. This check uses an exact lexical
query so the expected result does not depend on a model's semantic ranking.
Try the default hybrid mode next by omitting `--retrieval-mode bm25`.

Source verification is opt-in: it reads only regular local files underneath the
chosen directory, refuses symlinks, and never fetches URLs. It checks passage
bytes, SHA-256 (including the original indexer's legacy prefix), document version
and byte offsets against the current file. It does not verify factual truth.
Fabric generations can refer to historical or remote sources, which this local
check cannot validate. `citation_status: "complete"` only describes source
metadata presence; it is not equivalent to `source_verification: "verified"`.

`conservative_v1` is an estimate. The pack fits the budget measured by that
counter, not a guaranteed token limit for every model. `truncated` reports budget
trimming; `candidate_limited` reports a bounded candidate search. Neither means
that the returned context is a complete answer.

## Choose what to index

Hidden entries, symlinks, dependency/build folders, unsupported files, and
common credential/key filenames are excluded. This is not a secret-content
scanner. Inspect the dry run before indexing your own folder.

Use repeated `--exclude` options or a `.akidbignore` file at the indexing root:

```text
# Literal paths only; basenames match at any depth.
private
config/production.toml
```

Paths are relative to the selected root. Globs, negation, absolute paths and
parent components are rejected. Existing indexed files that become excluded
are scheduled for removal on a successful indexing run; the dry run previews
that cleanup. The same rules apply when selecting a file explicitly.

Each file is embedded before a bounded `ReplaceDocument` RPC commits it.
Queries see a complete old or new file revision. Replacement briefly excludes
other data-plane operations on the shard; a busy shard can return a retryable
conflict. Limits are 2,048 chunks and 3 MiB per encoded request, including vectors.
Split larger documents before indexing. Old servers and the coordinator reject
this protocol; there is no delete/insert fallback. Fabric generations retain
their separate publication contract.

The server checks every chunk before an unchanged file is skipped. An empty file
commits an empty revision; a removed or explicitly excluded file is pruned using
local indexing receipts. Preserve `--state-dir`: without its receipts, files
already missing from disk cannot be discovered for pruning. Unreadable,
oversized, or non-UTF-8 files preserve their previously indexed version.
After an ambiguous timeout, retry to resolve the committed state. A projection
failure makes the server unready until restart rebuilds its derived indexes.

## Diagnose and contribute

`akidb doctor --json` probes server readiness, collection model/dimensions and
the configured embedder with a fixed public phrase. Output omits URLs, local
paths, credentials, document text and remote error bodies. A model binding that
the server does not declare is marked unverified. No report is uploaded.

For a server requiring authentication, set `AKIDB_AUTH_TOKEN_FILE` to its token
file, or set `AKIDB_AUTH_TOKEN`. The `doctor`, `index`, and `pack` commands use
these explicit credentials. Set `AKIDB_WORKSPACE` for a non-default workspace.

Stop the foreground server with Ctrl-C. For multiple local instances, select
another `--grpc-port` and data directory, and pass the matching `--server` to
client commands. See [Contributing](../../CONTRIBUTING.md) to report failures.
For knowledge publication, use the [Fabric integration path](fabric-integration.md).
