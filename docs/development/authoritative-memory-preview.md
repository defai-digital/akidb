# Authoritative Memory Developer Preview

Status: experimental, single-process developer preview. The typed ledger,
bitemporal queries, retained replay, deletion workflow, and quality-extension
contracts are implemented for evaluation. They are not a production,
system-of-record, HA, or fleet qualification.

This profile is separate from AkiDB's legacy metadata-backed
`memory_write`/`memory_read` helpers. New integrations should use the
`MemoryService` gRPC API, the Python client's authoritative Memory methods, or
the explicitly named `memory_remember` and `memory_recall` MCP tools.

## Product boundary

The preview provides one authoritative workspace per server process:

- immutable assertions, versions, evidence, derivations, policy decisions,
  relations, lifecycle transitions, observations, and reinforcement records;
- synced canonical commits with idempotency and expected-head checks;
- ordered, checkpointed structured and incremental BM25 projections;
- deterministic bounded recall, explanation, and retained snapshots;
- current, valid-at, system-as-of, and valid-at-as-known-at queries;
- exact retained replay and re-execution against the retained artifact set;
- correction, retraction, forgetting, history, scoped export, and reviewable
  source/data-subject deletion;
- deterministic context-firewall and authority checks;
- compiler, consolidation, trajectory, AX integration, unified replay, and
  bounded evidence-graph contracts; and
- JSONL, legacy AkiDB, Mem0, Graphiti, and AX Studio SQLite import utilities.

Dense Memory retrieval is not part of the preview recipe. The active
`preview-bounded-bm25-v1` recipe is local and does not require an embedding,
model, cloud, SQL, or object-store service. Existing AkiDB vector and
Knowledge Serving profiles remain separate.

## Start the local profile

From the repository root:

```bash
./scripts/akidb-memory-preview.sh
```

The launcher creates separate legacy and authoritative-principal bearer token
files below `data/memory-preview/`, sets both to mode `0600`, and never prints
their values. The server binds to `127.0.0.1:50051` by default. Override that
loopback address with `AKIDB_MEMORY_PREVIEW_LISTEN`.

The configuration is
[`config/memory-preview.toml`](../../config/memory-preview.toml). It requires
authentication, rejects the legacy principal on `MemoryService`, derives
workspace/namespace/purpose/agent scope from the matching principal grant, and
keeps embedding and SQL disabled. Startup also resolves local data paths and
rejects any Memory ledger overlap with vector state, snapshots, WAL, or SQLite
metadata.

Inspect the server's honest capability response:

```bash
cargo run -p akidb-cli -- memory \
  --workspace memory-preview \
  --namespace memory/default \
  capabilities
```

The token can also be supplied through `AKIDB_MEMORY_PRINCIPAL_TOKEN`; the
environment value takes precedence over `--token-file`. Do not put bearer
tokens in command history, configuration, logs, or evidence artifacts.

## Typed CLI ritual

Commit a text fact:

```bash
cargo run -p akidb-cli -- memory \
  --workspace memory-preview \
  --namespace memory/default \
  remember-text \
  --entity-key service:checkout \
  --predicate recovery.procedure \
  --text "Drain traffic before restarting the checkout worker." \
  --source-plane operator \
  --source-id runbook-42 \
  --idempotency-key remember-runbook-42 \
  --reason "Capture reviewed recovery guidance"
```

The receipt is acknowledgement evidence. A successful synced commit reports a
canonical commit sequence and a visibility receipt for the complete active
projection set. Reusing an idempotency key with different canonical input is
rejected.

Recall deterministically:

```bash
cargo run -p akidb-cli -- memory \
  --workspace memory-preview \
  --namespace memory/default \
  recall \
  --query "checkout worker recovery" \
  --entity service:checkout
```

Keep the returned `snapshot_id`. Exact retained replay returns the stored
typed response and context bytes:

```bash
cargo run -p akidb-cli -- memory \
  --workspace memory-preview \
  --namespace memory/default \
  replay SNAPSHOT_ID
```

Add `--reexecute` to recompute with the snapshot's retained immutable
projection, tokenizer, policy, context-firewall, retrieval-recipe, and server
artifact references and compare the result with the original.

The CLI also exposes assertion history, scoped canonical export,
non-authoritative outcome reinforcement, and deletion plan/execute commands.
Use `cargo run -p akidb-cli -- memory --help` and the relevant subcommand
`--help` for their full arguments.

## Incident replay and MCP

Install the Python SDK and run the end-to-end incident ritual while the
preview server is running:

```bash
python3 -m venv sdks/python/.venv
sdks/python/.venv/bin/pip install -e sdks/python
sdks/python/.venv/bin/python scripts/agentic_memory_incident_replay.py
```

The script commits incorrect guidance, retains the recall that would have led
to a wrong action, commits a successor correction, verifies current recall
changed, and then reproduces the incident snapshot exactly.

For MCP:

```bash
./scripts/akidb-memory-preview.sh --mcp
```

This profile binds the principal credential and fixed scope to the MCP
process. It exposes `memory_remember` and `memory_recall`. The older
`memory_write` and `memory_read` tools remain visibly labeled
`LEGACY DOCUMENT MEMORY`.

## Durability, visibility, and recovery

The canonical RocksDB ledger is authoritative. A mutation and its idempotency
record commit atomically in a synced write batch before acknowledgement.
Projection outbox entries are replayed strictly in sequence. Readiness cannot
advance across a gap, and a commit's visibility receipt is sequence-specific
even when concurrent newer commits arrive.

Structured and lexical projections are rebuildable. Projection manifests bind
the complete immutable artifact set; activation is atomic, so recall cannot
mix tokenizer, lexical, policy, context-firewall, or recipe versions.
Canonical deletion tombstones are replayed into rebuilt projections and
prevent deleted content from returning.

The preview intentionally accepts only indefinite retention windows: zero in
the retention configuration means indefinite. A finite value is rejected
until physical garbage collection and retained-artifact expiry are qualified.
This prevents configuration from promising deletion that the runtime cannot
yet enforce.

## Temporal and replay semantics

`MemoryTemporalQuery` supports:

- `current`;
- `valid_at`, using nanosecond valid time;
- `system_as_of`, using a canonical commit sequence; and
- `valid_at_as_known_at`, combining both dimensions.

Corrections create successor versions and preserve lineage. Retraction and
forgetting are lifecycle mutations; neither rewrites history. Conflict and
derivation relations remain explicit. `ExplainRecall` describes every
candidate admitted to the bounded pool, but does not claim completeness
outside candidate generation.

Exact retained replay is byte reproduction of the stored recall response.
Re-execution is separately labeled and reports exact match, mismatch,
expected nondeterminism, missing/expired artifact, policy denial, or
corruption. Unified Memory-plus-Knowledge replay is available as a deterministic
contract/API helper; it is not a claim that the preview is a qualified
Knowledge Serving deployment.

## Deletion and reinforcement

Deletion is deliberately two-step:

1. `PlanDeletion` discovers the exact source or data-subject impact and
   produces an expiring checksum-bound plan.
2. `ExecuteDeletion` requires the exact prior plan ID and digest, a new
   idempotency key, and deletion authority.

Execution physically removes targeted content from canonical records,
snapshots, and rebuildable projections while retaining content-free
tombstones and audit identity. A full rebuild must not resurrect the deleted
bytes.

`Reinforce` attaches separately evidenced success, failure, or neutral outcome
records to a version. It cannot rewrite assertion content, source assurance,
decision authority, or scope.

## Evaluation and import utilities

The repository includes:

- `scripts/agentic_memory_evaluate.py` for deterministic fixture evaluation,
  paired bootstrap intervals, and permutation tests;
- `scripts/agentic_memory_import.py` for inspect-first, checksum-bound
  conversion from the supported import formats; and
- `scripts/agentic_memory_incident_replay.py` for the product ritual.

The evaluator never invents a competitor result. No-memory, hybrid-RAG, or
direct-competitor reports must come from real runnable systems under a
documented native or controlled-component configuration.

## Observability

When Prometheus metrics are enabled, the Memory surface reports bounded,
content-free labels for:

- commit count and latency;
- projection applied sequence, lag, and gap state;
- recall latency and snapshot outcome;
- authorization decisions;
- quarantine decisions;
- replay comparisons; and
- deletion plans/executions.

Namespace values, entity keys, content, query text, bearer tokens, and
unbounded identifiers are never metric labels. The AMD64 qualification runner
also rejects evidence if its metrics scrape contains the run namespace or
principal token.

## Qualification boundary

The repeatable Linux AMD64 harness is
[`scripts/qualify-agentic-memory-amd64.sh`](../../scripts/qualify-agentic-memory-amd64.sh).
It requires Linux `x86_64`, a clean source tree, fresh absolute work/evidence
directories, an explicit qualification host label, and release binaries by
default. It records correctness,
latency, throughput, disk growth, peak resident memory, live content-free
metrics, server logs, software revision, hardware metadata, and SHA-256
checksums without copying credentials.

Each published 1k/10k/100k size requires at least five independent fresh
process/data runs. Validate the complete four-host evidence tree with:

```bash
python3 scripts/summarize_agentic_memory_benchmarks.py \
  --evidence-dir /absolute/path/to/evidence \
  --expected-git-commit FULL_40_CHARACTER_SHA \
  --expected-host akidb-amd64-1 \
  --expected-host akidb-amd64-2 \
  --expected-host akidb-amd64-3 \
  --expected-host akidb-amd64-4 \
  --output /absolute/path/to/summary.json
```

The aggregator rejects missing or mismatched checksums, revisions, clean-tree
status, platform, host labels/machine identities, configuration, correctness
counters, latency samples, RSS sampling, run counts, or content-free metrics.
Passing summaries include the raw run values and a deterministic bootstrap
interval for every cross-run median; the underlying per-request distributions
remain in the checksum-bound run reports.

The measured four-host result is published in the
[Linux AMD64 Authoritative Memory qualification](../quality/linux-amd64-authoritative-memory-qualification.md),
with its [checksum-bound raw evidence](../quality/evidence/authoritative-memory-amd64-f61f0ba7c511/README.md).
It passed the bounded 1k/10k/100k synthetic systems profile at exact source
commit `f61f0ba7c5113ac6b061e6684f00d846ef1e2b1a`. That result does not change the
experimental, single-process product status or satisfy the external product
gates below. Local or dirty-tree smoke results remain development feedback,
not publishable evidence.

## Current limitations

- One authoritative workspace is configured per process. This is not a
  multi-tenant or fleet topology.
- TLS is disabled only because the shipped preview binds to loopback. Any
  remote listener requires transport security and a separately reviewed
  deployment profile.
- There is no authoritative Memory HA, quorum, automated placement,
  resharding, or cross-process convergence.
- Finite retention and artifact garbage collection are not enabled.
- Dense retrieval and broad framework-adapter packaging are not part of this
  profile.
- External quickstart attempts, pilots, competitive wins, statistically
  supported workload advantage, and independent reproduction remain product
  evidence gates. Repository tests cannot substitute for them.
- The implemented bitemporal surface must not be described publicly as a
  production system of record until its complete technical, operational, and
  external governance gates are accepted.

## Verification

Minimum focused checks:

```bash
cargo test -p akidb-contracts
cargo test -p akidb-storage
cargo test -p akidb-grpc
cargo test -p akidb-cli
cargo test -p akidb-benchmark
(cd sdks/python && pytest tests/ -v)
pytest scripts/tests -v
./sdks/check-proto-drift.sh
(cd sdks/python && \
  AKIDB_PROTO_PYTHON=/path/to/pinned-codegen-venv/bin/python \
  ./generate-proto.sh --check)
```

Create that isolated environment from
`sdks/python/codegen-requirements.txt`; the runtime SDK environment is
deliberately independent of the pinned generator.

Run `cargo test --workspace` before committing a candidate. A clean Linux
AMD64 evidence run must use the qualification script and exact committed
revision; the resulting systems benchmark does not establish semantic quality,
market parity, HA, or production readiness.
