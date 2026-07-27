# AkiDB Python SDK

A typed Python client for [AkiDB](../../README.md), a portable retrieval
database for private AI systems.

## Install

```bash
pip install -e ".[dev]"   # from sdks/python
```

## Usage

```python
from akidb import AkiDBClient

with AkiDBClient(
    "localhost:50051",
    timeout=5.0,          # per-call deadline (seconds)
    max_retries=3,        # retries on transient errors, exponential backoff
    tls=True,             # TLS channel
    auth_token="…",       # sent as `authorization: Bearer …`
) as client:
    client.insert("doc-1", embedding, text="the source text", metadata=b'{"lang":"en"}')

    result = client.text_search(
        "why does token refresh fail?",
        top_k=5, hybrid=True, rerank=True, diversity=True, pack=True, token_budget=1024,
    )
    for hit in result:
        print(hit.id, hit.score)
    print(result.context_pack)
```

### Async

```python
from akidb.aio import AsyncAkiDBClient

async with AsyncAkiDBClient("localhost:50051") as client:
    await client.insert("doc-1", embedding, text="hello")
    result = await client.text_search("hello", hybrid=True, pack=True)
```

### Legacy document memory

```python
client.memory_write("m1", embedding, "remember this", kind="note", conversation_id="c1")
hits = client.memory_read(query_embedding, conversation_id="c1")
```

These convenience methods are metadata-backed vector entries and do not
synthesize immutable history.

### Authoritative Memory developer preview (synchronous)

This surface is **experimental**, with one authoritative workspace per server
process. It implements current, valid-time, system-as-of, combined bitemporal,
and history queries for evaluation, but it does not carry a production,
system-of-record, or HA claim.

```python
from pathlib import Path
from akidb import AkiDBClient, MemoryContext, MemoryScope

token = Path("data/memory-preview/principal.token").read_text().strip()
context = MemoryContext(
    "memory-preview",
    "memory/default",
    "agent-memory",
    delegated_agent_id="agent:local-preview",
)
scope = MemoryScope(
    "service:ingestion",
    ("agent-memory",),
    owner_agent_id="agent:local-preview",
)

with AkiDBClient("127.0.0.1:50051", auth_token=token) as client:
    receipt = client.remember_text(
        context,
        scope,
        "uses recovery procedure",
        "Drain the queue before restarting ingestion.",
        idempotency_key="remember-procedure-v1",
        source_plane="operator-note",
        source_id="incident-42",
        reason="operator-confirmed recovery procedure",
    )
    recalled = client.recall(
        context,
        query_text="queue restart",
        max_context_tokens=256,
    )
    exact = client.replay_recall(context, recalled.snapshot_id)
    assert exact.exact_match
```

`observe`, `propose`, `commit_proposal`, `remember`, `remember_text`,
`memory_get`, `recall`, `explain_recall`, `replay_recall`, `correct`,
`retract`, `forget`, `reinforce`, `list_history`, `export_memory`,
`plan_deletion`, `execute_deletion`, and `memory_capabilities` use the separate
authoritative `MemoryService`. `MemoryTemporal` selects the temporal mode.
Credentials are transport metadata; request context can only narrow the
principal grants configured on the server.

### Exact unified Memory + Knowledge replay

`UnifiedRecallCoordinator` executes one typed Memory recall and one typed
Knowledge search, then retains their exact deterministic protobuf bytes and
artifact/generation evidence in a mode-0600 JSON envelope:

```python
from akidb import UnifiedRecallArtifact, UnifiedRecallCoordinator

artifact = UnifiedRecallCoordinator(memory_client, knowledge_client).capture(
    memory_request,
    knowledge_request,
    output_path="unified-recall.json",
)
replayed = UnifiedRecallArtifact.load("unified-recall.json").replay_exact()
assert replayed.rendered_context == artifact.replay_exact().rendered_context
```

The no-clobber, mode-`0600` envelope writer never stores credentials. Loading
fails closed on byte, digest, scope, snapshot, projection-manifest, or
Knowledge-generation mismatch.

## Features

- Per-call **deadlines**, **retry with exponential backoff** on transient codes
  (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`).
- **TLS** + **bearer-token auth**; custom channel options and metadata.
- **Typed errors** (`NotFoundError`, `InvalidArgumentError`, `UnavailableError`, …)
  mapped from gRPC status codes; see `akidb.errors`.
- Full vector/retrieval RPC coverage plus legacy `memory_write`/`memory_read`
  and the synchronous authoritative Memory developer-preview path.
- Sync (`AkiDBClient`) and async (`AsyncAkiDBClient`) clients.

## Tests

```bash
pytest tests/ -v
```

Unit tests mock the gRPC stub, so no running server is required. A live
integration test (`tests/test_live.py`) runs only when `AKIDB_SERVER_ADDR` is set
(with `AKIDB_TEST_DIM` matching the server's index dimension); it is exercised in
CI (`.github/workflows/sdks.yml`) against a real server.

## Observability & resilience knobs

`timeout`, `max_retries`, `retry_backoff` (jittered), `on_retry=(attempt, error)`,
`tls`/`ca_cert`, `auth_token`, `metadata`, and gRPC `interceptors` are all
constructor options on both `AkiDBClient` and `AsyncAkiDBClient`.

## Regenerating gRPC stubs / proto drift

The committed `akidb/akidb_pb2*.py` are generated from the vendored
`proto/akidb.proto` (a copy of `crates/proto/proto/akidb.proto`). Use the
isolated, pinned codegen toolchain so generated files do not accidentally raise
the SDK's runtime minimum:

```bash
python -m venv .codegen-venv
.codegen-venv/bin/pip install -r codegen-requirements.txt
AKIDB_PROTO_PYTHON=.codegen-venv/bin/python ./generate-proto.sh
AKIDB_PROTO_PYTHON=.codegen-venv/bin/python ./generate-proto.sh --check
```

`../check-proto-drift.sh` (and `test_proto_drift.py`) fail if the vendored proto
drifts from the canonical engine proto.
