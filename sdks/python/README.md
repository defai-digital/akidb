# AkiDB Python SDK

A typed, production-grade Python client for [AkiDB](../../README.md) — a Mac-native
retrieval memory engine for private AI agents.

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

### Agent memory

```python
client.memory_write("m1", embedding, "remember this", kind="note", conversation_id="c1")
hits = client.memory_read(query_embedding, conversation_id="c1")
```

## Features

- Per-call **deadlines**, **retry with exponential backoff** on transient codes
  (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`).
- **TLS** + **bearer-token auth**; custom channel options and metadata.
- **Typed errors** (`NotFoundError`, `InvalidArgumentError`, `UnavailableError`, …)
  mapped from gRPC status codes; see `akidb.errors`.
- Full RPC coverage: insert, insert_batch, update, get, delete, search,
  search_batch, text_search, health, cluster_state, plus memory_write/read.
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
`proto/akidb.proto` (a copy of `crates/grpc-server/proto/akidb.proto`):

```bash
python -m grpc_tools.protoc -I proto \
  --python_out=akidb --grpc_python_out=akidb proto/akidb.proto
# then make the grpc import relative: `from . import akidb_pb2 as akidb__pb2`
```

`../check-proto-drift.sh` (and `test_proto_drift.py`) fail if the vendored proto
drifts from the canonical engine proto.
