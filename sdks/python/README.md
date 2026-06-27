# AkiDB Python SDK

A typed Python client for [AkiDB](../../README.md) — a Mac-native retrieval
memory engine for private AI agents.

## Install

```bash
pip install -e ".[dev]"   # from sdks/python
```

## Usage

```python
from akidb import AkiDBClient

with AkiDBClient("localhost:50051") as client:
    client.insert("doc-1", embedding, text="the source text", metadata=b'{"lang":"en"}')

    # Hybrid search with reranking, diversity, and a cited context pack:
    result = client.text_search(
        "why does token refresh fail?",
        top_k=5,
        hybrid=True,
        rerank=True,
        diversity=True,
        pack=True,
        token_budget=1024,
    )
    for hit in result:
        print(hit.id, hit.score)
    print(result.context_pack)
```

## Regenerating gRPC stubs

The committed `akidb/akidb_pb2*.py` are generated from
`crates/grpc-server/proto/akidb.proto`:

```bash
python -m grpc_tools.protoc -I ../../crates/grpc-server/proto \
  --python_out=akidb --grpc_python_out=akidb \
  ../../crates/grpc-server/proto/akidb.proto
# then make the grpc import relative: `from . import akidb_pb2 as akidb__pb2`
```

## Tests

```bash
pytest tests/ -v
```

Tests mock the gRPC stub, so no running server is required.
