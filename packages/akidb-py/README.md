# AkiDB Python SDK

Embedded vector database for Python, powered by Rust.

## Installation

```bash
pip install akidb
```

## Quick Start

```python
from akidb import AkiDB

with AkiDB("./my_db") as db:
    # Create a collection
    db.create_collection(
        collection_id="docs",
        dimension=384,
        metric="cosine",
        embedding_model_id="bge-small-en-v1.5",
    )

    # Insert records
    db.upsert_batch("docs", [
        {
            "chunk_id": "c1",
            "doc_id": "d1",
            "vector": [0.1] * 384,
            "metadata": {"title": "Hello World"},
            "chunk_text": "Hello world document text",
        },
    ])

    # Publish a searchable snapshot
    db.publish("docs", "bge-small-en-v1.5", "v1")

    # Search
    results = db.search("docs", query_vector=[0.1] * 384, top_k=5)
    for r in results["results"]:
        print(f"{r['chunk_id']}: {r['score']:.4f}")
```

## Features

- HNSW index backend
- FP16 and SQ8 vector quantization
- Keyword (BM25) and hybrid (RRF) search
- Manifest-based versioning with rollback
- Background compaction
- Context manager support
- Full type stubs for IDE autocompletion

## Development

```bash
# Install maturin
pip install maturin

# Build and install locally
cd packages/akidb-py
maturin develop

# Run tests
pip install pytest
pytest tests/
```
