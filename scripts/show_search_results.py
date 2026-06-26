#!/usr/bin/env python3
"""Show text content for search result IDs."""
import json
import base64
import sys

# Top search result IDs
top_ids = ["doc1_chunk_0016", "doc1_chunk_0017", "doc1_chunk_0037", "doc1_chunk_0035", "doc1_chunk_0000"]

found = {}
for batch_num in range(8):
    batch_file = f"/data/batch_{batch_num:04d}.json"
    try:
        with open(batch_file) as f:
            data = json.load(f)
        for vec in data["vectors"]:
            if vec["id"] in top_ids:
                meta = json.loads(base64.b64decode(vec["metadata"]).decode())
                found[vec["id"]] = meta["text"]
    except Exception as e:
        pass

# Print in order of results
for vid in top_ids:
    if vid in found:
        print(f"=== {vid} ===")
        print(found[vid])
        print()
