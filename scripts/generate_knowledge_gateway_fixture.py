#!/usr/bin/env python3
"""Generate deterministic BM25 or graph gateway qualification cases."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def positive(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--workspace", default="qualification")
    parser.add_argument("--collection", default="knowledge")
    parser.add_argument("--cases", type=positive, default=100)
    parser.add_argument("--documents", type=positive, default=250)
    parser.add_argument("--chunks-per-document", type=positive, default=4)
    parser.add_argument("--mode", choices=("bm25", "graph"), default="graph")
    return parser.parse_args()


def main() -> None:
    options = args()
    if options.cases > options.documents:
        raise SystemExit("--cases cannot exceed --documents")
    if len(options.manifest_sha256) != 64 or any(
        char not in "0123456789abcdef" for char in options.manifest_sha256
    ):
        raise SystemExit("--manifest-sha256 must be lowercase SHA-256")

    cases = []
    for document_index in range(options.cases):
        first_chunk = document_index * options.chunks_per_document
        expected_chunks = [
            f"qualification-chunk-{index:08d}"
            for index in range(first_chunk, first_chunk + options.chunks_per_document)
        ]
        query_index = first_chunk + (document_index % options.chunks_per_document)
        cases.append(
            {
                "case_id": f"{options.mode}-{document_index:04d}",
                "query": f"qualification record {query_index}",
                "options": {
                    "topK": 10,
                    "hybrid": True,
                    "pack": True,
                    "tokenBudget": 4096,
                    "retrievalMode": options.mode,
                    "includeDiagnostics": True,
                    "graphMaxDepth": 2,
                    "graphPerSeedFanout": 8,
                    "graphMaxExpandedNodes": 64,
                },
                "expected_chunk_ids": expected_chunks,
                "expected_document_ids": [
                    f"qualification-document-{document_index:08d}"
                ],
                "expected_edge_ids": [],
                "expected_predicates": [],
                "forbidden_chunk_ids": [],
                "forbidden_document_ids": [],
                "expected_resolved_mode": options.mode,
                "minimum_graph_expanded_nodes": 3 if options.mode == "graph" else 0,
            }
        )

    fixture = {
        "schema_version": 1,
        "fixture_id": (
            f"deterministic-document-graph-{options.mode}-"
            f"{options.documents}x{options.chunks_per_document}"
        ),
        "expected_serving": {
            "workspace_id": options.workspace,
            "collection": options.collection,
            "generation_id": options.generation_id,
            "manifest_sha256": options.manifest_sha256,
            "minimum_sequence": 0,
        },
        "cases": cases,
    }
    options.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = options.output.with_name(f".{options.output.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(fixture, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, options.output)
    print(
        json.dumps(
            {
                "output": str(options.output),
                "fixture_id": fixture["fixture_id"],
                "cases": len(cases),
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
