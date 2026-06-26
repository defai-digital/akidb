#!/usr/bin/env python3
"""Query AkiDB via gRPC API.

This script:
1. Parses PDF using doc-parser service
2. Chunks text and generates embeddings via vLLM
3. Inserts vectors into AkiDB via gRPC
4. Queries AkiDB via gRPC for semantic search
"""

import argparse
import json
import re
import struct
import socket
import httpx

# AkiDB gRPC uses a simple binary protocol
# We'll implement a minimal client without proto compilation

THOR_HOST = "192.168.1.61"
DOC_PARSER_URL = f"http://{THOR_HOST}:8080"
VLLM_URL = f"http://{THOR_HOST}:8000"
AKIDB_HOST = THOR_HOST
AKIDB_PORT = 50051

COLLECTION = "documents"
EMBEDDING_DIM = 4096


def parse_pdf(pdf_path: str) -> str:
    """Parse PDF using doc-parser service."""
    print(f"Parsing PDF: {pdf_path}")
    with open(pdf_path, "rb") as f:
        content = f.read()

    response = httpx.post(
        f"{DOC_PARSER_URL}/parse",
        files={"file": (pdf_path.split("/")[-1], content, "application/pdf")},
        timeout=300.0
    )
    response.raise_for_status()
    result = response.json()
    print(f"Parsed {result.get('page_count', 0)} pages, {len(result.get('text', ''))} chars")
    return result.get("text", "")


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[tuple[str, str]]:
    """Split text into overlapping chunks with IDs."""
    chunks = []
    sentences = re.split(r'(?<=[。.!?！？\n])', text)

    current_chunk = ""
    chunk_id = 0

    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current_chunk) + len(sentence) > chunk_size and current_chunk:
            chunks.append((f"chunk_{chunk_id:04d}", current_chunk.strip()))
            chunk_id += 1
            # Keep overlap
            words = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
            current_chunk = words + sentence
        else:
            current_chunk += sentence

    if current_chunk.strip():
        chunks.append((f"chunk_{chunk_id:04d}", current_chunk.strip()))

    print(f"Created {len(chunks)} chunks")
    return chunks


def get_embedding(text: str) -> list[float]:
    """Get embedding from vLLM service."""
    response = httpx.post(
        f"{VLLM_URL}/v1/embeddings",
        json={
            "model": "Qwen/Qwen3-Embedding-8B",
            "input": text
        },
        timeout=60.0
    )
    response.raise_for_status()
    result = response.json()
    return result["data"][0]["embedding"]


def get_embeddings_batch(texts: list[str], batch_size: int = 10) -> list[list[float]]:
    """Get embeddings for multiple texts in batches."""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        print(f"Getting embeddings for batch {i//batch_size + 1}/{(len(texts) + batch_size - 1)//batch_size}")
        response = httpx.post(
            f"{VLLM_URL}/v1/embeddings",
            json={
                "model": "Qwen/Qwen3-Embedding-8B",
                "input": batch
            },
            timeout=120.0
        )
        response.raise_for_status()
        result = response.json()
        for item in result["data"]:
            all_embeddings.append(item["embedding"])
    return all_embeddings


# Simple gRPC implementation for AkiDB
# Using HTTP/2 with gRPC framing

def make_grpc_request(host: str, port: int, service: str, method: str, request_data: bytes) -> bytes:
    """Make a gRPC request using raw HTTP/2 framing over TCP."""
    # For simplicity, we'll use grpcurl via subprocess or a simple HTTP/2 client
    # Since we need to query AkiDB, let's use the httpx with h2 support
    import subprocess
    import base64

    # Use grpcurl if available, otherwise fall back to direct socket
    try:
        # First try grpcurl
        result = subprocess.run(
            ["which", "grpcurl"],
            capture_output=True
        )
        if result.returncode == 0:
            return _grpcurl_request(host, port, service, method, request_data)
    except Exception:
        pass

    # Fall back to raw gRPC over HTTP/2
    return _raw_grpc_request(host, port, service, method, request_data)


def _grpcurl_request(host: str, port: int, service: str, method: str, json_data: str) -> dict:
    """Use grpcurl for gRPC requests."""
    import subprocess

    cmd = [
        "grpcurl",
        "-plaintext",
        "-d", json_data,
        f"{host}:{port}",
        f"{service}/{method}"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"grpcurl failed: {result.stderr}")

    return json.loads(result.stdout) if result.stdout else {}


def check_health() -> dict:
    """Check AkiDB health."""
    print(f"Checking AkiDB health at {AKIDB_HOST}:{AKIDB_PORT}")
    return _grpcurl_request(AKIDB_HOST, AKIDB_PORT, "akidb.v1.Akidb", "Health", "{}")


def insert_vectors(vectors: list[tuple[str, list[float], str]]) -> dict:
    """Insert vectors into AkiDB.

    Args:
        vectors: List of (id, embedding, metadata_json) tuples
    """
    print(f"Inserting {len(vectors)} vectors into AkiDB")

    # Build the InsertBatch request
    vector_list = []
    for vid, embedding, metadata in vectors:
        vector_list.append({
            "id": vid,
            "embedding": embedding,
            "metadata": metadata.encode().hex() if metadata else ""
        })

    request = {
        "collection": COLLECTION,
        "vectors": vector_list
    }

    return _grpcurl_request(AKIDB_HOST, AKIDB_PORT, "akidb.v1.Akidb", "InsertBatch", json.dumps(request))


def search_vectors(query_embedding: list[float], top_k: int = 5) -> dict:
    """Search for similar vectors in AkiDB."""
    print(f"Searching AkiDB for top {top_k} results")

    request = {
        "collection": COLLECTION,
        "query": query_embedding,
        "topK": top_k
    }

    return _grpcurl_request(AKIDB_HOST, AKIDB_PORT, "akidb.v1.Akidb", "Search", json.dumps(request))


def ingest_pdf(pdf_path: str) -> None:
    """Ingest a PDF into AkiDB."""
    # Parse PDF
    text = parse_pdf(pdf_path)

    # Chunk text
    chunks = chunk_text(text)

    # Get embeddings
    chunk_texts = [c[1] for c in chunks]
    embeddings = get_embeddings_batch(chunk_texts)

    # Prepare vectors with metadata
    vectors = []
    for (chunk_id, chunk_text), embedding in zip(chunks, embeddings):
        metadata = json.dumps({
            "source": pdf_path.split("/")[-1],
            "chunk_id": chunk_id,
            "text": chunk_text[:500]  # Store first 500 chars as preview
        })
        vectors.append((chunk_id, embedding, metadata))

    # Insert into AkiDB in batches
    batch_size = 50
    for i in range(0, len(vectors), batch_size):
        batch = vectors[i:i+batch_size]
        print(f"Inserting batch {i//batch_size + 1}/{(len(vectors) + batch_size - 1)//batch_size}")
        result = insert_vectors(batch)
        print(f"Result: {result}")


def query_akidb(query: str, top_k: int = 5) -> list[dict]:
    """Query AkiDB with a text query."""
    print(f"Query: {query}")

    # Get query embedding
    query_embedding = get_embedding(query)
    print(f"Got query embedding ({len(query_embedding)} dims)")

    # Search AkiDB
    result = search_vectors(query_embedding, top_k)

    print(f"\nSearch Results:")
    print(f"  Latency: {result.get('latencyUs', 0)}us")
    print(f"  Coverage: {result.get('coverage', 0)}")
    print(f"  Within SLO: {result.get('withinSlo', False)}")

    results = []
    for i, r in enumerate(result.get("results", [])):
        metadata = {}
        if r.get("metadata"):
            try:
                # Metadata might be hex-encoded or plain JSON
                meta_str = r["metadata"]
                if all(c in '0123456789abcdef' for c in meta_str.lower()):
                    meta_str = bytes.fromhex(meta_str).decode()
                metadata = json.loads(meta_str)
            except Exception:
                metadata = {"raw": r.get("metadata", "")}

        print(f"\n  [{i+1}] ID: {r.get('id')}, Score: {r.get('score', 0):.4f}")
        if metadata.get("text"):
            print(f"      Text: {metadata['text'][:200]}...")

        results.append({
            "id": r.get("id"),
            "score": r.get("score"),
            "metadata": metadata
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="AkiDB Query Tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Health check
    health_parser = subparsers.add_parser("health", help="Check AkiDB health")

    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest a PDF into AkiDB")
    ingest_parser.add_argument("pdf_path", help="Path to PDF file")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query AkiDB")
    query_parser.add_argument("query", help="Search query text")
    query_parser.add_argument("--top-k", type=int, default=5, help="Number of results")

    args = parser.parse_args()

    if args.command == "health":
        result = check_health()
        print(f"Health: {json.dumps(result, indent=2)}")

    elif args.command == "ingest":
        ingest_pdf(args.pdf_path)

    elif args.command == "query":
        query_akidb(args.query, args.top_k)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
