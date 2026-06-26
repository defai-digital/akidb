#!/usr/bin/env python3
"""Ingest parsed text into AkiDB with embeddings.

This script generates embeddings and writes batch files for grpcurl to process.
"""
import base64
import json
import re
import sys

def main():
    # Read parsed text
    with open("/data/parsed_text.txt", "r") as f:
        text = f.read()

    print(f"Text length: {len(text)}")

    # Chunk text
    chunks = []
    sentences = re.split(r"(?<=[。.!?！？\n])", text)
    current_chunk = ""
    chunk_id = 0

    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current_chunk) + len(sentence) > 500 and current_chunk:
            chunks.append((f"doc1_chunk_{chunk_id:04d}", current_chunk.strip()))
            chunk_id += 1
            overlap = current_chunk[-100:] if len(current_chunk) > 100 else current_chunk
            current_chunk = overlap + sentence
        else:
            current_chunk += sentence

    if current_chunk.strip():
        chunks.append((f"doc1_chunk_{chunk_id:04d}", current_chunk.strip()))

    print(f"Created {len(chunks)} chunks")

    # Load embedding model
    print("Loading embedding model...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-mpnet-base-v2")
    dim = model.get_sentence_embedding_dimension()
    print(f"Model loaded, embedding dim: {dim}")

    # Generate embeddings and write batch files
    COLLECTION = "documents"
    print("Generating embeddings and writing batch files...")

    batch_files = []
    for i in range(0, len(chunks), 5):
        batch = chunks[i:i+5]
        vectors = []
        for cid, ctext in batch:
            embedding = model.encode(ctext).tolist()
            metadata = json.dumps({
                "source": "2026012101176_c.pdf",
                "chunk_id": cid,
                "text": ctext[:500]
            }, ensure_ascii=False)
            vectors.append({
                "id": cid,
                "embedding": embedding,
                "metadata": base64.b64encode(metadata.encode()).decode()
            })

        request = {"collection": COLLECTION, "vectors": vectors}

        # Write batch request to file
        batch_file = f"/data/batch_{i//5:04d}.json"
        with open(batch_file, "w") as f:
            json.dump(request, f)
        batch_files.append(batch_file)
        print(f"Wrote batch {i//5 + 1} to {batch_file}")

    # Write list of batch files for processing
    with open("/data/batch_files.txt", "w") as f:
        for bf in batch_files:
            f.write(bf + "\n")

    print(f"Done! Generated {len(batch_files)} batch files.")
    print("Run grpcurl on host to insert into AkiDB.")

if __name__ == "__main__":
    main()
