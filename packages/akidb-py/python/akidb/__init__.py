"""AkiDB — embedded vector database for Python (Rust-powered).

Usage:

    from akidb import AkiDB

    with AkiDB("/path/to/db") as db:
        db.create_collection("docs", dimension=384, metric="cosine",
                             embedding_model_id="bge-small-en-v1.5")
        db.upsert_batch("docs", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [...], "metadata": {"title": "Hello"}},
        ])
        db.publish("docs", embedding_model_id="bge-small-en-v1.5", pipeline_signature="v1")
        results = db.search("docs", query_vector=[...], top_k=5)
"""

from akidb._native import AkiDB

__all__ = ["AkiDB"]
__version__ = "3.0.0"
