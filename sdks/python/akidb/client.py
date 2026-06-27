"""AkiDB Python client.

A thin, typed wrapper over the AkiDB gRPC API for AI application developers
(INT-002). Construct with a target address, or inject a stub for testing.

    from akidb import AkiDBClient

    client = AkiDBClient("localhost:50051")
    client.insert("doc-1", [0.1, 0.2, 0.3], text="hello world")
    results = client.text_search("hello", top_k=5, hybrid=True)
"""

from __future__ import annotations

import json
from typing import Any, Optional, Sequence

from . import akidb_pb2 as pb
from . import akidb_pb2_grpc as pb_grpc

DEFAULT_COLLECTION = "default"


class SearchHit:
    """A single retrieval result."""

    __slots__ = ("id", "score", "metadata")

    def __init__(self, id: str, score: float, metadata: str):
        self.id = id
        self.score = score
        self.metadata = metadata

    def metadata_json(self) -> Optional[dict]:
        """Parse the metadata field as JSON, or None if absent/invalid."""
        if not self.metadata:
            return None
        try:
            return json.loads(self.metadata)
        except json.JSONDecodeError:
            return None

    def __repr__(self) -> str:
        return f"SearchHit(id={self.id!r}, score={self.score:.4f})"


class AkiDBClient:
    """Client for the AkiDB vector + retrieval service."""

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        collection: str = DEFAULT_COLLECTION,
        channel: Any = None,
        stub: Any = None,
    ):
        self.collection = collection
        self._owns_channel = False
        if stub is not None:
            self._stub = stub
            self._channel = channel
        else:
            import grpc  # imported lazily so tests can inject a stub without grpc

            self._channel = channel or grpc.insecure_channel(target)
            self._owns_channel = channel is None
            self._stub = pb_grpc.AkidbStub(self._channel)

    # -- writes ------------------------------------------------------------

    def insert(
        self,
        id: str,
        vector: Sequence[float],
        *,
        metadata: bytes = b"",
        text: str = "",
    ) -> pb.InsertResponse:
        """Insert a vector with optional JSON metadata and source text."""
        return self._stub.Insert(
            pb.InsertRequest(
                collection=self.collection,
                id=id,
                vector=list(vector),
                metadata=metadata,
                text=text,
            )
        )

    def delete(self, id: str) -> pb.DeleteResponse:
        return self._stub.Delete(
            pb.DeleteRequest(collection=self.collection, id=id)
        )

    # -- reads -------------------------------------------------------------

    def search(self, vector: Sequence[float], top_k: int = 10) -> list[SearchHit]:
        """Dense vector search."""
        resp = self._stub.Search(
            pb.SearchRequest(
                collection=self.collection, query=list(vector), top_k=top_k
            )
        )
        return [SearchHit(r.id, r.score, r.metadata) for r in resp.results]

    def text_search(
        self,
        text: str,
        *,
        top_k: int = 10,
        hybrid: bool = True,
        rerank: bool = False,
        diversity: bool = False,
        pack: bool = False,
        token_budget: Optional[int] = None,
    ) -> "TextSearchResult":
        """Embedding-based search with optional hybrid fusion, reranking,
        diversity, and source-grounded context packing."""
        req = pb.TextSearchRequest(
            collection=self.collection,
            text=text,
            top_k=top_k,
            hybrid=hybrid,
            rerank=rerank,
            diversity=diversity,
            pack=pack,
        )
        if token_budget is not None:
            req.pack_token_budget = token_budget
        resp = self._stub.TextSearch(req)
        hits = [SearchHit(r.id, r.score, r.metadata) for r in resp.results]
        return TextSearchResult(hits=hits, context_pack=resp.context_pack)

    def health(self) -> pb.HealthResponse:
        return self._stub.Health(pb.HealthRequest())

    def close(self) -> None:
        if self._owns_channel and self._channel is not None:
            self._channel.close()

    def __enter__(self) -> "AkiDBClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class TextSearchResult:
    """Result of a text/hybrid search: ranked hits plus an optional context pack."""

    __slots__ = ("hits", "context_pack")

    def __init__(self, hits: list[SearchHit], context_pack: str):
        self.hits = hits
        self.context_pack = context_pack

    def __iter__(self):
        return iter(self.hits)

    def __len__(self) -> int:
        return len(self.hits)
