"""AkiDB Python client.

A typed, production-grade wrapper over the AkiDB gRPC API (INT-002):

    from akidb import AkiDBClient

    with AkiDBClient("localhost:50051", timeout=5.0, max_retries=3) as client:
        client.insert("doc-1", embedding, text="hello world")
        result = client.text_search("hello", top_k=5, hybrid=True, pack=True)
        print(result.context_pack)

Features: per-call deadlines, automatic retry with exponential backoff and jitter
on transient errors, an ``on_retry`` observability hook, optional TLS and
bearer-token auth, gRPC interceptor support, a typed error hierarchy (see
:mod:`akidb.errors`), typed results, and full coverage of the service surface.
For testing, inject a ``stub``.
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

import grpc

from . import akidb_pb2 as pb
from . import akidb_pb2_grpc as pb_grpc
from .errors import RETRYABLE_CODES, NotFoundError, map_grpc_error

DEFAULT_COLLECTION = "default"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 0.1

OnRetry = Callable[[int, BaseException], None]


@dataclass
class SearchHit:
    """A single retrieval result."""

    id: str
    score: float
    metadata: str = ""

    def metadata_json(self) -> Optional[dict]:
        """Parse the metadata field as JSON, or None if absent/invalid."""
        if not self.metadata:
            return None
        try:
            return json.loads(self.metadata)
        except json.JSONDecodeError:
            return None


@dataclass
class TextSearchResult:
    """Result of a text/hybrid search: ranked hits plus an optional context pack."""

    hits: list[SearchHit] = field(default_factory=list)
    context_pack: str = ""

    def __iter__(self):
        return iter(self.hits)

    def __len__(self) -> int:
        return len(self.hits)


@dataclass
class GetResult:
    """Result of a point lookup."""

    id: str
    vector: list[float]
    metadata: str
    found: bool


@dataclass
class InsertResult:
    success: bool
    id: str
    internal_id: int = 0


@dataclass
class DeleteResult:
    success: bool
    id: str
    status: int = 0
    visibility: str = ""


@dataclass
class UpdateResult:
    success: bool
    id: str
    status: int = 0


@dataclass
class BatchInsertResult:
    success: bool
    inserted_count: int
    failed_ids: list[str] = field(default_factory=list)


@dataclass
class HealthStatus:
    healthy: bool
    ready: bool
    message: str
    total_vectors: int
    active_vectors: int
    using_gpu: bool


def _hits(results) -> list[SearchHit]:
    return [SearchHit(r.id, r.score, r.metadata) for r in results]


class AkiDBClient:
    """Synchronous client for the AkiDB vector + retrieval service."""

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        collection: str = DEFAULT_COLLECTION,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff: float = DEFAULT_BACKOFF,
        on_retry: Optional[OnRetry] = None,
        tls: bool = False,
        ca_cert: Optional[bytes] = None,
        auth_token: Optional[str] = None,
        metadata: Optional[Sequence[tuple[str, str]]] = None,
        channel_options: Optional[Sequence[tuple[str, Any]]] = None,
        interceptors: Optional[Sequence[grpc.UnaryUnaryClientInterceptor]] = None,
        channel: Any = None,
        stub: Any = None,
    ):
        self.collection = collection
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._on_retry = on_retry
        self._owns_channel = False

        md: list[tuple[str, str]] = list(metadata or [])
        if auth_token:
            md.append(("authorization", f"Bearer {auth_token}"))
        self._metadata = md or None

        if stub is not None:
            self._stub = stub
            self._channel = channel
        else:
            if channel is not None:
                self._channel = channel
            elif tls:
                creds = grpc.ssl_channel_credentials(root_certificates=ca_cert)
                self._channel = grpc.secure_channel(target, creds, options=channel_options)
                self._owns_channel = True
            else:
                self._channel = grpc.insecure_channel(target, options=channel_options)
                self._owns_channel = True
            if interceptors:
                self._channel = grpc.intercept_channel(self._channel, *interceptors)
            self._stub = pb_grpc.AkidbStub(self._channel)

    # -- core call wrapper -------------------------------------------------

    def _invoke(self, method, request):
        """Invoke a unary RPC with deadline, metadata, jittered retries, and error mapping."""
        attempt = 0
        while True:
            try:
                return method(request, timeout=self._timeout, metadata=self._metadata)
            except grpc.RpcError as exc:
                code = exc.code() if hasattr(exc, "code") else None
                if code in RETRYABLE_CODES and attempt < self._max_retries:
                    if self._on_retry is not None:
                        self._on_retry(attempt, exc)
                    time.sleep(_jittered(self._retry_backoff, attempt))
                    attempt += 1
                    continue
                raise map_grpc_error(exc) from exc

    # -- writes ------------------------------------------------------------

    def insert(self, id: str, vector: Sequence[float], *, metadata: bytes = b"", text: str = "") -> InsertResult:
        r = self._invoke(
            self._stub.Insert,
            pb.InsertRequest(
                collection=self.collection, id=id, vector=list(vector), metadata=metadata, text=text
            ),
        )
        return InsertResult(success=r.success, id=r.id, internal_id=r.internal_id)

    def insert_batch(self, vectors: Sequence["VectorInput"]) -> BatchInsertResult:
        proto_vectors = [
            pb.Vector(id=v.id, embedding=list(v.vector), metadata=v.metadata, text=v.text)
            for v in vectors
        ]
        r = self._invoke(
            self._stub.InsertBatch,
            pb.InsertBatchRequest(collection=self.collection, vectors=proto_vectors),
        )
        return BatchInsertResult(success=r.success, inserted_count=r.inserted_count, failed_ids=list(r.failed_ids))

    def update(self, id: str, vector: Sequence[float], *, metadata: bytes = b"") -> UpdateResult:
        r = self._invoke(
            self._stub.Update,
            pb.UpdateRequest(collection=self.collection, id=id, vector=list(vector), metadata=metadata),
        )
        return UpdateResult(success=r.success, id=r.id, status=r.status)

    def delete(self, id: str) -> DeleteResult:
        r = self._invoke(self._stub.Delete, pb.DeleteRequest(collection=self.collection, id=id))
        return DeleteResult(success=r.success, id=r.id, status=r.status, visibility=r.visibility)

    # -- reads -------------------------------------------------------------

    def get(self, id: str) -> GetResult:
        # The server signals a missing vector with a NOT_FOUND error; normalize
        # that to a `found=False` result so callers don't branch on exceptions.
        try:
            resp = self._invoke(self._stub.Get, pb.GetRequest(collection=self.collection, id=id))
        except NotFoundError:
            return GetResult(id=id, vector=[], metadata="", found=False)
        return GetResult(id=resp.id, vector=list(resp.vector), metadata=resp.metadata, found=resp.found)

    def search(
        self,
        vector: Sequence[float],
        top_k: int = 10,
        *,
        nprobe: Optional[int] = None,
        tag_filter: Optional[pb.TagFilter] = None,
    ) -> list[SearchHit]:
        req = pb.SearchRequest(collection=self.collection, query=list(vector), top_k=top_k)
        if nprobe is not None:
            req.nprobe = nprobe
        if tag_filter is not None:
            req.tag_filter.CopyFrom(tag_filter)
        return _hits(self._invoke(self._stub.Search, req).results)

    def search_batch(self, vectors: Sequence[Sequence[float]], top_k: int = 10) -> list[list[SearchHit]]:
        req = pb.SearchBatchRequest(
            collection=self.collection,
            queries=[pb.Query(vector=list(v)) for v in vectors],
            top_k=top_k,
        )
        resp = self._invoke(self._stub.SearchBatch, req)
        return [_hits(r.results) for r in resp.results]

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
        filter: Optional[bytes] = None,
        tag_filter: Optional[pb.TagFilter] = None,
    ) -> TextSearchResult:
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
        if filter is not None:
            req.filter = filter
        if tag_filter is not None:
            req.tag_filter.CopyFrom(tag_filter)
        resp = self._invoke(self._stub.TextSearch, req)
        return TextSearchResult(hits=_hits(resp.results), context_pack=resp.context_pack)

    def health(self) -> HealthStatus:
        r = self._invoke(self._stub.Health, pb.HealthRequest())
        return HealthStatus(
            healthy=r.healthy,
            ready=r.ready,
            message=r.message,
            total_vectors=r.total_vectors,
            active_vectors=r.active_vectors,
            using_gpu=r.using_gpu,
        )

    def cluster_state(self) -> pb.GetClusterStateResponse:
        return self._invoke(self._stub.GetClusterState, pb.GetClusterStateRequest())

    # -- agent memory convenience -----------------------------------------

    def memory_write(
        self,
        id: str,
        vector: Sequence[float],
        text: str,
        *,
        kind: str = "note",
        conversation_id: Optional[str] = None,
        task_id: Optional[str] = None,
        tool: Optional[str] = None,
        source_uri: Optional[str] = None,
        timestamp: Optional[int] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> InsertResult:
        """Store an agent-memory entry. The caller supplies the embedding; this
        builds the canonical memory metadata and inserts it."""
        meta = build_memory_metadata(
            kind=kind,
            conversation_id=conversation_id,
            task_id=task_id,
            tool=tool,
            source_uri=source_uri,
            timestamp=timestamp,
            tags=tags,
        )
        return self.insert(id, vector, metadata=json.dumps(meta).encode(), text=text)

    def memory_read(
        self,
        query_vector: Sequence[float],
        *,
        conversation_id: Optional[str] = None,
        kind: Optional[str] = None,
        top_k: int = 10,
    ) -> list[SearchHit]:
        """Retrieve agent memory, optionally scoped by conversation and/or kind."""
        conditions = []
        if conversation_id is not None:
            conditions.append(_eq_condition("conversation_id", conversation_id))
        if kind is not None:
            conditions.append(_eq_condition("memory_kind", kind))
        tag_filter = _combine(conditions)
        return self.search(query_vector, top_k=top_k, tag_filter=tag_filter)

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        if self._owns_channel and self._channel is not None:
            self._channel.close()

    def __enter__(self) -> "AkiDBClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@dataclass
class VectorInput:
    """A vector for batch insertion."""

    id: str
    vector: Sequence[float]
    metadata: bytes = b""
    text: str = ""


def _jittered(base: float, attempt: int) -> float:
    """Equal-jitter exponential backoff: half fixed, half random, to avoid a
    thundering herd. ``base == 0`` yields 0 (used by tests)."""
    if base <= 0:
        return 0.0
    window = base * (2**attempt)
    return window / 2 + random.uniform(0, window / 2)


def build_memory_metadata(
    *,
    kind: str = "note",
    conversation_id: Optional[str] = None,
    task_id: Optional[str] = None,
    tool: Optional[str] = None,
    source_uri: Optional[str] = None,
    timestamp: Optional[int] = None,
    tags: Optional[dict[str, str]] = None,
) -> dict:
    """Build the canonical agent-memory metadata object (matches the engine's
    memory schema). Reserved keys take precedence over custom tags."""
    meta: dict[str, Any] = {"memory_kind": kind}
    if conversation_id is not None:
        meta["conversation_id"] = conversation_id
    if task_id is not None:
        meta["task_id"] = task_id
    if tool is not None:
        meta["tool"] = tool
    if source_uri is not None:
        meta["source_uri"] = source_uri
    if timestamp is not None:
        meta["timestamp"] = timestamp
    reserved = {"memory_kind", "conversation_id", "task_id", "tool", "source_uri", "timestamp"}
    for k, v in (tags or {}).items():
        if k not in reserved:
            meta[k] = v
    return meta


def _eq_condition(key: str, value: str) -> pb.TagFilter:
    return pb.TagFilter(
        condition=pb.TagCondition(
            key=key, value=pb.TagValue(text=value), op=pb.TagOperator.TAG_OP_EQ
        )
    )


def _combine(conditions: list[pb.TagFilter]) -> Optional[pb.TagFilter]:
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]
    # `and` is a Python keyword; set the oneof field via kwargs expansion.
    return pb.TagFilter(**{"and": pb.AndFilter(filters=conditions)})
