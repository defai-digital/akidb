"""Asyncio AkiDB client (grpc.aio).

Same surface, retries (with jitter), observability hook, and typed errors/results
as the synchronous :class:`akidb.client.AkiDBClient`, for asyncio applications:

    from akidb.aio import AsyncAkiDBClient

    async with AsyncAkiDBClient("localhost:50051") as client:
        await client.insert("doc-1", embedding, text="hello")
        result = await client.text_search("hello", hybrid=True, pack=True)
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional, Sequence

import grpc

from . import akidb_pb2 as pb
from . import akidb_pb2_grpc as pb_grpc
from .client import (
    DEFAULT_BACKOFF,
    DEFAULT_COLLECTION,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    BatchInsertResult,
    DeleteResult,
    GetResult,
    HealthStatus,
    InsertResult,
    OnRetry,
    SearchHit,
    TextSearchResult,
    UpdateResult,
    VectorInput,
    _combine,
    _eq_condition,
    _hits,
    _jittered,
    build_memory_metadata,
)
from .errors import RETRYABLE_CODES, NotFoundError, map_grpc_error


class AsyncAkiDBClient:
    """Asynchronous client for the AkiDB vector + retrieval service."""

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
        interceptors: Optional[Sequence[Any]] = None,
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
        elif channel is not None:
            self._channel = channel
            self._stub = pb_grpc.AkidbStub(channel)
        else:
            kwargs: dict[str, Any] = {"options": channel_options, "interceptors": interceptors}
            if tls:
                creds = grpc.ssl_channel_credentials(root_certificates=ca_cert)
                self._channel = grpc.aio.secure_channel(target, creds, **kwargs)
            else:
                self._channel = grpc.aio.insecure_channel(target, **kwargs)
            self._owns_channel = True
            self._stub = pb_grpc.AkidbStub(self._channel)

    async def _invoke(self, method, request):
        attempt = 0
        while True:
            try:
                return await method(request, timeout=self._timeout, metadata=self._metadata)
            except grpc.RpcError as exc:
                code = exc.code() if hasattr(exc, "code") else None
                if code in RETRYABLE_CODES and attempt < self._max_retries:
                    if self._on_retry is not None:
                        self._on_retry(attempt, exc)
                    await asyncio.sleep(_jittered(self._retry_backoff, attempt))
                    attempt += 1
                    continue
                raise map_grpc_error(exc) from exc

    async def insert(
        self, id: str, vector: Sequence[float], *, metadata: bytes = b"", text: str = ""
    ) -> InsertResult:
        r = await self._invoke(
            self._stub.Insert,
            pb.InsertRequest(
                collection=self.collection, id=id, vector=list(vector), metadata=metadata, text=text
            ),
        )
        return InsertResult(success=r.success, id=r.id, internal_id=r.internal_id)

    async def insert_batch(self, vectors: Sequence[VectorInput]) -> BatchInsertResult:
        proto_vectors = [
            pb.Vector(id=v.id, embedding=list(v.vector), metadata=v.metadata, text=v.text)
            for v in vectors
        ]
        r = await self._invoke(
            self._stub.InsertBatch,
            pb.InsertBatchRequest(collection=self.collection, vectors=proto_vectors),
        )
        return BatchInsertResult(success=r.success, inserted_count=r.inserted_count, failed_ids=list(r.failed_ids))

    async def update(self, id: str, vector: Sequence[float], *, metadata: bytes = b"") -> UpdateResult:
        r = await self._invoke(
            self._stub.Update,
            pb.UpdateRequest(collection=self.collection, id=id, vector=list(vector), metadata=metadata),
        )
        return UpdateResult(success=r.success, id=r.id, status=r.status)

    async def delete(self, id: str) -> DeleteResult:
        r = await self._invoke(self._stub.Delete, pb.DeleteRequest(collection=self.collection, id=id))
        return DeleteResult(success=r.success, id=r.id, status=r.status, visibility=r.visibility)

    async def get(self, id: str) -> GetResult:
        try:
            resp = await self._invoke(self._stub.Get, pb.GetRequest(collection=self.collection, id=id))
        except NotFoundError:
            return GetResult(id=id, vector=[], metadata="", found=False)
        return GetResult(id=resp.id, vector=list(resp.vector), metadata=resp.metadata, found=resp.found)

    async def search(
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
        resp = await self._invoke(self._stub.Search, req)
        return _hits(resp.results)

    async def search_batch(self, vectors: Sequence[Sequence[float]], top_k: int = 10) -> list[list[SearchHit]]:
        req = pb.SearchBatchRequest(
            collection=self.collection,
            queries=[pb.Query(vector=list(v)) for v in vectors],
            top_k=top_k,
        )
        resp = await self._invoke(self._stub.SearchBatch, req)
        return [_hits(r.results) for r in resp.results]

    async def text_search(
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
        retrieval_mode: Optional[str] = None,
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
        if retrieval_mode is not None:
            req.retrieval_mode = retrieval_mode
        resp = await self._invoke(self._stub.TextSearch, req)
        return TextSearchResult(hits=_hits(resp.results), context_pack=resp.context_pack)

    async def health(self) -> HealthStatus:
        r = await self._invoke(self._stub.Health, pb.HealthRequest())
        return HealthStatus(
            healthy=r.healthy,
            ready=r.ready,
            message=r.message,
            total_vectors=r.total_vectors,
            active_vectors=r.active_vectors,
            using_gpu=r.using_gpu,
        )

    async def cluster_state(self) -> pb.GetClusterStateResponse:
        return await self._invoke(self._stub.GetClusterState, pb.GetClusterStateRequest())

    async def memory_write(
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
        meta = build_memory_metadata(
            kind=kind,
            conversation_id=conversation_id,
            task_id=task_id,
            tool=tool,
            source_uri=source_uri,
            timestamp=timestamp,
            tags=tags,
        )
        return await self.insert(id, vector, metadata=json.dumps(meta).encode(), text=text)

    async def memory_read(
        self,
        query_vector: Sequence[float],
        *,
        conversation_id: Optional[str] = None,
        kind: Optional[str] = None,
        top_k: int = 10,
    ) -> list[SearchHit]:
        conditions = []
        if conversation_id is not None:
            conditions.append(_eq_condition("conversation_id", conversation_id))
        if kind is not None:
            conditions.append(_eq_condition("memory_kind", kind))
        return await self.search(query_vector, top_k=top_k, tag_filter=_combine(conditions))

    async def close(self) -> None:
        if self._owns_channel and self._channel is not None:
            await self._channel.close()

    async def __aenter__(self) -> "AsyncAkiDBClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
