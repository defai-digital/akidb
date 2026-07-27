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

import hashlib
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


@dataclass(frozen=True)
class MemoryContext:
    """Principal-narrowed context for the authoritative Memory preview.

    Credentials remain gRPC metadata. These fields may only narrow the grants
    bound to those credentials; the server never treats them as identity.
    """

    workspace_id: str
    namespace: str
    request_purpose: str
    delegated_agent_id: Optional[str] = None
    request_id: Optional[str] = None
    entity_keys: Sequence[str] = ()
    data_subject_ids: Sequence[str] = ()
    session_ids: Sequence[str] = ()
    task_ids: Sequence[str] = ()
    maximum_sensitivity: Optional[int] = None


@dataclass(frozen=True)
class MemoryScope:
    """Immutable placement and disclosure scope for one Memory version."""

    entity_key: str
    allowed_purposes: Sequence[str]
    sensitivity: int = pb.MEMORY_SENSITIVITY_INTERNAL
    data_subject_id: Optional[str] = None
    owner_agent_id: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None


@dataclass(frozen=True)
class MemoryTemporal:
    """Exact valid/system-time selector for Memory Get and Recall."""

    mode: int = pb.MEMORY_TEMPORAL_MODE_CURRENT
    valid_at_unix_nanos: Optional[int] = None
    commit_sequence: Optional[int] = None


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
        memory_stub: Any = None,
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
            self._memory_stub = memory_stub if memory_stub is not None else stub
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
            self._memory_stub = (
                memory_stub
                if memory_stub is not None
                else pb_grpc.MemoryServiceStub(self._channel)
            )

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

    def _invoke_stream(self, method, request):
        """Collect a server stream without retrying after a partial response."""
        attempt = 0
        while True:
            values = []
            try:
                for value in method(
                    request,
                    timeout=self._timeout,
                    metadata=self._metadata,
                ):
                    values.append(value)
                return values
            except grpc.RpcError as exc:
                code = exc.code() if hasattr(exc, "code") else None
                if (
                    not values
                    and code in RETRYABLE_CODES
                    and attempt < self._max_retries
                ):
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

    # -- authoritative Memory developer preview --------------------------

    def memory_capabilities(self) -> pb.MemoryServerCapabilities:
        """Return the server's explicit Memory profile and optional features."""
        response = self._invoke(
            self._memory_stub.GetMemoryCapabilities,
            pb.GetMemoryCapabilitiesRequest(),
        )
        return response.capabilities

    def observe(
        self,
        context: MemoryContext,
        scope: MemoryScope,
        *,
        source_plane: str,
        source_id: str,
        content_sha256: str,
        idempotency_key: str,
        reason: str,
        retained_payload: bytes = b"",
        source_version: Optional[str] = None,
        observed_at_ms: Optional[int] = None,
        observed_at_unix_nanos: Optional[int] = None,
    ) -> pb.MemoryObserveReceipt:
        """Persist immutable raw evidence without activating a belief."""
        request = pb.MemoryObserveRequest(
            context=_memory_context_proto(context, idempotency_key),
            scope=_memory_scope_proto(scope),
            source_plane=source_plane,
            source_id=source_id,
            content_sha256=content_sha256,
            retained_payload=retained_payload,
            reason=reason,
        )
        if source_version is not None:
            request.source_version = source_version
        if observed_at_ms is not None:
            request.observed_at_ms = observed_at_ms
        if observed_at_unix_nanos is not None:
            request.observed_at_unix_nanos = observed_at_unix_nanos
        return self._invoke(self._memory_stub.Observe, request)

    def propose(
        self,
        context: MemoryContext,
        candidate: pb.MemoryVersionInput,
        *,
        idempotency_key: str,
    ) -> pb.MemoryMutationReceipt:
        """Persist an untrusted candidate for policy review or quarantine."""
        return self._invoke(
            self._memory_stub.Propose,
            pb.MemoryProposeRequest(
                context=_memory_context_proto(context, idempotency_key),
                candidate=candidate,
            ),
        )

    def commit_proposal(
        self,
        context: MemoryContext,
        proposal_version_id: str,
        *,
        idempotency_key: str,
        expected_head_version_ids: Sequence[str],
        reason: str,
    ) -> pb.MemoryMutationReceipt:
        """Revalidate and atomically activate one previously proposed version."""
        return self._invoke(
            self._memory_stub.Commit,
            pb.MemoryCommitRequest(
                context=_memory_context_proto(context, idempotency_key),
                proposal_version_id=proposal_version_id,
                expected_head_version_ids=list(expected_head_version_ids),
                reason=reason,
            ),
        )

    def remember(
        self,
        context: MemoryContext,
        scope: MemoryScope,
        predicate: str,
        content: pb.MemoryContent,
        *,
        idempotency_key: str,
        evidence: Sequence[pb.MemoryEvidenceInput],
        epistemic_formation: int = pb.MEMORY_FORMATION_HUMAN_STATEMENT,
        confidence: Optional[float] = None,
        expected_head_version_ids: Sequence[str] = (),
        valid_from_ms: Optional[int] = None,
        valid_to_ms: Optional[int] = None,
        valid_from_unix_nanos: Optional[int] = None,
        valid_to_unix_nanos: Optional[int] = None,
        compiler_artifact_id: Optional[str] = None,
        derivation: Optional[pb.MemoryDerivationInput] = None,
        reason: str,
    ) -> pb.MemoryMutationReceipt:
        """Commit one typed immutable version through MemoryService.

        This API is an explicitly experimental developer preview. The
        idempotency key makes automatic transient-error retries safe.
        """
        request = pb.MemoryRememberRequest(
            context=_memory_context_proto(context, idempotency_key),
            scope=_memory_scope_proto(scope),
            predicate=predicate,
            content=content,
            epistemic_formation=epistemic_formation,
            evidence=list(evidence),
            expected_head_version_ids=list(expected_head_version_ids),
            reason=reason,
        )
        if confidence is not None:
            request.confidence = confidence
        if valid_from_ms is not None:
            request.valid_from_ms = valid_from_ms
        if valid_to_ms is not None:
            request.valid_to_ms = valid_to_ms
        if valid_from_unix_nanos is not None:
            request.valid_from_unix_nanos = valid_from_unix_nanos
        if valid_to_unix_nanos is not None:
            request.valid_to_unix_nanos = valid_to_unix_nanos
        if compiler_artifact_id is not None:
            request.compiler_artifact_id = compiler_artifact_id
        if derivation is not None:
            request.derivation.CopyFrom(derivation)
        return self._invoke(self._memory_stub.Remember, request)

    def remember_text(
        self,
        context: MemoryContext,
        scope: MemoryScope,
        predicate: str,
        text: str,
        *,
        idempotency_key: str,
        source_plane: str,
        source_id: str,
        language: Optional[str] = None,
        source_version: Optional[str] = None,
        observed_at_ms: Optional[int] = None,
        observed_at_unix_nanos: Optional[int] = None,
        source_principal_id: Optional[str] = None,
        epistemic_formation: int = pb.MEMORY_FORMATION_HUMAN_STATEMENT,
        confidence: Optional[float] = None,
        additional_evidence: Sequence[pb.MemoryEvidenceInput] = (),
        expected_head_version_ids: Sequence[str] = (),
        valid_from_ms: Optional[int] = None,
        valid_to_ms: Optional[int] = None,
        valid_from_unix_nanos: Optional[int] = None,
        valid_to_unix_nanos: Optional[int] = None,
        reason: str,
    ) -> pb.MemoryMutationReceipt:
        """Ergonomic typed-text form of :meth:`remember`."""
        fact = pb.MemoryTextFact(text=text)
        if language is not None:
            fact.language = language
        evidence_values: dict[str, Any] = {
            "source_plane": source_plane,
            "source_id": source_id,
            "content_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        if source_version is not None:
            evidence_values["source_version"] = source_version
        if observed_at_ms is not None:
            evidence_values["observed_at_ms"] = observed_at_ms
        if observed_at_unix_nanos is not None:
            evidence_values["observed_at_unix_nanos"] = observed_at_unix_nanos
        if source_principal_id is not None:
            evidence_values["source_principal_id"] = source_principal_id
        evidence = [pb.MemoryEvidenceInput(**evidence_values), *additional_evidence]
        return self.remember(
            context,
            scope,
            predicate,
            pb.MemoryContent(text_fact=fact),
            idempotency_key=idempotency_key,
            epistemic_formation=epistemic_formation,
            confidence=confidence,
            evidence=evidence,
            expected_head_version_ids=expected_head_version_ids,
            valid_from_ms=valid_from_ms,
            valid_to_ms=valid_to_ms,
            valid_from_unix_nanos=valid_from_unix_nanos,
            valid_to_unix_nanos=valid_to_unix_nanos,
            reason=reason,
        )

    def memory_get(
        self,
        context: MemoryContext,
        *,
        assertion_id: Optional[str] = None,
        version_id: Optional[str] = None,
        canonical_at_sequence: Optional[int] = None,
        temporal: Optional[MemoryTemporal] = None,
    ) -> pb.MemoryGetResponse:
        """Fetch one authorized immutable Memory version."""
        if (assertion_id is None) == (version_id is None):
            raise ValueError("exactly one of assertion_id or version_id is required")
        request = pb.MemoryGetRequest(context=_memory_context_proto(context))
        if assertion_id is not None:
            request.assertion_id = assertion_id
        else:
            request.version_id = version_id
        if canonical_at_sequence is not None:
            request.canonical_at_sequence = canonical_at_sequence
        if temporal is not None:
            request.temporal_query.CopyFrom(_memory_temporal_proto(temporal))
        return self._invoke(self._memory_stub.Get, request)

    def recall(
        self,
        context: MemoryContext,
        *,
        query_text: Optional[str] = None,
        structured_predicates: Sequence[str] = (),
        entity_keys: Sequence[str] = (),
        max_items: int = 10,
        max_context_tokens: Optional[int] = None,
        deterministic: bool = True,
        include_explanation_summary: bool = True,
        canonical_at_sequence: Optional[int] = None,
        temporal: Optional[MemoryTemporal] = None,
        include_conflicts: bool = False,
        recipe: Optional[str] = None,
    ) -> pb.MemoryRecallResponse:
        """Run bounded policy-aware retrieval and retain an exact snapshot."""
        request = pb.MemoryRecallRequest(
            context=_memory_context_proto(context),
            structured_predicates=list(structured_predicates),
            entity_keys=list(entity_keys),
            max_items=max_items,
            deterministic=deterministic,
            include_explanation_summary=include_explanation_summary,
            include_conflicts=include_conflicts,
        )
        if query_text is not None:
            request.query_text = query_text
        if max_context_tokens is not None:
            request.max_context_tokens = max_context_tokens
        if canonical_at_sequence is not None:
            request.canonical_at_sequence = canonical_at_sequence
        if temporal is not None:
            request.temporal_query.CopyFrom(_memory_temporal_proto(temporal))
        if recipe is not None:
            request.recipe = recipe
        return self._invoke(self._memory_stub.Recall, request)

    def explain_recall(
        self,
        context: MemoryContext,
        snapshot_id: str,
    ) -> pb.MemoryExplainRecallResponse:
        """Return retained bounded-pool filter, ranking, and packing evidence."""
        return self._invoke(
            self._memory_stub.ExplainRecall,
            pb.MemoryExplainRecallRequest(
                context=_memory_context_proto(context),
                snapshot_id=snapshot_id,
            ),
        )

    def replay_recall(
        self,
        context: MemoryContext,
        snapshot_id: str,
        *,
        mode: int = pb.MEMORY_REPLAY_MODE_EXACT_RETAINED,
    ) -> pb.MemoryReplayRecallResponse:
        """Return the exact retained response for a named preview snapshot."""
        return self._invoke(
            self._memory_stub.ReplayRecall,
            pb.MemoryReplayRecallRequest(
                context=_memory_context_proto(context),
                snapshot_id=snapshot_id,
                mode=mode,
            ),
        )

    def correct(
        self,
        context: MemoryContext,
        successor: pb.MemoryVersionInput,
        *,
        idempotency_key: str,
    ) -> pb.MemoryMutationReceipt:
        """Append a bitemporal successor without rewriting prior history."""
        return self._invoke(
            self._memory_stub.Correct,
            pb.MemoryCorrectRequest(
                context=_memory_context_proto(context, idempotency_key),
                successor=successor,
            ),
        )

    def retract(
        self,
        context: MemoryContext,
        *,
        idempotency_key: str,
        reason: str,
        assertion_id: Optional[str] = None,
        version_id: Optional[str] = None,
        expected_head_version_ids: Sequence[str] = (),
    ) -> pb.MemoryMutationReceipt:
        """Retire an active version without asserting a replacement."""
        if (assertion_id is None) == (version_id is None):
            raise ValueError("exactly one of assertion_id or version_id is required")
        request = pb.MemoryRetractRequest(
            context=_memory_context_proto(context, idempotency_key),
            expected_head_version_ids=list(expected_head_version_ids),
            reason=reason,
        )
        if assertion_id is not None:
            request.assertion_id = assertion_id
        else:
            request.version_id = version_id
        return self._invoke(self._memory_stub.Retract, request)

    def forget(
        self,
        context: MemoryContext,
        *,
        idempotency_key: str,
        reason: str,
        assertion_id: Optional[str] = None,
        version_id: Optional[str] = None,
        expected_head_version_ids: Sequence[str] = (),
    ) -> pb.MemoryMutationReceipt:
        """Tombstone one exact preview target without destroying its history."""
        if (assertion_id is None) == (version_id is None):
            raise ValueError("exactly one of assertion_id or version_id is required")
        request = pb.MemoryForgetRequest(
            context=_memory_context_proto(context, idempotency_key),
            expected_head_version_ids=list(expected_head_version_ids),
            reason=reason,
        )
        if assertion_id is not None:
            request.assertion_id = assertion_id
        else:
            request.version_id = version_id
        return self._invoke(self._memory_stub.Forget, request)

    def reinforce(
        self,
        context: MemoryContext,
        version_id: str,
        evidence: Sequence[pb.MemoryEvidenceInput],
        *,
        outcome: int,
        outcome_id: str,
        utility_micros: int,
        idempotency_key: str,
        reason: str,
    ) -> pb.MemoryMutationReceipt:
        """Attach outcome evidence without rewriting content or authority."""
        return self._invoke(
            self._memory_stub.Reinforce,
            pb.MemoryReinforceRequest(
                context=_memory_context_proto(context, idempotency_key),
                version_id=version_id,
                evidence=list(evidence),
                outcome=outcome,
                outcome_id=outcome_id,
                utility_micros=utility_micros,
                reason=reason,
            ),
        )

    def plan_deletion(
        self,
        context: MemoryContext,
        *,
        reason: str,
        source_plane: Optional[str] = None,
        source_id: Optional[str] = None,
        data_subject_id: Optional[str] = None,
        expires_in_seconds: Optional[int] = None,
    ) -> pb.MemoryDeletionPlan:
        """Create an immutable dry-run plan for source or subject erasure."""
        source_selected = source_plane is not None or source_id is not None
        if source_selected == (data_subject_id is not None):
            raise ValueError(
                "select exactly one deletion mode: source_plane/source_id or data_subject_id"
            )
        if source_selected and (source_plane is None or source_id is None):
            raise ValueError("source_plane and source_id are both required")
        selector = pb.MemoryDeletionSelector()
        if data_subject_id is not None:
            selector.data_subject_id = data_subject_id
        else:
            selector.source.CopyFrom(
                pb.MemorySourceDeletionSelector(
                    source_plane=source_plane,
                    source_id=source_id,
                )
            )
        request = pb.MemoryPlanDeletionRequest(
            context=_memory_context_proto(context),
            selector=selector,
            reason=reason,
        )
        if expires_in_seconds is not None:
            request.expires_in_seconds = expires_in_seconds
        return self._invoke(self._memory_stub.PlanDeletion, request)

    def execute_deletion(
        self,
        context: MemoryContext,
        plan_id: str,
        plan_sha256: str,
        *,
        idempotency_key: str,
        reason: str,
    ) -> pb.MemoryDeletionExecutionReceipt:
        """Execute one fresh, checksum-bound deletion plan atomically."""
        return self._invoke(
            self._memory_stub.ExecuteDeletion,
            pb.MemoryExecuteDeletionRequest(
                context=_memory_context_proto(context, idempotency_key),
                plan_id=plan_id,
                plan_sha256=plan_sha256,
                reason=reason,
            ),
        )

    def list_history(
        self,
        context: MemoryContext,
        assertion_id: str,
        *,
        from_sequence: Optional[int] = None,
        to_sequence: Optional[int] = None,
        limit: int = 1000,
    ) -> pb.MemoryListHistoryResponse:
        """Return authorized immutable versions, transitions, and mutations."""
        request = pb.MemoryListHistoryRequest(
            context=_memory_context_proto(context),
            assertion_id=assertion_id,
            limit=limit,
        )
        if from_sequence is not None:
            request.from_sequence = from_sequence
        if to_sequence is not None:
            request.to_sequence = to_sequence
        return self._invoke(self._memory_stub.ListHistory, request)

    def export_memory(
        self,
        context: MemoryContext,
        *,
        limit: int = 10_000,
    ) -> list[pb.MemoryExportRecord]:
        """Collect a scoped stream of digest-bound canonical JSON records."""
        return self._invoke_stream(
            self._memory_stub.Export,
            pb.MemoryExportRequest(
                context=_memory_context_proto(context),
                limit=limit,
            ),
        )

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


def _memory_context_proto(
    context: MemoryContext,
    idempotency_key: Optional[str] = None,
) -> pb.MemoryRequestContext:
    values: dict[str, Any] = {
        "workspace_id": context.workspace_id,
        "namespace": context.namespace,
        "request_purpose": context.request_purpose,
    }
    if context.delegated_agent_id is not None:
        values["delegated_agent_id"] = context.delegated_agent_id
    if idempotency_key is not None:
        values["idempotency_key"] = idempotency_key
    if context.request_id is not None:
        values["request_id"] = context.request_id
    if (
        context.entity_keys
        or context.data_subject_ids
        or context.session_ids
        or context.task_ids
        or context.maximum_sensitivity is not None
    ):
        narrowing = pb.MemoryScopeNarrowing(
            entity_keys=list(context.entity_keys),
            data_subject_ids=list(context.data_subject_ids),
            session_ids=list(context.session_ids),
            task_ids=list(context.task_ids),
        )
        if context.maximum_sensitivity is not None:
            narrowing.maximum_sensitivity = context.maximum_sensitivity
        values["scope_narrowing"] = narrowing
    return pb.MemoryRequestContext(**values)


def _memory_scope_proto(scope: MemoryScope) -> pb.MemoryScopeInput:
    values: dict[str, Any] = {
        "entity_key": scope.entity_key,
        "sensitivity": scope.sensitivity,
        "allowed_purposes": list(scope.allowed_purposes),
    }
    for field_name in (
        "data_subject_id",
        "owner_agent_id",
        "session_id",
        "task_id",
    ):
        value = getattr(scope, field_name)
        if value is not None:
            values[field_name] = value
    return pb.MemoryScopeInput(**values)


def _memory_temporal_proto(temporal: MemoryTemporal) -> pb.MemoryTemporalQuery:
    values: dict[str, Any] = {"mode": temporal.mode}
    if temporal.valid_at_unix_nanos is not None:
        values["valid_at_unix_nanos"] = temporal.valid_at_unix_nanos
    if temporal.commit_sequence is not None:
        values["commit_sequence"] = temporal.commit_sequence
    return pb.MemoryTemporalQuery(**values)


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
