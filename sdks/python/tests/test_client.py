"""Unit tests for the AkiDB Python client.

These mock the gRPC stub so they validate request construction, response parsing,
retry/backoff, and error mapping without a running server.
"""

import json
import hashlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest

# Make the package importable without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from akidb import (  # noqa: E402
    AkiDBClient,
    MemoryContext,
    MemoryScope,
    MemoryTemporal,
    NotFoundError,
    UnavailableError,
    VectorInput,
    build_memory_metadata,
)
from akidb import akidb_pb2 as pb  # noqa: E402


class FakeRpcError(grpc.RpcError):
    """A grpc.RpcError with a controllable status code/details."""

    def __init__(self, code, details="boom"):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def make_client(**kwargs):
    stub = MagicMock()
    client = AkiDBClient(stub=stub, retry_backoff=0.0, **kwargs)
    return client, stub


def test_insert_builds_request_with_deadline_and_metadata():
    client, stub = make_client(auth_token="secret")
    stub.Insert.return_value = pb.InsertResponse(success=True, id="a")
    client.insert("a", [1.0, 2.0, 3.0], text="hello", metadata=b"{}")
    req = stub.Insert.call_args[0][0]
    kwargs = stub.Insert.call_args[1]
    assert req.id == "a"
    assert list(req.vector) == [1.0, 2.0, 3.0]
    assert req.text == "hello"
    assert kwargs["timeout"] == client._timeout
    assert ("authorization", "Bearer secret") in kwargs["metadata"]


def test_search_parses_hits():
    client, stub = make_client()
    stub.Search.return_value = pb.SearchResponse(
        results=[
            pb.SearchResult(id="a", score=0.9, metadata='{"k":"v"}'),
            pb.SearchResult(id="b", score=0.5, metadata=""),
        ]
    )
    hits = client.search([0.1, 0.2], top_k=2)
    req = stub.Search.call_args[0][0]
    assert req.top_k == 2
    assert list(req.query) == pytest.approx([0.1, 0.2])
    assert [h.id for h in hits] == ["a", "b"]
    assert hits[0].metadata_json() == {"k": "v"}
    assert hits[1].metadata_json() is None


def test_text_search_sets_flags_and_budget():
    client, stub = make_client()
    stub.TextSearch.return_value = pb.SearchResponse(
        results=[pb.SearchResult(id="x", score=1.0, metadata="")],
        context_pack="[x] packed",
    )
    result = client.text_search(
        "q", top_k=7, hybrid=True, rerank=True, diversity=True, pack=True, token_budget=256
    )
    req = stub.TextSearch.call_args[0][0]
    assert (req.top_k, req.hybrid, req.rerank, req.diversity, req.pack) == (7, True, True, True, True)
    assert req.pack_token_budget == 256
    assert result.context_pack == "[x] packed"
    assert [h.id for h in result] == ["x"]


def test_text_search_sets_metadata_filters():
    client, stub = make_client()
    stub.TextSearch.return_value = pb.SearchResponse()
    tag_filter = pb.TagFilter(
        condition=pb.TagCondition(
            key="tenant",
            value=pb.TagValue(text="a"),
            op=pb.TAG_OP_EQ,
        )
    )

    client.text_search("q", filter=b'{"tenant":"a"}', tag_filter=tag_filter, retrieval_mode="bm25")

    req = stub.TextSearch.call_args[0][0]
    assert req.filter == b'{"tenant":"a"}'
    assert req.tag_filter.condition.key == "tenant"
    assert req.retrieval_mode == "bm25"


def test_insert_batch_and_search_batch():
    client, stub = make_client()
    stub.InsertBatch.return_value = pb.InsertBatchResponse(success=True, inserted_count=2)
    resp = client.insert_batch([VectorInput("a", [1.0], text="t1"), VectorInput("b", [2.0])])
    req = stub.InsertBatch.call_args[0][0]
    assert [v.id for v in req.vectors] == ["a", "b"]
    assert req.vectors[0].text == "t1"
    assert resp.inserted_count == 2

    stub.SearchBatch.return_value = pb.SearchBatchResponse(
        results=[
            pb.SearchResponse(results=[pb.SearchResult(id="a", score=1.0)]),
            pb.SearchResponse(results=[pb.SearchResult(id="b", score=0.5)]),
        ]
    )
    batches = client.search_batch([[1.0], [2.0]], top_k=1)
    assert [h.id for batch in batches for h in batch] == ["a", "b"]


def test_get_and_update():
    client, stub = make_client()
    stub.Get.return_value = pb.GetResponse(id="a", vector=[1.0, 2.0], metadata="{}", found=True)
    got = client.get("a")
    assert got.found and got.id == "a" and list(got.vector) == pytest.approx([1.0, 2.0])

    stub.Update.return_value = pb.UpdateResponse(success=True, id="a")
    client.update("a", [3.0, 4.0])
    assert list(stub.Update.call_args[0][0].vector) == [3.0, 4.0]


def test_get_missing_returns_found_false_not_exception():
    client, stub = make_client()
    stub.Get.side_effect = FakeRpcError(grpc.StatusCode.NOT_FOUND, "missing")
    got = client.get("nope")
    assert got.found is False and got.id == "nope" and got.vector == []


def test_retries_on_unavailable_then_succeeds():
    client, stub = make_client(max_retries=3)
    stub.Search.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        pb.SearchResponse(results=[pb.SearchResult(id="ok", score=1.0)]),
    ]
    hits = client.search([0.1])
    assert stub.Search.call_count == 3
    assert hits[0].id == "ok"


def test_non_retryable_error_maps_immediately():
    client, stub = make_client(max_retries=5)
    stub.Search.side_effect = FakeRpcError(grpc.StatusCode.NOT_FOUND, "missing")
    with pytest.raises(NotFoundError) as ei:
        client.search([0.1])
    assert stub.Search.call_count == 1  # not retried
    assert ei.value.code == grpc.StatusCode.NOT_FOUND


def test_retries_exhausted_raises_mapped_error():
    client, stub = make_client(max_retries=2)
    stub.Search.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    with pytest.raises(UnavailableError):
        client.search([0.1])
    assert stub.Search.call_count == 3  # initial + 2 retries


def test_memory_write_builds_metadata_and_inserts():
    client, stub = make_client()
    stub.Insert.return_value = pb.InsertResponse(success=True, id="m1")
    client.memory_write(
        "m1", [1.0], "remember this", kind="note", conversation_id="c1", tags={"project": "akidb"}
    )
    req = stub.Insert.call_args[0][0]
    meta = json.loads(req.metadata.decode())
    assert meta["memory_kind"] == "note"
    assert meta["conversation_id"] == "c1"
    assert meta["project"] == "akidb"
    assert req.text == "remember this"


def test_memory_read_builds_tag_filter():
    client, stub = make_client()
    stub.Search.return_value = pb.SearchResponse(results=[pb.SearchResult(id="m1", score=1.0)])
    client.memory_read([0.1], conversation_id="c1", kind="note")
    req = stub.Search.call_args[0][0]
    # Two conditions => AND filter.
    assert req.tag_filter.WhichOneof("filter_type") == "and"
    keys = {c.condition.key for c in getattr(req.tag_filter, "and").filters}
    assert keys == {"conversation_id", "memory_kind"}


def test_memory_read_single_condition_uses_plain_condition():
    client, stub = make_client()
    stub.Search.return_value = pb.SearchResponse(results=[])
    client.memory_read([0.1], conversation_id="c1")
    req = stub.Search.call_args[0][0]
    assert req.tag_filter.WhichOneof("filter_type") == "condition"
    assert req.tag_filter.condition.key == "conversation_id"


def test_authoritative_memory_preview_builds_typed_requests():
    client, stub = make_client(auth_token="principal-token")
    context = MemoryContext(
        workspace_id="workspace-a",
        namespace="repo/akidb",
        request_purpose="debugging",
        delegated_agent_id="agent:codex",
        request_id="request-1",
        entity_keys=("service:ingestion",),
        session_ids=("session-1",),
        task_ids=("task-1",),
        maximum_sensitivity=pb.MEMORY_SENSITIVITY_INTERNAL,
    )
    scope = MemoryScope(
        entity_key="service:ingestion",
        allowed_purposes=("debugging",),
        owner_agent_id="agent:codex",
        session_id="session-1",
        task_id="task-1",
    )

    stub.GetMemoryCapabilities.return_value = pb.GetMemoryCapabilitiesResponse(
        capabilities=pb.MemoryServerCapabilities(profile_status="EXPERIMENTAL")
    )
    assert client.memory_capabilities().profile_status == "EXPERIMENTAL"

    stub.Remember.return_value = pb.MemoryMutationReceipt(
        mutation_id="mem_m_1",
        assertion_id="mem_a_1",
        version_ids=["mem_v_1"],
        commit_sequence=1,
    )
    remembered = client.remember_text(
        context,
        scope,
        "uses recovery procedure",
        "Drain the queue before restarting.",
        language="en",
        idempotency_key="remember-1",
        source_plane="operator-note",
        source_id="incident-42",
        confidence=0.9,
        reason="operator-confirmed procedure",
    )
    assert remembered.version_ids == ["mem_v_1"]
    remember_request = stub.Remember.call_args[0][0]
    assert remember_request.context.idempotency_key == "remember-1"
    assert remember_request.context.workspace_id == "workspace-a"
    assert list(remember_request.context.scope_narrowing.entity_keys) == [
        "service:ingestion"
    ]
    assert remember_request.context.scope_narrowing.maximum_sensitivity == (
        pb.MEMORY_SENSITIVITY_INTERNAL
    )
    assert remember_request.scope.entity_key == "service:ingestion"
    assert list(remember_request.scope.allowed_purposes) == ["debugging"]
    assert remember_request.content.text_fact.text.startswith("Drain")
    assert remember_request.evidence[0].source_plane == "operator-note"
    assert remember_request.evidence[0].source_id == "incident-42"
    assert remember_request.evidence[0].content_sha256 == hashlib.sha256(
        b"Drain the queue before restarting."
    ).hexdigest()
    assert remember_request.HasField("confidence")
    assert ("authorization", "Bearer principal-token") in stub.Remember.call_args[1]["metadata"]

    stub.Get.return_value = pb.MemoryGetResponse(found=True)
    client.memory_get(context, version_id="mem_v_1", canonical_at_sequence=1)
    get_request = stub.Get.call_args[0][0]
    assert get_request.WhichOneof("target") == "version_id"
    assert get_request.canonical_at_sequence == 1

    stub.Recall.return_value = pb.MemoryRecallResponse(snapshot_id="mem_s_1")
    recalled = client.recall(
        context,
        query_text="queue restart",
        max_context_tokens=256,
        canonical_at_sequence=1,
    )
    assert recalled.snapshot_id == "mem_s_1"
    recall_request = stub.Recall.call_args[0][0]
    assert recall_request.deterministic
    assert recall_request.query_text == "queue restart"
    assert recall_request.max_context_tokens == 256

    stub.ReplayRecall.return_value = pb.MemoryReplayRecallResponse(
        replay_mode="EXACT_RETAINED",
        exact_match=True,
    )
    replayed = client.replay_recall(context, "mem_s_1")
    assert replayed.exact_match
    assert stub.ReplayRecall.call_args[0][0].snapshot_id == "mem_s_1"

    stub.Forget.return_value = pb.MemoryMutationReceipt(commit_sequence=2)
    forgotten = client.forget(
        context,
        version_id="mem_v_1",
        expected_head_version_ids=("mem_v_1",),
        idempotency_key="forget-1",
        reason="remove from current recall",
    )
    assert forgotten.commit_sequence == 2
    forget_request = stub.Forget.call_args[0][0]
    assert forget_request.WhichOneof("target") == "version_id"
    assert forget_request.context.idempotency_key == "forget-1"


def test_authoritative_memory_lifecycle_temporal_explain_and_export_requests():
    client, stub = make_client(auth_token="principal-token")
    context = MemoryContext("workspace-a", "repo/akidb", "debugging")
    scope = MemoryScope("service:ingestion", ("debugging",))
    payload = b"raw incident evidence"

    stub.Observe.return_value = pb.MemoryObserveReceipt(observation_id="mem_o_1")
    observed = client.observe(
        context,
        scope,
        source_plane="incident-stream",
        source_id="incident-42",
        content_sha256=hashlib.sha256(payload).hexdigest(),
        retained_payload=payload,
        observed_at_unix_nanos=1_784_995_200_123_456_789,
        idempotency_key="observe-1",
        reason="retain evidence",
    )
    assert observed.observation_id == "mem_o_1"
    observe_request = stub.Observe.call_args[0][0]
    assert observe_request.context.idempotency_key == "observe-1"
    assert observe_request.observed_at_unix_nanos == 1_784_995_200_123_456_789

    candidate = pb.MemoryVersionInput(
        scope=pb.MemoryScopeInput(
            entity_key="service:ingestion",
            sensitivity=pb.MEMORY_SENSITIVITY_INTERNAL,
            allowed_purposes=["debugging"],
        ),
        predicate="uses recovery procedure",
        content=pb.MemoryContent(
            text_fact=pb.MemoryTextFact(text="Drain the queue before restart.")
        ),
        epistemic_formation=pb.MEMORY_FORMATION_HUMAN_STATEMENT,
        evidence=[
            pb.MemoryEvidenceInput(
                source_plane="operator-note",
                source_id="incident-42",
                content_sha256=hashlib.sha256(b"source").hexdigest(),
            )
        ],
        reason="candidate",
    )
    stub.Propose.return_value = pb.MemoryMutationReceipt(version_ids=["mem_v_proposed"])
    assert client.propose(
        context, candidate, idempotency_key="propose-1"
    ).version_ids == ["mem_v_proposed"]
    assert stub.Propose.call_args[0][0].candidate.predicate == "uses recovery procedure"

    stub.Commit.return_value = pb.MemoryMutationReceipt(commit_sequence=3)
    client.commit_proposal(
        context,
        "mem_v_proposed",
        idempotency_key="commit-1",
        expected_head_version_ids=(),
        reason="approved",
    )
    assert stub.Commit.call_args[0][0].proposal_version_id == "mem_v_proposed"

    temporal = MemoryTemporal(
        mode=pb.MEMORY_TEMPORAL_MODE_VALID_AT_AS_KNOWN_AT,
        valid_at_unix_nanos=1_784_995_200_123_456_789,
        commit_sequence=3,
    )
    stub.Get.return_value = pb.MemoryGetResponse(found=True)
    client.memory_get(context, version_id="mem_v_proposed", temporal=temporal)
    get_request = stub.Get.call_args[0][0]
    assert get_request.temporal_query.commit_sequence == 3
    assert (
        get_request.temporal_query.mode
        == pb.MEMORY_TEMPORAL_MODE_VALID_AT_AS_KNOWN_AT
    )

    stub.Recall.return_value = pb.MemoryRecallResponse(snapshot_id="mem_s_1")
    client.recall(
        context,
        query_text="queue restart",
        temporal=temporal,
        recipe="preview-bounded-bm25-v1",
    )
    assert stub.Recall.call_args[0][0].temporal_query.commit_sequence == 3

    stub.ExplainRecall.return_value = pb.MemoryExplainRecallResponse(snapshot_id="mem_s_1")
    assert client.explain_recall(context, "mem_s_1").snapshot_id == "mem_s_1"

    stub.ReplayRecall.return_value = pb.MemoryReplayRecallResponse(
        replay_mode="REEXECUTE"
    )
    client.replay_recall(
        context,
        "mem_s_1",
        mode=pb.MEMORY_REPLAY_MODE_REEXECUTE,
    )
    assert (
        stub.ReplayRecall.call_args[0][0].mode
        == pb.MEMORY_REPLAY_MODE_REEXECUTE
    )

    stub.Correct.return_value = pb.MemoryMutationReceipt(commit_sequence=4)
    client.correct(context, candidate, idempotency_key="correct-1")
    assert stub.Correct.call_args[0][0].successor.predicate == candidate.predicate

    stub.Retract.return_value = pb.MemoryMutationReceipt(commit_sequence=5)
    client.retract(
        context,
        version_id="mem_v_corrected",
        expected_head_version_ids=("mem_v_corrected",),
        idempotency_key="retract-1",
        reason="obsolete",
    )
    assert (
        stub.Retract.call_args[0][0].WhichOneof("target")
        == "version_id"
    )

    stub.ListHistory.return_value = pb.MemoryListHistoryResponse(found=True)
    assert client.list_history(context, "mem_a_1", from_sequence=1, to_sequence=5).found
    history_request = stub.ListHistory.call_args[0][0]
    assert history_request.from_sequence == 1
    assert history_request.to_sequence == 5

    stub.Export.return_value = iter(
        [pb.MemoryExportRecord(record_type="version", record_id="mem_v_1")]
    )
    exported = client.export_memory(context, limit=100)
    assert [record.record_id for record in exported] == ["mem_v_1"]
    assert stub.Export.call_args[0][0].limit == 100


def test_authoritative_memory_reinforcement_and_deletion_requests():
    client, stub = make_client(auth_token="principal-token")
    context = MemoryContext("workspace-a", "repo/akidb", "debugging")
    evidence = pb.MemoryEvidenceInput(
        source_plane="task-outcome",
        source_id="run-42",
        content_sha256=hashlib.sha256(b"succeeded").hexdigest(),
    )

    stub.Reinforce.return_value = pb.MemoryMutationReceipt(commit_sequence=7)
    reinforced = client.reinforce(
        context,
        "mem_v_1",
        [evidence],
        outcome=pb.MEMORY_REINFORCEMENT_OUTCOME_SUCCEEDED,
        outcome_id="task-run-42",
        utility_micros=850_000,
        idempotency_key="reinforce-1",
        reason="procedure succeeded",
    )
    assert reinforced.commit_sequence == 7
    reinforce_request = stub.Reinforce.call_args[0][0]
    assert reinforce_request.version_id == "mem_v_1"
    assert reinforce_request.context.idempotency_key == "reinforce-1"
    assert reinforce_request.evidence[0].source_id == "run-42"
    assert reinforce_request.utility_micros == 850_000

    stub.PlanDeletion.return_value = pb.MemoryDeletionPlan(
        plan_id="mem_dp_1",
        plan_sha256="a" * 64,
        affected_version_ids=["mem_v_1"],
    )
    plan = client.plan_deletion(
        context,
        data_subject_id="subject-42",
        reason="privacy request",
        expires_in_seconds=600,
    )
    assert plan.plan_id == "mem_dp_1"
    plan_request = stub.PlanDeletion.call_args[0][0]
    assert plan_request.selector.WhichOneof("selector") == "data_subject_id"
    assert plan_request.selector.data_subject_id == "subject-42"
    assert plan_request.expires_in_seconds == 600

    client.plan_deletion(
        context,
        source_plane="document",
        source_id="source-42",
        reason="source withdrawal",
    )
    source_selector = stub.PlanDeletion.call_args[0][0].selector
    assert source_selector.WhichOneof("selector") == "source"
    assert source_selector.source.source_plane == "document"
    assert source_selector.source.source_id == "source-42"

    stub.ExecuteDeletion.return_value = pb.MemoryDeletionExecutionReceipt(
        execution_id="mem_dx_1",
        commit_sequence=8,
    )
    executed = client.execute_deletion(
        context,
        "mem_dp_1",
        "a" * 64,
        idempotency_key="delete-1",
        reason="reviewed erasure",
    )
    assert executed.execution_id == "mem_dx_1"
    execute_request = stub.ExecuteDeletion.call_args[0][0]
    assert execute_request.context.idempotency_key == "delete-1"
    assert execute_request.plan_sha256 == "a" * 64


def test_authoritative_memory_deletion_selector_is_unambiguous():
    client, _ = make_client()
    context = MemoryContext("workspace-a", "repo/akidb", "debugging")
    with pytest.raises(ValueError, match="exactly one"):
        client.plan_deletion(context, reason="missing selector")
    with pytest.raises(ValueError, match="both required"):
        client.plan_deletion(
            context,
            source_plane="document",
            reason="incomplete source selector",
        )
    with pytest.raises(ValueError, match="exactly one"):
        client.plan_deletion(
            context,
            source_plane="document",
            source_id="source-42",
            data_subject_id="subject-42",
            reason="ambiguous selector",
        )


def test_authoritative_memory_preview_requires_one_exact_target():
    client, _ = make_client()
    context = MemoryContext("workspace-a", "repo/akidb", "debugging")

    with pytest.raises(ValueError, match="exactly one"):
        client.memory_get(context)
    with pytest.raises(ValueError, match="exactly one"):
        client.forget(
            context,
            assertion_id="mem_a_1",
            version_id="mem_v_1",
            idempotency_key="forget-1",
            reason="invalid ambiguous target",
        )
    with pytest.raises(ValueError, match="exactly one"):
        client.retract(
            context,
            idempotency_key="retract-1",
            reason="missing target",
        )


def test_build_memory_metadata_protects_reserved_keys():
    meta = build_memory_metadata(kind="task", tags={"conversation_id": "HACK", "topic": "x"})
    assert meta["memory_kind"] == "task"
    assert "conversation_id" not in meta  # reserved key not clobbered by a tag
    assert meta["topic"] == "x"


def test_on_retry_hook_invoked():
    seen = []
    client, stub = make_client(max_retries=2, on_retry=lambda attempt, err: seen.append(attempt))
    stub.Search.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        pb.SearchResponse(results=[]),
    ]
    client.search([0.1])
    assert seen == [0, 1]  # one callback per retry, with the attempt index


def test_insert_returns_typed_result():
    client, stub = make_client()
    stub.Insert.return_value = pb.InsertResponse(success=True, id="a", internal_id=7)
    res = client.insert("a", [1.0])
    assert (res.success, res.id, res.internal_id) == (True, "a", 7)


def test_health_returns_typed_status():
    client, stub = make_client()
    stub.Health.return_value = pb.HealthResponse(
        healthy=True, ready=True, message="ok", total_vectors=10, active_vectors=9, using_gpu=False
    )
    h = client.health()
    assert h.healthy and h.ready and h.total_vectors == 10 and h.active_vectors == 9


def test_delete_returns_typed_result():
    client, stub = make_client()
    stub.Delete.return_value = pb.DeleteResponse(success=True, id="x", visibility="immediate")
    d = client.delete("x")
    assert d.success and d.id == "x" and d.visibility == "immediate"


def test_custom_collection_used():
    client, stub = make_client(collection="mycoll")
    stub.Insert.return_value = pb.InsertResponse(success=True, id="a")
    client.insert("a", [1.0])
    assert stub.Insert.call_args[0][0].collection == "mycoll"


def test_tls_builds_secure_channel():
    with patch("akidb.client.grpc.secure_channel") as sec, patch(
        "akidb.client.grpc.ssl_channel_credentials"
    ) as creds:
        AkiDBClient("host:443", tls=True)
        assert sec.called
        assert creds.called


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
