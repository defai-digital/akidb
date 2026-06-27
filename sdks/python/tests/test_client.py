"""Unit tests for the AkiDB Python client.

These mock the gRPC stub so they validate request construction, response parsing,
retry/backoff, and error mapping without a running server.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest

# Make the package importable without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from akidb import (  # noqa: E402
    AkiDBClient,
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
