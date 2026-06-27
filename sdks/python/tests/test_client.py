"""Unit tests for the AkiDB Python client.

These mock the gRPC stub so they validate request construction and response
parsing without a running server.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make the package importable without installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from akidb import AkiDBClient, SearchHit  # noqa: E402
from akidb import akidb_pb2 as pb  # noqa: E402


def make_client():
    stub = MagicMock()
    client = AkiDBClient(stub=stub)
    return client, stub


def test_insert_builds_request():
    client, stub = make_client()
    stub.Insert.return_value = pb.InsertResponse(success=True, id="a")
    resp = client.insert("a", [1.0, 2.0, 3.0], text="hello", metadata=b"{}")
    req = stub.Insert.call_args[0][0]
    assert req.id == "a"
    assert list(req.vector) == [1.0, 2.0, 3.0]
    assert req.text == "hello"
    assert req.metadata == b"{}"
    assert req.collection == "default"
    assert resp.success is True


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
    # proto `query` is float32, so compare approximately.
    assert list(req.query) == pytest.approx([0.1, 0.2])
    assert [h.id for h in hits] == ["a", "b"]
    assert hits[0].metadata_json() == {"k": "v"}
    assert hits[1].metadata_json() is None


def test_text_search_sets_flags_and_budget():
    client, stub = make_client()
    stub.TextSearch.return_value = pb.SearchResponse(
        results=[pb.SearchResult(id="x", score=1.0, metadata="")],
        context_pack="[x] packed context",
    )
    result = client.text_search(
        "query",
        top_k=7,
        hybrid=True,
        rerank=True,
        diversity=True,
        pack=True,
        token_budget=256,
    )
    req = stub.TextSearch.call_args[0][0]
    assert req.text == "query"
    assert req.top_k == 7
    assert req.hybrid is True
    assert req.rerank is True
    assert req.diversity is True
    assert req.pack is True
    assert req.pack_token_budget == 256
    assert result.context_pack == "[x] packed context"
    assert len(result) == 1
    assert list(result)[0].id == "x"


def test_delete_builds_request():
    client, stub = make_client()
    stub.Delete.return_value = pb.DeleteResponse(success=True, id="gone")
    resp = client.delete("gone")
    req = stub.Delete.call_args[0][0]
    assert req.id == "gone"
    assert resp.success is True


def test_search_hit_repr_and_metadata():
    hit = SearchHit("id1", 0.1234, '{"a":1}')
    assert "id1" in repr(hit)
    assert hit.metadata_json() == {"a": 1}


def test_custom_collection_used():
    stub = MagicMock()
    stub.Insert.return_value = pb.InsertResponse(success=True, id="a")
    client = AkiDBClient(stub=stub, collection="mycoll")
    client.insert("a", [1.0])
    assert stub.Insert.call_args[0][0].collection == "mycoll"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
