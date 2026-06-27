"""Unit tests for the async AkiDB client (no pytest-asyncio; uses asyncio.run)."""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from akidb import NotFoundError, UnavailableError  # noqa: E402
from akidb import akidb_pb2 as pb  # noqa: E402
from akidb.aio import AsyncAkiDBClient  # noqa: E402
from tests.test_client import FakeRpcError  # noqa: E402


def make_client(**kwargs):
    stub = MagicMock()
    client = AsyncAkiDBClient(stub=stub, retry_backoff=0.0, **kwargs)
    return client, stub


def run(coro):
    return asyncio.run(coro)


def test_async_insert_and_search():
    client, stub = make_client(auth_token="tok")
    stub.Insert = AsyncMock(return_value=pb.InsertResponse(success=True, id="a"))
    stub.Search = AsyncMock(
        return_value=pb.SearchResponse(results=[pb.SearchResult(id="a", score=0.9)])
    )

    async def scenario():
        await client.insert("a", [1.0, 2.0], text="hi")
        return await client.search([0.1, 0.2], top_k=3)

    hits = run(scenario())
    assert hits[0].id == "a"
    assert ("authorization", "Bearer tok") in stub.Insert.call_args[1]["metadata"]
    assert stub.Search.call_args[0][0].top_k == 3


def test_async_text_search_pack():
    client, stub = make_client()
    stub.TextSearch = AsyncMock(
        return_value=pb.SearchResponse(
            results=[pb.SearchResult(id="x", score=1.0)], context_pack="[x] ctx"
        )
    )
    result = run(client.text_search("q", hybrid=True, pack=True, token_budget=128))
    assert result.context_pack == "[x] ctx"
    assert stub.TextSearch.call_args[0][0].pack_token_budget == 128


def test_async_retries_then_succeeds():
    client, stub = make_client(max_retries=2)
    stub.Search = AsyncMock(
        side_effect=[
            FakeRpcError(grpc.StatusCode.UNAVAILABLE),
            pb.SearchResponse(results=[pb.SearchResult(id="ok", score=1.0)]),
        ]
    )
    hits = run(client.search([0.1]))
    assert hits[0].id == "ok"
    assert stub.Search.call_count == 2


def test_async_non_retryable_maps_error():
    client, stub = make_client()
    stub.Get = AsyncMock(side_effect=FakeRpcError(grpc.StatusCode.NOT_FOUND, "missing"))
    try:
        run(client.get("nope"))
        assert False, "expected NotFoundError"
    except NotFoundError:
        pass
    assert stub.Get.call_count == 1


def test_async_retries_exhausted():
    client, stub = make_client(max_retries=1)
    stub.Search = AsyncMock(side_effect=FakeRpcError(grpc.StatusCode.UNAVAILABLE))
    try:
        run(client.search([0.1]))
        assert False, "expected UnavailableError"
    except UnavailableError:
        pass
    assert stub.Search.call_count == 2  # initial + 1 retry


def test_async_memory_write_and_read():
    client, stub = make_client()
    stub.Insert = AsyncMock(return_value=pb.InsertResponse(success=True, id="m1"))
    stub.Search = AsyncMock(return_value=pb.SearchResponse(results=[pb.SearchResult(id="m1", score=1.0)]))

    async def scenario():
        await client.memory_write("m1", [1.0], "note text", kind="note", conversation_id="c1")
        return await client.memory_read([0.1], conversation_id="c1")

    hits = run(scenario())
    meta = json.loads(stub.Insert.call_args[0][0].metadata.decode())
    assert meta["memory_kind"] == "note" and meta["conversation_id"] == "c1"
    assert stub.Search.call_args[0][0].tag_filter.condition.key == "conversation_id"
    assert hits[0].id == "m1"
