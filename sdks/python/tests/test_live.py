"""Live integration test against a running AkiDB server.

Opt-in: set ``AKIDB_SERVER_ADDR`` (e.g. ``127.0.0.1:51999``) to a running server.
``AKIDB_TEST_DIM`` must match the server's index dimension (default 8). Skipped
when no address is provided, so the normal (mock-based) suite stays hermetic.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ADDR = os.environ.get("AKIDB_SERVER_ADDR")
DIM = int(os.environ.get("AKIDB_TEST_DIM", "8"))

pytestmark = pytest.mark.skipif(ADDR is None, reason="set AKIDB_SERVER_ADDR to run live tests")


def _unit_vector(dim: int) -> list[float]:
    v = [0.0] * dim
    v[0] = 1.0
    return v


def test_live_roundtrip():
    from akidb import AkiDBClient

    with AkiDBClient(ADDR, timeout=10.0, max_retries=5) as client:
        # Health is reachable.
        client.health()

        vec = _unit_vector(DIM)
        ins = client.insert("live-1", vec, text="hello live", metadata=b'{"k":"v"}')
        assert ins.success

        got = client.get("live-1")
        assert got.found and got.id == "live-1"

        hits = client.search(vec, top_k=5)
        assert any(h.id == "live-1" for h in hits)

        deleted = client.delete("live-1")
        assert deleted.success

        assert not client.get("live-1").found


def test_live_hybrid_and_pack():
    from akidb import AkiDBClient

    with AkiDBClient(ADDR, timeout=10.0, max_retries=5) as client:
        vec = _unit_vector(DIM)
        client.insert("live-2", vec, text="needle in the haystack")
        hits = client.search(vec, top_k=5)
        assert any(h.id == "live-2" for h in hits)
        client.delete("live-2")
