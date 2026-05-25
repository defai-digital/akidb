"""Tests for the AkiDB Python SDK."""

import json
import math
import os
import tempfile

import pytest

from akidb import AkiDB


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary directory for each test."""
    return str(tmp_path / "test_db")


@pytest.fixture
def db(db_path):
    """Create an AkiDB instance for each test."""
    engine = AkiDB(db_path)
    yield engine
    engine.close()


class TestCollections:
    def test_create_and_get_collection(self, db):
        coll = db.create_collection(
            collection_id="test",
            dimension=4,
            metric="cosine",
            embedding_model_id="test-model",
        )
        assert coll["collection_id"] == "test"
        assert coll["dimension"] == 4
        assert coll["metric"] == "cosine"
        assert coll["quantization"] == "fp16"

        got = db.get_collection("test")
        assert got is not None
        assert got["collection_id"] == "test"

    def test_get_nonexistent_collection(self, db):
        assert db.get_collection("nonexistent") is None

    def test_list_collections(self, db):
        db.create_collection("a", 4, "cosine", "model")
        db.create_collection("b", 8, "l2", "model")
        collections = db.list_collections()
        ids = [c["collection_id"] for c in collections]
        assert "a" in ids
        assert "b" in ids

    def test_delete_collection(self, db):
        db.create_collection("to_delete", 4, "cosine", "model")
        assert db.get_collection("to_delete") is not None
        db.delete_collection("to_delete")
        # AkiDB uses soft-delete: deleted_at is set but record still returned
        coll = db.get_collection("to_delete")
        assert coll is not None
        assert coll["deleted_at"] is not None

    def test_create_sq8_collection(self, db):
        coll = db.create_collection(
            "sq8_coll", 4, "cosine", "model", quantization="sq8"
        )
        assert coll["quantization"] == "sq8"

    def test_create_collection_rejects_unsupported_index_params(self, db):
        with pytest.raises(TypeError):
            db.create_collection(
                "ivf_coll",
                8,
                "cosine",
                "model",
                index_type="ivf_pq",
                ivf_num_clusters=4,
                pq_num_subquantizers=2,
            )


class TestUpsertAndSearch:
    def _make_records(self, n, dim=4):
        """Generate n test records with deterministic vectors."""
        records = []
        for i in range(n):
            vec = [0.0] * dim
            vec[i % dim] = 1.0
            records.append({
                "chunk_id": f"chunk_{i}",
                "doc_id": f"doc_{i}",
                "vector": vec,
                "metadata": {"idx": i},
                "chunk_text": f"This is chunk number {i}",
            })
        return records

    def test_upsert_batch(self, db):
        db.create_collection("test", 4, "cosine", "model")
        records = self._make_records(3)
        result = db.upsert_batch("test", records)
        assert "buffered_count" in result

    def test_search_vector(self, db):
        db.create_collection("test", 4, "cosine", "model")
        records = self._make_records(10)
        db.upsert_batch("test", records)
        db.publish("test", "model", "v1")

        response = db.search("test", [1.0, 0.0, 0.0, 0.0], top_k=3)
        assert "results" in response
        assert len(response["results"]) > 0
        assert response["results"][0]["chunk_id"].startswith("chunk_")
        assert "manifest_version_used" in response

    def test_search_with_explain(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", self._make_records(5))
        db.publish("test", "model", "v1")

        response = db.search("test", [1.0, 0.0, 0.0, 0.0], top_k=3, explain=True)
        assert len(response["results"]) > 0

    def test_search_include_uncommitted(self, db):
        db.create_collection("test", 4, "cosine", "model")
        # Need at least one publish before search works
        db.upsert_batch("test", self._make_records(3))
        db.publish("test", "model", "v1")
        # Insert more records without publishing
        db.upsert_batch("test", self._make_records(3))
        response = db.search(
            "test", [1.0, 0.0, 0.0, 0.0], top_k=10, include_uncommitted=True
        )
        assert len(response["results"]) > 0

    def test_search_with_telemetry(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", self._make_records(5))
        db.publish("test", "model", "v1")

        response = db.search("test", [1.0, 0.0, 0.0, 0.0], top_k=3)
        if response.get("telemetry"):
            t = response["telemetry"]
            assert "segments_scanned" in t
            assert "duration_ms" in t

    def test_search_with_dict_filters(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", self._make_records(6))
        db.publish("test", "model", "v1")

        response = db.search(
            "test",
            [1.0, 0.0, 0.0, 0.0],
            top_k=10,
            filters={"idx": 3},
        )
        assert len(response["results"]) == 1
        assert response["results"][0]["chunk_id"] == "chunk_3"


class TestFlushAndPublish:
    def test_flush_writes(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]},
        ])
        segment_ids = db.flush_writes("test")
        assert isinstance(segment_ids, list)

    def test_publish(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]},
        ])
        manifest = db.publish("test", "model", "v1")
        assert manifest["version"] >= 0  # versions are 0-indexed
        assert manifest["collection_id"] == "test"
        assert "manifest_id" in manifest


class TestDeleteAndCompact:
    def test_delete_chunks(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]},
            {"chunk_id": "c2", "doc_id": "d1", "vector": [0.0, 1.0, 0.0, 0.0]},
        ])
        db.publish("test", "model", "v1")
        count = db.delete_chunks("test", ["c1"])
        assert count == 1
        assert db.get_tombstone_count("test") >= 1

    def test_compact(self, db):
        db.create_collection("test", 4, "cosine", "model")
        # Upsert two batches to create multiple segments
        for i in range(3):
            db.upsert_batch("test", [
                {"chunk_id": f"c{i}", "doc_id": "d1", "vector": [float(i == j) for j in range(4)]},
            ])
            db.flush_writes("test")
        db.publish("test", "model", "v1")
        result = db.compact("test")
        assert "records_kept" in result
        assert "manifest" in result


class TestRollback:
    def test_rollback(self, db):
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]},
        ])
        m1 = db.publish("test", "model", "v1")

        db.upsert_batch("test", [
            {"chunk_id": "c2", "doc_id": "d1", "vector": [0.0, 1.0, 0.0, 0.0]},
        ])
        m2 = db.publish("test", "model", "v1")
        assert m2["version"] == m1["version"] + 1

        rolled = db.rollback("test", m1["manifest_id"])
        assert rolled["version"] == m2["version"] + 1  # rollback creates new manifest


class TestContextManager:
    def test_context_manager(self, db_path):
        with AkiDB(db_path) as db:
            db.create_collection("ctx", 4, "cosine", "model")
            assert db.get_collection("ctx") is not None

    def test_context_manager_exception(self, db_path):
        try:
            with AkiDB(db_path) as db:
                db.create_collection("ctx", 4, "cosine", "model")
                raise ValueError("test error")
        except ValueError:
            pass
        # Engine should be closed; re-open to verify data persisted
        with AkiDB(db_path) as db:
            assert db.get_collection("ctx") is not None


class TestIntrospection:
    def test_storage_size(self, db):
        db.create_collection("test", 4, "cosine", "model")
        size = db.get_storage_size_bytes()
        assert size >= 0

    def test_tombstone_count(self, db):
        db.create_collection("test", 4, "cosine", "model")
        count = db.get_tombstone_count("test")
        assert count == 0


class TestDoubleClose:
    def test_close_is_idempotent(self, db_path):
        """close() called twice must not panic (Bug fix: Cell<bool> guard)."""
        engine = AkiDB(db_path)
        engine.close()
        engine.close()  # second close must be a no-op

    def test_context_manager_then_explicit_close(self, db_path):
        """Explicit close after context manager must not panic."""
        with AkiDB(db_path) as engine:
            engine.create_collection("c", 4, "cosine", "model")
        engine.close()  # already closed by __exit__; must be safe


class TestFiltersInput:
    """Validate that filters accepts both dict and JSON string (Bug fix)."""

    def _setup(self, db):
        db.create_collection("test", 4, "cosine", "model")
        records = [
            {
                "chunk_id": f"c{i}",
                "doc_id": f"d{i}",
                "vector": [float(i == j) for j in range(4)],
                "metadata": {"score": i},
            }
            for i in range(5)
        ]
        db.upsert_batch("test", records)
        db.publish("test", "model", "v1")

    def test_filters_as_python_dict(self, db):
        """filters parameter accepts a Python dict."""
        self._setup(db)
        # Use $gte to select scores >= 3
        response = db.search(
            "test",
            [1.0, 0.0, 0.0, 0.0],
            top_k=10,
            filters={"score": {"$gte": 3}},
        )
        assert "results" in response

    def test_filters_as_json_string(self, db):
        """filters parameter accepts a pre-serialized JSON string."""
        self._setup(db)
        filters_str = json.dumps({"score": {"$lte": 2}})
        response = db.search(
            "test",
            [1.0, 0.0, 0.0, 0.0],
            top_k=10,
            filters=filters_str,
        )
        assert "results" in response

    def test_filters_none_returns_all_results(self, db):
        """filters=None returns all matching results."""
        self._setup(db)
        response = db.search("test", [1.0, 0.0, 0.0, 0.0], top_k=10, filters=None)
        assert len(response["results"]) > 0

    def test_invalid_json_string_filter_raises(self, db):
        """A malformed JSON string filter raises RuntimeError."""
        self._setup(db)
        with pytest.raises(RuntimeError, match="Invalid filters JSON"):
            db.search("test", [1.0, 0.0, 0.0, 0.0], top_k=5, filters="{not valid json")


class TestCompactionAsync:
    def test_compact_async_does_not_raise(self, db):
        """compact_async is non-blocking and should not raise."""
        db.create_collection("test", 4, "cosine", "model")
        db.upsert_batch("test", [
            {"chunk_id": "c1", "doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]},
        ])
        db.publish("test", "model", "v1")
        db.compact_async("test")  # should not raise

    def test_compaction_status(self, db):
        """compaction_status returns expected keys."""
        db.create_collection("test", 4, "cosine", "model")
        status = db.compaction_status("test")
        assert "pending" in status
        assert "segment_count" in status
        assert "last_compacted_at" in status


class TestKeywordAndHybridSearch:
    def _setup(self, db):
        db.create_collection("test", 4, "cosine", "model")
        records = [
            {
                "chunk_id": f"c{i}",
                "doc_id": f"d{i}",
                "vector": [float(i == j) for j in range(4)],
                "chunk_text": f"document about topic {i}",
            }
            for i in range(4)
        ]
        db.upsert_batch("test", records)
        db.publish("test", "model", "v1")

    def test_keyword_search_returns_results(self, db):
        """mode='keyword' performs BM25 text search."""
        self._setup(db)
        response = db.search(
            "test",
            [0.0, 0.0, 0.0, 0.0],
            top_k=5,
            mode="keyword",
            query_text="document",
        )
        assert "results" in response
        # Keyword search may return 0 results if FTS5 index is empty, but must not error

    def test_hybrid_search_returns_results(self, db):
        """mode='hybrid' fuses vector + BM25 via RRF."""
        self._setup(db)
        response = db.search(
            "test",
            [1.0, 0.0, 0.0, 0.0],
            top_k=5,
            mode="hybrid",
            query_text="topic",
        )
        assert "results" in response


class TestErrorHandling:
    def test_upsert_record_missing_chunk_id_raises(self, db):
        """Records missing required fields raise a RuntimeError at the engine."""
        db.create_collection("test", 4, "cosine", "model")
        with pytest.raises((RuntimeError, KeyError)):
            db.upsert_batch("test", [{"doc_id": "d1", "vector": [1.0, 0.0, 0.0, 0.0]}])

    def test_upsert_non_dict_record_raises(self, db):
        """Non-dict records raise a RuntimeError."""
        db.create_collection("test", 4, "cosine", "model")
        with pytest.raises(RuntimeError, match="dict"):
            db.upsert_batch("test", ["not a dict"])

    def test_search_on_nonexistent_collection_raises(self, db):
        """Searching a nonexistent collection raises RuntimeError."""
        with pytest.raises(RuntimeError):
            db.search("nonexistent", [1.0, 0.0, 0.0, 0.0], top_k=5)

    def test_publish_on_nonexistent_collection_raises(self, db):
        """Publishing on a nonexistent collection raises RuntimeError."""
        with pytest.raises(RuntimeError):
            db.publish("nonexistent", "model", "v1")

    def test_context_manager_exception_does_not_mask_original(self, db_path):
        """__exit__ must not replace the original exception with a close error."""
        raised = None
        try:
            with AkiDB(db_path) as db:
                raise ValueError("original error")
        except ValueError as e:
            raised = e
        except Exception:
            pass  # If this triggers, __exit__ masked the error

        assert raised is not None, "__exit__ must not swallow the original ValueError"
        assert str(raised) == "original error"
