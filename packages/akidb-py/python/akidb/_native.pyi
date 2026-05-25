"""Type stubs for akidb._native (Rust extension module)."""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict

class _CollectionDict(TypedDict):
    collection_id: str
    dimension: int
    metric: str
    embedding_model_id: str
    schema_version: str
    created_at: str
    deleted_at: str | None
    quantization: str
    hnsw_m: int
    hnsw_ef_construction: int
    hnsw_ef_search: int

class _ManifestDict(TypedDict):
    manifest_id: str
    collection_id: str
    version: int
    segment_ids: list[str]
    tombstone_ids: list[str]
    embedding_model_id: str
    pipeline_signature: str
    created_at: str
    checksum: str

class _UpsertResultDict(TypedDict):
    segment_ids: list[str]
    buffered_count: int

class _ExplainDict(TypedDict, total=False):
    vector_score: float
    bm25_score: float
    rrf_score: float
    vector_rank: int
    bm25_rank: int
    chunk_preview: str
    matched_terms: list[str]

class _SearchResultDict(TypedDict):
    chunk_id: str
    score: float
    committed: NotRequired[bool | None]
    explain: NotRequired[_ExplainDict | None]

class _TelemetryDict(TypedDict):
    segments_scanned: int
    segments_cache_hits: int
    segments_cache_misses: int
    ef_used: int
    filter_selectivity: float | None
    buffer_records_scanned: int
    total_candidates_before_dedup: int
    duration_ms: float

class _SearchResponseDict(TypedDict):
    results: list[_SearchResultDict]
    manifest_version_used: int
    telemetry: NotRequired[_TelemetryDict | None]

class _CompactResultDict(TypedDict):
    records_kept: int
    records_removed: int
    space_reclaimed_bytes: int
    manifest: _ManifestDict

class _CompactionStatusDict(TypedDict):
    pending: bool
    last_compacted_at: str | None
    segment_count: int

class _RecordDict(TypedDict, total=False):
    chunk_id: str
    doc_id: str
    vector: list[float]
    metadata: dict[str, Any]
    chunk_text: str

class AkiDB:
    """Embedded vector database engine (Rust-powered).

    Args:
        storage_path: Path to the database directory.
        disable_wal: Disable write-ahead log (default: False).
    """

    def __init__(self, storage_path: str, disable_wal: bool = False) -> None: ...
    def __enter__(self) -> AkiDB: ...
    def __exit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_val: BaseException | None = None,
        _exc_tb: Any | None = None,
    ) -> bool: ...

    def create_collection(
        self,
        collection_id: str,
        dimension: int,
        metric: str,
        embedding_model_id: str,
        quantization: str = "fp16",
        hnsw_m: int = 16,
        hnsw_ef_construction: int = 200,
        hnsw_ef_search: int = 100,
    ) -> _CollectionDict:
        """Create a new vector collection."""
        ...

    def get_collection(self, collection_id: str) -> _CollectionDict | None:
        """Get a collection by ID. Returns None if not found."""
        ...

    def list_collections(self) -> list[_CollectionDict]:
        """List all collections."""
        ...

    def delete_collection(self, collection_id: str) -> None:
        """Delete a collection."""
        ...

    def upsert_batch(
        self, collection_id: str, records: list[_RecordDict]
    ) -> _UpsertResultDict:
        """Upsert a batch of records into a collection."""
        ...

    def flush_writes(self, collection_id: str) -> list[str]:
        """Flush in-memory write buffer to disk segments."""
        ...

    def publish(
        self,
        collection_id: str,
        embedding_model_id: str,
        pipeline_signature: str,
    ) -> _ManifestDict:
        """Publish a new manifest (snapshot) for the collection."""
        ...

    def search(
        self,
        collection_id: str,
        query_vector: list[float],
        top_k: int = 10,
        filters: dict[str, Any] | str | None = None,
        manifest_version: int | None = None,
        include_uncommitted: bool = True,
        mode: str = "vector",
        query_text: str | None = None,
        vector_weight: float = 1.0,
        keyword_weight: float = 1.0,
        explain: bool = False,
        ef_search: int | None = None,
    ) -> _SearchResponseDict:
        """Search a collection.

        Args:
            collection_id: Collection to search.
            query_vector: Query embedding vector.
            top_k: Number of results to return (default: 10).
            filters: Optional JSON string for metadata filters.
            manifest_version: Pin search to a specific manifest version.
            include_uncommitted: Include unflushed buffer records (default: True).
            mode: Search mode — "vector", "keyword", or "hybrid".
            query_text: Text query for keyword/hybrid search.
            vector_weight: Weight for vector results in hybrid RRF fusion.
            keyword_weight: Weight for keyword results in hybrid RRF fusion.
            explain: Include per-result scoring breakdown.
            ef_search: Per-query ef_search override.
        """
        ...

    def delete_chunks(
        self,
        collection_id: str,
        chunk_ids: list[str],
        reason: str = "manual_revoke",
    ) -> int:
        """Delete chunks by ID. Returns number of tombstones created."""
        ...

    def compact(self, collection_id: str) -> _CompactResultDict:
        """Compact a collection: merge segments, apply tombstones."""
        ...

    def compact_async(self, collection_id: str) -> None:
        """Compatibility API for async compaction."""
        ...

    def compaction_status(self, collection_id: str) -> _CompactionStatusDict:
        """Compatibility API for compaction status."""
        ...

    def rollback(self, collection_id: str, manifest_id: str) -> _ManifestDict:
        """Rollback to a previous manifest version."""
        ...

    def get_storage_size_bytes(self) -> int:
        """Get total storage size in bytes."""
        ...

    def get_tombstone_count(self, collection_id: str) -> int:
        """Get number of active tombstones for a collection."""
        ...

    def close(self) -> None:
        """Close the engine and release resources."""
        ...
