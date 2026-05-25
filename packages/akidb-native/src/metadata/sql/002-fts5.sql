-- AkiDB Metadata Layer — FTS5 for Hybrid Search
-- Migration 002: Adds full-text search index for keyword/BM25 search.

-- FTS5 virtual table for chunk text content.
-- Stores chunk_id (UNINDEXED — not full-text searchable, used for retrieval),
-- collection_id (UNINDEXED — for collection filtering), and chunk_text (searchable).
CREATE VIRTUAL TABLE IF NOT EXISTS chunk_text_fts USING fts5(
    chunk_id UNINDEXED,
    collection_id UNINDEXED,
    chunk_text,
    tokenize='unicode61'
);

-- Track whether a collection stores chunk text for hybrid search.
ALTER TABLE collections ADD COLUMN store_chunk_text INTEGER NOT NULL DEFAULT 1;

-- Record migration version
INSERT INTO schema_version (version) VALUES (2);
