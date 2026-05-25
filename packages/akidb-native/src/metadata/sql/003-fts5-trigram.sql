-- AkiDB Metadata Layer — FTS5 Trigram Index for CJK Support
-- Migration 003: Adds trigram-based full-text search for languages without
-- whitespace word boundaries (Japanese, Korean, Chinese).
-- The unicode61 tokenizer (002) handles space-delimited languages well.
-- This trigram index provides substring matching that works for all scripts.

CREATE VIRTUAL TABLE IF NOT EXISTS chunk_text_trigram USING fts5(
    chunk_id UNINDEXED,
    collection_id UNINDEXED,
    chunk_text,
    tokenize='trigram'
);

-- Populate from existing unicode61 FTS5 data (if any).
INSERT OR IGNORE INTO chunk_text_trigram(chunk_id, collection_id, chunk_text)
    SELECT chunk_id, collection_id, chunk_text FROM chunk_text_fts;

-- Record migration version
INSERT INTO schema_version (version) VALUES (3);
