-- Migration 004: Add quantization and HNSW parameters to collections.
-- These columns are nullable with defaults for backward compatibility.

ALTER TABLE collections ADD COLUMN quantization TEXT NOT NULL DEFAULT 'fp16'
  CHECK (quantization IN ('fp16', 'sq8'));

ALTER TABLE collections ADD COLUMN hnsw_m INTEGER NOT NULL DEFAULT 16
  CHECK (hnsw_m BETWEEN 4 AND 64);

ALTER TABLE collections ADD COLUMN hnsw_ef_construction INTEGER NOT NULL DEFAULT 200
  CHECK (hnsw_ef_construction BETWEEN 50 AND 800);

ALTER TABLE collections ADD COLUMN hnsw_ef_search INTEGER NOT NULL DEFAULT 100
  CHECK (hnsw_ef_search BETWEEN 10 AND 500);

INSERT INTO schema_version (version) VALUES (4);
