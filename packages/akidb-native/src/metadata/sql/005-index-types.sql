-- Migration 005: Index type support (HNSW + IVF_PQ)
-- Adds index_type column and IVF_PQ parameters to collections.

ALTER TABLE collections ADD COLUMN index_type TEXT NOT NULL DEFAULT 'hnsw'
  CHECK (index_type IN ('hnsw', 'ivf_pq'));

ALTER TABLE collections ADD COLUMN ivf_num_clusters INTEGER
  CHECK (ivf_num_clusters IS NULL OR ivf_num_clusters > 0);

ALTER TABLE collections ADD COLUMN ivf_num_probes INTEGER
  CHECK (ivf_num_probes IS NULL OR ivf_num_probes > 0);

ALTER TABLE collections ADD COLUMN pq_num_subquantizers INTEGER
  CHECK (pq_num_subquantizers IS NULL OR pq_num_subquantizers > 0);

INSERT INTO schema_version (version) VALUES (5);
