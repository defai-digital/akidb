-- AkiDB Metadata Layer — Initial Schema
-- Migration 001: Creates the four core registries.

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
  version   INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ─── Collection Registry ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS collections (
  collection_id     TEXT    PRIMARY KEY,
  dimension         INTEGER NOT NULL CHECK (dimension > 0),
  metric            TEXT    NOT NULL CHECK (metric IN ('cosine', 'l2', 'dot')),
  embedding_model_id TEXT   NOT NULL,
  schema_version    TEXT    NOT NULL,
  created_at        TEXT    NOT NULL,
  deleted_at        TEXT             -- NULL means active
);

CREATE INDEX IF NOT EXISTS idx_collections_deleted
  ON collections (deleted_at);

-- ─── Segment Registry ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS segments (
  segment_id     TEXT    PRIMARY KEY,
  collection_id  TEXT    NOT NULL REFERENCES collections (collection_id),
  record_count   INTEGER NOT NULL CHECK (record_count >= 0),
  dimension      INTEGER NOT NULL CHECK (dimension > 0),
  size_bytes     INTEGER NOT NULL CHECK (size_bytes >= 0),
  checksum       TEXT    NOT NULL,
  status         TEXT    NOT NULL CHECK (status IN ('building', 'ready', 'archived')),
  storage_path   TEXT    NOT NULL,
  created_at     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_segments_collection
  ON segments (collection_id);

CREATE INDEX IF NOT EXISTS idx_segments_status
  ON segments (status);

-- ─── Manifest Registry ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS manifests (
  manifest_id        TEXT    PRIMARY KEY,
  collection_id      TEXT    NOT NULL REFERENCES collections (collection_id),
  version            INTEGER NOT NULL CHECK (version >= 0),
  segment_ids        TEXT    NOT NULL DEFAULT '[]',   -- JSON array of segment IDs
  tombstone_ids      TEXT    NOT NULL DEFAULT '[]',   -- JSON array of tombstone chunk IDs
  embedding_model_id TEXT    NOT NULL,
  pipeline_signature TEXT    NOT NULL,
  created_at         TEXT    NOT NULL,
  checksum           TEXT    NOT NULL,
  UNIQUE (collection_id, version)
);

CREATE INDEX IF NOT EXISTS idx_manifests_collection
  ON manifests (collection_id);

CREATE INDEX IF NOT EXISTS idx_manifests_version
  ON manifests (collection_id, version DESC);

-- ─── Tombstone Registry ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tombstones (
  chunk_id       TEXT PRIMARY KEY,
  collection_id  TEXT NOT NULL REFERENCES collections (collection_id),
  deleted_at     TEXT NOT NULL,
  reason_code    TEXT NOT NULL CHECK (reason_code IN ('file_deleted', 'file_updated', 'manual_revoke'))
);

CREATE INDEX IF NOT EXISTS idx_tombstones_collection
  ON tombstones (collection_id);

CREATE INDEX IF NOT EXISTS idx_tombstones_reason
  ON tombstones (reason_code);

-- Record migration version
INSERT INTO schema_version (version) VALUES (1);
