//! MetadataStore — rusqlite-backed metadata layer for AkiDB.
//!
//! Manages four registries:
//!   1. Collection Registry — collection definitions & lifecycle
//!   2. Segment Registry    — segment metadata, checksums, locations
//!   3. Manifest Registry   — manifest versions, segment/tombstone lists
//!   4. Tombstone Registry  — logical deletes pending compaction
//!
//! Wire-compatible with the TypeScript `better-sqlite3` MetadataStore:
//! same SQL schema, same column types, same JSON encoding for array columns.

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{AkiDbError, Result};

const MIGRATION_001: &str = include_str!("sql/001-initial.sql");
const MIGRATION_002: &str = include_str!("sql/002-fts5.sql");
const MIGRATION_003: &str = include_str!("sql/003-fts5-trigram.sql");
const MIGRATION_004: &str = include_str!("sql/004-collection-params.sql");
const MIGRATION_005: &str = include_str!("sql/005-index-types.sql");
const FTS_TABLES: [&str; 2] = ["chunk_text_fts", "chunk_text_trigram"];

// ─── Data types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub collection_id: String,
    pub dimension: i64,
    pub metric: String,
    pub embedding_model_id: String,
    pub schema_version: String,
    pub created_at: String,
    pub deleted_at: Option<String>,
    pub quantization: String,
    pub hnsw_m: i64,
    pub hnsw_ef_construction: i64,
    pub hnsw_ef_search: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub segment_id: String,
    pub collection_id: String,
    pub record_count: i64,
    pub dimension: i64,
    pub size_bytes: i64,
    pub checksum: String,
    pub status: String,
    pub storage_path: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_id: String,
    pub collection_id: String,
    pub version: i64,
    pub segment_ids: Vec<String>,
    pub tombstone_ids: Vec<String>,
    pub embedding_model_id: String,
    pub pipeline_signature: String,
    pub created_at: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub chunk_id: String,
    pub collection_id: String,
    pub deleted_at: String,
    pub reason_code: String,
}

// ─── MetadataStore ───────────────────────────────────────────────────────────

pub struct MetadataStore {
    conn: Connection,
}

impl MetadataStore {
    /// Opens (or creates) a SQLite database at `db_path` and runs migrations.
    /// Pass `:memory:` for an ephemeral in-memory database (useful for tests).
    pub fn open(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let store = Self { conn };
        store.apply_pragmas()?;
        store.run_migrations()?;
        Ok(store)
    }

    // ── Pragmas ──────────────────────────────────────────────────────────────

    fn apply_pragmas(&self) -> Result<()> {
        self.conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )?;
        Ok(())
    }

    // ── Migrations ───────────────────────────────────────────────────────────

    fn run_migrations(&self) -> Result<()> {
        let current = self.get_schema_version();
        if current < 1 {
            self.conn.execute_batch(MIGRATION_001)?;
        }
        if current < 2 {
            self.conn.execute_batch(MIGRATION_002)?;
        }
        if current < 3 {
            self.conn.execute_batch(MIGRATION_003)?;
        }
        if current < 4 {
            self.conn.execute_batch(MIGRATION_004)?;
        }
        if current < 5 {
            self.conn.execute_batch(MIGRATION_005)?;
        }
        Ok(())
    }

    fn get_schema_version(&self) -> i64 {
        self.conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .unwrap_or(Some(0))
            .unwrap_or(0)
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  1. Collection Registry
    // ═════════════════════════════════════════════════════════════════════════

    pub fn create_collection(&self, c: &Collection) -> Result<()> {
        self.conn.execute(
            "INSERT INTO collections
               (collection_id, dimension, metric, embedding_model_id, schema_version, created_at, deleted_at,
                quantization, hnsw_m, hnsw_ef_construction, hnsw_ef_search)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                c.collection_id,
                c.dimension,
                c.metric,
                c.embedding_model_id,
                c.schema_version,
                c.created_at,
                c.deleted_at,
                c.quantization,
                c.hnsw_m,
                c.hnsw_ef_construction,
                c.hnsw_ef_search
            ],
        )?;
        Ok(())
    }

    pub fn get_collection(&self, collection_id: &str) -> Result<Option<Collection>> {
        let result = self
            .conn
            .query_row(
                "SELECT collection_id, dimension, metric, embedding_model_id, schema_version, created_at, deleted_at,
                        quantization, hnsw_m, hnsw_ef_construction, hnsw_ef_search
                 FROM collections WHERE collection_id = ?1",
                params![collection_id],
                |row| {
                    Ok(Collection {
                        collection_id: row.get(0)?,
                        dimension: row.get(1)?,
                        metric: row.get(2)?,
                        embedding_model_id: row.get(3)?,
                        schema_version: row.get(4)?,
                        created_at: row.get(5)?,
                        deleted_at: row.get(6)?,
                        quantization: row.get(7)?,
                        hnsw_m: row.get(8)?,
                        hnsw_ef_construction: row.get(9)?,
                        hnsw_ef_search: row.get(10)?,
                    })
                },
            )
            .optional()?;
        Ok(result)
    }

    pub fn list_collections(&self) -> Result<Vec<Collection>> {
        let mut stmt = self.conn.prepare(
            "SELECT collection_id, dimension, metric, embedding_model_id, schema_version, created_at, deleted_at,
                    quantization, hnsw_m, hnsw_ef_construction, hnsw_ef_search
             FROM collections WHERE deleted_at IS NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Collection {
                collection_id: row.get(0)?,
                dimension: row.get(1)?,
                metric: row.get(2)?,
                embedding_model_id: row.get(3)?,
                schema_version: row.get(4)?,
                created_at: row.get(5)?,
                deleted_at: row.get(6)?,
                quantization: row.get(7)?,
                hnsw_m: row.get(8)?,
                hnsw_ef_construction: row.get(9)?,
                hnsw_ef_search: row.get(10)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn soft_delete_collection(&self, collection_id: &str, deleted_at: &str) -> Result<bool> {
        let changes = self.conn.execute(
            "UPDATE collections SET deleted_at = ?1 WHERE collection_id = ?2 AND deleted_at IS NULL",
            params![deleted_at, collection_id],
        )?;
        Ok(changes > 0)
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  2. Segment Registry
    // ═════════════════════════════════════════════════════════════════════════

    pub fn create_segment(&self, s: &SegmentMetadata) -> Result<()> {
        self.conn.execute(
            "INSERT INTO segments
               (segment_id, collection_id, record_count, dimension, size_bytes, checksum, status, storage_path, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                s.segment_id,
                s.collection_id,
                s.record_count,
                s.dimension,
                s.size_bytes,
                s.checksum,
                s.status,
                s.storage_path,
                s.created_at
            ],
        )?;
        Ok(())
    }

    pub fn get_segment(&self, segment_id: &str) -> Result<Option<SegmentMetadata>> {
        let result = self
            .conn
            .query_row(
                "SELECT segment_id, collection_id, record_count, dimension, size_bytes, checksum, status, storage_path, created_at
                 FROM segments WHERE segment_id = ?1",
                params![segment_id],
                |row| {
                    Ok(SegmentMetadata {
                        segment_id: row.get(0)?,
                        collection_id: row.get(1)?,
                        record_count: row.get(2)?,
                        dimension: row.get(3)?,
                        size_bytes: row.get(4)?,
                        checksum: row.get(5)?,
                        status: row.get(6)?,
                        storage_path: row.get(7)?,
                        created_at: row.get(8)?,
                    })
                },
            )
            .optional()?;
        Ok(result)
    }

    pub fn list_segments(
        &self,
        collection_id: &str,
        status: Option<&str>,
    ) -> Result<Vec<SegmentMetadata>> {
        if let Some(status) = status {
            let mut stmt = self.conn.prepare(
                "SELECT segment_id, collection_id, record_count, dimension, size_bytes, checksum, status, storage_path, created_at
                 FROM segments WHERE collection_id = ?1 AND status = ?2",
            )?;
            let rows = stmt.query_map(params![collection_id, status], |row| {
                Ok(SegmentMetadata {
                    segment_id: row.get(0)?,
                    collection_id: row.get(1)?,
                    record_count: row.get(2)?,
                    dimension: row.get(3)?,
                    size_bytes: row.get(4)?,
                    checksum: row.get(5)?,
                    status: row.get(6)?,
                    storage_path: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.into())
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT segment_id, collection_id, record_count, dimension, size_bytes, checksum, status, storage_path, created_at
                 FROM segments WHERE collection_id = ?1",
            )?;
            let rows = stmt.query_map(params![collection_id], |row| {
                Ok(SegmentMetadata {
                    segment_id: row.get(0)?,
                    collection_id: row.get(1)?,
                    record_count: row.get(2)?,
                    dimension: row.get(3)?,
                    size_bytes: row.get(4)?,
                    checksum: row.get(5)?,
                    status: row.get(6)?,
                    storage_path: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.into())
        }
    }

    pub fn update_segment_status(&self, segment_id: &str, status: &str) -> Result<bool> {
        let changes = self.conn.execute(
            "UPDATE segments SET status = ?1 WHERE segment_id = ?2",
            params![status, segment_id],
        )?;
        Ok(changes > 0)
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  3. Manifest Registry
    // ═════════════════════════════════════════════════════════════════════════

    pub fn create_manifest(&self, m: &Manifest) -> Result<()> {
        let segment_ids_json = serde_json::to_string(&m.segment_ids)?;
        let tombstone_ids_json = serde_json::to_string(&m.tombstone_ids)?;

        self.conn.execute(
            "INSERT INTO manifests
               (manifest_id, collection_id, version, segment_ids, tombstone_ids,
                embedding_model_id, pipeline_signature, created_at, checksum)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                m.manifest_id,
                m.collection_id,
                m.version,
                segment_ids_json,
                tombstone_ids_json,
                m.embedding_model_id,
                m.pipeline_signature,
                m.created_at,
                m.checksum
            ],
        )?;
        Ok(())
    }

    pub fn get_manifest(&self, manifest_id: &str) -> Result<Option<Manifest>> {
        let result = self
            .conn
            .query_row(
                "SELECT manifest_id, collection_id, version, segment_ids, tombstone_ids,
                        embedding_model_id, pipeline_signature, created_at, checksum
                 FROM manifests WHERE manifest_id = ?1",
                params![manifest_id],
                |row| Ok(deserialize_manifest_row(row)),
            )
            .optional()?;
        match result {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub fn get_latest_manifest(&self, collection_id: &str) -> Result<Option<Manifest>> {
        let result = self
            .conn
            .query_row(
                "SELECT manifest_id, collection_id, version, segment_ids, tombstone_ids,
                        embedding_model_id, pipeline_signature, created_at, checksum
                 FROM manifests WHERE collection_id = ?1 ORDER BY version DESC LIMIT 1",
                params![collection_id],
                |row| Ok(deserialize_manifest_row(row)),
            )
            .optional()?;
        match result {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub fn get_manifest_by_version(
        &self,
        collection_id: &str,
        version: i64,
    ) -> Result<Option<Manifest>> {
        let result = self
            .conn
            .query_row(
                "SELECT manifest_id, collection_id, version, segment_ids, tombstone_ids,
                        embedding_model_id, pipeline_signature, created_at, checksum
                 FROM manifests WHERE collection_id = ?1 AND version = ?2 LIMIT 1",
                params![collection_id, version],
                |row| Ok(deserialize_manifest_row(row)),
            )
            .optional()?;
        match result {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub fn list_manifests(&self, collection_id: &str) -> Result<Vec<Manifest>> {
        let mut stmt = self.conn.prepare(
            "SELECT manifest_id, collection_id, version, segment_ids, tombstone_ids,
                    embedding_model_id, pipeline_signature, created_at, checksum
             FROM manifests WHERE collection_id = ?1 ORDER BY version ASC",
        )?;
        let rows = stmt.query_map(params![collection_id], |row| {
            Ok(deserialize_manifest_row(row))
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row??);
        }
        Ok(result)
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  4. Tombstone Registry
    // ═════════════════════════════════════════════════════════════════════════

    pub fn create_tombstone(&self, t: &Tombstone) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tombstones (chunk_id, collection_id, deleted_at, reason_code)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(chunk_id) DO NOTHING",
            params![t.chunk_id, t.collection_id, t.deleted_at, t.reason_code],
        )?;
        Ok(())
    }

    pub fn list_tombstones(
        &self,
        collection_id: &str,
        reason_code: Option<&str>,
    ) -> Result<Vec<Tombstone>> {
        if let Some(reason) = reason_code {
            let mut stmt = self.conn.prepare(
                "SELECT chunk_id, collection_id, deleted_at, reason_code
                 FROM tombstones WHERE collection_id = ?1 AND reason_code = ?2",
            )?;
            let rows = stmt.query_map(params![collection_id, reason], |row| {
                Ok(Tombstone {
                    chunk_id: row.get(0)?,
                    collection_id: row.get(1)?,
                    deleted_at: row.get(2)?,
                    reason_code: row.get(3)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.into())
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT chunk_id, collection_id, deleted_at, reason_code
                 FROM tombstones WHERE collection_id = ?1",
            )?;
            let rows = stmt.query_map(params![collection_id], |row| {
                Ok(Tombstone {
                    chunk_id: row.get(0)?,
                    collection_id: row.get(1)?,
                    deleted_at: row.get(2)?,
                    reason_code: row.get(3)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.into())
        }
    }

    pub fn list_tombstone_chunk_ids(&self, collection_id: &str) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_id FROM tombstones WHERE collection_id = ?1 ORDER BY chunk_id ASC",
        )?;
        let rows = stmt.query_map(params![collection_id], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| e.into())
    }

    pub fn delete_tombstones(&self, chunk_ids: &[String]) -> Result<usize> {
        if chunk_ids.is_empty() {
            return Ok(0);
        }
        // SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999; batch to stay under it.
        const BATCH_SIZE: usize = 999;
        let mut total_deleted = 0usize;
        for batch in chunk_ids.chunks(BATCH_SIZE) {
            let placeholders: Vec<String> = (1..=batch.len()).map(|i| format!("?{i}")).collect();
            let sql = format!(
                "DELETE FROM tombstones WHERE chunk_id IN ({})",
                placeholders.join(", ")
            );
            let params: Vec<&dyn rusqlite::types::ToSql> = batch
                .iter()
                .map(|s| s as &dyn rusqlite::types::ToSql)
                .collect();
            total_deleted += self.conn.execute(&sql, params.as_slice())?;
        }
        Ok(total_deleted)
    }

    pub fn get_tombstone_count(&self, collection_id: &str) -> Result<i64> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM tombstones WHERE collection_id = ?1",
            params![collection_id],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    // ═════════════════════════════════════════════════════════════════════════
    //  5. FTS5 Full-Text Search Index
    // ═════════════════════════════════════════════════════════════════════════

    /// Insert a chunk's text into both FTS5 indexes (unicode61 + trigram).
    pub fn fts_insert(&self, chunk_id: &str, collection_id: &str, chunk_text: &str) -> Result<()> {
        // Delete-then-insert to make re-ingestion idempotent.
        // FTS5 virtual tables don't support ON CONFLICT / UPSERT.
        for table in FTS_TABLES {
            self.conn.execute(
                &format!("DELETE FROM {table} WHERE chunk_id = ?1 AND collection_id = ?2"),
                params![chunk_id, collection_id],
            )?;
            self.conn.execute(
                &format!(
                    "INSERT INTO {table}(chunk_id, collection_id, chunk_text)
                     VALUES (?1, ?2, ?3)"
                ),
                params![chunk_id, collection_id, chunk_text],
            )?;
        }
        Ok(())
    }

    /// Insert multiple chunks' text into both FTS5 indexes in a single transaction.
    /// Dramatically faster than calling `fts_insert` in a loop — avoids per-row autocommit overhead.
    pub fn fts_insert_batch(&self, records: &[(&str, &str, &str)]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // unchecked_transaction() takes &self (no &mut required), safe here because
        // MetadataStore is the sole owner of the connection.
        let tx = self.conn.unchecked_transaction()?;
        for (chunk_id, collection_id, chunk_text) in records {
            for table in FTS_TABLES {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE chunk_id = ?1 AND collection_id = ?2"),
                    params![chunk_id, collection_id],
                )?;
                tx.execute(
                    &format!(
                        "INSERT INTO {table}(chunk_id, collection_id, chunk_text)
                         VALUES (?1, ?2, ?3)"
                    ),
                    params![chunk_id, collection_id, chunk_text],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Remove a chunk's text from both FTS5 indexes.
    pub fn fts_delete(&self, chunk_id: &str, collection_id: &str) -> Result<()> {
        for table in FTS_TABLES {
            self.conn.execute(
                &format!("DELETE FROM {table} WHERE chunk_id = ?1 AND collection_id = ?2"),
                params![chunk_id, collection_id],
            )?;
        }
        Ok(())
    }

    /// Remove all chunk text rows for a collection from both FTS5 indexes.
    pub fn fts_delete_collection(&self, collection_id: &str) -> Result<()> {
        for table in FTS_TABLES {
            self.conn.execute(
                &format!("DELETE FROM {table} WHERE collection_id = ?1"),
                params![collection_id],
            )?;
        }
        Ok(())
    }

    /// Retrieve the chunk text stored in the FTS5 index for a given chunk_id.
    #[allow(dead_code)]
    pub fn fts_get_text(&self, chunk_id: &str) -> Result<Option<String>> {
        let map = self.fts_get_texts(&[chunk_id.to_string()])?;
        Ok(map.get(chunk_id).cloned())
    }

    /// Retrieve chunk texts for many chunk IDs in batched `IN (...)` queries.
    pub fn fts_get_texts(&self, chunk_ids: &[String]) -> Result<HashMap<String, String>> {
        if chunk_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999.
        const BATCH_SIZE: usize = 999;
        let mut out = HashMap::with_capacity(chunk_ids.len());

        for batch in chunk_ids.chunks(BATCH_SIZE) {
            let placeholders: Vec<String> = (1..=batch.len()).map(|i| format!("?{i}")).collect();
            let sql = format!(
                "SELECT chunk_id, chunk_text FROM chunk_text_fts WHERE chunk_id IN ({})",
                placeholders.join(", ")
            );

            let params: Vec<&dyn rusqlite::types::ToSql> = batch
                .iter()
                .map(|id| id as &dyn rusqlite::types::ToSql)
                .collect();

            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params.as_slice(), |row| {
                let chunk_id: String = row.get(0)?;
                let chunk_text: String = row.get(1)?;
                Ok((chunk_id, chunk_text))
            })?;

            for row in rows {
                let (chunk_id, chunk_text) = row?;
                out.insert(chunk_id, chunk_text);
            }
        }

        Ok(out)
    }

    /// BM25 keyword search using FTS5 MATCH.
    /// Returns (chunk_id, bm25_score) pairs sorted by relevance (lower bm25 = more relevant).
    pub fn fts_search(
        &self,
        collection_id: &str,
        query_text: &str,
        top_k: usize,
    ) -> Result<Vec<(String, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_id, bm25(chunk_text_fts)
             FROM chunk_text_fts
             WHERE chunk_text MATCH ?1 AND collection_id = ?2
             ORDER BY bm25(chunk_text_fts)
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![query_text, collection_id, top_k as i64], |row| {
            let chunk_id: String = row.get(0)?;
            let score: f64 = row.get(1)?;
            Ok((chunk_id, score))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Trigram-based substring search using the chunk_text_trigram FTS5 index.
    /// Works for CJK and other scripts without whitespace word boundaries.
    /// Returns (chunk_id, score) pairs. Trigram doesn't support BM25, so
    /// scores are based on match count ranking.
    pub fn fts_trigram_search(
        &self,
        collection_id: &str,
        query_text: &str,
        top_k: usize,
    ) -> Result<Vec<(String, f64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_id, rank
             FROM chunk_text_trigram
             WHERE chunk_text MATCH ?1 AND collection_id = ?2
             ORDER BY rank
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![query_text, collection_id, top_k as i64], |row| {
            let chunk_id: String = row.get(0)?;
            let score: f64 = row.get(1)?;
            Ok((chunk_id, score))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}

// ─── Manifest deserialization helper ─────────────────────────────────────────

fn deserialize_manifest_row(row: &rusqlite::Row) -> Result<Manifest> {
    let segment_ids_json: String = row.get(3).map_err(AkiDbError::Metadata)?;
    let tombstone_ids_json: String = row.get(4).map_err(AkiDbError::Metadata)?;

    Ok(Manifest {
        manifest_id: row.get(0).map_err(AkiDbError::Metadata)?,
        collection_id: row.get(1).map_err(AkiDbError::Metadata)?,
        version: row.get(2).map_err(AkiDbError::Metadata)?,
        segment_ids: serde_json::from_str(&segment_ids_json)?,
        tombstone_ids: serde_json::from_str(&tombstone_ids_json)?,
        embedding_model_id: row.get(5).map_err(AkiDbError::Metadata)?,
        pipeline_signature: row.get(6).map_err(AkiDbError::Metadata)?,
        created_at: row.get(7).map_err(AkiDbError::Metadata)?,
        checksum: row.get(8).map_err(AkiDbError::Metadata)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> MetadataStore {
        MetadataStore::open(":memory:").unwrap()
    }

    fn sample_collection(id: &str) -> Collection {
        Collection {
            collection_id: id.to_string(),
            dimension: 384,
            metric: "cosine".to_string(),
            embedding_model_id: "text-embedding-3-small".to_string(),
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            deleted_at: None,
            quantization: "fp16".to_string(),
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 100,
        }
    }

    #[test]
    fn create_and_get_collection() {
        let store = test_store();
        let c = sample_collection("test-coll");
        store.create_collection(&c).unwrap();

        let got = store.get_collection("test-coll").unwrap().unwrap();
        assert_eq!(got.collection_id, "test-coll");
        assert_eq!(got.dimension, 384);
        assert_eq!(got.metric, "cosine");
    }

    #[test]
    fn get_missing_collection() {
        let store = test_store();
        assert!(store.get_collection("ghost").unwrap().is_none());
    }

    #[test]
    fn list_collections_excludes_deleted() {
        let store = test_store();
        store.create_collection(&sample_collection("a")).unwrap();
        store.create_collection(&sample_collection("b")).unwrap();
        store
            .soft_delete_collection("b", "2026-01-02T00:00:00Z")
            .unwrap();

        let list = store.list_collections().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].collection_id, "a");
    }

    #[test]
    fn soft_delete_returns_false_for_missing() {
        let store = test_store();
        let changed = store
            .soft_delete_collection("ghost", "2026-01-01T00:00:00Z")
            .unwrap();
        assert!(!changed);
    }

    #[test]
    fn create_and_get_segment() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        let seg = SegmentMetadata {
            segment_id: "seg-001".to_string(),
            collection_id: "coll-1".to_string(),
            record_count: 100,
            dimension: 384,
            size_bytes: 50000,
            checksum: "abc123".to_string(),
            status: "ready".to_string(),
            storage_path: "coll-1/seg-001.bin".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };
        store.create_segment(&seg).unwrap();

        let got = store.get_segment("seg-001").unwrap().unwrap();
        assert_eq!(got.record_count, 100);
        assert_eq!(got.status, "ready");
    }

    #[test]
    fn list_segments_with_status_filter() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for (id, status) in [("s1", "ready"), ("s2", "building"), ("s3", "ready")] {
            store
                .create_segment(&SegmentMetadata {
                    segment_id: id.to_string(),
                    collection_id: "coll-1".to_string(),
                    record_count: 10,
                    dimension: 384,
                    size_bytes: 1000,
                    checksum: "x".to_string(),
                    status: status.to_string(),
                    storage_path: format!("coll-1/{id}.bin"),
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                })
                .unwrap();
        }

        let all = store.list_segments("coll-1", None).unwrap();
        assert_eq!(all.len(), 3);

        let ready = store.list_segments("coll-1", Some("ready")).unwrap();
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn update_segment_status() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();
        store
            .create_segment(&SegmentMetadata {
                segment_id: "s1".to_string(),
                collection_id: "coll-1".to_string(),
                record_count: 10,
                dimension: 384,
                size_bytes: 1000,
                checksum: "x".to_string(),
                status: "building".to_string(),
                storage_path: "coll-1/s1.bin".to_string(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
            })
            .unwrap();

        let changed = store.update_segment_status("s1", "ready").unwrap();
        assert!(changed);

        let got = store.get_segment("s1").unwrap().unwrap();
        assert_eq!(got.status, "ready");
    }

    #[test]
    fn segment_status_update_controls_listing() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();
        store
            .create_segment(&SegmentMetadata {
                segment_id: "s1".to_string(),
                collection_id: "coll-1".to_string(),
                record_count: 10,
                dimension: 384,
                size_bytes: 1000,
                checksum: "x".to_string(),
                status: "building".to_string(),
                storage_path: "coll-1/s1.bin".to_string(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
            })
            .unwrap();

        // Only "ready" segments are returned by list_segments with status filter.
        let ready = store.list_segments("coll-1", Some("ready")).unwrap();
        assert!(ready.is_empty());

        store.update_segment_status("s1", "ready").unwrap();
        let ready = store.list_segments("coll-1", Some("ready")).unwrap();
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn create_and_get_manifest() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        let m = Manifest {
            manifest_id: "m-001".to_string(),
            collection_id: "coll-1".to_string(),
            version: 1,
            segment_ids: vec!["seg-1".to_string(), "seg-2".to_string()],
            tombstone_ids: vec!["t-1".to_string()],
            embedding_model_id: "text-embedding-3-small".to_string(),
            pipeline_signature: "v1-sha256".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            checksum: "manifest-hash".to_string(),
        };
        store.create_manifest(&m).unwrap();

        let got = store.get_manifest("m-001").unwrap().unwrap();
        assert_eq!(got.segment_ids, vec!["seg-1", "seg-2"]);
        assert_eq!(got.tombstone_ids, vec!["t-1"]);
        assert_eq!(got.version, 1);
    }

    #[test]
    fn get_latest_manifest() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for v in 1..=3 {
            store
                .create_manifest(&Manifest {
                    manifest_id: format!("m-{v}"),
                    collection_id: "coll-1".to_string(),
                    version: v,
                    segment_ids: vec![],
                    tombstone_ids: vec![],
                    embedding_model_id: "model".to_string(),
                    pipeline_signature: "sig".to_string(),
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    checksum: "hash".to_string(),
                })
                .unwrap();
        }

        let latest = store.get_latest_manifest("coll-1").unwrap().unwrap();
        assert_eq!(latest.version, 3);
    }

    #[test]
    fn get_manifest_by_version() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for v in 1..=3 {
            store
                .create_manifest(&Manifest {
                    manifest_id: format!("m-{v}"),
                    collection_id: "coll-1".to_string(),
                    version: v,
                    segment_ids: vec![format!("seg-{v}")],
                    tombstone_ids: vec![],
                    embedding_model_id: "model".to_string(),
                    pipeline_signature: "sig".to_string(),
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    checksum: "hash".to_string(),
                })
                .unwrap();
        }

        let manifest = store.get_manifest_by_version("coll-1", 2).unwrap().unwrap();
        assert_eq!(manifest.version, 2);
        assert_eq!(manifest.segment_ids, vec!["seg-2"]);
        assert!(store
            .get_manifest_by_version("coll-1", 99)
            .unwrap()
            .is_none());
    }

    #[test]
    fn list_manifests_ordered_by_version() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for v in [3, 1, 2] {
            store
                .create_manifest(&Manifest {
                    manifest_id: format!("m-{v}"),
                    collection_id: "coll-1".to_string(),
                    version: v,
                    segment_ids: vec![],
                    tombstone_ids: vec![],
                    embedding_model_id: "model".to_string(),
                    pipeline_signature: "sig".to_string(),
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    checksum: "hash".to_string(),
                })
                .unwrap();
        }

        let manifests = store.list_manifests("coll-1").unwrap();
        let versions: Vec<i64> = manifests.iter().map(|m| m.version).collect();
        assert_eq!(versions, vec![1, 2, 3]);
    }

    #[test]
    fn create_and_list_tombstones() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        store
            .create_tombstone(&Tombstone {
                chunk_id: "c-1".to_string(),
                collection_id: "coll-1".to_string(),
                deleted_at: "2026-01-01T00:00:00Z".to_string(),
                reason_code: "file_deleted".to_string(),
            })
            .unwrap();
        store
            .create_tombstone(&Tombstone {
                chunk_id: "c-2".to_string(),
                collection_id: "coll-1".to_string(),
                deleted_at: "2026-01-01T00:00:00Z".to_string(),
                reason_code: "manual_revoke".to_string(),
            })
            .unwrap();

        let all = store.list_tombstones("coll-1", None).unwrap();
        assert_eq!(all.len(), 2);

        let filtered = store
            .list_tombstones("coll-1", Some("file_deleted"))
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].chunk_id, "c-1");

        let chunk_ids = store.list_tombstone_chunk_ids("coll-1").unwrap();
        assert_eq!(chunk_ids, vec!["c-1".to_string(), "c-2".to_string()]);
    }

    #[test]
    fn delete_tombstones_bulk_removes_entries() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();
        store
            .create_tombstone(&Tombstone {
                chunk_id: "c-1".to_string(),
                collection_id: "coll-1".to_string(),
                deleted_at: "2026-01-01T00:00:00Z".to_string(),
                reason_code: "file_deleted".to_string(),
            })
            .unwrap();
        store
            .create_tombstone(&Tombstone {
                chunk_id: "c-2".to_string(),
                collection_id: "coll-1".to_string(),
                deleted_at: "2026-01-01T00:00:00Z".to_string(),
                reason_code: "file_deleted".to_string(),
            })
            .unwrap();

        let removed = store.delete_tombstones(&["c-1".to_string()]).unwrap();
        assert_eq!(removed, 1);

        let remaining = store.list_tombstones("coll-1", None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].chunk_id, "c-2");
    }

    #[test]
    fn bulk_delete_tombstones() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for i in 1..=5 {
            store
                .create_tombstone(&Tombstone {
                    chunk_id: format!("c-{i}"),
                    collection_id: "coll-1".to_string(),
                    deleted_at: "2026-01-01T00:00:00Z".to_string(),
                    reason_code: "file_deleted".to_string(),
                })
                .unwrap();
        }

        let ids = vec!["c-1".to_string(), "c-3".to_string(), "c-5".to_string()];
        let deleted = store.delete_tombstones(&ids).unwrap();
        assert_eq!(deleted, 3);

        let remaining = store.list_tombstones("coll-1", None).unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn create_tombstone_rejects_invalid_reason_code() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        let err = store
            .create_tombstone(&Tombstone {
                chunk_id: "c-1".to_string(),
                collection_id: "coll-1".to_string(),
                deleted_at: "2026-01-01T00:00:00Z".to_string(),
                reason_code: "invalid_reason".to_string(),
            })
            .unwrap_err();

        assert!(err.to_string().contains("CHECK constraint failed"));
        assert!(store.list_tombstones("coll-1", None).unwrap().is_empty());
    }

    #[test]
    fn tombstone_count() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        for i in 1..=3 {
            store
                .create_tombstone(&Tombstone {
                    chunk_id: format!("c-{i}"),
                    collection_id: "coll-1".to_string(),
                    deleted_at: "2026-01-01T00:00:00Z".to_string(),
                    reason_code: "file_deleted".to_string(),
                })
                .unwrap();
        }

        assert_eq!(store.get_tombstone_count("coll-1").unwrap(), 3);
    }

    #[test]
    fn migrations_are_idempotent() {
        let store = test_store();
        store.create_collection(&sample_collection("a")).unwrap();

        // Open again — migrations should detect current schema version and skip.
        // Since we can't re-open :memory:, just verify the version check works.
        let version = store.get_schema_version();
        assert_eq!(version, 5);
    }

    #[test]
    fn fts_insert_and_search() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        // Verify the latest schema version is applied.
        let version = store.get_schema_version();
        assert_eq!(version, 5, "All migrations should have been applied");

        // Verify the FTS5 table exists.
        let table_exists: bool = store.conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='chunk_text_fts'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(table_exists, "chunk_text_fts table should exist");

        store
            .fts_insert(
                "c-1",
                "coll-1",
                "The quick brown fox jumps over the lazy dog",
            )
            .unwrap();
        store
            .fts_insert(
                "c-2",
                "coll-1",
                "Machine learning and artificial intelligence",
            )
            .unwrap();
        store
            .fts_insert("c-3", "coll-1", "The brown fox was very quick today")
            .unwrap();

        // FTS5 MATCH queries use term-based matching.
        let results = store.fts_search("coll-1", "quick", 10).unwrap();
        assert!(
            !results.is_empty(),
            "FTS5 search for 'quick' should find results"
        );
        let ids: Vec<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
        assert!(ids.contains(&"c-1"));

        // Multi-term search.
        let results2 = store.fts_search("coll-1", "brown fox", 10).unwrap();
        assert!(
            !results2.is_empty(),
            "FTS5 search for 'brown fox' should find results"
        );
    }

    #[test]
    fn fts_trigram_cjk_search() {
        let store = test_store();
        store
            .create_collection(&sample_collection("coll-1"))
            .unwrap();

        // Verify the trigram table exists.
        let trigram_exists: bool = store
            .conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE name='chunk_text_trigram'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(trigram_exists, "chunk_text_trigram table should exist");

        // Insert CJK text (Japanese, Korean, Chinese).
        store
            .fts_insert("ja-1", "coll-1", "すべての人間は生れながらにして自由であり")
            .unwrap();
        store
            .fts_insert(
                "ko-1",
                "coll-1",
                "모든 인류 구성원의 천부의 존엄성과 동등하고",
            )
            .unwrap();
        store
            .fts_insert("zh-1", "coll-1", "人人生而自由在尊严和权利上一律平等")
            .unwrap();
        store
            .fts_insert("fr-1", "coll-1", "Tous les êtres humains naissent libres")
            .unwrap();
        store
            .fts_insert(
                "de-1",
                "coll-1",
                "Alle Menschen sind frei und gleich an Würde",
            )
            .unwrap();

        // Verify data is in the trigram table.
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM chunk_text_trigram", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 5, "Should have 5 rows in trigram table");

        // Trigram search needs at least 3 characters. Use a longer CJK substring.
        // FTS5 trigram tokenizer works on character trigrams, so "自由であり" should work.
        let trigram_results = store
            .fts_trigram_search("coll-1", "人間は生れ", 10)
            .unwrap();
        eprintln!("trigram results for '人間は生れ': {:?}", trigram_results);
        assert!(
            !trigram_results.is_empty(),
            "Trigram should find Japanese text"
        );
        let ids: Vec<&str> = trigram_results.iter().map(|(id, _)| id.as_str()).collect();
        assert!(ids.contains(&"ja-1"), "Should find Japanese chunk");

        // Trigram search for Korean (3+ chars).
        let ko_results = store.fts_trigram_search("coll-1", "존엄성과", 10).unwrap();
        eprintln!("trigram results for '존엄성과': {:?}", ko_results);
        assert!(!ko_results.is_empty(), "Trigram should find Korean text");
        let ko_ids: Vec<&str> = ko_results.iter().map(|(id, _)| id.as_str()).collect();
        assert!(ko_ids.contains(&"ko-1"));

        // Chinese substring.
        let zh_results = store
            .fts_trigram_search("coll-1", "自由在尊严", 10)
            .unwrap();
        eprintln!("trigram results for '自由在尊严': {:?}", zh_results);
        assert!(!zh_results.is_empty(), "Trigram should find Chinese text");

        // Trigram also works for Latin scripts (substring match).
        let fr_results = store
            .fts_trigram_search("coll-1", "êtres humains", 10)
            .unwrap();
        assert!(!fr_results.is_empty(), "Trigram should find French text");

        // German with umlaut.
        let de_results = store.fts_trigram_search("coll-1", "Würde", 10).unwrap();
        assert!(
            !de_results.is_empty(),
            "Trigram should find German text with umlaut"
        );
    }
}
