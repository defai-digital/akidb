//! Document State Tracker
//!
//! SQLite-based tracking of document processing state.

use rusqlite::{params, types::Type, Connection};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tracing::info;

use crate::{IngestionError, Result};

fn usize_to_sqlite_i64(value: usize, field: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| IngestionError::State(format!("{field} exceeds SQLite INTEGER range")))
}

fn sqlite_i64_to_usize(value: i64, column: usize, field: &str) -> rusqlite::Result<usize> {
    usize::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{field} must be non-negative, got {value}"),
            )),
        )
    })
}

/// Document processing state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentState {
    /// Document is queued for processing
    Queued,
    /// Document is being parsed
    Parsing,
    /// Document is being chunked
    Chunking,
    /// Document is being embedded
    Embedding,
    /// Document is being inserted into AkiDB
    Inserting,
    /// Document processing completed
    Completed,
    /// Document processing failed
    Failed,
}

impl DocumentState {
    fn as_str(&self) -> &'static str {
        match self {
            DocumentState::Queued => "queued",
            DocumentState::Parsing => "parsing",
            DocumentState::Chunking => "chunking",
            DocumentState::Embedding => "embedding",
            DocumentState::Inserting => "inserting",
            DocumentState::Completed => "completed",
            DocumentState::Failed => "failed",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "queued" => DocumentState::Queued,
            "parsing" => DocumentState::Parsing,
            "chunking" => DocumentState::Chunking,
            "embedding" => DocumentState::Embedding,
            "inserting" => DocumentState::Inserting,
            "completed" => DocumentState::Completed,
            "failed" => DocumentState::Failed,
            _ => DocumentState::Queued,
        }
    }
}

/// Document record
#[derive(Debug, Clone)]
pub struct DocumentRecord {
    /// Document ID (content hash)
    pub id: String,
    /// Source file path/key
    pub source: String,
    /// Current state
    pub state: DocumentState,
    /// Number of chunks created
    pub chunk_count: usize,
    /// Error message if failed
    pub error: Option<String>,
    /// Created timestamp
    pub created_at: String,
    /// Updated timestamp
    pub updated_at: String,
}

/// SQLite-based document state tracker
/// Thread-safe: Connection is wrapped in Mutex for concurrent access
pub struct StateTracker {
    conn: Mutex<Connection>,
}

impl StateTracker {
    /// Create a new state tracker with the given database path
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Create tables
        conn.execute(
            "CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued',
                chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
                error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_state ON documents(state)",
            [],
        )?;

        info!("State tracker initialized");

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory state tracker (for testing)
    pub fn in_memory() -> Result<Self> {
        Self::new(":memory:")
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| IngestionError::State(format!("Failed to acquire lock: {}", e)))
    }

    /// Record a new document
    pub fn record_document(&self, id: &str, source: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT OR REPLACE INTO documents (id, source, state, created_at, updated_at)
             VALUES (?1, ?2, 'queued', datetime('now'), datetime('now'))",
            params![id, source],
        )?;
        Ok(())
    }

    /// Update document state
    pub fn update_state(&self, id: &str, state: DocumentState) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE documents SET state = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![state.as_str(), id],
        )?;
        Ok(())
    }

    /// Update document state with error
    pub fn update_state_with_error(
        &self,
        id: &str,
        state: DocumentState,
        error: &str,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE documents SET state = ?1, error = ?2, updated_at = datetime('now') WHERE id = ?3",
            params![state.as_str(), error, id],
        )?;
        Ok(())
    }

    /// Update chunk count
    pub fn update_chunk_count(&self, id: &str, count: usize) -> Result<()> {
        let conn = self.lock_conn()?;
        let count = usize_to_sqlite_i64(count, "chunk_count")?;
        conn.execute(
            "UPDATE documents SET chunk_count = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![count, id],
        )?;
        Ok(())
    }

    /// Get document record
    pub fn get_document(&self, id: &str) -> Result<Option<DocumentRecord>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, source, state, chunk_count, error, created_at, updated_at
             FROM documents WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![id], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                source: row.get(1)?,
                state: DocumentState::from_str(&row.get::<_, String>(2)?),
                chunk_count: sqlite_i64_to_usize(row.get::<_, i64>(3)?, 3, "chunk_count")?,
                error: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        });

        match result {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get documents by state
    pub fn get_by_state(&self, state: DocumentState, limit: usize) -> Result<Vec<DocumentRecord>> {
        let conn = self.lock_conn()?;
        let limit = usize_to_sqlite_i64(limit, "limit")?;
        let mut stmt = conn.prepare(
            "SELECT id, source, state, chunk_count, error, created_at, updated_at
             FROM documents WHERE state = ?1 ORDER BY created_at LIMIT ?2",
        )?;

        let records = stmt.query_map(params![state.as_str(), limit], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                source: row.get(1)?,
                state: DocumentState::from_str(&row.get::<_, String>(2)?),
                chunk_count: sqlite_i64_to_usize(row.get::<_, i64>(3)?, 3, "chunk_count")?,
                error: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        })?;

        records
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Get statistics
    pub fn stats(&self) -> Result<StateStats> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM documents GROUP BY state")?;

        let mut stats = StateStats::default();

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for row in rows {
            let (state, count) = row?;
            let count = sqlite_i64_to_usize(count, 1, "state count")?;
            match state.as_str() {
                "queued" => stats.queued = count,
                "parsing" => stats.parsing = count,
                "chunking" => stats.chunking = count,
                "embedding" => stats.embedding = count,
                "inserting" => stats.inserting = count,
                "completed" => stats.completed = count,
                "failed" => stats.failed = count,
                _ => {}
            }
        }

        Ok(stats)
    }
}

/// State statistics
#[derive(Debug, Clone, Default)]
pub struct StateStats {
    pub queued: usize,
    pub parsing: usize,
    pub chunking: usize,
    pub embedding: usize,
    pub inserting: usize,
    pub completed: usize,
    pub failed: usize,
}

impl StateStats {
    pub fn total(&self) -> usize {
        self.queued
            + self.parsing
            + self.chunking
            + self.embedding
            + self.inserting
            + self.completed
            + self.failed
    }

    pub fn in_progress(&self) -> usize {
        self.parsing + self.chunking + self.embedding + self.inserting
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_tracker() -> Result<()> {
        let tracker = StateTracker::in_memory()?;

        tracker.record_document("hash1", "file1.pdf")?;

        let doc = tracker.get_document("hash1")?.unwrap();
        assert_eq!(doc.state, DocumentState::Queued);

        tracker.update_state("hash1", DocumentState::Parsing)?;
        let doc = tracker.get_document("hash1")?.unwrap();
        assert_eq!(doc.state, DocumentState::Parsing);

        Ok(())
    }

    #[test]
    fn test_state_stats() -> Result<()> {
        let tracker = StateTracker::in_memory()?;

        tracker.record_document("hash1", "file1.pdf")?;
        tracker.record_document("hash2", "file2.pdf")?;
        tracker.update_state("hash1", DocumentState::Completed)?;

        let stats = tracker.stats()?;
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.total(), 2);

        Ok(())
    }

    #[test]
    fn test_update_chunk_count_rejects_sqlite_integer_overflow() -> Result<()> {
        let tracker = StateTracker::in_memory()?;
        tracker.record_document("hash1", "file1.pdf")?;

        let result = tracker.update_chunk_count("hash1", usize::MAX);

        assert!(
            matches!(result, Err(IngestionError::State(message)) if message.contains("chunk_count"))
        );
        let doc = tracker.get_document("hash1")?.unwrap();
        assert_eq!(doc.chunk_count, 0);
        Ok(())
    }

    #[test]
    fn test_get_by_state_rejects_limit_sqlite_integer_overflow() -> Result<()> {
        let tracker = StateTracker::in_memory()?;

        let result = tracker.get_by_state(DocumentState::Queued, usize::MAX);

        assert!(matches!(result, Err(IngestionError::State(message)) if message.contains("limit")));
        Ok(())
    }

    #[test]
    fn test_negative_legacy_chunk_count_is_rejected() -> Result<()> {
        let tracker = StateTracker::in_memory()?;
        {
            let conn = tracker.lock_conn()?;
            conn.execute("PRAGMA ignore_check_constraints = ON", [])?;
            conn.execute(
                "INSERT INTO documents (id, source, state, chunk_count, created_at, updated_at)
                 VALUES (?1, ?2, 'queued', -7, datetime('now'), datetime('now'))",
                params!["bad", "bad.pdf"],
            )?;
        }

        let result = tracker.get_document("bad");

        assert!(result.is_err(), "negative chunk_count must not wrap");
        Ok(())
    }
}
