//! Document State Tracker
//!
//! SQLite-based tracking of document processing state.

use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;
use tracing::info;

use crate::Result;

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
                chunk_count INTEGER DEFAULT 0,
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

        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Create an in-memory state tracker (for testing)
    pub fn in_memory() -> Result<Self> {
        Self::new(":memory:")
    }

    /// Record a new document
    pub fn record_document(&self, id: &str, source: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        conn.execute(
            "INSERT OR REPLACE INTO documents (id, source, state, created_at, updated_at)
             VALUES (?1, ?2, 'queued', datetime('now'), datetime('now'))",
            params![id, source],
        )?;
        Ok(())
    }

    /// Update document state
    pub fn update_state(&self, id: &str, state: DocumentState) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        conn.execute(
            "UPDATE documents SET state = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![state.as_str(), id],
        )?;
        Ok(())
    }

    /// Update document state with error
    pub fn update_state_with_error(&self, id: &str, state: DocumentState, error: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        conn.execute(
            "UPDATE documents SET state = ?1, error = ?2, updated_at = datetime('now') WHERE id = ?3",
            params![state.as_str(), error, id],
        )?;
        Ok(())
    }

    /// Update chunk count
    pub fn update_chunk_count(&self, id: &str, count: usize) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        conn.execute(
            "UPDATE documents SET chunk_count = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![count as i64, id],
        )?;
        Ok(())
    }

    /// Get document record
    pub fn get_document(&self, id: &str) -> Result<Option<DocumentRecord>> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        let mut stmt = conn.prepare(
            "SELECT id, source, state, chunk_count, error, created_at, updated_at
             FROM documents WHERE id = ?1"
        )?;

        let result = stmt.query_row(params![id], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                source: row.get(1)?,
                state: DocumentState::from_str(&row.get::<_, String>(2)?),
                chunk_count: row.get::<_, i64>(3)? as usize,
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
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        let mut stmt = conn.prepare(
            "SELECT id, source, state, chunk_count, error, created_at, updated_at
             FROM documents WHERE state = ?1 ORDER BY created_at LIMIT ?2"
        )?;

        let records = stmt.query_map(params![state.as_str(), limit as i64], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                source: row.get(1)?,
                state: DocumentState::from_str(&row.get::<_, String>(2)?),
                chunk_count: row.get::<_, i64>(3)? as usize,
                error: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        })?;

        records.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get statistics
    pub fn stats(&self) -> Result<StateStats> {
        let conn = self.conn.lock().map_err(|e| {
            crate::IngestionError::State(format!("Failed to acquire lock: {}", e))
        })?;
        let mut stmt = conn.prepare(
            "SELECT state, COUNT(*) FROM documents GROUP BY state"
        )?;

        let mut stats = StateStats::default();

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for row in rows {
            let (state, count) = row?;
            match state.as_str() {
                "queued" => stats.queued = count as usize,
                "parsing" => stats.parsing = count as usize,
                "chunking" => stats.chunking = count as usize,
                "embedding" => stats.embedding = count as usize,
                "inserting" => stats.inserting = count as usize,
                "completed" => stats.completed = count as usize,
                "failed" => stats.failed = count as usize,
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
        self.queued + self.parsing + self.chunking + self.embedding
            + self.inserting + self.completed + self.failed
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
}
