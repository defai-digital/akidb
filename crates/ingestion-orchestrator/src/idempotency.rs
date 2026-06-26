//! Idempotency Layer
//!
//! Content-hash based deduplication to prevent processing the same document twice.
//! Persists processed hashes to SQLite to survive restarts.

use sha2::{Sha256, Digest};
use indexmap::IndexSet;
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Mutex, RwLock};
use tracing::{info, warn, debug};

/// Idempotency checker using content hashing with SQLite persistence
///
/// Hashes are stored both in-memory (for fast lookups) and in SQLite (for persistence).
/// On restart, hashes are loaded from SQLite to restore deduplication state.
pub struct IdempotencyChecker {
    /// Set of processed content hashes (ordered by insertion time)
    processed: RwLock<IndexSet<String>>,

    /// Maximum number of hashes to keep in memory
    max_entries: usize,

    /// SQLite connection for persistence (None for in-memory only mode)
    db: Option<Mutex<Connection>>,
}

impl IdempotencyChecker {
    /// Create a new idempotency checker with SQLite persistence
    pub fn new_persistent<P: AsRef<Path>>(db_path: P, max_entries: usize) -> Result<Self, String> {
        let conn = Connection::open(db_path.as_ref())
            .map_err(|e| format!("Failed to open idempotency database: {}", e))?;

        // Create table if not exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_hashes (
                hash TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )",
            [],
        ).map_err(|e| format!("Failed to create idempotency table: {}", e))?;

        // Create index for efficient eviction
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created_at ON processed_hashes(created_at)",
            [],
        ).map_err(|e| format!("Failed to create index: {}", e))?;

        // Load existing hashes from SQLite (ordered by creation time)
        let mut processed = IndexSet::new();
        {
            let mut stmt = conn.prepare(
                "SELECT hash FROM processed_hashes ORDER BY created_at ASC LIMIT ?"
            ).map_err(|e| format!("Failed to prepare query: {}", e))?;

            let rows = stmt.query_map([max_entries as i64], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Failed to query hashes: {}", e))?;

            for row in rows {
                if let Ok(hash) = row {
                    processed.insert(hash);
                }
            }
        } // stmt is dropped here, releasing the borrow on conn

        let loaded_count = processed.len();
        if loaded_count > 0 {
            info!(count = loaded_count, "Loaded existing idempotency hashes from SQLite");
        }

        Ok(Self {
            processed: RwLock::new(processed),
            max_entries,
            db: Some(Mutex::new(conn)),
        })
    }

    /// Create a new in-memory only idempotency checker (no persistence)
    pub fn new(max_entries: usize) -> Self {
        warn!("Creating in-memory idempotency checker (state will be lost on restart)");
        Self {
            processed: RwLock::new(IndexSet::new()),
            max_entries,
            db: None,
        }
    }

    /// Compute content hash
    pub fn hash_content(content: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content);
        hex::encode(hasher.finalize())
    }

    /// Check if content has already been processed
    pub fn is_duplicate(&self, content: &[u8]) -> bool {
        let hash = Self::hash_content(content);
        let processed = self.processed.read().unwrap();
        processed.contains(&hash)
    }

    /// Mark content as processed
    pub fn mark_processed(&self, content: &[u8]) -> String {
        let hash = Self::hash_content(content);

        let mut processed = self.processed.write().unwrap();

        // Evict oldest entries (from front of IndexSet) if at capacity
        while processed.len() >= self.max_entries {
            if let Some(evicted_hash) = processed.shift_remove_index(0) {
                // FIX BUG-H056: Log SQLite eviction errors instead of silently ignoring
                // If eviction fails in SQLite but succeeds in memory, the database grows
                // unbounded and "evicted" hashes reappear after restart.
                if let Some(ref db) = self.db {
                    if let Ok(conn) = db.lock() {
                        if let Err(e) = conn.execute("DELETE FROM processed_hashes WHERE hash = ?", [&evicted_hash]) {
                            warn!(
                                error = %e,
                                hash = %evicted_hash,
                                "Failed to evict hash from SQLite - database may grow unbounded"
                            );
                        }
                    }
                }
            }
        }

        processed.insert(hash.clone());

        // Persist to SQLite
        if let Some(ref db) = self.db {
            if let Ok(conn) = db.lock() {
                if let Err(e) = conn.execute(
                    "INSERT OR IGNORE INTO processed_hashes (hash) VALUES (?)",
                    [&hash],
                ) {
                    warn!(error = %e, "Failed to persist hash to SQLite");
                }
            }
        }

        hash
    }

    /// Check and mark in one operation (returns true if duplicate)
    pub fn check_and_mark(&self, content: &[u8]) -> (bool, String) {
        let hash = Self::hash_content(content);

        let mut processed = self.processed.write().unwrap();
        let is_dup = processed.contains(&hash);

        if !is_dup {
            // Evict oldest entries from front
            while processed.len() >= self.max_entries {
                if let Some(evicted_hash) = processed.shift_remove_index(0) {
                    // FIX BUG-H056: Log SQLite eviction errors instead of silently ignoring
                    if let Some(ref db) = self.db {
                        if let Ok(conn) = db.lock() {
                            if let Err(e) = conn.execute("DELETE FROM processed_hashes WHERE hash = ?", [&evicted_hash]) {
                                warn!(
                                    error = %e,
                                    hash = %evicted_hash,
                                    "Failed to evict hash from SQLite - database may grow unbounded"
                                );
                            }
                        }
                    }
                }
            }
            processed.insert(hash.clone());

            // Persist to SQLite
            if let Some(ref db) = self.db {
                if let Ok(conn) = db.lock() {
                    if let Err(e) = conn.execute(
                        "INSERT OR IGNORE INTO processed_hashes (hash) VALUES (?)",
                        [&hash],
                    ) {
                        warn!(error = %e, "Failed to persist hash to SQLite");
                    }
                }
            }
        }

        (is_dup, hash)
    }

    /// Get number of tracked hashes
    pub fn len(&self) -> usize {
        self.processed.read().unwrap().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all tracked hashes
    ///
    /// FIX BUG-H028: Also clear the SQLite table, not just in-memory state.
    /// Previously, calling clear() and then restarting would reload the
    /// "cleared" hashes from SQLite, making clear() ineffective across restarts.
    pub fn clear(&self) -> Result<(), String> {
        // Clear in-memory state
        self.processed.write().unwrap().clear();

        // FIX BUG-H028: Also clear SQLite table
        if let Some(ref db) = self.db {
            let conn = db.lock().map_err(|e| format!("Failed to lock database: {}", e))?;
            conn.execute("DELETE FROM processed_hashes", [])
                .map_err(|e| format!("Failed to clear idempotency table: {}", e))?;
            info!("Cleared all idempotency hashes from SQLite");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_content() {
        let hash1 = IdempotencyChecker::hash_content(b"hello");
        let hash2 = IdempotencyChecker::hash_content(b"hello");
        let hash3 = IdempotencyChecker::hash_content(b"world");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_eq!(hash1.len(), 64); // SHA256 hex = 64 chars
    }

    #[test]
    fn test_duplicate_detection() {
        let checker = IdempotencyChecker::new(100);

        assert!(!checker.is_duplicate(b"content1"));
        checker.mark_processed(b"content1");
        assert!(checker.is_duplicate(b"content1"));
        assert!(!checker.is_duplicate(b"content2"));
    }

    #[test]
    fn test_check_and_mark() {
        let checker = IdempotencyChecker::new(100);

        let (is_dup1, hash1) = checker.check_and_mark(b"content");
        assert!(!is_dup1);

        let (is_dup2, hash2) = checker.check_and_mark(b"content");
        assert!(is_dup2);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_eviction() {
        let checker = IdempotencyChecker::new(10);

        for i in 0..15 {
            checker.mark_processed(format!("content{}", i).as_bytes());
        }

        // Should have evicted some entries
        assert!(checker.len() <= 10);
    }
}
