//! Idempotency Layer
//!
//! Content-hash based deduplication to prevent processing the same document twice.
//! Persists processed hashes to SQLite to survive restarts.

use indexmap::IndexSet;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{Mutex, RwLock};
use tracing::{info, warn};

fn normalize_max_entries(max_entries: usize) -> usize {
    max_entries.max(1)
}

fn sqlite_limit(max_entries: usize) -> Result<i64, String> {
    i64::try_from(max_entries).map_err(|_| "max_entries exceeds SQLite LIMIT range".to_string())
}

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
        let max_entries = normalize_max_entries(max_entries);
        let conn = Connection::open(db_path.as_ref())
            .map_err(|e| format!("Failed to open idempotency database: {}", e))?;

        // Create table if not exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_hashes (
                hash TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )
        .map_err(|e| format!("Failed to create idempotency table: {}", e))?;

        // Create index for efficient eviction
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created_at ON processed_hashes(created_at)",
            [],
        )
        .map_err(|e| format!("Failed to create index: {}", e))?;

        // Load the newest hashes from SQLite, then restore in-memory order
        // from oldest to newest so runtime eviction still removes the oldest.
        let mut processed = IndexSet::new();
        {
            let mut stmt = conn
                .prepare(
                    "SELECT hash FROM processed_hashes
                     ORDER BY created_at DESC, rowid DESC
                     LIMIT ?",
                )
                .map_err(|e| format!("Failed to prepare query: {}", e))?;

            let rows = stmt
                .query_map([sqlite_limit(max_entries)?], |row| row.get::<_, String>(0))
                .map_err(|e| format!("Failed to query hashes: {}", e))?;

            let mut loaded_hashes = Vec::new();
            for hash in rows.flatten() {
                loaded_hashes.push(hash);
            }

            loaded_hashes.reverse();
            for hash in loaded_hashes {
                processed.insert(hash);
            }
        } // stmt is dropped here, releasing the borrow on conn

        let loaded_count = processed.len();
        if loaded_count > 0 {
            info!(
                count = loaded_count,
                "Loaded existing idempotency hashes from SQLite"
            );
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
            max_entries: normalize_max_entries(max_entries),
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
        // Recover from poisoned lock to preserve idempotency state
        let processed = self.processed.read().unwrap_or_else(|e| e.into_inner());
        processed.contains(&hash)
    }

    /// Mark content as processed
    pub fn mark_processed(&self, content: &[u8]) -> String {
        let hash = Self::hash_content(content);

        // Recover from poisoned lock to preserve idempotency state
        let mut processed = self.processed.write().unwrap_or_else(|e| e.into_inner());

        // Evict oldest entries (from front of IndexSet) if at capacity
        while processed.len() >= self.max_entries {
            if let Some(evicted_hash) = processed.shift_remove_index(0) {
                // FIX BUG-H056: Log SQLite eviction errors instead of silently ignoring
                // If eviction fails in SQLite but succeeds in memory, the database grows
                // unbounded and "evicted" hashes reappear after restart.
                if let Some(ref db) = self.db {
                    if let Ok(conn) = db.lock() {
                        if let Err(e) = conn.execute(
                            "DELETE FROM processed_hashes WHERE hash = ?",
                            [&evicted_hash],
                        ) {
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

        // Recover from poisoned lock to preserve idempotency state
        let mut processed = self.processed.write().unwrap_or_else(|e| e.into_inner());
        let is_dup = processed.contains(&hash);

        if !is_dup {
            // Evict oldest entries from front
            while processed.len() >= self.max_entries {
                if let Some(evicted_hash) = processed.shift_remove_index(0) {
                    // FIX BUG-H056: Log SQLite eviction errors instead of silently ignoring
                    if let Some(ref db) = self.db {
                        if let Ok(conn) = db.lock() {
                            if let Err(e) = conn.execute(
                                "DELETE FROM processed_hashes WHERE hash = ?",
                                [&evicted_hash],
                            ) {
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

    /// Remove a previously marked hash.
    ///
    /// This is used to roll back the in-flight idempotency mark when processing
    /// fails after `check_and_mark`, allowing the same content to be retried.
    pub fn unmark_hash(&self, hash: &str) -> Result<(), String> {
        // Recover from poisoned lock to preserve idempotency state
        self.processed.write().unwrap_or_else(|e| e.into_inner()).shift_remove(hash);

        if let Some(ref db) = self.db {
            let conn = db
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
            conn.execute("DELETE FROM processed_hashes WHERE hash = ?", [hash])
                .map_err(|e| format!("Failed to unmark hash from SQLite: {}", e))?;
        }

        Ok(())
    }

    /// Get number of tracked hashes
    pub fn len(&self) -> usize {
        // Recover from poisoned lock to preserve idempotency state
        self.processed.read().unwrap_or_else(|e| e.into_inner()).len()
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
        // Clear in-memory state (recover from poisoned lock)
        self.processed.write().unwrap_or_else(|e| e.into_inner()).clear();

        // FIX BUG-H028: Also clear SQLite table
        if let Some(ref db) = self.db {
            let conn = db
                .lock()
                .map_err(|e| format!("Failed to lock database: {}", e))?;
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
    fn test_unmark_hash_allows_failed_content_to_retry() {
        let checker = IdempotencyChecker::new(100);

        let (is_dup, hash) = checker.check_and_mark(b"content");
        assert!(!is_dup);
        assert!(checker.is_duplicate(b"content"));

        checker.unmark_hash(&hash).unwrap();

        assert!(!checker.is_duplicate(b"content"));
        let (is_dup_after_unmark, same_hash) = checker.check_and_mark(b"content");
        assert!(!is_dup_after_unmark);
        assert_eq!(same_hash, hash);
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

    #[test]
    fn test_zero_max_entries_is_normalized() {
        let checker = IdempotencyChecker::new(0);

        checker.mark_processed(b"content1");
        checker.mark_processed(b"content2");
        let (is_dup, _) = checker.check_and_mark(b"content2");

        assert!(is_dup);
        assert_eq!(checker.len(), 1);
        assert!(checker.is_duplicate(b"content2"));
    }

    #[test]
    fn test_persistent_zero_max_entries_is_normalized() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("idempotency.sqlite");

        {
            let checker = IdempotencyChecker::new_persistent(&db_path, 0).unwrap();
            checker.mark_processed(b"content1");
            checker.mark_processed(b"content2");

            assert_eq!(checker.len(), 1);
            assert!(checker.is_duplicate(b"content2"));
        }

        let reloaded = IdempotencyChecker::new_persistent(&db_path, 0).unwrap();
        assert_eq!(reloaded.len(), 1);
        assert!(reloaded.is_duplicate(b"content2"));
    }

    #[test]
    fn test_persistent_unmark_removes_hash_from_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("idempotency.sqlite");
        let hash = {
            let checker = IdempotencyChecker::new_persistent(&db_path, 100).unwrap();
            let (_, hash) = checker.check_and_mark(b"content");

            checker.unmark_hash(&hash).unwrap();

            assert!(!checker.is_duplicate(b"content"));
            hash
        };

        let reloaded = IdempotencyChecker::new_persistent(&db_path, 100).unwrap();

        assert!(!reloaded.is_duplicate(b"content"));
        let (is_dup, reloaded_hash) = reloaded.check_and_mark(b"content");
        assert!(!is_dup);
        assert_eq!(reloaded_hash, hash);
    }

    #[test]
    fn test_persistent_load_keeps_newest_hashes_when_db_exceeds_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("idempotency.sqlite");

        {
            let _checker = IdempotencyChecker::new_persistent(&db_path, 10).unwrap();
        }

        let conn = Connection::open(&db_path).unwrap();
        conn.execute("DELETE FROM processed_hashes", []).unwrap();

        for (content, created_at) in [
            (b"old".as_slice(), 1_i64),
            (b"middle".as_slice(), 2_i64),
            (b"new".as_slice(), 3_i64),
        ] {
            let hash = IdempotencyChecker::hash_content(content);
            conn.execute(
                "INSERT INTO processed_hashes (hash, created_at) VALUES (?, ?)",
                rusqlite::params![hash, created_at],
            )
            .unwrap();
        }
        drop(conn);

        let checker = IdempotencyChecker::new_persistent(&db_path, 2).unwrap();

        assert_eq!(checker.len(), 2);
        assert!(!checker.is_duplicate(b"old"));
        assert!(checker.is_duplicate(b"middle"));
        assert!(checker.is_duplicate(b"new"));

        checker.mark_processed(b"newer");

        assert_eq!(checker.len(), 2);
        assert!(!checker.is_duplicate(b"middle"));
        assert!(checker.is_duplicate(b"new"));
        assert!(checker.is_duplicate(b"newer"));
    }
}
