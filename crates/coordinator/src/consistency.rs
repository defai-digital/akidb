//! Read-your-writes consistency for the coordinator
//!
//! This module tracks recent writes and ensures that reads from the same
//! client (or within a short time window) see those writes.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Entry tracking a recent write
#[derive(Debug, Clone)]
pub struct WriteEntry {
    /// The shard where this write was routed
    pub shard_id: String,
    /// When the write occurred
    pub written_at: Instant,
    /// Write sequence number for ordering
    pub sequence: u64,
    /// Whether the write was confirmed by the shard
    pub confirmed: bool,
}

/// Configuration for the consistency tracker
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// How long to track writes (default: 5 seconds)
    pub write_ttl: Duration,
    /// How often to clean up stale entries (default: 10 seconds)
    pub cleanup_interval: Duration,
    /// Maximum entries to track (default: 100,000)
    pub max_entries: usize,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            write_ttl: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(10),
            max_entries: 100_000,
        }
    }
}

/// Tracks recent writes for read-your-writes consistency
pub struct ConsistencyTracker {
    /// Map of vector ID -> recent write info
    writes: DashMap<String, WriteEntry>,
    /// Monotonically increasing sequence number
    sequence: AtomicU64,
    /// Configuration
    config: ConsistencyConfig,
    /// Last cleanup time
    last_cleanup: parking_lot::Mutex<Instant>,
}

impl ConsistencyTracker {
    /// Create a new consistency tracker with default config
    pub fn new() -> Self {
        Self::with_config(ConsistencyConfig::default())
    }

    /// Create a new consistency tracker with custom config
    pub fn with_config(config: ConsistencyConfig) -> Self {
        Self {
            writes: DashMap::new(),
            sequence: AtomicU64::new(0),
            config,
            last_cleanup: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Record a write for a vector ID
    pub fn record_write(&self, vector_id: &str, shard_id: &str) -> u64 {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        let entry = WriteEntry {
            shard_id: shard_id.to_string(),
            written_at: Instant::now(),
            sequence: seq,
            confirmed: false,
        };

        self.writes.insert(vector_id.to_string(), entry);

        // Maybe cleanup
        self.maybe_cleanup();

        seq
    }

    /// Confirm that a write was successful
    pub fn confirm_write(&self, vector_id: &str) {
        if let Some(mut entry) = self.writes.get_mut(vector_id) {
            entry.confirmed = true;
        }
    }

    /// Record a delete for a vector ID
    pub fn record_delete(&self, vector_id: &str) {
        self.writes.remove(vector_id);
    }

    /// Get the shard where a vector was recently written
    ///
    /// Returns the shard ID if the vector was written within the TTL window,
    /// None otherwise.
    pub fn get_recent_write(&self, vector_id: &str) -> Option<WriteEntry> {
        let entry = self.writes.get(vector_id)?;
        let age = entry.written_at.elapsed();

        if age <= self.config.write_ttl {
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Check if a vector was recently written
    pub fn was_recently_written(&self, vector_id: &str) -> bool {
        self.get_recent_write(vector_id).is_some()
    }

    /// Get all vector IDs recently written to a specific shard
    pub fn get_recent_writes_for_shard(&self, shard_id: &str) -> Vec<String> {
        let now = Instant::now();
        let ttl = self.config.write_ttl;

        self.writes
            .iter()
            .filter(|entry| {
                entry.shard_id == shard_id && now.duration_since(entry.written_at) <= ttl
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get statistics about tracked writes
    pub fn stats(&self) -> ConsistencyStats {
        let now = Instant::now();
        let ttl = self.config.write_ttl;

        let total = self.writes.len();
        let active = self
            .writes
            .iter()
            .filter(|e| now.duration_since(e.written_at) <= ttl)
            .count();
        let confirmed = self
            .writes
            .iter()
            .filter(|e| e.confirmed && now.duration_since(e.written_at) <= ttl)
            .count();

        ConsistencyStats {
            total_tracked: total,
            active_writes: active,
            confirmed_writes: confirmed,
            current_sequence: self.sequence.load(Ordering::SeqCst),
        }
    }

    /// Cleanup stale entries
    fn maybe_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock();
        if last_cleanup.elapsed() < self.config.cleanup_interval {
            return;
        }
        *last_cleanup = Instant::now();
        drop(last_cleanup);

        // FIX BUG-H030: Remove stale entries with fresh timestamp check per entry
        // Previously, `now` was captured once before retain(), creating a TOCTOU race:
        // - A write could happen after `now` was captured but before retain() checked it
        // - If the write's timestamp was near the TTL boundary, it could be incorrectly removed
        // Fix: Use Instant::now() inside the retain closure to get fresh time for each entry
        let ttl = self.config.write_ttl;

        self.writes
            .retain(|_, entry| Instant::now().duration_since(entry.written_at) <= ttl);

        // If still over limit, remove oldest entries
        let current_len = self.writes.len();
        if current_len > self.config.max_entries {
            // Get entries sorted by sequence (oldest first)
            let mut entries: Vec<_> = self
                .writes
                .iter()
                .map(|e| (e.key().clone(), e.sequence))
                .collect();
            entries.sort_by_key(|(_, seq)| *seq);

            // Remove oldest entries until under limit
            // Use saturating_sub to prevent underflow if map size changed
            let to_remove = current_len.saturating_sub(self.config.max_entries);
            // FIX BUG-045: Only remove if sequence hasn't changed (entry wasn't updated)
            // This prevents removing fresh writes that were updated between collection and removal
            for (key, expected_seq) in entries.into_iter().take(to_remove) {
                self.writes
                    .remove_if(&key, |_, entry| entry.sequence == expected_seq);
            }
        }
    }

    /// Force cleanup of all stale entries
    ///
    /// FIX BUG-H030: Use fresh timestamp per entry to avoid TOCTOU race
    pub fn cleanup(&self) {
        let ttl = self.config.write_ttl;
        self.writes
            .retain(|_, entry| Instant::now().duration_since(entry.written_at) <= ttl);
    }
}

impl Default for ConsistencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the consistency tracker
#[derive(Debug, Clone)]
pub struct ConsistencyStats {
    /// Total entries in the tracker
    pub total_tracked: usize,
    /// Entries within the TTL window
    pub active_writes: usize,
    /// Confirmed writes within TTL
    pub confirmed_writes: usize,
    /// Current sequence number
    pub current_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_record_and_retrieve() {
        let tracker = ConsistencyTracker::new();

        tracker.record_write("vec-1", "shard-0");
        tracker.record_write("vec-2", "shard-1");

        assert!(tracker.was_recently_written("vec-1"));
        assert!(tracker.was_recently_written("vec-2"));
        assert!(!tracker.was_recently_written("vec-3"));

        let entry = tracker.get_recent_write("vec-1").unwrap();
        assert_eq!(entry.shard_id, "shard-0");
    }

    #[test]
    fn test_confirm_write() {
        let tracker = ConsistencyTracker::new();

        tracker.record_write("vec-1", "shard-0");
        assert!(!tracker.get_recent_write("vec-1").unwrap().confirmed);

        tracker.confirm_write("vec-1");
        assert!(tracker.get_recent_write("vec-1").unwrap().confirmed);
    }

    #[test]
    fn test_delete_removes_tracking() {
        let tracker = ConsistencyTracker::new();

        tracker.record_write("vec-1", "shard-0");
        assert!(tracker.was_recently_written("vec-1"));

        tracker.record_delete("vec-1");
        assert!(!tracker.was_recently_written("vec-1"));
    }

    #[test]
    fn test_ttl_expiration() {
        let config = ConsistencyConfig {
            write_ttl: Duration::from_millis(50),
            cleanup_interval: Duration::from_millis(10),
            max_entries: 1000,
        };
        let tracker = ConsistencyTracker::with_config(config);

        tracker.record_write("vec-1", "shard-0");
        assert!(tracker.was_recently_written("vec-1"));

        sleep(Duration::from_millis(100));

        assert!(!tracker.was_recently_written("vec-1"));
    }

    #[test]
    fn test_stats() {
        let tracker = ConsistencyTracker::new();

        tracker.record_write("vec-1", "shard-0");
        tracker.record_write("vec-2", "shard-1");
        tracker.confirm_write("vec-1");

        let stats = tracker.stats();
        assert_eq!(stats.total_tracked, 2);
        assert_eq!(stats.active_writes, 2);
        assert_eq!(stats.confirmed_writes, 1);
    }

    #[test]
    fn test_shard_filter() {
        let tracker = ConsistencyTracker::new();

        tracker.record_write("vec-1", "shard-0");
        tracker.record_write("vec-2", "shard-0");
        tracker.record_write("vec-3", "shard-1");

        let shard0_writes = tracker.get_recent_writes_for_shard("shard-0");
        assert_eq!(shard0_writes.len(), 2);
        assert!(shard0_writes.contains(&"vec-1".to_string()));
        assert!(shard0_writes.contains(&"vec-2".to_string()));
    }
}
