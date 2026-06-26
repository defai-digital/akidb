//! Storage backend trait and implementations

use crate::{AkiDbError, Result};
use rocksdb::{DB, Options, WriteBatch};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

/// Storage backend trait
pub trait StorageBackend: Send + Sync {
    /// Get a value by key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Check if a key exists
    fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Batch write operations
    fn write_batch(&self, operations: Vec<BatchOperation>) -> Result<()>;

    /// Get all keys with a prefix
    ///
    /// FIX BUG-061: Added optional limit parameter to prevent unbounded memory usage
    /// Use `scan_prefix_limited` for large datasets
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Get keys with a prefix, with optional limit
    ///
    /// FIX BUG-061: Prevents unbounded memory usage on large prefix scans
    /// - limit: Maximum number of results to return (None = unlimited)
    fn scan_prefix_limited(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Default implementation delegates to scan_prefix and truncates
        // Implementations should override for efficiency
        let results = self.scan_prefix(prefix)?;
        match limit {
            Some(n) => Ok(results.into_iter().take(n).collect()),
            None => Ok(results),
        }
    }

    /// Flush to disk
    fn flush(&self) -> Result<()>;
}

/// Batch operation type
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// RocksDB storage backend
pub struct RocksDbBackend {
    db: Arc<DB>,
}

impl RocksDbBackend {
    /// Open a RocksDB database at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening RocksDB at {:?}", path);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(1000);
        opts.set_keep_log_file_num(10);
        opts.set_max_total_wal_size(64 * 1024 * 1024); // 64MB

        // Performance optimizations
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024);

        let db = DB::open(&opts, path).map_err(|e| {
            AkiDbError::StorageError(format!("Failed to open RocksDB: {}", e))
        })?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get the underlying RocksDB handle (for advanced operations)
    pub fn inner(&self) -> &DB {
        &self.db
    }
}

impl StorageBackend for RocksDbBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB get error: {}", e)))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB put error: {}", e)))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB delete error: {}", e)))
    }

    fn exists(&self, key: &[u8]) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    fn write_batch(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let mut batch = WriteBatch::default();

        for op in operations {
            match op {
                BatchOperation::Put { key, value } => {
                    batch.put(&key, &value);
                }
                BatchOperation::Delete { key } => {
                    batch.delete(&key);
                }
            }
        }

        self.db
            .write(batch)
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB batch write error: {}", e)))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // FIX BUG-061: Default limit to 100,000 entries to prevent unbounded memory
        // For truly unlimited scans, use scan_prefix_limited(prefix, None)
        self.scan_prefix_limited(prefix, Some(100_000))
    }

    fn scan_prefix_limited(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // FIX BUG-061: Efficient implementation that stops early when limit is reached
        let mut results = Vec::new();
        let iter = self.db.prefix_iterator(prefix);

        for item in iter {
            // Check limit first to avoid unnecessary processing
            if let Some(max) = limit {
                if results.len() >= max {
                    break;
                }
            }

            match item {
                Ok((key, value)) => {
                    if key.starts_with(prefix) {
                        results.push((key.to_vec(), value.to_vec()));
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    return Err(AkiDbError::StorageError(format!(
                        "RocksDB scan error: {}",
                        e
                    )));
                }
            }
        }

        Ok(results)
    }

    fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| AkiDbError::StorageError(format!("RocksDB flush error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_rocksdb_basic() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();

        // Put and get
        backend.put(b"key1", b"value1").unwrap();
        let value = backend.get(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Exists
        assert!(backend.exists(b"key1").unwrap());
        assert!(!backend.exists(b"key2").unwrap());

        // Delete
        backend.delete(b"key1").unwrap();
        assert!(!backend.exists(b"key1").unwrap());
    }

    #[test]
    fn test_rocksdb_batch() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();

        let ops = vec![
            BatchOperation::Put {
                key: b"batch1".to_vec(),
                value: b"value1".to_vec(),
            },
            BatchOperation::Put {
                key: b"batch2".to_vec(),
                value: b"value2".to_vec(),
            },
        ];

        backend.write_batch(ops).unwrap();

        assert!(backend.exists(b"batch1").unwrap());
        assert!(backend.exists(b"batch2").unwrap());
    }

    #[test]
    fn test_rocksdb_scan_prefix() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();

        backend.put(b"prefix:a", b"1").unwrap();
        backend.put(b"prefix:b", b"2").unwrap();
        backend.put(b"prefix:c", b"3").unwrap();
        backend.put(b"other:x", b"4").unwrap();

        let results = backend.scan_prefix(b"prefix:").unwrap();
        assert_eq!(results.len(), 3);
    }
}
