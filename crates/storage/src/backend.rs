//! Storage backend trait and implementations

use crate::{AkiDbError, Result};
use rocksdb::{
    checkpoint::Checkpoint, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

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

    /// Atomically write a batch and synchronously persist its WAL record.
    ///
    /// Implementations that provide a native synced batch must override this
    /// method. The default is safe for test/simple backends but cannot make
    /// `write_batch` plus `flush` one storage-engine acknowledgement boundary.
    fn write_batch_sync(&self, operations: Vec<BatchOperation>) -> Result<()> {
        self.write_batch(operations)?;
        self.flush()
    }

    /// Get all keys with a prefix
    ///
    /// FIX BUG-061: Added optional limit parameter to prevent unbounded memory usage
    /// Use `scan_prefix_limited` for large datasets
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Get keys with a prefix, with optional limit
    ///
    /// FIX BUG-061: Prevents unbounded memory usage on large prefix scans
    /// - limit: Maximum number of results to return (None = unlimited)
    fn scan_prefix_limited(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Default implementation delegates to scan_prefix and truncates
        // Implementations should override for efficiency
        let results = self.scan_prefix(prefix)?;
        match limit {
            Some(n) => Ok(results.into_iter().take(n).collect()),
            None => Ok(results),
        }
    }

    /// Get keys under `prefix` ordered strictly after `start_exclusive`.
    ///
    /// The default preserves correctness for simple/test backends. Persistent
    /// engines should override this with an ordered seek so incremental
    /// consumers do not repeatedly scan an entire historical prefix.
    fn scan_prefix_from_limited(
        &self,
        prefix: &[u8],
        start_exclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .scan_prefix_limited(prefix, None)?
            .into_iter()
            .filter(|(key, _)| key.as_slice() > start_exclusive)
            .take(limit)
            .collect())
    }

    /// Count keys with a prefix without requiring callers to retain values.
    ///
    /// Backends should override this to stream over their native iterator.
    fn count_prefix(&self, prefix: &[u8]) -> Result<u64> {
        u64::try_from(self.scan_prefix_limited(prefix, None)?.len())
            .map_err(|_| AkiDbError::StorageError("prefix key count cannot fit in u64".to_string()))
    }

    /// Flush to disk
    fn flush(&self) -> Result<()>;
}

/// Batch operation type
#[derive(Debug, Clone, PartialEq, Eq)]
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

        let db = DB::open(&opts, path)
            .map_err(|e| AkiDbError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get the underlying RocksDB handle (for advanced operations)
    pub fn inner(&self) -> &DB {
        &self.db
    }

    /// Create a consistent RocksDB checkpoint suitable for a shadow
    /// generation revision. RocksDB may hard-link immutable SST files, while
    /// subsequent writes create independent manifests/WALs in the target.
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.flush()?;
        let checkpoint = Checkpoint::new(self.inner()).map_err(|error| {
            AkiDbError::StorageError(format!(
                "Failed to create RocksDB checkpoint handle: {error}"
            ))
        })?;
        checkpoint.create_checkpoint(path).map_err(|error| {
            AkiDbError::StorageError(format!("Failed to create RocksDB checkpoint: {error}"))
        })
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

    fn write_batch_sync(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for operation in operations {
            match operation {
                BatchOperation::Put { key, value } => batch.put(key, value),
                BatchOperation::Delete { key } => batch.delete(key),
            }
        }

        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        self.db.write_opt(batch, &options).map_err(|error| {
            AkiDbError::StorageError(format!("RocksDB synced batch write error: {error}"))
        })
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // FIX BUG-061: Default limit to 100,000 entries to prevent unbounded memory
        // For truly unlimited scans, use scan_prefix_limited(prefix, None)
        self.scan_prefix_limited(prefix, Some(100_000))
    }

    fn scan_prefix_limited(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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

    fn scan_prefix_from_limited(
        &self,
        prefix: &[u8],
        start_exclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut results = Vec::with_capacity(limit.min(1024));
        let iter = self
            .db
            .iterator(IteratorMode::From(start_exclusive, Direction::Forward));
        for item in iter {
            let (key, value) = item.map_err(|error| {
                AkiDbError::StorageError(format!("RocksDB range scan error: {error}"))
            })?;
            if !key.starts_with(prefix) {
                break;
            }
            if key.as_ref() == start_exclusive {
                continue;
            }
            results.push((key.to_vec(), value.to_vec()));
            if results.len() == limit {
                break;
            }
        }
        Ok(results)
    }

    fn count_prefix(&self, prefix: &[u8]) -> Result<u64> {
        let mut count = 0_u64;
        for item in self.db.prefix_iterator(prefix) {
            match item {
                Ok((key, _)) if key.starts_with(prefix) => {
                    count = count.checked_add(1).ok_or_else(|| {
                        AkiDbError::StorageError("prefix key count overflowed u64".to_string())
                    })?;
                }
                Ok(_) => break,
                Err(error) => {
                    return Err(AkiDbError::StorageError(format!(
                        "RocksDB prefix count error: {error}"
                    )));
                }
            }
        }
        Ok(count)
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
    fn test_rocksdb_synced_batch() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();

        backend
            .write_batch_sync(vec![
                BatchOperation::Put {
                    key: b"sync1".to_vec(),
                    value: b"value1".to_vec(),
                },
                BatchOperation::Put {
                    key: b"sync2".to_vec(),
                    value: b"value2".to_vec(),
                },
            ])
            .unwrap();

        assert_eq!(backend.get(b"sync1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(backend.get(b"sync2").unwrap(), Some(b"value2".to_vec()));
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
        assert_eq!(backend.count_prefix(b"prefix:").unwrap(), 3);
        assert_eq!(backend.count_prefix(b"missing:").unwrap(), 0);
    }

    #[test]
    fn test_rocksdb_scan_prefix_from_is_exclusive_and_bounded() {
        let dir = tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path()).unwrap();
        for suffix in [b"a", b"b", b"c", b"d"] {
            let mut key = b"ordered:".to_vec();
            key.extend_from_slice(suffix);
            backend.put(&key, suffix).unwrap();
        }
        backend.put(b"other:z", b"z").unwrap();

        let results = backend
            .scan_prefix_from_limited(b"ordered:", b"ordered:b", 2)
            .unwrap();
        assert_eq!(
            results.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![b"ordered:c".to_vec(), b"ordered:d".to_vec()]
        );

        let from_missing = backend
            .scan_prefix_from_limited(b"ordered:", b"ordered:bb", 1)
            .unwrap();
        assert_eq!(from_missing[0].0, b"ordered:c");
        assert!(backend
            .scan_prefix_from_limited(b"ordered:", b"ordered:a", 0)
            .unwrap()
            .is_empty());
    }
}
