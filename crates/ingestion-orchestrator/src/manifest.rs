//! Object Manifest Store
//!
//! Tracks MinIO objects for scheduled ingestion sync.
//! Maintains a manifest of all known objects with their ETags,
//! content hashes, and deletion states for reconciliation.

use std::sync::{Arc, Mutex};

use akidb_storage::{RocksDbBackend, StorageBackend};
use tracing::{debug, info, warn};

use akidb_common::types::{DeleteState, ObjectManifest};
use crate::{IngestionError, Result};

/// Prefix for manifest entries in RocksDB
const MANIFEST_PREFIX: &[u8] = b"manifest:";
/// Key for epoch counter
const EPOCH_KEY: &[u8] = b"manifest_meta:epoch";
/// Legacy epoch key used before metadata was moved outside MANIFEST_PREFIX.
const LEGACY_EPOCH_KEY: &[u8] = b"manifest:_epoch";

/// Object manifest store for tracking MinIO objects
pub struct ManifestStore {
    backend: Arc<RocksDbBackend>,
    /// BUG-002 FIX: Write lock for atomic read-modify-write operations
    write_lock: Mutex<()>,
}

impl ManifestStore {
    /// Open or create a manifest store at the given path
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let backend = RocksDbBackend::open(path).map_err(|e| {
            IngestionError::Manifest(format!("Failed to open manifest DB: {}", e))
        })?;

        info!("Manifest store opened");
        Ok(Self {
            backend: Arc::new(backend),
            write_lock: Mutex::new(()),
        })
    }

    /// Create from an existing RocksDB backend (for sharing)
    pub fn from_backend(backend: Arc<RocksDbBackend>) -> Self {
        Self {
            backend,
            write_lock: Mutex::new(()),
        }
    }

    /// Build the key for a manifest entry
    fn manifest_key(object_key: &str) -> Vec<u8> {
        let mut key = MANIFEST_PREFIX.to_vec();
        key.extend_from_slice(object_key.as_bytes());
        key
    }

    /// Get a manifest entry by MinIO object key
    pub fn get(&self, object_key: &str) -> Result<Option<ObjectManifest>> {
        let key = Self::manifest_key(object_key);
        match self.backend.get(&key) {
            Ok(Some(data)) => {
                let manifest: ObjectManifest = serde_json::from_slice(&data).map_err(|e| {
                    IngestionError::Manifest(format!("Failed to deserialize manifest: {}", e))
                })?;
                Ok(Some(manifest))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(IngestionError::Manifest(format!(
                "Failed to get manifest: {}",
                e
            ))),
        }
    }

    /// Internal upsert - assumes lock is already held
    /// Used by methods that already acquired the write_lock
    fn upsert_internal(&self, manifest: &ObjectManifest) -> Result<()> {
        let key = Self::manifest_key(&manifest.key);
        let data = serde_json::to_vec(manifest).map_err(|e| {
            IngestionError::Manifest(format!("Failed to serialize manifest: {}", e))
        })?;

        self.backend.put(&key, &data).map_err(|e| {
            IngestionError::Manifest(format!("Failed to write manifest: {}", e))
        })?;

        debug!(key = %manifest.key, "Manifest entry upserted");
        Ok(())
    }

    /// Insert or update a manifest entry
    ///
    /// BUG-H004 FIX: Uses write lock for thread safety
    pub fn upsert(&self, manifest: &ObjectManifest) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|e| {
            IngestionError::Manifest(format!("Write lock poisoned: {}", e))
        })?;
        self.upsert_internal(manifest)
    }

    /// Delete a manifest entry
    ///
    /// BUG-H004 FIX: Uses write lock for thread safety
    pub fn delete(&self, object_key: &str) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|e| {
            IngestionError::Manifest(format!("Write lock poisoned: {}", e))
        })?;

        let key = Self::manifest_key(object_key);
        self.backend.delete(&key).map_err(|e| {
            IngestionError::Manifest(format!("Failed to delete manifest: {}", e))
        })?;

        debug!(key = %object_key, "Manifest entry deleted");
        Ok(())
    }

    /// Increment missing count for an object and return new count
    ///
    /// BUG-002 FIX: Uses write lock for atomic read-modify-write
    pub fn increment_missing(&self, object_key: &str) -> Result<u8> {
        self.increment_missing_with_threshold(object_key, akidb_common::types::DELETION_THRESHOLD)
    }

    /// Increment missing count using a caller-supplied confirmation threshold.
    ///
    /// BUG-002 FIX: Uses write lock for atomic read-modify-write
    pub fn increment_missing_with_threshold(
        &self,
        object_key: &str,
        deletion_threshold: u8,
    ) -> Result<u8> {
        // Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            IngestionError::Manifest(format!("Write lock poisoned: {}", e))
        })?;

        let mut manifest = self.get(object_key)?.ok_or_else(|| {
            IngestionError::Manifest(format!("Manifest not found: {}", object_key))
        })?;

        let should_tombstone = manifest.increment_missing_with_threshold(deletion_threshold);
        self.upsert_internal(&manifest)?;

        if should_tombstone {
            info!(key = %object_key, count = manifest.missing_count, "Object confirmed deleted");
        }

        Ok(manifest.missing_count)
    }

    /// Mark an object as seen in the current sync cycle
    ///
    /// BUG-002/BUG-009 FIX: Uses write lock and returns error for missing manifests
    pub fn mark_seen(&self, object_key: &str, epoch: u64) -> Result<bool> {
        // Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            IngestionError::Manifest(format!("Write lock poisoned: {}", e))
        })?;

        if let Some(mut manifest) = self.get(object_key)? {
            manifest.mark_seen(epoch);
            self.upsert_internal(&manifest)?;
            Ok(true)
        } else {
            // BUG-009 FIX: Return false instead of silently succeeding
            warn!(key = %object_key, "mark_seen called for missing manifest");
            Ok(false)
        }
    }

    /// Get current epoch (sync cycle counter)
    pub fn current_epoch(&self) -> Result<u64> {
        match self.backend.get(EPOCH_KEY) {
            Ok(Some(data)) => decode_epoch(&data),
            Ok(None) => self.current_legacy_epoch(),
            Err(e) => Err(IngestionError::Manifest(format!(
                "Failed to get epoch: {}",
                e
            ))),
        }
    }

    fn current_legacy_epoch(&self) -> Result<u64> {
        match self.backend.get(LEGACY_EPOCH_KEY) {
            Ok(Some(data)) if is_epoch_bytes(&data) => decode_epoch(&data),
            Ok(Some(data)) if is_manifest_for_key(&data, "_epoch") => Ok(0),
            Ok(Some(_)) => Err(IngestionError::Manifest("Invalid epoch data".to_string())),
            Ok(None) => Ok(0),
            Err(e) => Err(IngestionError::Manifest(format!(
                "Failed to get legacy epoch: {}",
                e
            ))),
        }
    }

    /// Increment and return the new epoch
    ///
    /// BUG-002 FIX: Uses write lock for atomic read-modify-write
    pub fn increment_epoch(&self) -> Result<u64> {
        // Acquire write lock for atomic read-modify-write
        let _guard = self.write_lock.lock().map_err(|e| {
            IngestionError::Manifest(format!("Write lock poisoned: {}", e))
        })?;

        let epoch = self.current_epoch()? + 1;
        self.backend
            .put(EPOCH_KEY, &epoch.to_le_bytes())
            .map_err(|e| IngestionError::Manifest(format!("Failed to update epoch: {}", e)))?;
        if matches!(self.backend.get(LEGACY_EPOCH_KEY), Ok(Some(data)) if is_epoch_bytes(&data)) {
            self.backend.delete(LEGACY_EPOCH_KEY).map_err(|e| {
                IngestionError::Manifest(format!("Failed to delete legacy epoch: {}", e))
            })?;
        }
        debug!(epoch, "Epoch incremented");
        Ok(epoch)
    }

    /// Scan all manifest entries
    fn scan_all(&self) -> Result<Vec<ObjectManifest>> {
        let entries = self.backend.scan_prefix(MANIFEST_PREFIX).map_err(|e| {
            IngestionError::Manifest(format!("Failed to scan manifests: {}", e))
        })?;

        let mut manifests = Vec::new();
        for (key, value) in entries {
            // Skip legacy metadata only when it is actually an encoded epoch.
            if key.as_slice() == LEGACY_EPOCH_KEY && is_epoch_bytes(&value) {
                continue;
            }

            match serde_json::from_slice::<ObjectManifest>(&value) {
                Ok(manifest) => manifests.push(manifest),
                Err(e) => {
                    warn!(?e, "Failed to deserialize manifest entry");
                }
            }
        }

        Ok(manifests)
    }

    /// List all confirmed deletes (ready for tombstoning)
    pub fn list_confirmed_deletes(&self) -> Result<Vec<ObjectManifest>> {
        let manifests = self.scan_all()?;
        Ok(manifests
            .into_iter()
            .filter(|m| matches!(m.delete_state, DeleteState::ConfirmedMissing { .. }))
            .collect())
    }

    /// List all manifests ready for hard delete
    pub fn list_hard_delete_ready(&self) -> Result<Vec<ObjectManifest>> {
        let manifests = self.scan_all()?;
        Ok(manifests
            .into_iter()
            .filter(|m| m.delete_state.is_ready_for_hard_delete())
            .collect())
    }

    /// Get all manifests (for change detection)
    pub fn list_all(&self) -> Result<Vec<ObjectManifest>> {
        self.scan_all()
    }

    /// Get statistics about the manifest store
    pub fn stats(&self) -> Result<ManifestStats> {
        let manifests = self.scan_all()?;
        let mut stats = ManifestStats::default();

        for m in manifests {
            stats.total += 1;
            match m.delete_state {
                DeleteState::Active => stats.active += 1,
                DeleteState::MarkedForDeletion { .. } => stats.marked += 1,
                DeleteState::ConfirmedMissing { .. } => stats.confirmed += 1,
                DeleteState::HardDeleteScheduled { .. } => stats.scheduled += 1,
            }
        }

        stats.epoch = self.current_epoch()?;
        Ok(stats)
    }
}

fn decode_epoch(data: &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        data.try_into()
            .map_err(|_| IngestionError::Manifest("Invalid epoch data".to_string()))?,
    ))
}

fn is_epoch_bytes(data: &[u8]) -> bool {
    data.len() == std::mem::size_of::<u64>()
}

fn is_manifest_for_key(data: &[u8], key: &str) -> bool {
    serde_json::from_slice::<ObjectManifest>(data)
        .map(|manifest| manifest.key == key)
        .unwrap_or(false)
}

/// Manifest store statistics
#[derive(Debug, Clone, Default)]
pub struct ManifestStats {
    pub total: u64,
    pub active: u64,
    pub marked: u64,
    pub confirmed: u64,
    pub scheduled: u64,
    pub epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_common::types::DocumentIdentifier;
    use tempfile::tempdir;

    fn create_test_manifest(key: &str) -> ObjectManifest {
        let doc_id = DocumentIdentifier::new(b"test content", key.to_string());
        ObjectManifest::new(key.to_string(), "etag123".to_string(), doc_id)
    }

    #[test]
    fn test_manifest_crud() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        let retrieved = store.get("test/file.pdf").unwrap().unwrap();
        assert_eq!(retrieved.key, "test/file.pdf");
        assert_eq!(retrieved.etag, "etag123");

        store.delete("test/file.pdf").unwrap();
        assert!(store.get("test/file.pdf").unwrap().is_none());
    }

    #[test]
    fn test_manifest_epoch() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        assert_eq!(store.current_epoch().unwrap(), 0);

        let epoch1 = store.increment_epoch().unwrap();
        assert_eq!(epoch1, 1);

        let epoch2 = store.increment_epoch().unwrap();
        assert_eq!(epoch2, 2);
    }

    #[test]
    fn test_manifest_object_named_epoch_does_not_collide_with_epoch_counter() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        assert_eq!(store.increment_epoch().unwrap(), 1);

        let manifest = create_test_manifest("_epoch");
        store.upsert(&manifest).unwrap();

        assert_eq!(store.current_epoch().unwrap(), 1);
        assert_eq!(store.get("_epoch").unwrap().unwrap().key, "_epoch");

        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].key, "_epoch");

        let stats = store.stats().unwrap();
        assert_eq!(stats.total, 1);
        assert_eq!(stats.epoch, 1);
    }

    #[test]
    fn test_manifest_reads_and_migrates_legacy_epoch_key() {
        let dir = tempdir().unwrap();
        let backend = Arc::new(RocksDbBackend::open(dir.path()).unwrap());
        backend.put(LEGACY_EPOCH_KEY, &7u64.to_le_bytes()).unwrap();
        let store = ManifestStore::from_backend(Arc::clone(&backend));

        assert_eq!(store.current_epoch().unwrap(), 7);
        assert_eq!(store.increment_epoch().unwrap(), 8);
        assert_eq!(store.current_epoch().unwrap(), 8);
        assert!(backend.get(LEGACY_EPOCH_KEY).unwrap().is_none());
    }

    #[test]
    fn test_manifest_missing_count() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        // Increment missing count
        let count1 = store.increment_missing("test/file.pdf").unwrap();
        assert_eq!(count1, 1);

        let count2 = store.increment_missing("test/file.pdf").unwrap();
        assert_eq!(count2, 2);

        let count3 = store.increment_missing("test/file.pdf").unwrap();
        assert_eq!(count3, 3);

        // Should now be confirmed deleted
        let manifest = store.get("test/file.pdf").unwrap().unwrap();
        assert!(matches!(
            manifest.delete_state,
            DeleteState::ConfirmedMissing { .. }
        ));
    }

    #[test]
    fn test_increment_missing_uses_custom_threshold() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        let manifest = create_test_manifest("test/file.pdf");
        store.upsert(&manifest).unwrap();

        for expected_count in 1..=4 {
            let count = store
                .increment_missing_with_threshold("test/file.pdf", 5)
                .unwrap();
            assert_eq!(count, expected_count);
            let manifest = store.get("test/file.pdf").unwrap().unwrap();
            assert!(matches!(
                manifest.delete_state,
                DeleteState::MarkedForDeletion { .. }
            ));
        }

        let count = store
            .increment_missing_with_threshold("test/file.pdf", 5)
            .unwrap();
        assert_eq!(count, 5);
        let manifest = store.get("test/file.pdf").unwrap().unwrap();
        assert!(matches!(
            manifest.delete_state,
            DeleteState::ConfirmedMissing { .. }
        ));
    }

    #[test]
    fn test_list_confirmed_deletes() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        // Create some manifests in different states
        let m1 = create_test_manifest("active.pdf");
        store.upsert(&m1).unwrap();

        let m2 = create_test_manifest("delete.pdf");
        store.upsert(&m2).unwrap();

        // Mark for deletion until confirmed
        store.increment_missing("delete.pdf").unwrap();
        store.increment_missing("delete.pdf").unwrap();
        store.increment_missing("delete.pdf").unwrap();

        let deletes = store.list_confirmed_deletes().unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].key, "delete.pdf");
    }

    #[test]
    fn test_manifest_list_all() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        for i in 0..5 {
            let manifest = create_test_manifest(&format!("file{}.pdf", i));
            store.upsert(&manifest).unwrap();
        }

        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_manifest_stats() {
        let dir = tempdir().unwrap();
        let store = ManifestStore::open(dir.path()).unwrap();

        // Create manifests in different states
        store.upsert(&create_test_manifest("a.pdf")).unwrap();
        store.upsert(&create_test_manifest("b.pdf")).unwrap();
        store.upsert(&create_test_manifest("c.pdf")).unwrap();

        // Mark one for deletion
        store.increment_missing("b.pdf").unwrap();

        // Confirm delete another
        store.increment_missing("c.pdf").unwrap();
        store.increment_missing("c.pdf").unwrap();
        store.increment_missing("c.pdf").unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.active, 1);
        assert_eq!(stats.marked, 1);
        assert_eq!(stats.confirmed, 1);
    }
}
