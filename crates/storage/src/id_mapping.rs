//! ID mapping between external and internal IDs

use crate::{backend::BatchOperation, backend::StorageBackend, AkiDbError, Result};
use akidb_common::{InternalId, VectorId};
use akidb_invariants::debug_invariant;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::debug;

/// FIX BUG-HUNT-001: Number of lock stripes for concurrent access
/// Using 256 stripes provides good parallelism while bounded memory usage
const LOCK_STRIPE_COUNT: usize = 256;

/// Entry in the ID mapping table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdMappingEntry {
    /// Internal FAISS index ID
    pub internal_id: i64,
    /// Creation timestamp
    pub created_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
    /// Whether this ID has been deleted
    pub deleted: bool,
    /// Deletion timestamp (if deleted)
    pub deleted_at: Option<u64>,
}

/// Durable vector payload stored alongside the ID mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVectorEntry {
    /// External vector ID
    pub external_id: String,
    /// Internal index ID
    pub internal_id: i64,
    /// Vector embedding
    pub vector: Vec<f32>,
    /// Original request metadata
    pub metadata: Vec<u8>,
    /// Creation timestamp
    pub created_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
    /// Whether this vector has been deleted
    pub deleted: bool,
}

impl StoredVectorEntry {
    fn new(
        external_id: &VectorId,
        internal_id: InternalId,
        vector: &[f32],
        metadata: &[u8],
    ) -> Self {
        let now = current_timestamp_ms();

        Self {
            external_id: external_id.as_str().to_string(),
            internal_id: internal_id.0,
            vector: vector.to_vec(),
            metadata: metadata.to_vec(),
            created_at: now,
            updated_at: now,
            deleted: false,
        }
    }

    fn mark_deleted(&mut self) {
        self.deleted = true;
        self.updated_at = current_timestamp_ms();
    }
}

impl IdMappingEntry {
    /// FIX BUG-038: Use unwrap_or_default to handle pre-epoch clock gracefully
    pub fn new(internal_id: i64) -> Self {
        let now = current_timestamp_ms();

        Self {
            internal_id,
            created_at: now,
            updated_at: now,
            deleted: false,
            deleted_at: None,
        }
    }

    /// FIX BUG-038: Use unwrap_or_default to handle pre-epoch clock gracefully
    pub fn mark_deleted(&mut self) {
        let now = current_timestamp_ms();

        self.deleted = true;
        self.deleted_at = Some(now);
        self.updated_at = now;
    }
}

const ID_MAPPING_PREFIX: &[u8] = b"id:";
const VECTOR_PREFIX: &[u8] = b"vec:";
const TEXT_PREFIX: &[u8] = b"txt:";

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// ID mapping manager
pub struct IdMapping<S: StorageBackend> {
    storage: Arc<S>,
    collection: String,
    /// FIX BUG-HUNT-001: Striped locks to prevent TOCTOU race in create/update/delete
    /// Each stripe is a Mutex<()> - we only use it for synchronization, not data
    lock_stripes: Arc<[Mutex<()>; LOCK_STRIPE_COUNT]>,
}

impl<S: StorageBackend> IdMapping<S> {
    /// Create a new ID mapping manager
    pub fn new(storage: Arc<S>, collection: impl Into<String>) -> Self {
        // FIX BUG-HUNT-001: Initialize lock stripes
        // Using array initialization with const fn would be cleaner but Mutex::new isn't const
        let stripes: [Mutex<()>; LOCK_STRIPE_COUNT] = std::array::from_fn(|_| Mutex::new(()));
        Self {
            storage,
            collection: collection.into(),
            lock_stripes: Arc::new(stripes),
        }
    }

    /// FIX BUG-HUNT-001: Hash external ID to get stripe index
    /// Uses FNV-1a for fast, well-distributed hashing
    fn stripe_index(&self, external_id: &VectorId) -> usize {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET_BASIS;
        // Include collection in hash to ensure different collections use different stripes
        for byte in self.collection.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        for byte in external_id.as_str().as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        (hash as usize) % LOCK_STRIPE_COUNT
    }

    /// Build the storage key for an external ID
    ///
    /// FIX BUG-HUNT-004: Use length-prefixed encoding to prevent key collisions.
    /// Previously used colon separator which allowed collisions:
    ///   collection="foo", id="bar:baz" -> "id:foo:bar:baz"
    ///   collection="foo:bar", id="baz" -> "id:foo:bar:baz"  (COLLISION!)
    /// Now uses length prefix:
    ///   collection="foo", id="bar:baz" -> "id:3:foobar:baz"
    ///   collection="foo:bar", id="baz" -> "id:7:foo:barbaz" (no collision)
    fn make_key_with_prefix(&self, prefix: &[u8], external_id: &VectorId) -> Vec<u8> {
        let collection_len = self.collection.len();
        let len_str = collection_len.to_string();
        let mut key = Vec::with_capacity(
            prefix.len() + len_str.len() + 1 + collection_len + external_id.as_str().len(),
        );
        key.extend_from_slice(prefix);
        key.extend_from_slice(len_str.as_bytes());
        key.push(b':');
        key.extend_from_slice(self.collection.as_bytes());
        key.extend_from_slice(external_id.as_str().as_bytes());
        key
    }

    fn make_collection_prefix(&self, prefix: &[u8]) -> Vec<u8> {
        let collection_len = self.collection.len();
        let len_str = collection_len.to_string();
        let mut key = Vec::with_capacity(prefix.len() + len_str.len() + 1 + collection_len);
        key.extend_from_slice(prefix);
        key.extend_from_slice(len_str.as_bytes());
        key.push(b':');
        key.extend_from_slice(self.collection.as_bytes());
        key
    }

    fn make_key(&self, external_id: &VectorId) -> Vec<u8> {
        self.make_key_with_prefix(ID_MAPPING_PREFIX, external_id)
    }

    fn make_vector_key(&self, external_id: &VectorId) -> Vec<u8> {
        self.make_key_with_prefix(VECTOR_PREFIX, external_id)
    }

    fn make_text_key(&self, external_id: &VectorId) -> Vec<u8> {
        self.make_key_with_prefix(TEXT_PREFIX, external_id)
    }

    /// Persist the source text for a vector, used to rebuild the lexical (BM25)
    /// index and document store on startup. Stored under a separate key
    /// namespace so it does not affect the vector-payload schema.
    pub fn store_text(&self, external_id: &VectorId, text: &str) -> Result<()> {
        self.storage
            .put(&self.make_text_key(external_id), text.as_bytes())
    }

    /// Delete the persisted source text for a vector.
    pub fn delete_text(&self, external_id: &VectorId) -> Result<()> {
        self.storage.delete(&self.make_text_key(external_id))
    }

    /// Load all persisted (id, text) pairs for this collection, for rebuilding
    /// the in-memory lexical index and document store after a restart.
    pub fn load_all_texts(&self) -> Result<Vec<(VectorId, String)>> {
        let prefix = self.make_collection_prefix(TEXT_PREFIX);
        let entries = self.storage.scan_prefix_limited(&prefix, None)?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            // The id is the key remainder after the collection prefix (see
            // make_key_with_prefix: prefix + collection-prefix + id).
            if key.len() <= prefix.len() {
                continue;
            }
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|e| {
                    AkiDbError::SerializationError(format!(
                        "Invalid UTF-8 in persisted text vector id: {}",
                        e
                    ))
                })?
                .to_string();
            if id.is_empty() {
                continue;
            }
            let text = std::str::from_utf8(&value)
                .map_err(|e| {
                    AkiDbError::SerializationError(format!(
                        "Invalid UTF-8 in persisted text payload for {}: {}",
                        id, e
                    ))
                })?
                .to_string();
            out.push((VectorId::new(id), text));
        }
        Ok(out)
    }

    /// Count durable vector payload entries without loading their values.
    pub fn stored_vector_count(&self) -> Result<u64> {
        self.storage
            .count_prefix(&self.make_collection_prefix(VECTOR_PREFIX))
    }

    /// Count durable source-text entries without loading their values.
    pub fn stored_text_count(&self) -> Result<u64> {
        self.storage
            .count_prefix(&self.make_collection_prefix(TEXT_PREFIX))
    }

    fn serialize_mapping(entry: &IdMappingEntry) -> Result<Vec<u8>> {
        bincode::serialize(entry).map_err(|e| AkiDbError::SerializationError(e.to_string()))
    }

    fn serialize_vector(entry: &StoredVectorEntry) -> Result<Vec<u8>> {
        bincode::serialize(entry).map_err(|e| AkiDbError::SerializationError(e.to_string()))
    }

    /// Get the mapping entry for an external ID
    pub fn get(&self, external_id: &VectorId) -> Result<Option<IdMappingEntry>> {
        let key = self.make_key(external_id);
        match self.storage.get(&key)? {
            Some(data) => {
                let entry: IdMappingEntry = bincode::deserialize(&data)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Create a new mapping (fails if ID already exists and is not deleted)
    ///
    /// FIX BUG-HUNT-001: Uses striped locking to prevent TOCTOU race.
    /// The stripe lock ensures that concurrent creates for the same external_id
    /// are serialized, preventing orphan internal_ids.
    pub fn create(
        &self,
        external_id: &VectorId,
        internal_id: InternalId,
    ) -> Result<IdMappingEntry> {
        // FIX BUG-HUNT-001: Acquire stripe lock to prevent TOCTOU race
        let stripe_idx = self.stripe_index(external_id);
        let _lock = self.lock_stripes[stripe_idx].lock();

        let key = self.make_key(external_id);

        // Check if exists (now safe under lock)
        if let Some(existing) = self.get(external_id)? {
            if existing.deleted {
                // ID was deleted, cannot reuse
                return Err(AkiDbError::IdReuseForbidden(external_id.to_string()));
            } else {
                // ID exists and is active
                return Err(AkiDbError::VectorAlreadyExists(external_id.to_string()));
            }
        }

        let entry = IdMappingEntry::new(internal_id.0);
        let data = Self::serialize_mapping(&entry)?;

        self.storage.put(&key, &data)?;

        debug!("Created ID mapping: {} -> {}", external_id, internal_id.0);

        // INVARIANT: ID mapping bijectivity - verify round-trip
        // After create, get_internal_id should return the same internal_id
        debug_invariant!(
            self.get_internal_id(external_id)
                .ok()
                .flatten()
                .map(|id| id.0 == internal_id.0)
                .unwrap_or(false),
            "ID mapping bijectivity violated: created {} -> {} but lookup failed",
            external_id,
            internal_id.0
        );

        Ok(entry)
    }

    /// Update an existing mapping (for upsert)
    ///
    /// FIX BUG-HUNT-001: Uses striped locking to prevent TOCTOU race.
    /// The stripe lock ensures that concurrent updates for the same external_id
    /// are serialized.
    pub fn update(
        &self,
        external_id: &VectorId,
        new_internal_id: InternalId,
    ) -> Result<IdMappingEntry> {
        // FIX BUG-HUNT-001: Acquire stripe lock to prevent TOCTOU race
        let stripe_idx = self.stripe_index(external_id);
        let _lock = self.lock_stripes[stripe_idx].lock();

        let key = self.make_key(external_id);

        let mut entry = match self.get(external_id)? {
            Some(e) => {
                if e.deleted {
                    return Err(AkiDbError::IdReuseForbidden(external_id.to_string()));
                }
                e
            }
            None => {
                // Create new if doesn't exist (already under lock, call internal method)
                return self.create_internal(external_id, new_internal_id, &key);
            }
        };

        entry.internal_id = new_internal_id.0;
        // FIX BUG-038: Use unwrap_or_default to handle pre-epoch clock gracefully
        entry.updated_at = current_timestamp_ms();

        let data = Self::serialize_mapping(&entry)?;

        self.storage.put(&key, &data)?;

        debug!(
            "Updated ID mapping: {} -> {}",
            external_id, new_internal_id.0
        );

        Ok(entry)
    }

    /// Internal create method (assumes lock is already held)
    fn create_internal(
        &self,
        external_id: &VectorId,
        internal_id: InternalId,
        key: &[u8],
    ) -> Result<IdMappingEntry> {
        let entry = IdMappingEntry::new(internal_id.0);
        let data = Self::serialize_mapping(&entry)?;

        self.storage.put(key, &data)?;

        debug!("Created ID mapping: {} -> {}", external_id, internal_id.0);

        Ok(entry)
    }

    /// Mark an ID as deleted
    ///
    /// FIX BUG-HUNT-001: Uses striped locking to prevent TOCTOU race.
    /// The stripe lock ensures that concurrent deletes/updates for the same
    /// external_id are serialized.
    pub fn mark_deleted(&self, external_id: &VectorId) -> Result<Option<InternalId>> {
        // FIX BUG-HUNT-001: Acquire stripe lock to prevent TOCTOU race
        let stripe_idx = self.stripe_index(external_id);
        let _lock = self.lock_stripes[stripe_idx].lock();

        let key = self.make_key(external_id);

        let mut entry = match self.get(external_id)? {
            Some(e) => e,
            None => return Ok(None), // Not found, nothing to delete
        };

        if entry.deleted {
            // Already deleted
            return Ok(None);
        }

        let internal_id = entry.internal_id;
        entry.mark_deleted();

        let data = Self::serialize_mapping(&entry)?;
        let vector_key = self.make_vector_key(external_id);
        let vector_update = self
            .storage
            .get(&vector_key)?
            .map(|data| {
                bincode::deserialize::<StoredVectorEntry>(&data)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))
            })
            .transpose()?;

        let mut operations = vec![BatchOperation::Put { key, value: data }];
        if let Some(mut vector_entry) = vector_update {
            vector_entry.mark_deleted();
            operations.push(BatchOperation::Put {
                key: vector_key,
                value: Self::serialize_vector(&vector_entry)?,
            });
        }

        self.storage.write_batch(operations)?;

        debug!(
            "Marked ID as deleted: {} (internal: {})",
            external_id, internal_id
        );

        Ok(Some(InternalId(internal_id)))
    }

    /// Get internal ID for an external ID (returns None if deleted or not found)
    pub fn get_internal_id(&self, external_id: &VectorId) -> Result<Option<InternalId>> {
        match self.get(external_id)? {
            Some(entry) if !entry.deleted => Ok(Some(InternalId(entry.internal_id))),
            _ => Ok(None),
        }
    }

    /// Create or update an ID mapping and durable vector payload atomically.
    pub fn upsert_with_vector(
        &self,
        external_id: &VectorId,
        internal_id: InternalId,
        vector: &[f32],
        metadata: &[u8],
    ) -> Result<IdMappingEntry> {
        let stripe_idx = self.stripe_index(external_id);
        let _lock = self.lock_stripes[stripe_idx].lock();

        let mapping_key = self.make_key(external_id);
        let vector_key = self.make_vector_key(external_id);
        let now = current_timestamp_ms();

        let entry = match self.storage.get(&mapping_key)? {
            Some(data) => {
                let mut entry: IdMappingEntry = bincode::deserialize(&data)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
                if entry.deleted {
                    return Err(AkiDbError::IdReuseForbidden(external_id.to_string()));
                }
                entry.internal_id = internal_id.0;
                entry.updated_at = now;
                entry
            }
            None => IdMappingEntry::new(internal_id.0),
        };

        let stored_vector = StoredVectorEntry {
            updated_at: now,
            ..StoredVectorEntry::new(external_id, internal_id, vector, metadata)
        };

        self.storage.write_batch(vec![
            BatchOperation::Put {
                key: mapping_key,
                value: Self::serialize_mapping(&entry)?,
            },
            BatchOperation::Put {
                key: vector_key,
                value: Self::serialize_vector(&stored_vector)?,
            },
        ])?;

        Ok(entry)
    }

    /// Get a durable vector payload for an active external ID.
    pub fn get_vector(&self, external_id: &VectorId) -> Result<Option<StoredVectorEntry>> {
        if self.get_internal_id(external_id)?.is_none() {
            return Ok(None);
        }

        let key = self.make_vector_key(external_id);
        match self.storage.get(&key)? {
            Some(data) => {
                let entry: StoredVectorEntry = bincode::deserialize(&data)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
                if entry.deleted {
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
            None => Ok(None),
        }
    }

    /// Load the durable vector payload even when it is tombstoned.
    ///
    /// Authorization layers use this to enforce the original workspace owner
    /// without disclosing or allowing reuse of an ID after deletion.
    pub fn get_vector_including_deleted(
        &self,
        external_id: &VectorId,
    ) -> Result<Option<StoredVectorEntry>> {
        let key = self.make_vector_key(external_id);
        self.storage
            .get(&key)?
            .map(|data| {
                bincode::deserialize(&data)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))
            })
            .transpose()
    }

    /// Load all active durable vector payloads for this collection.
    pub fn load_active_vectors(&self) -> Result<Vec<StoredVectorEntry>> {
        let prefix = self.make_collection_prefix(VECTOR_PREFIX);
        let mut vectors = Vec::new();

        for (_, data) in self.storage.scan_prefix_limited(&prefix, None)? {
            let entry: StoredVectorEntry = bincode::deserialize(&data)
                .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
            if entry.deleted {
                continue;
            }

            let vector_id = VectorId::new(&entry.external_id);
            if self.get_internal_id(&vector_id)?.is_some() {
                vectors.push(entry);
            }
        }

        Ok(vectors)
    }

    /// Check if an ID exists (including deleted)
    pub fn exists(&self, external_id: &VectorId) -> Result<bool> {
        self.get(external_id).map(|e| e.is_some())
    }

    /// Check if an ID is deleted
    pub fn is_deleted(&self, external_id: &VectorId) -> Result<bool> {
        match self.get(external_id)? {
            Some(entry) => Ok(entry.deleted),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksDbBackend;
    use tempfile::tempdir;

    fn create_test_storage() -> Arc<RocksDbBackend> {
        let dir = tempdir().unwrap();
        Arc::new(RocksDbBackend::open(dir.path()).unwrap())
    }

    #[test]
    fn test_id_mapping_create() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        let int_id = InternalId(42);

        let entry = mapping.create(&ext_id, int_id).unwrap();
        assert_eq!(entry.internal_id, 42);
        assert!(!entry.deleted);
    }

    #[test]
    fn test_text_persistence_roundtrip() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        mapping
            .store_text(&VectorId::new("a"), "the quick brown fox")
            .unwrap();
        mapping.store_text(&VectorId::new("b"), "lazy dog").unwrap();

        let mut texts = mapping.load_all_texts().unwrap();
        texts.sort_by(|x, y| x.0.as_str().cmp(y.0.as_str()));
        assert_eq!(texts.len(), 2);
        assert_eq!(mapping.stored_text_count().unwrap(), 2);
        assert_eq!(
            texts[0],
            (VectorId::new("a"), "the quick brown fox".to_string())
        );
        assert_eq!(texts[1], (VectorId::new("b"), "lazy dog".to_string()));

        // Delete removes it from the loaded set.
        mapping.delete_text(&VectorId::new("a")).unwrap();
        let after = mapping.load_all_texts().unwrap();
        assert_eq!(after, vec![(VectorId::new("b"), "lazy dog".to_string())]);
        assert_eq!(mapping.stored_text_count().unwrap(), 1);
    }

    #[test]
    fn test_text_persistence_isolated_by_collection() {
        let storage = create_test_storage();
        let c1 = IdMapping::new(storage.clone(), "c1");
        let c2 = IdMapping::new(storage, "c2");
        c1.store_text(&VectorId::new("x"), "in c1").unwrap();
        c2.store_text(&VectorId::new("x"), "in c2").unwrap();
        assert_eq!(
            c1.load_all_texts().unwrap(),
            vec![(VectorId::new("x"), "in c1".to_string())]
        );
        assert_eq!(
            c2.load_all_texts().unwrap(),
            vec![(VectorId::new("x"), "in c2".to_string())]
        );
    }

    #[test]
    fn test_text_persistence_rejects_invalid_utf8_id() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage.clone(), "test_collection");

        let mut key = mapping.make_collection_prefix(TEXT_PREFIX);
        key.push(0xff);
        storage.put(&key, b"valid text").unwrap();

        let err = mapping.load_all_texts().unwrap_err();
        assert!(matches!(err, AkiDbError::SerializationError(_)));
    }

    #[test]
    fn test_text_persistence_rejects_invalid_utf8_payload() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage.clone(), "test_collection");

        let key = mapping.make_text_key(&VectorId::new("bad-payload"));
        storage.put(&key, &[0xff]).unwrap();

        let err = mapping.load_all_texts().unwrap_err();
        assert!(matches!(err, AkiDbError::SerializationError(_)));
    }

    #[test]
    fn test_id_mapping_get_internal() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        let int_id = InternalId(42);

        mapping.create(&ext_id, int_id).unwrap();

        let result = mapping.get_internal_id(&ext_id).unwrap();
        assert_eq!(result, Some(InternalId(42)));
    }

    #[test]
    fn test_id_mapping_delete() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        mapping.create(&ext_id, InternalId(42)).unwrap();

        // Delete
        let deleted = mapping.mark_deleted(&ext_id).unwrap();
        assert_eq!(deleted, Some(InternalId(42)));

        // Should not return internal ID after deletion
        let result = mapping.get_internal_id(&ext_id).unwrap();
        assert_eq!(result, None);

        // Should report as deleted
        assert!(mapping.is_deleted(&ext_id).unwrap());
    }

    #[test]
    fn test_id_mapping_delete_returns_none_when_already_deleted() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        mapping.create(&ext_id, InternalId(42)).unwrap();

        assert_eq!(mapping.mark_deleted(&ext_id).unwrap(), Some(InternalId(42)));
        assert_eq!(mapping.mark_deleted(&ext_id).unwrap(), None);
    }

    #[test]
    fn test_id_reuse_forbidden() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        mapping.create(&ext_id, InternalId(42)).unwrap();
        mapping.mark_deleted(&ext_id).unwrap();

        // Try to reuse deleted ID
        let result = mapping.create(&ext_id, InternalId(100));
        assert!(matches!(result, Err(AkiDbError::IdReuseForbidden(_))));
    }

    #[test]
    fn test_vector_payload_persists() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        mapping
            .upsert_with_vector(&ext_id, InternalId(42), &[0.1, 0.2, 0.3], br#"{"k":"v"}"#)
            .unwrap();

        let stored = mapping.get_vector(&ext_id).unwrap().unwrap();
        assert_eq!(stored.external_id, "vec-1");
        assert_eq!(stored.internal_id, 42);
        assert_eq!(stored.vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(stored.metadata, br#"{"k":"v"}"#);

        let active = mapping.load_active_vectors().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].external_id, "vec-1");
    }

    #[test]
    fn test_deleted_vector_payload_not_loaded() {
        let storage = create_test_storage();
        let mapping = IdMapping::new(storage, "test_collection");

        let ext_id = VectorId::new("vec-1");
        mapping
            .upsert_with_vector(&ext_id, InternalId(42), &[0.1, 0.2, 0.3], &[])
            .unwrap();
        mapping.mark_deleted(&ext_id).unwrap();

        assert!(mapping.get_vector(&ext_id).unwrap().is_none());
        let tombstone = mapping
            .get_vector_including_deleted(&ext_id)
            .unwrap()
            .unwrap();
        assert!(tombstone.deleted);
        assert_eq!(tombstone.external_id, "vec-1");
        assert!(mapping.load_active_vectors().unwrap().is_empty());
    }
}
