//! One synced batch is the document commit point; projections are rebuildable.

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentHead {
    pub workspace_id: String,
    pub revision: String,
    pub document_version: String,
    pub settings_fingerprint: String,
    pub request_hash: String,
    pub source_uri: String,
    pub chunk_ids: Vec<String>,
}

pub struct DocumentVector {
    pub id: VectorId,
    pub internal_id: InternalId,
    pub vector: Vec<f32>,
    pub metadata: Vec<u8>,
    pub text: String,
}

/// Distinguish a rejected batch from an ambiguous storage acknowledgement.
#[derive(Debug, thiserror::Error)]
pub enum DocumentWriteError {
    #[error("document rejected before commit: {0}")]
    BeforeCommit(#[from] AkiDbError),
    #[error("document commit acknowledgement failed: {0}")]
    CommitUncertain(AkiDbError),
}

#[derive(Serialize, Deserialize)]
struct DocumentRecovery {
    workspace_id: String,
    retired_ids: Vec<String>,
}

impl<S: StorageBackend> IdMapping<S> {
    fn document_key(&self, scope_key: &str) -> Vec<u8> {
        self.make_key_with_prefix(b"doc:", &VectorId::new(scope_key))
    }

    fn document_dirty_key(&self) -> Vec<u8> {
        self.make_collection_prefix(b"doc-dirty:")
    }

    pub fn get_document(&self, scope_key: &str) -> Result<Option<DocumentHead>> {
        self.storage
            .get(&self.document_key(scope_key))?
            .map(|bytes| {
                bincode::deserialize(&bytes)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))
            })
            .transpose()
    }

    pub fn document_is_complete(&self, head: &DocumentHead) -> Result<bool> {
        for id in &head.chunk_ids {
            let id = VectorId::new(id);
            let Some(vector) = self.get_vector(&id)? else {
                return Ok(false);
            };
            let Some(text) = self.storage.get(&self.make_text_key(&id))? else {
                return Ok(false);
            };
            let metadata: serde_json::Value = serde_json::from_slice(&vector.metadata)
                .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
            use sha2::{Digest, Sha256};
            if metadata["document_version"] != head.document_version
                || metadata["content_hash"] != format!("{:x}", Sha256::digest(&text))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn document_retired_chunks(&self) -> Result<Vec<(String, String)>> {
        let Some(bytes) = self.storage.get(&self.document_dirty_key())? else {
            return Ok(vec![]);
        };
        let recovery: DocumentRecovery = bincode::deserialize(&bytes)
            .map_err(|e| AkiDbError::SerializationError(e.to_string()))?;
        Ok(recovery
            .retired_ids
            .into_iter()
            .map(|id| (recovery.workspace_id.clone(), id))
            .collect())
    }

    pub fn document_projections_dirty(&self) -> Result<bool> {
        self.storage.exists(&self.document_dirty_key())
    }

    pub fn finish_document_projections(&self) -> Result<()> {
        self.storage.write_batch_sync(vec![BatchOperation::Delete {
            key: self.document_dirty_key(),
        }])
    }

    /// Bounded migration scan. Tombstoned and missing middle chunks do not end it.
    pub fn vectors_with_id_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<StoredVectorEntry>> {
        let key = self.make_vector_key(&VectorId::new(prefix));
        self.storage
            .scan_prefix_limited(&key, Some(limit))?
            .into_iter()
            .map(|(_, bytes)| {
                bincode::deserialize(&bytes)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))
            })
            .collect()
    }

    /// Includes tombstones so recovery can remove retired graph projections.
    pub fn load_vectors_including_deleted(&self) -> Result<Vec<StoredVectorEntry>> {
        self.storage
            .scan_prefix_limited(&self.make_collection_prefix(VECTOR_PREFIX), None)?
            .into_iter()
            .map(|(_, bytes)| {
                bincode::deserialize(&bytes)
                    .map_err(|e| AkiDbError::SerializationError(e.to_string()))
            })
            .collect()
    }

    /// The service holds its exclusive serving gate; stripes additionally
    /// exclude lower-level point mutations during validation and the batch.
    pub fn replace_document(
        &self,
        scope_key: &str,
        expected_revision: &str,
        head: &DocumentHead,
        vectors: &[DocumentVector],
        retired: &[String],
    ) -> std::result::Result<(), DocumentWriteError> {
        let _guards: Vec<_> = self.lock_stripes.iter().map(|lock| lock.lock()).collect();
        let current = self.get_document(scope_key)?;
        if current
            .as_ref()
            .map(|h| h.revision.as_str())
            .unwrap_or_default()
            != expected_revision
        {
            return Err(AkiDbError::InvalidParameter("document revision conflict".into()).into());
        }
        let mut operations = Vec::new();
        let mut ids = std::collections::HashSet::new();
        for vector in vectors {
            if !ids.insert(vector.id.as_str()) || self.exists(&vector.id)? {
                return Err(AkiDbError::IdReuseForbidden(vector.id.to_string()).into());
            }
            operations.push(BatchOperation::Put {
                key: self.make_key(&vector.id),
                value: Self::serialize_mapping(&IdMappingEntry::new(vector.internal_id.0))?,
            });
            operations.push(BatchOperation::Put {
                key: self.make_vector_key(&vector.id),
                value: Self::serialize_vector(&StoredVectorEntry::new(
                    &vector.id,
                    vector.internal_id,
                    &vector.vector,
                    &vector.metadata,
                ))?,
            });
            operations.push(BatchOperation::Put {
                key: self.make_text_key(&vector.id),
                value: vector.text.as_bytes().to_vec(),
            });
        }
        for id in retired {
            let id = VectorId::new(id);
            if ids.contains(id.as_str()) {
                return Err(AkiDbError::IdReuseForbidden(id.to_string()).into());
            }
            if let Some(mut mapping) = self.get(&id)? {
                mapping.mark_deleted();
                operations.push(BatchOperation::Put {
                    key: self.make_key(&id),
                    value: Self::serialize_mapping(&mapping)?,
                });
            }
            if id
                .as_str()
                .starts_with(akidb_contracts::document::DOCUMENT_CHUNK_PREFIX)
            {
                // Keep the ID tombstone (no reuse), not historical embeddings.
                // The synced recovery journal retains IDs/workspace needed to
                // remove their projections after a crash.
                operations.push(BatchOperation::Delete {
                    key: self.make_vector_key(&id),
                });
            } else if let Some(mut vector) = self.get_vector_including_deleted(&id)? {
                vector.mark_deleted();
                operations.push(BatchOperation::Put {
                    key: self.make_vector_key(&id),
                    value: Self::serialize_vector(&vector)?,
                });
            }
            operations.push(BatchOperation::Delete {
                key: self.make_text_key(&id),
            });
        }
        operations.push(BatchOperation::Put {
            key: self.document_key(scope_key),
            value: bincode::serialize(head)
                .map_err(|e| AkiDbError::SerializationError(e.to_string()))?,
        });
        operations.push(BatchOperation::Put {
            key: self.document_dirty_key(),
            value: bincode::serialize(&DocumentRecovery {
                workspace_id: head.workspace_id.clone(),
                retired_ids: retired.to_vec(),
            })
            .map_err(|e| AkiDbError::SerializationError(e.to_string()))?,
        });
        self.storage
            .write_batch_sync(operations)
            .map_err(DocumentWriteError::CommitUncertain)
    }
}
