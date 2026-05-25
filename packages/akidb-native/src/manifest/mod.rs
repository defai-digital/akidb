//! ManifestManager — manages manifest lifecycle for collections.
//!
//! A manifest is a point-in-time snapshot of a collection's state:
//! which segments are active and which chunk_ids are tombstoned.

use sha2::{Digest, Sha256};

use crate::error::{AkiDbError, Result};
use crate::metadata::{Manifest, MetadataStore};

pub struct PublishManifestOptions {
    pub segment_ids: Vec<String>,
    pub tombstone_ids: Vec<String>,
    pub embedding_model_id: String,
    pub pipeline_signature: String,
}

pub struct ManifestManager;

impl ManifestManager {
    /// Publish a new manifest version for a collection.
    pub fn publish(
        metadata: &MetadataStore,
        collection_id: &str,
        opts: &PublishManifestOptions,
    ) -> Result<Manifest> {
        let next_version = Self::compute_next_version(metadata, collection_id)?;

        let mut manifest = Manifest {
            manifest_id: uuid::Uuid::new_v4().to_string(),
            collection_id: collection_id.to_string(),
            version: next_version,
            segment_ids: opts.segment_ids.clone(),
            tombstone_ids: opts.tombstone_ids.clone(),
            embedding_model_id: opts.embedding_model_id.clone(),
            pipeline_signature: opts.pipeline_signature.clone(),
            created_at: iso_now(),
            checksum: String::new(),
        };

        manifest.checksum = compute_manifest_checksum(&manifest);
        metadata.create_manifest(&manifest)?;
        Ok(manifest)
    }

    /// Rollback to a previous manifest by creating a new version with same content.
    pub fn rollback(
        metadata: &MetadataStore,
        collection_id: &str,
        manifest_id: &str,
    ) -> Result<Manifest> {
        let target = metadata
            .get_manifest(manifest_id)?
            .ok_or_else(|| AkiDbError::ManifestNotFound(manifest_id.to_string()))?;

        if target.collection_id != collection_id {
            return Err(AkiDbError::InvalidArgument(format!(
                "Manifest \"{}\" does not belong to collection \"{}\"",
                manifest_id, collection_id
            )));
        }

        Self::publish(
            metadata,
            collection_id,
            &PublishManifestOptions {
                segment_ids: target.segment_ids,
                tombstone_ids: target.tombstone_ids,
                embedding_model_id: target.embedding_model_id,
                pipeline_signature: target.pipeline_signature,
            },
        )
    }

    /// Get the current (latest version) manifest for a collection.
    pub fn get_current(
        metadata: &MetadataStore,
        collection_id: &str,
    ) -> Result<Option<Manifest>> {
        metadata.get_latest_manifest(collection_id)
    }

    fn compute_next_version(metadata: &MetadataStore, collection_id: &str) -> Result<i64> {
        let latest = metadata.get_latest_manifest(collection_id)?;
        Ok(latest.map(|m| m.version + 1).unwrap_or(0))
    }
}

fn compute_manifest_checksum(manifest: &Manifest) -> String {
    // Exclude checksum field, serialize remaining with sorted keys.
    let content = serde_json::json!({
        "collection_id": manifest.collection_id,
        "created_at": manifest.created_at,
        "embedding_model_id": manifest.embedding_model_id,
        "manifest_id": manifest.manifest_id,
        "pipeline_signature": manifest.pipeline_signature,
        "segment_ids": manifest.segment_ids,
        "tombstone_ids": manifest.tombstone_ids,
        "version": manifest.version,
    });
    let json = serde_json::to_string(&content).unwrap_or_default();

    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn iso_now() -> String {
    crate::write::epoch_to_iso8601(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}
