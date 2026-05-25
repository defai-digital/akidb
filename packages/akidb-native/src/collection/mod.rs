//! CollectionManager — validated facade over MetadataStore collection operations.

use crate::error::{AkiDbError, Result};
use crate::metadata::{Collection, MetadataStore};

pub struct CreateCollectionOptions {
    pub collection_id: String,
    pub dimension: i64,
    pub metric: String,
    pub embedding_model_id: String,
    pub schema_version: String,
    pub quantization: String,
    pub hnsw_m: i64,
    pub hnsw_ef_construction: i64,
    pub hnsw_ef_search: i64,
}

pub struct CollectionManager;

impl CollectionManager {
    /// Create a new collection after validating inputs.
    pub fn create(metadata: &MetadataStore, opts: &CreateCollectionOptions) -> Result<Collection> {
        Self::validate_create_opts(opts)?;

        let existing = metadata.get_collection(&opts.collection_id)?;
        if let Some(ref c) = existing
            && c.deleted_at.is_none() {
                return Err(AkiDbError::InvalidArgument(format!(
                    "Collection \"{}\" already exists",
                    opts.collection_id
                )));
            }

        let collection = Collection {
            collection_id: opts.collection_id.clone(),
            dimension: opts.dimension,
            metric: opts.metric.clone(),
            embedding_model_id: opts.embedding_model_id.clone(),
            schema_version: opts.schema_version.clone(),
            created_at: iso_now(),
            deleted_at: None,
            quantization: opts.quantization.clone(),
            hnsw_m: opts.hnsw_m,
            hnsw_ef_construction: opts.hnsw_ef_construction,
            hnsw_ef_search: opts.hnsw_ef_search,
        };

        metadata.create_collection(&collection)?;
        Ok(collection)
    }

    /// Soft-delete a collection.
    pub fn delete(metadata: &MetadataStore, collection_id: &str) -> Result<()> {
        let deleted = metadata.soft_delete_collection(collection_id, &iso_now())?;
        if !deleted {
            return Err(AkiDbError::CollectionNotFound(
                collection_id.to_string(),
            ));
        }
        Ok(())
    }

    /// List all active (non-deleted) collections.
    pub fn list(metadata: &MetadataStore) -> Result<Vec<Collection>> {
        metadata.list_collections()
    }

    fn validate_create_opts(opts: &CreateCollectionOptions) -> Result<()> {
        if opts.collection_id.trim().is_empty() {
            return Err(AkiDbError::InvalidArgument(
                "collectionId must be a non-empty string".into(),
            ));
        }
        if opts.dimension <= 0 {
            return Err(AkiDbError::InvalidArgument(format!(
                "dimension must be a positive integer, got {}",
                opts.dimension
            )));
        }
        if !["cosine", "l2", "dot"].contains(&opts.metric.as_str()) {
            return Err(AkiDbError::InvalidArgument(format!(
                "metric must be one of cosine, l2, dot — got \"{}\"",
                opts.metric
            )));
        }
        if opts.embedding_model_id.trim().is_empty() {
            return Err(AkiDbError::InvalidArgument(
                "embeddingModelId must be a non-empty string".into(),
            ));
        }
        if opts.schema_version.trim().is_empty() {
            return Err(AkiDbError::InvalidArgument(
                "schemaVersion must be a non-empty string".into(),
            ));
        }
        if !["fp16", "sq8"].contains(&opts.quantization.as_str()) {
            return Err(AkiDbError::InvalidArgument(format!(
                "quantization must be one of fp16, sq8 — got \"{}\"",
                opts.quantization
            )));
        }
        if !(4..=64).contains(&opts.hnsw_m) {
            return Err(AkiDbError::InvalidArgument(format!(
                "hnsw_m must be between 4 and 64, got {}",
                opts.hnsw_m
            )));
        }
        if !(50..=800).contains(&opts.hnsw_ef_construction) {
            return Err(AkiDbError::InvalidArgument(format!(
                "hnsw_ef_construction must be between 50 and 800, got {}",
                opts.hnsw_ef_construction
            )));
        }
        if !(10..=500).contains(&opts.hnsw_ef_search) {
            return Err(AkiDbError::InvalidArgument(format!(
                "hnsw_ef_search must be between 10 and 500, got {}",
                opts.hnsw_ef_search
            )));
        }
        Ok(())
    }
}

fn iso_now() -> String {
    crate::write::epoch_to_iso8601(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}
