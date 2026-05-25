//! Compaction — merge multiple segments into fewer, larger segments.
//!
//! Reads segments, filters tombstoned records, builds a new merged segment,
//! publishes a new manifest, and archives old segments.

use std::collections::HashSet;

use crate::error::{AkiDbError, Result};
use crate::hnsw::HnswGraph;
use crate::write::HnswParams;
use crate::manifest::{ManifestManager, PublishManifestOptions};
use crate::metadata::{Manifest, MetadataStore};
use crate::segment::builder::SegmentBuilder;
use crate::segment::checksum::checksum_hex;
use crate::segment::reader::SegmentReader;
use crate::storage::LocalFsBackend;

struct ExtractedRecord {
    chunk_id: String,
    vector: Vec<f32>,
    metadata: serde_json::Value,
    chunk_text: Option<String>,
}

pub struct CompactResult {
    pub manifest: Manifest,
    pub records_kept: usize,
    pub records_removed: usize,
    pub space_reclaimed_bytes: i64,
}

/// Compact all segments in the current manifest for a collection.
pub fn compact(
    metadata: &MetadataStore,
    storage: &LocalFsBackend,
    collection_id: &str,
) -> Result<CompactResult> {
    let collection = metadata
        .get_collection(collection_id)?
        .ok_or_else(|| AkiDbError::CollectionNotFound(collection_id.to_string()))?;

    let current_manifest = ManifestManager::get_current(metadata, collection_id)?
        .ok_or_else(|| {
            AkiDbError::ManifestNotFound(format!(
                "No manifest published for collection \"{}\"",
                collection_id
            ))
        })?;

    let tombstone_ids_to_apply: Vec<String> = current_manifest
        .tombstone_ids
        .iter()
        .cloned()
        .chain(metadata.list_tombstone_chunk_ids(collection_id)?)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let tombstone_set: HashSet<String> = tombstone_ids_to_apply.iter().cloned().collect();
    let old_segment_ids = &current_manifest.segment_ids;

    // Calculate old total size.
    let old_total_bytes: i64 = old_segment_ids
        .iter()
        .filter_map(|id| metadata.get_segment(id).ok().flatten())
        .map(|s| s.size_bytes)
        .sum();

    // Extract all non-tombstoned records.
    let mut all_records = Vec::new();
    let mut total_records_in_segments = 0usize;

    for segment_id in old_segment_ids {
        let (records, count) =
            extract_from_segment(metadata, storage, segment_id, &tombstone_set)?;
        total_records_in_segments += count;
        all_records.extend(records);
    }

    let records_removed = total_records_in_segments - all_records.len();

    // Build new compacted segment(s).
    let (new_segment_ids, new_total_bytes) = if !all_records.is_empty() {
        let (seg_id, seg_bytes) = build_compacted_segment(
            metadata,
            storage,
            collection_id,
            &all_records,
            collection.dimension as usize,
            &collection.metric,
            HnswParams {
                m: collection.hnsw_m as usize,
                ef_construction: collection.hnsw_ef_construction as usize,
                ef_search: collection.hnsw_ef_search as usize,
            },
        )?;
        (vec![seg_id], seg_bytes)
    } else {
        (Vec::new(), 0i64)
    };

    // Publish new manifest (no tombstones — they've been applied).
    let new_manifest = ManifestManager::publish(
        metadata,
        collection_id,
        &PublishManifestOptions {
            segment_ids: new_segment_ids,
            tombstone_ids: Vec::new(),
            embedding_model_id: current_manifest.embedding_model_id.clone(),
            pipeline_signature: current_manifest.pipeline_signature.clone(),
        },
    )?;

    // Archive old segments and delete their physical files.
    // Archiving without deletion leaves orphaned .bin files on disk that
    // continue to count toward the storage budget (get_storage_size_bytes
    // walks the filesystem), so space_reclaimed_bytes would be a lie.
    for seg_id in old_segment_ids {
        // Collect storage path before updating status (get_segment uses status).
        let storage_path = metadata
            .get_segment(seg_id)
            .ok()
            .flatten()
            .map(|s| s.storage_path);
        let _ = metadata.update_segment_status(seg_id, "archived");
        if let Some(path) = storage_path {
            let _ = storage.delete_object(&path);
        }
    }

    // Clean up tombstones.
    if !tombstone_ids_to_apply.is_empty() {
        metadata.delete_tombstones(&tombstone_ids_to_apply)?;
    }

    Ok(CompactResult {
        manifest: new_manifest,
        records_kept: all_records.len(),
        records_removed,
        space_reclaimed_bytes: old_total_bytes - new_total_bytes,
    })
}

fn extract_from_segment(
    metadata: &MetadataStore,
    storage: &LocalFsBackend,
    segment_id: &str,
    tombstone_set: &HashSet<String>,
) -> Result<(Vec<ExtractedRecord>, usize)> {
    let seg_meta = metadata
        .get_segment(segment_id)?
        .ok_or_else(|| AkiDbError::SegmentNotFound(segment_id.to_string()))?;

    let buffer = storage.get_object(&seg_meta.storage_path)?;
    let reader = SegmentReader::from_buffer(buffer)?;

    let chunk_ids = reader.get_chunk_ids()?;
    let vectors = reader.get_vectors()?;
    let metadata_list = reader.get_metadata()?;
    let chunk_texts = reader.get_chunk_texts();

    let total_count = chunk_ids.len();
    let mut records = Vec::new();

    for i in 0..chunk_ids.len() {
        if tombstone_set.contains(&chunk_ids[i]) {
            continue;
        }
        let text = chunk_texts.as_ref().and_then(|texts| texts.get(i).cloned());
        records.push(ExtractedRecord {
            chunk_id: chunk_ids[i].clone(),
            vector: vectors[i].clone(),
            metadata: metadata_list[i].clone(),
            chunk_text: text,
        });
    }

    Ok((records, total_count))
}

fn build_compacted_segment(
    metadata: &MetadataStore,
    storage: &LocalFsBackend,
    collection_id: &str,
    records: &[ExtractedRecord],
    dimension: usize,
    metric: &str,
    hnsw: HnswParams,
) -> Result<(String, i64)> {
    // Collect vectors first (one clone each) so they can be moved into the
    // builder after HNSW construction — avoiding the double-clone per record
    // that occurs when both the builder and the HNSW graph need ownership.
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(records.len());
    for rec in records {
        vectors.push(rec.vector.clone());
    }

    // Build HNSW index with collection-configured parameters.
    let mut graph = HnswGraph::new(metric, dimension, hnsw.m, hnsw.ef_construction, hnsw.ef_search);
    graph.build(&vectors);
    let index_data = graph.serialize();

    // Feed builder by moving vectors — no additional clone per record.
    let mut builder = SegmentBuilder::new();
    for (rec, vector) in records.iter().zip(vectors) {
        builder.add_record_with_text(rec.chunk_id.clone(), vector, rec.metadata.clone(), rec.chunk_text.clone())?;
    }

    let result = builder.build(Some(&index_data))?;

    // Store.
    let storage_path = format!("segments/{}/{}.bin", collection_id, result.segment_id);
    storage.put_object(&storage_path, &result.buffer)?;

    let size_bytes = result.buffer.len() as i64;

    // Register in metadata.
    let now = crate::write::epoch_to_iso8601(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    );
    metadata.create_segment(&crate::metadata::SegmentMetadata {
        segment_id: result.segment_id.clone(),
        collection_id: collection_id.to_string(),
        record_count: records.len() as i64,
        dimension: dimension as i64,
        size_bytes,
        checksum: checksum_hex(&result.buffer),
        status: "ready".to_string(),
        storage_path,
        created_at: now,
    })?;

    Ok((result.segment_id, size_bytes))
}
