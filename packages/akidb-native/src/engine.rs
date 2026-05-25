//! EngineInner — orchestrates all AkiDB modules.
//!
//! This is the internal engine that holds all state. It is wrapped by
//! `AkiDbEngine` in lib.rs behind an `RwLock` for thread safety.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::collection::{CollectionManager, CreateCollectionOptions};
use crate::compaction::{compact, CompactResult};
use crate::error::{AkiDbError, Result};
use crate::index::{ExplainInfo, SearchResult};
use crate::manifest::{ManifestManager, PublishManifestOptions};
use crate::metadata::{Collection, Manifest, MetadataStore, Tombstone};
use crate::query::{
    hybrid, keyword, tombstone_fingerprint, QueryEngine, SearchMode, SearchOptions, SearchResponse,
    VectorSearchSnapshot,
};
use crate::storage::LocalFsBackend;
use crate::write::{HnswParams, NativeRecord, UpsertResult, WritePath, WritePathOptions};

pub struct EngineOptions {
    pub storage_path: PathBuf,
    pub disable_wal: bool,
}

pub struct EngineInner {
    pub metadata: Arc<Mutex<MetadataStore>>,
    pub storage: Arc<LocalFsBackend>,
    pub query_engine: QueryEngine,
    write_paths: Mutex<HashMap<String, Arc<Mutex<WritePath>>>>,
    disable_wal: bool,
    storage_path: PathBuf,
}

struct ManifestSearchSnapshot {
    manifest: Manifest,
    live_tombstone_set: std::collections::HashSet<String>,
}

impl EngineInner {
    pub fn open(opts: EngineOptions) -> Result<Self> {
        std::fs::create_dir_all(&opts.storage_path)?;

        let db_path = opts.storage_path.join("metadata.db");
        let db_path_str = db_path.to_str().ok_or_else(|| {
            AkiDbError::InvalidArgument("Storage path contains non-UTF-8 characters".to_string())
        })?;
        let metadata = Arc::new(Mutex::new(MetadataStore::open(db_path_str)?));
        let storage = Arc::new(LocalFsBackend::open(&opts.storage_path)?);
        let query_engine = QueryEngine::new(None);

        Ok(Self {
            metadata,
            storage,
            query_engine,
            write_paths: Mutex::new(HashMap::new()),
            disable_wal: opts.disable_wal,
            storage_path: opts.storage_path,
        })
    }

    // ─── Collection operations ──────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn create_collection(
        &self,
        collection_id: &str,
        dimension: i64,
        metric: &str,
        embedding_model_id: &str,
        quantization: &str,
        hnsw_m: i64,
        hnsw_ef_construction: i64,
        hnsw_ef_search: i64,
    ) -> Result<Collection> {
        let metadata = self.lock_metadata()?;
        CollectionManager::create(
            &metadata,
            &CreateCollectionOptions {
                collection_id: collection_id.to_string(),
                dimension,
                metric: metric.to_string(),
                embedding_model_id: embedding_model_id.to_string(),
                schema_version: "1".to_string(),
                quantization: quantization.to_string(),
                hnsw_m,
                hnsw_ef_construction,
                hnsw_ef_search,
            },
        )
    }

    pub fn get_collection(&self, collection_id: &str) -> Result<Option<Collection>> {
        let metadata = self.lock_metadata()?;
        metadata.get_collection(collection_id)
    }

    pub fn list_collections(&self) -> Result<Vec<Collection>> {
        let metadata = self.lock_metadata()?;
        CollectionManager::list(&metadata)
    }

    pub fn delete_collection(&self, collection_id: &str) -> Result<()> {
        let metadata = self.lock_metadata()?;
        CollectionManager::delete(&metadata, collection_id)
    }

    // ─── Write operations ───────────────────────────────────────────────────

    pub fn upsert_batch(
        &self,
        collection_id: &str,
        records: &[NativeRecord],
    ) -> Result<UpsertResult> {
        let collection = self
            .lock_metadata()?
            .get_collection(collection_id)?
            .ok_or_else(|| AkiDbError::CollectionNotFound(collection_id.to_string()))?;

        if collection.deleted_at.is_some() {
            return Err(AkiDbError::InvalidArgument(format!(
                "Collection \"{}\" has been deleted",
                collection_id
            )));
        }

        // Validate record dimensions — all records must have a "vector" field with the right length.
        for rec in records {
            if rec.chunk_id.is_empty() || rec.doc_id.is_empty() {
                return Err(AkiDbError::InvalidArgument(
                    "Records must include non-empty chunk_id and doc_id".to_string(),
                ));
            }
            let vector = if rec.vector.is_empty() && collection.dimension > 0 {
                return Err(AkiDbError::DimensionMismatch {
                    expected: collection.dimension as usize,
                    actual: 0,
                    chunk_id: rec.chunk_id.clone(),
                });
            } else {
                &rec.vector
            };
            if vector.len() != collection.dimension as usize {
                return Err(AkiDbError::DimensionMismatch {
                    expected: collection.dimension as usize,
                    actual: vector.len(),
                    chunk_id: rec.chunk_id.clone(),
                });
            }
        }

        // Get or create write path for this collection.
        let write_path = self.get_or_create_write_path(collection_id)?;
        let mut write_path = write_path
            .lock()
            .map_err(|_| AkiDbError::InvalidArgument("Write path lock poisoned".to_string()))?;
        write_path.upsert_batch(
            collection_id,
            records,
            collection.dimension as usize,
            &collection.metric,
            HnswParams {
                m: collection.hnsw_m as usize,
                ef_construction: collection.hnsw_ef_construction as usize,
                ef_search: collection.hnsw_ef_search as usize,
            },
        )
    }

    pub fn flush_writes(&self, collection_id: &str) -> Result<Vec<String>> {
        let collection = self
            .lock_metadata()?
            .get_collection(collection_id)?
            .ok_or_else(|| AkiDbError::CollectionNotFound(collection_id.to_string()))?;

        let write_path = {
            let write_paths = self.lock_write_paths()?;
            write_paths.get(collection_id).cloned()
        };
        if let Some(wp) = write_path {
            let mut wp = wp
                .lock()
                .map_err(|_| AkiDbError::InvalidArgument("Write path lock poisoned".to_string()))?;
            wp.flush(
                collection_id,
                collection.dimension as usize,
                &collection.metric,
                HnswParams {
                    m: collection.hnsw_m as usize,
                    ef_construction: collection.hnsw_ef_construction as usize,
                    ef_search: collection.hnsw_ef_search as usize,
                },
            )
        } else {
            Ok(Vec::new())
        }
    }

    // ─── Publish ────────────────────────────────────────────────────────────

    pub fn publish(
        &self,
        collection_id: &str,
        segment_ids: Vec<String>,
        tombstone_ids: Vec<String>,
        embedding_model_id: &str,
        pipeline_signature: &str,
    ) -> Result<Manifest> {
        let metadata = self.lock_metadata()?;
        ManifestManager::publish(
            &metadata,
            collection_id,
            &PublishManifestOptions {
                segment_ids,
                tombstone_ids,
                embedding_model_id: embedding_model_id.to_string(),
                pipeline_signature: pipeline_signature.to_string(),
            },
        )
    }

    /// Auto-publish: flush writes, gather all ready segments + tombstones,
    /// and publish a new manifest.
    pub fn auto_publish(
        &self,
        collection_id: &str,
        embedding_model_id: &str,
        pipeline_signature: &str,
    ) -> Result<Manifest> {
        // Flush pending writes.
        self.flush_writes(collection_id)?;

        let (manifest, segment_paths, tombstone_ids, collection) = {
            let metadata = self.lock_metadata()?;
            let segments = metadata.list_segments(collection_id, Some("ready"))?;
            let segment_ids: Vec<String> = segments.iter().map(|s| s.segment_id.clone()).collect();
            let segment_paths: Vec<(String, String)> = segments
                .iter()
                .map(|s| (s.segment_id.clone(), s.storage_path.clone()))
                .collect();
            let pending_tombstone_ids = metadata.list_tombstone_chunk_ids(collection_id)?;
            let mut tombstone_ids = metadata
                .get_latest_manifest(collection_id)?
                .map(|manifest| manifest.tombstone_ids)
                .unwrap_or_default()
                .into_iter()
                .chain(pending_tombstone_ids.iter().cloned())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            tombstone_ids.sort();
            let manifest = ManifestManager::publish(
                &metadata,
                collection_id,
                &PublishManifestOptions {
                    segment_ids,
                    tombstone_ids: tombstone_ids.clone(),
                    embedding_model_id: embedding_model_id.to_string(),
                    pipeline_signature: pipeline_signature.to_string(),
                },
            )?;
            if !pending_tombstone_ids.is_empty() {
                metadata.delete_tombstones(&pending_tombstone_ids)?;
            }
            let collection = metadata.get_collection(collection_id)?;
            (manifest, segment_paths, tombstone_ids, collection)
        };

        // Prewarm index cache: load HNSW graphs for all new segments so the
        // first search query doesn't pay deserialization cost (best-effort).
        if let Some(collection) = collection {
            let tombstone_set: std::collections::HashSet<String> =
                tombstone_ids.into_iter().collect();
            let hnsw = HnswParams {
                m: collection.hnsw_m as usize,
                ef_construction: collection.hnsw_ef_construction as usize,
                ef_search: collection.hnsw_ef_search as usize,
            };
            let metric = collection.metric.clone();
            let storage = Arc::clone(&self.storage);
            // Prewarm is best-effort; a panic on a corrupt segment must not
            // shadow the already-committed manifest.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.query_engine.prewarm_paths(
                    &storage,
                    &segment_paths,
                    manifest.version,
                    &tombstone_set,
                    tombstone_fingerprint(&tombstone_set),
                    hnsw,
                    &metric,
                );
            }));
        }

        Ok(manifest)
    }

    // ─── Search ─────────────────────────────────────────────────────────────

    pub fn search(&self, opts: SearchOptions) -> Result<SearchResponse> {
        let write_path = {
            let write_paths = self.lock_write_paths()?;
            write_paths.get(&opts.collection_id).cloned()
        };

        // Snapshot the write buffer before touching metadata so search does not
        // invert the lock order used by flush/build paths (write path -> metadata).
        // The clone is limited to uncommitted records for the queried collection.
        let buffer_records = if let Some(write_path) = write_path {
            let write_path = write_path
                .lock()
                .map_err(|_| AkiDbError::InvalidArgument("Write path lock poisoned".to_string()))?;
            write_path.peek_buffer().to_vec()
        } else {
            Vec::new()
        };

        if opts.mode == SearchMode::Vector {
            let snapshot = self.build_vector_snapshot(&opts, &buffer_records)?;
            let mut response = self.query_engine.search_vector_with_snapshot(
                &self.storage,
                &opts,
                &buffer_records,
                &snapshot,
            )?;

            if opts.explain {
                let metadata = self.lock_metadata()?;
                let preview_map = Self::load_previews(&metadata, &response.results);
                let matched_terms = Self::matched_terms(opts.query_text.as_deref());
                for (rank, r) in response.results.iter_mut().enumerate() {
                    r.explain = Some(Self::build_explain(
                        &preview_map,
                        &r.chunk_id,
                        &matched_terms,
                        Some(r.score),
                        None,
                        None,
                        Some(rank + 1),
                        None,
                    ));
                }
            }

            Ok(response)
        } else if opts.mode == SearchMode::Keyword {
            let snapshot = self.build_manifest_search_snapshot(&opts)?;
            let mut response = {
                let metadata = self.lock_metadata()?;
                let query_text = opts.query_text.as_deref().unwrap_or("");
                let results = keyword::keyword_search(
                    &metadata,
                    &opts.collection_id,
                    query_text,
                    opts.top_k,
                    &snapshot.live_tombstone_set,
                )?;
                SearchResponse {
                    results,
                    manifest_version_used: snapshot.manifest.version,
                }
            };

            if opts.explain {
                let preview_map = {
                    let metadata = self.lock_metadata()?;
                    Self::load_previews(&metadata, &response.results)
                };
                let matched_terms = Self::matched_terms(opts.query_text.as_deref());
                for (rank, r) in response.results.iter_mut().enumerate() {
                    r.explain = Some(Self::build_explain(
                        &preview_map,
                        &r.chunk_id,
                        &matched_terms,
                        None,
                        Some(r.score),
                        None,
                        None,
                        Some(rank + 1),
                    ));
                }
            }

            Ok(response)
        } else if opts.mode == SearchMode::Hybrid {
            let snapshot = self.build_vector_snapshot(&opts, &buffer_records)?;
            let vector_opts = SearchOptions {
                collection_id: opts.collection_id.clone(),
                query_vector: opts.query_vector.clone(),
                top_k: opts.top_k * 2,
                filters: opts.filters.clone(),
                manifest_version: opts.manifest_version,
                include_uncommitted: opts.include_uncommitted,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: opts.vector_weight,
                keyword_weight: opts.keyword_weight,
                explain: false,
                ef_search: opts.ef_search,
            };
            let vector_response = self.query_engine.search_vector_with_snapshot(
                &self.storage,
                &vector_opts,
                &buffer_records,
                &snapshot,
            )?;

            let (keyword_results, preview_map) = {
                let metadata = self.lock_metadata()?;
                let query_text = opts.query_text.as_deref().unwrap_or("");
                let keyword_results = keyword::keyword_search(
                    &metadata,
                    &opts.collection_id,
                    query_text,
                    opts.top_k * 2,
                    &snapshot.live_tombstone_set,
                )?;
                let preview_map = if opts.explain {
                    let fused_preview_ids = hybrid::rrf_fuse(
                        &vector_response.results,
                        &keyword_results,
                        opts.top_k,
                        opts.vector_weight,
                        opts.keyword_weight,
                    )
                    .into_iter()
                    .map(|r| r.chunk_id)
                    .collect::<Vec<_>>();
                    metadata
                        .fts_get_texts(&fused_preview_ids)
                        .unwrap_or_default()
                } else {
                    HashMap::new()
                };
                (keyword_results, preview_map)
            };

            let mut fused = hybrid::rrf_fuse(
                &vector_response.results,
                &keyword_results,
                opts.top_k,
                opts.vector_weight,
                opts.keyword_weight,
            );

            if opts.explain {
                let matched_terms = Self::matched_terms(opts.query_text.as_deref());
                let vector_map: HashMap<String, (usize, f64)> = vector_response
                    .results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (r.chunk_id.clone(), (i + 1, r.score)))
                    .collect();
                let keyword_map: HashMap<String, (usize, f64)> = keyword_results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (r.chunk_id.clone(), (i + 1, r.score)))
                    .collect();

                for r in &mut fused {
                    let v_info = vector_map.get(&r.chunk_id);
                    let k_info = keyword_map.get(&r.chunk_id);
                    r.explain = Some(Self::build_explain(
                        &preview_map,
                        &r.chunk_id,
                        &matched_terms,
                        v_info.map(|(_, s)| *s),
                        k_info.map(|(_, s)| *s),
                        Some(r.score),
                        v_info.map(|(rank, _)| *rank),
                        k_info.map(|(rank, _)| *rank),
                    ));
                }
            }

            Ok(SearchResponse {
                results: std::mem::take(&mut fused),
                manifest_version_used: snapshot.manifest.version,
            })
        } else {
            let metadata = self.lock_metadata()?;
            self.query_engine
                .search(&metadata, &self.storage, &opts, &buffer_records)
        }
    }

    // ─── Delete ─────────────────────────────────────────────────────────────

    pub fn delete_chunks(
        &self,
        collection_id: &str,
        chunk_ids: &[String],
        reason_code: &str,
    ) -> Result<usize> {
        let metadata = self.lock_metadata()?;
        let now = iso_now();
        let mut count = 0;
        for chunk_id in chunk_ids {
            metadata.create_tombstone(&Tombstone {
                chunk_id: chunk_id.clone(),
                collection_id: collection_id.to_string(),
                deleted_at: now.clone(),
                reason_code: reason_code.to_string(),
            })?;
            // Remove from FTS5 index.
            let _ = metadata.fts_delete(chunk_id, collection_id);
            count += 1;
        }
        Ok(count)
    }

    // ─── Compaction ─────────────────────────────────────────────────────────

    pub fn compact(&self, collection_id: &str) -> Result<CompactResult> {
        let metadata = self.lock_metadata()?;
        compact(&metadata, &self.storage, collection_id)
    }

    // ─── Rollback ───────────────────────────────────────────────────────────

    pub fn rollback(&self, collection_id: &str, manifest_id: &str) -> Result<Manifest> {
        let metadata = self.lock_metadata()?;
        let manifest = ManifestManager::rollback(&metadata, collection_id, manifest_id)?;
        let pending_tombstone_ids = metadata.list_tombstone_chunk_ids(collection_id)?;
        if !pending_tombstone_ids.is_empty() {
            metadata.delete_tombstones(&pending_tombstone_ids)?;
        }
        self.rebuild_fts_for_manifest(&metadata, &manifest)?;
        Ok(manifest)
    }

    // ─── Introspection ──────────────────────────────────────────────────────

    pub fn get_storage_size_bytes(&self) -> Result<u64> {
        let mut total = 0u64;
        let segments_dir = self.storage_path.join("segments");
        if segments_dir.exists() {
            total += dir_size(&segments_dir)?;
        }
        let db_path = self.storage_path.join("metadata.db");
        if db_path.exists() {
            total += std::fs::metadata(&db_path)?.len();
        }
        Ok(total)
    }

    pub fn get_tombstone_count(&self, collection_id: &str) -> Result<i64> {
        let metadata = self.lock_metadata()?;
        metadata.get_tombstone_count(collection_id)
    }

    pub fn get_segment_count(&self, collection_id: &str) -> Result<i64> {
        let metadata = self.lock_metadata()?;
        let segments = metadata.list_segments(collection_id, Some("ready"))?;
        Ok(segments.len() as i64)
    }

    // ─── Close ──────────────────────────────────────────────────────────────

    pub fn close(&self) -> Result<()> {
        let mut write_paths = self.lock_write_paths()?;
        for wp in write_paths.values() {
            let mut wp = wp
                .lock()
                .map_err(|_| AkiDbError::InvalidArgument("Write path lock poisoned".to_string()))?;
            wp.close();
        }
        write_paths.clear();
        Ok(())
    }

    // ─── Internal ───────────────────────────────────────────────────────────

    fn get_or_create_write_path(&self, collection_id: &str) -> Result<Arc<Mutex<WritePath>>> {
        let mut write_paths = self.lock_write_paths()?;
        if !write_paths.contains_key(collection_id) {
            let wal_path = if self.disable_wal {
                None
            } else {
                Some(self.storage_path.join(format!("wal/{}.wal", collection_id)))
            };

            let wp = Arc::new(Mutex::new(WritePath::new(
                Arc::clone(&self.metadata),
                Arc::clone(&self.storage),
                WritePathOptions {
                    max_records: None,
                    max_bytes: None,
                    wal_path,
                },
            )?));

            write_paths.insert(collection_id.to_string(), wp);
        }
        Ok(write_paths.get(collection_id).cloned().unwrap())
    }

    fn build_vector_snapshot(
        &self,
        opts: &SearchOptions,
        _buffer_records: &[NativeRecord],
    ) -> Result<VectorSearchSnapshot> {
        let metadata = self.lock_metadata()?;
        let collection = metadata
            .get_collection(&opts.collection_id)?
            .ok_or_else(|| AkiDbError::CollectionNotFound(opts.collection_id.clone()))?;
        let snapshot = build_manifest_search_snapshot_from_metadata(&metadata, opts)?;
        let live_tombstone_fingerprint = tombstone_fingerprint(&snapshot.live_tombstone_set);
        let segment_paths: Vec<(String, String)> = snapshot
            .manifest
            .segment_ids
            .iter()
            .map(|segment_id| {
                metadata
                    .get_segment(segment_id)?
                    .map(|segment| (segment_id.clone(), segment.storage_path))
                    .ok_or_else(|| AkiDbError::SegmentNotFound(segment_id.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(VectorSearchSnapshot {
            collection,
            manifest: snapshot.manifest,
            segment_paths,
            live_tombstone_set: snapshot.live_tombstone_set,
            live_tombstone_fingerprint,
        })
    }

    fn build_manifest_search_snapshot(
        &self,
        opts: &SearchOptions,
    ) -> Result<ManifestSearchSnapshot> {
        let metadata = self.lock_metadata()?;
        build_manifest_search_snapshot_from_metadata(&metadata, opts)
    }

    fn lock_metadata(&self) -> Result<MutexGuard<'_, MetadataStore>> {
        self.metadata
            .lock()
            .map_err(|_| AkiDbError::InvalidArgument("Metadata lock poisoned".to_string()))
    }

    fn lock_write_paths(&self) -> Result<MutexGuard<'_, HashMap<String, Arc<Mutex<WritePath>>>>> {
        self.write_paths
            .lock()
            .map_err(|_| AkiDbError::InvalidArgument("Write paths lock poisoned".to_string()))
    }

    fn load_previews(
        metadata: &MetadataStore,
        results: &[SearchResult],
    ) -> HashMap<String, String> {
        metadata
            .fts_get_texts(
                &results
                    .iter()
                    .map(|result| result.chunk_id.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_default()
    }

    fn matched_terms(query_text: Option<&str>) -> Vec<String> {
        query_text
            .map(|text| {
                text.split_whitespace()
                    .map(|term| term.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    fn build_explain(
        preview_map: &HashMap<String, String>,
        chunk_id: &str,
        matched_terms: &[String],
        vector_score: Option<f64>,
        bm25_score: Option<f64>,
        rrf_score: Option<f64>,
        vector_rank: Option<usize>,
        bm25_rank: Option<usize>,
    ) -> ExplainInfo {
        ExplainInfo {
            vector_score,
            bm25_score,
            rrf_score,
            vector_rank,
            bm25_rank,
            chunk_preview: preview_map
                .get(chunk_id)
                .map(|text| text.chars().take(200).collect()),
            matched_terms: matched_terms.to_vec(),
        }
    }

    fn rebuild_fts_for_manifest(
        &self,
        metadata: &MetadataStore,
        manifest: &Manifest,
    ) -> Result<()> {
        metadata.fts_delete_collection(&manifest.collection_id)?;

        let tombstone_set: std::collections::HashSet<&str> =
            manifest.tombstone_ids.iter().map(String::as_str).collect();
        let mut fts_owned: Vec<(String, String, String)> = Vec::new();

        for segment_id in &manifest.segment_ids {
            let segment = metadata
                .get_segment(segment_id)?
                .ok_or_else(|| AkiDbError::SegmentNotFound(segment_id.clone()))?;
            let buffer = self.storage.get_object(&segment.storage_path)?;
            let reader = crate::segment::reader::SegmentReader::from_buffer(buffer)?;
            let chunk_ids = reader.get_chunk_ids()?;
            let Some(chunk_texts) = reader.get_chunk_texts() else {
                continue;
            };

            for (chunk_id, chunk_text) in chunk_ids.into_iter().zip(chunk_texts) {
                if tombstone_set.contains(chunk_id.as_str()) || chunk_text.is_empty() {
                    continue;
                }
                fts_owned.push((chunk_id, manifest.collection_id.clone(), chunk_text));
            }
        }

        if !fts_owned.is_empty() {
            let fts_refs: Vec<(&str, &str, &str)> = fts_owned
                .iter()
                .map(|(chunk_id, collection_id, chunk_text)| {
                    (
                        chunk_id.as_str(),
                        collection_id.as_str(),
                        chunk_text.as_str(),
                    )
                })
                .collect();
            metadata.fts_insert_batch(&fts_refs)?;
        }

        Ok(())
    }
}

fn dir_size(path: &std::path::Path) -> Result<u64> {
    let mut total = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_file() {
                total += entry.metadata()?.len();
            } else if ft.is_dir() {
                total += dir_size(&entry.path())?;
            }
        }
    }
    Ok(total)
}

fn build_manifest_search_snapshot_from_metadata(
    metadata: &MetadataStore,
    opts: &SearchOptions,
) -> Result<ManifestSearchSnapshot> {
    let manifest = if let Some(version) = opts.manifest_version {
        metadata
            .get_manifest_by_version(&opts.collection_id, version)?
            .ok_or_else(|| {
                AkiDbError::ManifestNotFound(format!(
                    "version {} for collection \"{}\"",
                    version, opts.collection_id
                ))
            })?
    } else {
        metadata
            .get_latest_manifest(&opts.collection_id)?
            .ok_or_else(|| {
                AkiDbError::ManifestNotFound(format!(
                    "no manifest published for collection \"{}\"",
                    opts.collection_id
                ))
            })?
    };

    let live_tombstone_set = if opts.include_uncommitted && opts.manifest_version.is_none() {
        manifest
            .tombstone_ids
            .iter()
            .cloned()
            .chain(metadata.list_tombstone_chunk_ids(&opts.collection_id)?)
            .collect()
    } else {
        manifest.tombstone_ids.iter().cloned().collect()
    };

    Ok(ManifestSearchSnapshot {
        manifest,
        live_tombstone_set,
    })
}

fn iso_now() -> String {
    crate::write::epoch_to_iso8601(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_search_errors_when_manifest_references_missing_segment() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EngineInner::open(EngineOptions {
            storage_path: dir.path().to_path_buf(),
            disable_wal: true,
        })
        .unwrap();

        engine
            .create_collection("docs", 4, "cosine", "model", "fp16", 16, 200, 100)
            .unwrap();

        {
            let metadata = engine.lock_metadata().unwrap();
            metadata
                .create_manifest(&Manifest {
                    manifest_id: "m-0".to_string(),
                    collection_id: "docs".to_string(),
                    version: 0,
                    segment_ids: vec!["missing-segment".to_string()],
                    tombstone_ids: Vec::new(),
                    embedding_model_id: "model".to_string(),
                    pipeline_signature: "sig".to_string(),
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    checksum: "checksum".to_string(),
                })
                .unwrap();
        }

        let result = engine.search(SearchOptions {
            collection_id: "docs".to_string(),
            query_vector: vec![0.1, 0.2, 0.3, 0.4],
            top_k: 5,
            filters: None,
            manifest_version: None,
            include_uncommitted: false,
            mode: SearchMode::Vector,
            query_text: None,
            vector_weight: 1.0,
            keyword_weight: 1.0,
            explain: false,
            ef_search: None,
        });

        assert!(
            matches!(result, Err(AkiDbError::SegmentNotFound(segment_id)) if segment_id == "missing-segment")
        );
    }

    #[test]
    fn search_excludes_unpublished_tombstones_even_without_buffered_writes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EngineInner::open(EngineOptions {
            storage_path: dir.path().to_path_buf(),
            disable_wal: true,
        })
        .unwrap();

        engine
            .create_collection("docs", 2, "cosine", "model", "fp16", 16, 200, 100)
            .unwrap();

        let records = vec![
            NativeRecord {
                chunk_id: "chunk-a".to_string(),
                doc_id: "doc-a".to_string(),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"topic": "alpha"}),
                chunk_text: Some("alpha document".to_string()),
            },
            NativeRecord {
                chunk_id: "chunk-b".to_string(),
                doc_id: "doc-b".to_string(),
                vector: vec![0.0, 1.0],
                metadata: serde_json::json!({"topic": "beta"}),
                chunk_text: Some("beta document".to_string()),
            },
        ];

        engine.upsert_batch("docs", &records).unwrap();
        engine.auto_publish("docs", "model", "sig").unwrap();
        engine
            .delete_chunks("docs", &[String::from("chunk-a")], "manual_revoke")
            .unwrap();

        {
            let metadata = engine.lock_metadata().unwrap();
            let tombstones = metadata.list_tombstone_chunk_ids("docs").unwrap();
            assert_eq!(tombstones, vec![String::from("chunk-a")]);
        }

        let snapshot = engine
            .build_vector_snapshot(
                &SearchOptions {
                    collection_id: "docs".to_string(),
                    query_vector: vec![1.0, 0.0],
                    top_k: 5,
                    filters: None,
                    manifest_version: None,
                    include_uncommitted: true,
                    mode: SearchMode::Vector,
                    query_text: None,
                    vector_weight: 1.0,
                    keyword_weight: 1.0,
                    explain: false,
                    ef_search: None,
                },
                &[],
            )
            .unwrap();
        assert!(snapshot.live_tombstone_set.contains("chunk-a"));

        let vector_result = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            vector_result
                .results
                .iter()
                .all(|r| r.chunk_id != "chunk-a"),
            "vector search should exclude chunk-a after an unpublished tombstone"
        );

        let hybrid_result = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Hybrid,
                query_text: Some("alpha".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            hybrid_result
                .results
                .iter()
                .all(|r| r.chunk_id != "chunk-a"),
            "hybrid search should exclude chunk-a after an unpublished tombstone"
        );

        let published_manifest = engine.auto_publish("docs", "model", "sig-v2").unwrap();
        assert_eq!(
            published_manifest.tombstone_ids,
            vec![String::from("chunk-a")]
        );

        let published_result = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            published_result
                .results
                .iter()
                .all(|r| r.chunk_id != "chunk-a"),
            "vector search should exclude chunk-a after the tombstone is published"
        );

        let rollback_manifest = engine
            .rollback("docs", &snapshot.manifest.manifest_id)
            .unwrap();
        assert!(rollback_manifest.tombstone_ids.is_empty());

        let rollback_result = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            rollback_result
                .results
                .iter()
                .any(|r| r.chunk_id == "chunk-a"),
            "vector search should restore chunk-a after rollback to a manifest without tombstones"
        );

        let rollback_keyword = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Keyword,
                query_text: Some("alpha".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            rollback_keyword
                .results
                .iter()
                .any(|r| r.chunk_id == "chunk-a"),
            "keyword search should restore chunk-a after rollback to a manifest without tombstones"
        );

        let rollback_hybrid = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Hybrid,
                query_text: Some("alpha".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            rollback_hybrid
                .results
                .iter()
                .any(|r| r.chunk_id == "chunk-a"),
            "hybrid search should restore chunk-a after rollback to a manifest without tombstones"
        );
    }

    #[test]
    fn rollback_rebuilds_keyword_state_for_current_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EngineInner::open(EngineOptions {
            storage_path: dir.path().to_path_buf(),
            disable_wal: true,
        })
        .unwrap();

        engine
            .create_collection("docs", 2, "cosine", "model", "fp16", 16, 200, 100)
            .unwrap();

        engine
            .upsert_batch(
                "docs",
                &[NativeRecord {
                    chunk_id: "chunk-v0".to_string(),
                    doc_id: "doc-v0".to_string(),
                    vector: vec![1.0, 0.0],
                    metadata: serde_json::json!({"topic": "baseline"}),
                    chunk_text: Some("baseline release notes".to_string()),
                }],
            )
            .unwrap();
        let manifest0 = engine.auto_publish("docs", "model", "sig-v0").unwrap();

        engine
            .upsert_batch(
                "docs",
                &[NativeRecord {
                    chunk_id: "chunk-v1".to_string(),
                    doc_id: "doc-v1".to_string(),
                    vector: vec![0.0, 1.0],
                    metadata: serde_json::json!({"topic": "new"}),
                    chunk_text: Some("new feature launch".to_string()),
                }],
            )
            .unwrap();
        engine.auto_publish("docs", "model", "sig-v1").unwrap();

        let before_rollback = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![0.0, 1.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Keyword,
                query_text: Some("new feature".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(before_rollback
            .results
            .iter()
            .any(|r| r.chunk_id == "chunk-v1"));

        engine.rollback("docs", &manifest0.manifest_id).unwrap();

        let after_rollback_old = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Keyword,
                query_text: Some("baseline release".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(after_rollback_old
            .results
            .iter()
            .any(|r| r.chunk_id == "chunk-v0"));

        let after_rollback_new = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![0.0, 1.0],
                top_k: 5,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Keyword,
                query_text: Some("new feature".to_string()),
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(
            after_rollback_new
                .results
                .iter()
                .all(|r| r.chunk_id != "chunk-v1"),
            "keyword search should not return chunks from manifests newer than the rollback target"
        );
    }

    #[test]
    fn compact_applies_pending_tombstones_and_preserves_history() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EngineInner::open(EngineOptions {
            storage_path: dir.path().to_path_buf(),
            disable_wal: true,
        })
        .unwrap();

        engine
            .create_collection("docs", 2, "cosine", "model", "fp16", 16, 200, 100)
            .unwrap();

        let first_batch: Vec<NativeRecord> = (0..5)
            .map(|i| NativeRecord {
                chunk_id: format!("chunk-{i}"),
                doc_id: format!("doc-{i}"),
                vector: vec![1.0 - (i as f32 * 0.1), i as f32 * 0.1],
                metadata: serde_json::json!({ "batch": 1, "offset": i }),
                chunk_text: Some(format!("batch one chunk {i}")),
            })
            .collect();
        let second_batch: Vec<NativeRecord> = (5..10)
            .map(|i| NativeRecord {
                chunk_id: format!("chunk-{i}"),
                doc_id: format!("doc-{i}"),
                vector: vec![1.0 - (i as f32 * 0.05), i as f32 * 0.05],
                metadata: serde_json::json!({ "batch": 2, "offset": i }),
                chunk_text: Some(format!("batch two chunk {i}")),
            })
            .collect();

        engine.upsert_batch("docs", &first_batch).unwrap();
        engine.flush_writes("docs").unwrap();
        engine.upsert_batch("docs", &second_batch).unwrap();
        engine.flush_writes("docs").unwrap();

        let manifest0 = engine.auto_publish("docs", "model", "sig-v1").unwrap();
        assert_eq!(manifest0.segment_ids.len(), 2);

        engine
            .delete_chunks("docs", &[String::from("chunk-3")], "manual_revoke")
            .unwrap();

        let compact_result = engine.compact("docs").unwrap();
        assert_eq!(compact_result.manifest.segment_ids.len(), 1);
        assert!(compact_result.manifest.tombstone_ids.is_empty());
        assert_eq!(compact_result.records_kept, 9);
        assert_eq!(compact_result.records_removed, 1);

        let latest = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 20,
                filters: None,
                manifest_version: None,
                include_uncommitted: true,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(latest.results.iter().all(|r| r.chunk_id != "chunk-3"));

        let historical = engine
            .search(SearchOptions {
                collection_id: "docs".to_string(),
                query_vector: vec![1.0, 0.0],
                top_k: 20,
                filters: None,
                manifest_version: Some(manifest0.version),
                include_uncommitted: true,
                mode: SearchMode::Vector,
                query_text: None,
                vector_weight: 1.0,
                keyword_weight: 1.0,
                explain: false,
                ef_search: None,
            })
            .unwrap();
        assert!(historical.results.iter().any(|r| r.chunk_id == "chunk-3"));
    }
}
