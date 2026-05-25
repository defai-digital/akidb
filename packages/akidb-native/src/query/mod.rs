//! QueryEngine — manifest-bound search across segments.
//!
//! Supports three search modes:
//!   - **Vector** (default): ANN search via HNSW index.
//!   - **Keyword**: BM25 full-text search via FTS5.
//!   - **Hybrid**: Combines vector + keyword using Reciprocal Rank Fusion (RRF).
//!
//! For vector search, the engine:
//!   1. Resolves the target manifest (latest or a specific version).
//!   2. Loads every segment from storage.
//!   3. Builds/deserializes indexes and performs ANN search.
//!   4. Filters out tombstoned chunk_ids.
//!   5. Applies optional metadata filters.
//!   6. Optionally scans the in-memory write buffer.
//!   7. Merges and deduplicates results.
//!   8. Returns top-K with deterministic tie-breaking.

pub mod hybrid;
pub mod keyword;

use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use crate::distance;
use crate::error::{AkiDbError, Result};
use crate::hnsw::HnswGraph;
use crate::index::{ExplainInfo, SearchResult};
use crate::metadata::{Collection, Manifest, MetadataStore};
use crate::segment::reader::SegmentReader;
use crate::storage::LocalFsBackend;
use crate::write::{HnswParams, NativeRecord};

// ─── Types ───────────────────────────────────────────────────────────────────

/// Search mode: vector (ANN), keyword (BM25), or hybrid (RRF fusion).
#[derive(Debug, Clone, PartialEq)]
pub enum SearchMode {
    Vector,
    Keyword,
    Hybrid,
}

impl SearchMode {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "keyword" => Self::Keyword,
            "hybrid" => Self::Hybrid,
            _ => Self::Vector,
        }
    }
}

#[derive(Clone)]
pub struct SearchOptions {
    pub collection_id: String,
    pub query_vector: Vec<f32>,
    pub top_k: usize,
    pub filters: Option<serde_json::Value>,
    pub manifest_version: Option<i64>,
    pub include_uncommitted: bool,
    pub mode: SearchMode,
    pub query_text: Option<String>,
    pub vector_weight: f64,
    pub keyword_weight: f64,
    pub explain: bool,
    /// Per-query ef_search override. If None, uses the collection's default.
    pub ef_search: Option<usize>,
}

pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub manifest_version_used: i64,
}

pub struct VectorSearchSnapshot {
    pub collection: Collection,
    pub manifest: Manifest,
    pub segment_paths: Vec<(String, String)>,
    pub live_tombstone_set: HashSet<String>,
    pub live_tombstone_fingerprint: u64,
}

// ─── LRU Index Cache ─────────────────────────────────────────────────────────

struct CachedIndex {
    graph: HnswGraph,
    /// Chunk IDs of the active (non-tombstoned) nodes in graph-node order.
    active_chunk_ids: Vec<String>,
    /// Maps graph node index → original segment position (for metadata filter closure).
    node_to_segment: Vec<usize>,
    memory_size_bytes: usize,
}

struct IndexCache {
    entries: HashMap<String, Arc<CachedIndex>>,
    max_memory_bytes: usize,
    current_memory_bytes: usize,
    // LRU order tracking (Vec used as simplified LRU).
    order: Vec<String>,
}

impl IndexCache {
    fn new(max_memory_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_memory_bytes,
            current_memory_bytes: 0,
            order: Vec::new(),
        }
    }

    fn cache_key(segment_id: &str, manifest_version: i64, tombstone_fingerprint: u64) -> String {
        format!("{segment_id}:v{manifest_version}:t{tombstone_fingerprint:016x}")
    }

    fn get(&mut self, key: &str) -> Option<Arc<CachedIndex>> {
        if self.entries.contains_key(key) {
            // Move to end of LRU order.
            self.order.retain(|k| k != key);
            self.order.push(key.to_string());
            self.entries.get(key).cloned()
        } else {
            None
        }
    }

    fn set(&mut self, key: String, entry: CachedIndex) {
        let entry = Arc::new(entry);
        // Remove existing entry if present.
        if let Some(old) = self.entries.remove(&key) {
            self.current_memory_bytes = self.current_memory_bytes.saturating_sub(old.memory_size_bytes);
            self.order.retain(|k| k != &key);
        }

        // Evict LRU entries until we have room.
        while self.current_memory_bytes + entry.memory_size_bytes > self.max_memory_bytes
            && !self.order.is_empty()
        {
            let oldest = self.order.remove(0);
            if let Some(removed) = self.entries.remove(&oldest) {
                self.current_memory_bytes = self.current_memory_bytes.saturating_sub(removed.memory_size_bytes);
            }
        }

        self.current_memory_bytes += entry.memory_size_bytes;
        self.order.push(key.clone());
        self.entries.insert(key, entry);
    }

}

// ─── SegmentSearchParams ─────────────────────────────────────────────────────

struct SegmentSearchParams<'a> {
    query_vector: &'a [f32],
    top_k: usize,
    metric: &'a str,
    tombstone_set: &'a HashSet<String>,
    filters: Option<&'a serde_json::Value>,
    hnsw: HnswParams,
    manifest_version: i64,
    tombstone_fingerprint: u64,
}

// ─── QueryEngine ─────────────────────────────────────────────────────────────

pub struct QueryEngine {
    // Mutex gives interior mutability so `search` can take `&self`, allowing the
    // caller to hold a read lock on EngineInner while concurrent searches run.
    index_cache: Mutex<IndexCache>,
}

impl QueryEngine {
    pub fn new(cache_max_memory_bytes: Option<usize>) -> Self {
        Self {
            index_cache: Mutex::new(IndexCache::new(
                cache_max_memory_bytes.unwrap_or(512 * 1024 * 1024),
            )),
        }
    }

    /// Proactively load HNSW graphs for the given segment storage paths into the LRU cache.
    ///
    /// Called after `publish()` so the first search query avoids the cold-start
    /// deserialization cost.  Errors are silently ignored — prewarm is best-effort.
    #[allow(clippy::too_many_arguments)]
    pub fn prewarm_paths(
        &self,
        storage: &LocalFsBackend,
        segment_paths: &[(String, String)],
        manifest_version: i64,
        tombstone_set: &HashSet<String>,
        tombstone_fingerprint: u64,
        hnsw: HnswParams,
        metric: &str,
    ) {
        for (segment_id, storage_path) in segment_paths {
            let Ok(buffer) = storage.get_object(storage_path) else { continue };
            let Ok(reader) = SegmentReader::from_buffer(buffer) else { continue };
            let Ok(chunk_ids) = reader.get_chunk_ids() else { continue };

            let all_active_indices: Vec<usize> = (0..chunk_ids.len())
                .filter(|i| !tombstone_set.contains(&chunk_ids[*i]))
                .collect();
            if all_active_indices.is_empty() {
                continue;
            }

            let active_chunk_ids: Vec<String> =
                all_active_indices.iter().map(|&i| chunk_ids[i].clone()).collect();
            let cache_key =
                IndexCache::cache_key(segment_id, manifest_version, tombstone_fingerprint);

            // Skip if already warm (brief lock, no I/O held).
            {
                let mut cache = self.index_cache.lock().unwrap_or_else(|e| e.into_inner());
                if cache.get(&cache_key).is_some() {
                    continue;
                }
            }

            // Load vectors only after confirming a cache miss.
            let Ok(vectors) = reader.get_vectors() else { continue };
            let active_vectors: Vec<Vec<f32>> =
                all_active_indices.iter().map(|&i| vectors[i].clone()).collect();
            let node_to_segment: Vec<usize> = all_active_indices.clone();
            let dimension = reader.dimension() as usize;
            let mut graph = HnswGraph::new(metric, dimension, hnsw.m, hnsw.ef_construction, hnsw.ef_search);

            let index_data = reader.get_index_data();
            if !index_data.is_empty() && all_active_indices.len() == chunk_ids.len() {
                if graph.deserialize(index_data).is_err() {
                    graph.build(&active_vectors);
                }
            } else {
                graph.build(&active_vectors);
            }

            let vector_bytes = active_vectors.len() * dimension * 4;
            let graph_bytes = active_vectors.len() * hnsw.m * 4;
            let mut cache = self.index_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.set(
                cache_key,
                CachedIndex {
                    graph,
                    active_chunk_ids,
                    node_to_segment,
                    memory_size_bytes: vector_bytes + graph_bytes,
                },
            );
        }
    }

    /// Search across all segments in a manifest.
    ///
    /// Routes to vector, keyword, or hybrid search based on `opts.mode`.
    pub fn search(
        &self,
        metadata: &MetadataStore,
        storage: &LocalFsBackend,
        opts: &SearchOptions,
        buffer_records: &[NativeRecord],
    ) -> Result<SearchResponse> {
        self.validate_search_opts(opts)?;

        let manifest = self.resolve_manifest(metadata, &opts.collection_id, opts.manifest_version)?;

        let mut response = match opts.mode {
            SearchMode::Vector => {
                self.search_vector(metadata, storage, opts, buffer_records, &manifest)
            }
            SearchMode::Keyword => {
                self.search_keyword(metadata, opts, &manifest)
            }
            SearchMode::Hybrid => {
                self.search_hybrid(metadata, storage, opts, buffer_records, &manifest)
            }
        }?;

        // Populate explain info for vector/keyword-only modes (hybrid populates its own).
        if opts.explain && opts.mode != SearchMode::Hybrid {
            let query_text = opts.query_text.as_deref();
            let preview_map = metadata
                .fts_get_texts(
                    &response
                        .results
                        .iter()
                        .map(|r| r.chunk_id.clone())
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_default();
            for (rank, r) in response.results.iter_mut().enumerate() {
                let preview = preview_map
                    .get(&r.chunk_id)
                    .map(|t| t.chars().take(200).collect());
                r.explain = Some(ExplainInfo {
                    vector_score: if opts.mode == SearchMode::Vector { Some(r.score) } else { None },
                    bm25_score: if opts.mode == SearchMode::Keyword { Some(r.score) } else { None },
                    rrf_score: None,
                    vector_rank: if opts.mode == SearchMode::Vector { Some(rank + 1) } else { None },
                    bm25_rank: if opts.mode == SearchMode::Keyword { Some(rank + 1) } else { None },
                    chunk_preview: preview,
                    matched_terms: query_text
                        .map(|qt| qt.split_whitespace().map(|s| s.to_string()).collect())
                        .unwrap_or_default(),
                });
            }
        }

        Ok(response)
    }

    pub fn search_vector_with_snapshot(
        &self,
        storage: &LocalFsBackend,
        opts: &SearchOptions,
        buffer_records: &[NativeRecord],
        snapshot: &VectorSearchSnapshot,
    ) -> Result<SearchResponse> {
        let tombstone_set = &snapshot.live_tombstone_set;
        let tombstone_fingerprint = snapshot.live_tombstone_fingerprint;
        let collection = &snapshot.collection;
        let manifest = &snapshot.manifest;

        let ef_search = opts.ef_search.unwrap_or(collection.hnsw_ef_search as usize);
        let mut all_results: Vec<SearchResult> = Vec::new();

        for (segment_id, storage_path) in &snapshot.segment_paths {
            let seg_results = self.search_segment_by_path(
                storage,
                segment_id,
                storage_path,
                &SegmentSearchParams {
                    query_vector: &opts.query_vector,
                    top_k: opts.top_k,
                    metric: &collection.metric,
                    tombstone_set,
                    filters: opts.filters.as_ref(),
                    hnsw: HnswParams {
                        m: collection.hnsw_m as usize,
                        ef_construction: collection.hnsw_ef_construction as usize,
                        ef_search,
                    },
                    manifest_version: manifest.version,
                    tombstone_fingerprint,
                },
            )?;
            for mut r in seg_results {
                r.committed = Some(true);
                all_results.push(r);
            }
        }

        if opts.include_uncommitted && !buffer_records.is_empty() {
            let buffer_results = self.search_buffer_brute_force(
                buffer_records,
                &opts.query_vector,
                opts.top_k,
                &collection.metric,
                opts.filters.as_ref(),
                tombstone_set,
            );
            for mut r in buffer_results {
                r.committed = Some(false);
                all_results.push(r);
            }
        }

        Ok(SearchResponse {
            results: Self::merge_and_dedup(all_results, opts.top_k),
            manifest_version_used: manifest.version,
        })
    }

    /// Vector-only search (original path).
    fn search_vector(
        &self,
        metadata: &MetadataStore,
        storage: &LocalFsBackend,
        opts: &SearchOptions,
        buffer_records: &[NativeRecord],
        manifest: &Manifest,
    ) -> Result<SearchResponse> {
        let collection = metadata
            .get_collection(&opts.collection_id)?
            .ok_or_else(|| AkiDbError::CollectionNotFound(opts.collection_id.clone()))?;

        let tombstone_set: HashSet<String> = manifest.tombstone_ids.iter().cloned().collect();

        // Resolve ef_search: per-query override > collection default.
        let ef_search = opts.ef_search.unwrap_or(collection.hnsw_ef_search as usize);

        let mut all_results: Vec<SearchResult> = Vec::new();

        // Search each segment.
        for segment_id in &manifest.segment_ids {
            let seg_results = self.search_segment(
                metadata,
                storage,
                segment_id,
                &SegmentSearchParams {
                    query_vector: &opts.query_vector,
                    top_k: opts.top_k,
                    metric: &collection.metric,
                    tombstone_set: &tombstone_set,
                    filters: opts.filters.as_ref(),
                    hnsw: HnswParams {
                        m: collection.hnsw_m as usize,
                        ef_construction: collection.hnsw_ef_construction as usize,
                        ef_search,
                    },
                    manifest_version: manifest.version,
                    tombstone_fingerprint: tombstone_fingerprint(&tombstone_set),
                },
            )?;
            for mut r in seg_results {
                r.committed = Some(true);
                all_results.push(r);
            }
        }

        // Streaming search: scan buffer.
        if opts.include_uncommitted && !buffer_records.is_empty() {
            // Build a comprehensive tombstone set that covers both published tombstones
            // (from the manifest) and live unpublished tombstones (from deleteChunks calls
            // that haven't been included in a manifest yet).
            let live_tombstone_set: HashSet<String> = metadata
                .list_tombstone_chunk_ids(&opts.collection_id)
                .map(|ids| ids.into_iter().collect())
                .unwrap_or_else(|_| tombstone_set.clone());

            let buffer_results = self.search_buffer_brute_force(
                buffer_records,
                &opts.query_vector,
                opts.top_k,
                &collection.metric,
                opts.filters.as_ref(),
                &live_tombstone_set,
            );
            for mut r in buffer_results {
                r.committed = Some(false);
                all_results.push(r);
            }
        }

        let deduped = Self::merge_and_dedup(all_results, opts.top_k);

        Ok(SearchResponse {
            results: deduped,
            manifest_version_used: manifest.version,
        })
    }

    /// Keyword-only search via FTS5 BM25.
    fn search_keyword(
        &self,
        metadata: &MetadataStore,
        opts: &SearchOptions,
        manifest: &Manifest,
    ) -> Result<SearchResponse> {
        let query_text = opts.query_text.as_deref().unwrap_or("");
        let tombstone_set: HashSet<String> = manifest.tombstone_ids.iter().cloned().collect();

        let results = keyword::keyword_search(
            metadata,
            &opts.collection_id,
            query_text,
            opts.top_k,
            &tombstone_set,
        )?;

        Ok(SearchResponse {
            results,
            manifest_version_used: manifest.version,
        })
    }

    /// Hybrid search: vector + keyword fused via RRF.
    fn search_hybrid(
        &self,
        metadata: &MetadataStore,
        storage: &LocalFsBackend,
        opts: &SearchOptions,
        buffer_records: &[NativeRecord],
        manifest: &Manifest,
    ) -> Result<SearchResponse> {
        // Run vector search (over-fetch for better fusion).
        let vector_response = self.search_vector(
            metadata,
            storage,
            &SearchOptions {
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
            },
            buffer_records,
            manifest,
        )?;

        // Run keyword search.
        let query_text = opts.query_text.as_deref().unwrap_or("");
        let tombstone_set: HashSet<String> = manifest.tombstone_ids.iter().cloned().collect();
        let keyword_results = keyword::keyword_search(
            metadata,
            &opts.collection_id,
            query_text,
            opts.top_k * 2,
            &tombstone_set,
        )?;

        // Fuse with RRF.
        let mut fused = hybrid::rrf_fuse(
            &vector_response.results,
            &keyword_results,
            opts.top_k,
            opts.vector_weight,
            opts.keyword_weight,
        );

        // Populate explain info for hybrid results.
        if opts.explain {
            let query_text = opts.query_text.as_deref();
            let preview_map = metadata
                .fts_get_texts(&fused.iter().map(|r| r.chunk_id.clone()).collect::<Vec<_>>())
                .unwrap_or_default();

            // Build lookup maps for ranks and scores from each list.
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
                let preview = preview_map
                    .get(&r.chunk_id)
                    .map(|t| t.chars().take(200).collect());

                r.explain = Some(ExplainInfo {
                    vector_score: v_info.map(|(_, s)| *s),
                    bm25_score: k_info.map(|(_, s)| *s),
                    rrf_score: Some(r.score),
                    vector_rank: v_info.map(|(rank, _)| *rank),
                    bm25_rank: k_info.map(|(rank, _)| *rank),
                    chunk_preview: preview,
                    matched_terms: query_text
                        .map(|qt| qt.split_whitespace().map(|s| s.to_string()).collect())
                        .unwrap_or_default(),
                });
            }
        }

        Ok(SearchResponse {
            results: fused,
            manifest_version_used: manifest.version,
        })
    }

    /// Merge, sort, and deduplicate results by descending score.
    fn merge_and_dedup(mut results: Vec<SearchResult>, top_k: usize) -> Vec<SearchResult> {
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.chunk_id.cmp(&b.chunk_id))
        });

        let mut seen = HashSet::new();
        let mut deduped = Vec::new();
        for r in results {
            if seen.contains(&r.chunk_id) {
                continue;
            }
            seen.insert(r.chunk_id.clone());
            deduped.push(r);
            if deduped.len() >= top_k {
                break;
            }
        }
        deduped
    }

    fn resolve_manifest(
        &self,
        metadata: &MetadataStore,
        collection_id: &str,
        version: Option<i64>,
    ) -> Result<Manifest> {
        if let Some(v) = version {
            return metadata
                .get_manifest_by_version(collection_id, v)?
                .ok_or_else(|| {
                AkiDbError::ManifestNotFound(format!(
                    "version {} for collection \"{}\"",
                    v, collection_id
                ))
            });
        }

        metadata
            .get_latest_manifest(collection_id)?
            .ok_or_else(|| {
                AkiDbError::ManifestNotFound(format!(
                    "no manifest published for collection \"{}\"",
                    collection_id
                ))
            })
    }

    fn search_segment(
        &self,
        metadata: &MetadataStore,
        storage: &LocalFsBackend,
        segment_id: &str,
        p: &SegmentSearchParams<'_>,
    ) -> Result<Vec<SearchResult>> {
        let seg_meta = metadata
            .get_segment(segment_id)?
            .ok_or_else(|| AkiDbError::SegmentNotFound(segment_id.to_string()))?;

        self.search_segment_by_path(storage, segment_id, &seg_meta.storage_path, p)
    }

    fn search_segment_by_path(
        &self,
        storage: &LocalFsBackend,
        segment_id: &str,
        storage_path: &str,
        p: &SegmentSearchParams<'_>,
    ) -> Result<Vec<SearchResult>> {
        let SegmentSearchParams {
            query_vector,
            top_k,
            metric,
            tombstone_set,
            filters,
            hnsw,
            manifest_version,
            tombstone_fingerprint,
        } = p;
        let (top_k, hnsw, manifest_version, tombstone_fingerprint) =
            (*top_k, *hnsw, *manifest_version, *tombstone_fingerprint);
        let cache_key = IndexCache::cache_key(segment_id, manifest_version, tombstone_fingerprint);

        // Fast path: unfiltered cache hit — no disk I/O required.
        // The previous code only checked the cache inside `if let Some(f) = filters`,
        // so every unfiltered query bypassed the cache and re-read the segment from disk.
        if filters.is_none() {
            let cached_arc: Option<Arc<CachedIndex>> = {
                let mut cache = self.index_cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.get(&cache_key)
            };
            if let Some(cached) = cached_arc {
                let results = cached.graph.search(query_vector, top_k);
                return Ok(convert_search_results(&results, &cached.active_chunk_ids));
            }
        }

        // Cache miss (or filtered query) — must read the segment from disk.
        let buffer = storage.get_object(storage_path)?;
        let reader = SegmentReader::from_buffer(buffer)?;

        // Filtered cache hit: graph is cached, only need metadata for predicate evaluation.
        if let Some(f) = filters {
            let cached_arc: Option<Arc<CachedIndex>> = {
                let mut cache = self.index_cache.lock().unwrap_or_else(|e| e.into_inner());
                cache.get(&cache_key)
            };
            if let Some(cached) = cached_arc {
                let metadata_list = reader.get_metadata()?;
                let indices =
                    self.apply_metadata_filter(&reader, &cached.node_to_segment, &metadata_list, f);
                if indices.is_empty() {
                    return Ok(Vec::new());
                }
                let filter_set: HashSet<usize> = indices.into_iter().collect();
                let results = cached.graph.search_filtered(query_vector, top_k, |node_id| {
                    let seg_idx = cached.node_to_segment[node_id as usize];
                    filter_set.contains(&seg_idx)
                });
                return Ok(convert_search_results(&results, &cached.active_chunk_ids));
            }
        }

        let chunk_ids = reader.get_chunk_ids()?;

        // Active (non-tombstoned) indices.
        let all_active_indices: Vec<usize> = (0..chunk_ids.len())
            .filter(|i| !tombstone_set.contains(&chunk_ids[*i]))
            .collect();

        if all_active_indices.is_empty() {
            return Ok(Vec::new());
        }

        // Build filter set from metadata filters (bitmap-accelerated or brute-force).
        // Metadata is loaded lazily — only when a filter predicate is present.
        let filter_set: Option<HashSet<usize>> = if let Some(f) = filters {
            let metadata_list = reader.get_metadata()?;
            let indices = self.apply_metadata_filter(&reader, &all_active_indices, &metadata_list, f);
            if indices.is_empty() {
                return Ok(Vec::new());
            }
            Some(indices.into_iter().collect())
        } else {
            None
        };

        let active_chunk_ids: Vec<String> = all_active_indices
            .iter()
            .map(|&i| chunk_ids[i].clone())
            .collect();

        // ── Cache miss path: load vectors and build / deserialize graph ───────
        let vectors = reader.get_vectors()?;
        let active_vectors: Vec<Vec<f32>> =
            all_active_indices.iter().map(|&i| vectors[i].clone()).collect();
        // Maps graph node index → original segment position (for filter closure).
        let node_to_segment: Vec<usize> = all_active_indices.clone();

        let dimension = reader.dimension() as usize;
        let mut graph = HnswGraph::new(metric, dimension, hnsw.m, hnsw.ef_construction, hnsw.ef_search);

        let index_data = reader.get_index_data();
        if !index_data.is_empty() && all_active_indices.len() == chunk_ids.len() {
            if graph.deserialize(index_data).is_err() {
                graph.build(&active_vectors);
            }
        } else {
            graph.build(&active_vectors);
        }

        let search_results = match &filter_set {
            Some(fs) => graph.search_filtered(query_vector, top_k, |node_id| {
                let seg_idx = node_to_segment[node_id as usize];
                fs.contains(&seg_idx)
            }),
            None => graph.search(query_vector, top_k),
        };

        // Cache the graph with its active-set mapping, filter-agnostic and
        // reusable across different filter predicates on the same active set.
        let vector_bytes = active_vectors.len() * dimension * 4;
        let graph_bytes = active_vectors.len() * hnsw.m * 4;
        {
            let mut cache = self.index_cache.lock().unwrap_or_else(|e| e.into_inner());
            cache.set(
                cache_key,
                CachedIndex {
                    graph,
                    active_chunk_ids: active_chunk_ids.clone(),
                    node_to_segment,
                    memory_size_bytes: vector_bytes + graph_bytes,
                },
            );
        }

        Ok(convert_search_results(&search_results, &active_chunk_ids))
    }

    fn apply_metadata_filter(
        &self,
        reader: &SegmentReader,
        active_indices: &[usize],
        metadata_list: &[serde_json::Value],
        filters: &serde_json::Value,
    ) -> Vec<usize> {
        // Separate equality fields (bitmap-eligible) from operator fields ($gt, $lt, etc.).
        // The bitmap index can pre-filter equality matches even in compound filters that
        // also contain operator predicates — narrowing the candidate set before brute-force.
        let (equality_filters, has_operator_filters) =
            if let Some(obj) = filters.as_object() {
                let eq_map: serde_json::Map<String, serde_json::Value> = obj
                    .iter()
                    .filter(|(_, v)| !v.is_object())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let has_ops = obj.values().any(|v| v.is_object());
                (
                    if eq_map.is_empty() { None } else { Some(serde_json::Value::Object(eq_map)) },
                    has_ops,
                )
            } else {
                (None, false)
            };

        // Try bitmap index for equality predicates (even in compound filters).
        let bitmap_candidates: Option<HashSet<usize>> =
            if let Some(eq_f) = &equality_filters
                && let Some(bitmap_index) = reader.get_bitmap_index()
                && let Some(bitmap_result) = bitmap_index.evaluate(eq_f)
            {
                if bitmap_result.is_empty() {
                    return Vec::new();
                }
                let active_set: HashSet<usize> = active_indices.iter().copied().collect();
                Some(bitmap_result.into_iter().filter(|i| active_set.contains(i)).collect())
            } else {
                None
            };

        // If bitmap narrowed the candidate set and there are no operator filters,
        // return the bitmap result directly (fast path).
        if !has_operator_filters && let Some(bitmap_set) = bitmap_candidates {
            return bitmap_set.into_iter().collect();
        }

        // Brute-force over the (possibly bitmap-narrowed) candidate set.
        // Handles operator filters ($gt, $lt, etc.) and any field not in the bitmap index.
        let candidates: Box<dyn Iterator<Item = usize> + '_> =
            if let Some(ref bitmap_set) = bitmap_candidates {
                Box::new(active_indices.iter().copied().filter(|i| bitmap_set.contains(i)))
            } else {
                Box::new(active_indices.iter().copied())
            };

        candidates
            .filter(|&i| {
                // Use get() instead of [] to return a clean non-match rather than
                // panicking if a corrupt segment has fewer metadata entries than chunk IDs.
                metadata_list
                    .get(i)
                    .map(|meta| matches_filter(meta, filters))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn search_buffer_brute_force(
        &self,
        records: &[NativeRecord],
        query_vector: &[f32],
        top_k: usize,
        metric: &str,
        filters: Option<&serde_json::Value>,
        tombstones: &HashSet<String>,
    ) -> Vec<SearchResult> {
        let dist_fn = distance::get_distance_fn(metric);
        let mut results: Vec<SearchResult> = Vec::new();

        for rec in records {
            if tombstones.contains(&rec.chunk_id) {
                continue;
            }

            if let Some(f) = filters
                && !matches_filter(&rec.metadata, f)
            {
                continue;
            }

            let raw_distance = dist_fn(query_vector, &rec.vector);
            let score = distance::distance_to_score(metric, raw_distance) as f64;
            results.push(SearchResult {
                chunk_id: rec.chunk_id.clone(),
                score,
                committed: Some(false),
                explain: None,
            });
        }

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.chunk_id.cmp(&b.chunk_id))
        });
        results.truncate(top_k);
        results
    }

    fn validate_search_opts(&self, opts: &SearchOptions) -> Result<()> {
        if opts.collection_id.is_empty() {
            return Err(AkiDbError::InvalidArgument(
                "collectionId is required".into(),
            ));
        }
        if opts.top_k == 0 {
            return Err(AkiDbError::InvalidArgument(
                "topK must be a positive integer".into(),
            ));
        }
        match opts.mode {
            SearchMode::Vector => {
                if opts.query_vector.is_empty() {
                    return Err(AkiDbError::InvalidArgument(
                        "queryVector must be non-empty for vector search".into(),
                    ));
                }
            }
            SearchMode::Keyword => {
                if opts.query_text.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
                    return Err(AkiDbError::InvalidArgument(
                        "queryText is required for keyword search".into(),
                    ));
                }
            }
            SearchMode::Hybrid => {
                if opts.query_vector.is_empty() {
                    return Err(AkiDbError::InvalidArgument(
                        "queryVector must be non-empty for hybrid search".into(),
                    ));
                }
                if opts.query_text.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
                    return Err(AkiDbError::InvalidArgument(
                        "queryText is required for hybrid search".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn convert_search_results(results: &[(u32, f32)], chunk_ids: &[String]) -> Vec<SearchResult> {
    results
        .iter()
        .filter_map(|(node_id, score)| {
            let chunk_id = chunk_ids.get(*node_id as usize)?;
            Some(SearchResult {
                chunk_id: chunk_id.clone(),
                score: *score as f64,
                committed: None,
                explain: None,
            })
        })
        .collect()
}

pub fn tombstone_fingerprint(tombstone_set: &HashSet<String>) -> u64 {
    let mut tombstone_ids: Vec<&String> = tombstone_set.iter().collect();
    tombstone_ids.sort_unstable();

    let mut hasher = DefaultHasher::new();
    for tombstone_id in tombstone_ids {
        tombstone_id.hash(&mut hasher);
    }
    hasher.finish()
}

fn matches_filter(metadata: &serde_json::Value, filters: &serde_json::Value) -> bool {
    let Some(filter_obj) = filters.as_object() else {
        return true;
    };
    let Some(meta_obj) = metadata.as_object() else {
        return false;
    };

    for (key, expected) in filter_obj {
        let actual = meta_obj.get(key);

        if let Some(op_obj) = expected.as_object() {
            // Operator-based filter: {"$gt": 5, "$lt": 10}
            for (op, op_val) in op_obj {
                if !eval_operator(actual, op.as_str(), op_val) {
                    return false;
                }
            }
        } else if let Some(arr) = expected.as_array() {
            // OR match (backward compat).
            match actual {
                Some(val) if arr.contains(val) => {}
                _ => return false,
            }
        } else if !expected.is_null() {
            // Exact match (backward compat).
            match actual {
                Some(val) if val == expected => {}
                _ => return false,
            }
        }
    }
    true
}

fn eval_operator(
    actual: Option<&serde_json::Value>,
    op: &str,
    op_val: &serde_json::Value,
) -> bool {
    use std::cmp::Ordering;
    match op {
        "$gt" => compare_values(actual, op_val) == Some(Ordering::Greater),
        "$gte" => matches!(
            compare_values(actual, op_val),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        "$lt" => compare_values(actual, op_val) == Some(Ordering::Less),
        "$lte" => matches!(
            compare_values(actual, op_val),
            Some(Ordering::Less | Ordering::Equal)
        ),
        "$ne" => actual.map(|a| a != op_val).unwrap_or(true),
        "$in" => {
            let Some(arr) = op_val.as_array() else {
                return false;
            };
            actual.map(|a| arr.contains(a)).unwrap_or(false)
        }
        "$nin" => {
            let Some(arr) = op_val.as_array() else {
                return true;
            };
            actual.map(|a| !arr.contains(a)).unwrap_or(true)
        }
        _ => false, // Unknown operators do not match — prevents silent data leaks.
    }
}

fn compare_values(
    actual: Option<&serde_json::Value>,
    expected: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    let actual = actual?;
    match (actual, expected) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            a.as_f64()?.partial_cmp(&b.as_f64()?)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_filter_gt_lt_range() {
        let meta = json!({"year": 2022, "status": "published"});
        // $gte 2020, $lt 2025 → 2022 matches.
        assert!(matches_filter(&meta, &json!({"year": {"$gte": 2020, "$lt": 2025}})));
        // $gte 2023 → 2022 fails.
        assert!(!matches_filter(&meta, &json!({"year": {"$gte": 2023}})));
        // $gt 2022 → 2022 fails (strict greater).
        assert!(!matches_filter(&meta, &json!({"year": {"$gt": 2022}})));
        // $gte 2022 → 2022 passes.
        assert!(matches_filter(&meta, &json!({"year": {"$gte": 2022}})));
        // $lte 2022 → passes.
        assert!(matches_filter(&meta, &json!({"year": {"$lte": 2022}})));
        // $lt 2022 → fails (strict less).
        assert!(!matches_filter(&meta, &json!({"year": {"$lt": 2022}})));
    }

    #[test]
    fn matches_filter_ne() {
        let meta = json!({"status": "published", "category": "science"});
        // $ne "draft" → "published" is not "draft", passes.
        assert!(matches_filter(&meta, &json!({"status": {"$ne": "draft"}})));
        // $ne "published" → fails.
        assert!(!matches_filter(&meta, &json!({"status": {"$ne": "published"}})));
        // $ne on missing field → passes (null != value).
        assert!(matches_filter(&meta, &json!({"missing": {"$ne": "anything"}})));
    }

    #[test]
    fn matches_filter_in_nin() {
        let meta = json!({"tag": "science", "year": 2022});
        // $in with matching value.
        assert!(matches_filter(&meta, &json!({"tag": {"$in": ["science", "math"]}})));
        // $in without matching value.
        assert!(!matches_filter(&meta, &json!({"tag": {"$in": ["history", "art"]}})));
        // $nin excludes matching value.
        assert!(!matches_filter(&meta, &json!({"tag": {"$nin": ["science", "math"]}})));
        // $nin passes when value not in exclusion list.
        assert!(matches_filter(&meta, &json!({"tag": {"$nin": ["history", "art"]}})));
        // $in on missing field → fails.
        assert!(!matches_filter(&meta, &json!({"missing": {"$in": ["a"]}})));
        // $nin on missing field → passes.
        assert!(matches_filter(&meta, &json!({"missing": {"$nin": ["a"]}})));
    }

    #[test]
    fn matches_filter_backward_compat() {
        let meta = json!({"source": "a.pdf", "category": "science"});
        // Exact match (bare value).
        assert!(matches_filter(&meta, &json!({"source": "a.pdf"})));
        assert!(!matches_filter(&meta, &json!({"source": "b.pdf"})));
        // OR match (bare array).
        assert!(matches_filter(&meta, &json!({"category": ["science", "math"]})));
        assert!(!matches_filter(&meta, &json!({"category": ["history"]})));
        // Null filter value → skipped.
        assert!(matches_filter(&meta, &json!({"source": null})));
    }

    #[test]
    fn matches_filter_mixed_operators_and_exact() {
        let meta = json!({"source": "a.pdf", "year": 2023, "status": "published"});
        // Mix exact match + operator.
        assert!(matches_filter(
            &meta,
            &json!({"source": "a.pdf", "year": {"$gt": 2020}})
        ));
        // Exact fails, operator would pass.
        assert!(!matches_filter(
            &meta,
            &json!({"source": "b.pdf", "year": {"$gt": 2020}})
        ));
        // Exact passes, operator fails.
        assert!(!matches_filter(
            &meta,
            &json!({"source": "a.pdf", "year": {"$gt": 2025}})
        ));
    }

    #[test]
    fn matches_filter_unknown_operator_rejects() {
        let meta = json!({"year": 2022});
        // Unknown operator $foo does NOT silently pass — prevents data leaks.
        assert!(!matches_filter(&meta, &json!({"year": {"$foo": 999}})));
        // Unknown + known: $foo rejects, so compound filter is false regardless of $gt.
        assert!(!matches_filter(&meta, &json!({"year": {"$foo": 999, "$gt": 2020}})));
        assert!(!matches_filter(&meta, &json!({"year": {"$foo": 999, "$gt": 2025}})));
    }

    #[test]
    fn matches_filter_string_comparison() {
        let meta = json!({"name": "charlie"});
        assert!(matches_filter(&meta, &json!({"name": {"$gt": "bob"}})));
        assert!(!matches_filter(&meta, &json!({"name": {"$gt": "dave"}})));
        assert!(matches_filter(&meta, &json!({"name": {"$gte": "charlie"}})));
        assert!(matches_filter(&meta, &json!({"name": {"$lt": "dave"}})));
    }

    #[test]
    fn matches_filter_type_mismatch_returns_no_match() {
        let meta = json!({"year": 2022});
        // Comparing number to string → no match (safe default).
        assert!(!matches_filter(&meta, &json!({"year": {"$gt": "2020"}})));
    }
}
