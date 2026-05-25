// ─── Module declarations ─────────────────────────────────────────────────────
mod collection;
mod compaction;
mod distance;
mod engine;
mod error;
mod fp16;
mod hnsw;
mod index;
mod manifest;
mod metadata;
mod query;
mod segment;
mod storage;
mod wal;
mod write;

pub use crate::engine::{EngineInner, EngineOptions};
pub use crate::error::AkiDbError;
pub use crate::metadata::{Collection, Manifest};
pub use crate::query::{SearchMode, SearchOptions};
pub use crate::write::NativeRecord;
#[doc(hidden)]
pub mod bench_support {
    pub use crate::distance::{cosine_distance, dot_product, l2_distance, normalize};
    pub use crate::hnsw::HnswGraph;
    pub use crate::write::NativeRecord;
}

#[cfg(feature = "node-api")]
mod node_api {
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::engine::{EngineInner, EngineOptions};
use crate::write::NativeRecord;

// ═════════════════════════════════════════════════════════════════════════════
// AkiDbEngine — the new v2.5 NAPI surface (full engine)
// ═════════════════════════════════════════════════════════════════════════════

/// NAPI object types for the AkiDB engine.
#[napi(object)]
pub struct EngineOptionsJs {
    pub storage_path: String,
    pub disable_wal: Option<bool>,
}

#[napi(object)]
pub struct CollectionJs {
    pub collection_id: String,
    pub dimension: i64,
    pub metric: String,
    pub embedding_model_id: String,
    pub schema_version: String,
    pub created_at: String,
    pub deleted_at: Option<String>,
    pub quantization: String,
    pub hnsw_m: i64,
    pub hnsw_ef_construction: i64,
    pub hnsw_ef_search: i64,
}

#[napi(object)]
pub struct CreateCollectionOptsJs {
    pub collection_id: String,
    pub dimension: i64,
    pub metric: String,
    pub embedding_model_id: String,
    /// Vector quantization: "fp16" (default) or "sq8".
    pub quantization: Option<String>,
    /// HNSW M parameter (max connections per node). Default: 16, range: 4-64.
    pub hnsw_m: Option<i64>,
    /// HNSW efConstruction parameter. Default: 200, range: 50-800.
    pub hnsw_ef_construction: Option<i64>,
    /// HNSW efSearch parameter. Default: 100, range: 10-500.
    pub hnsw_ef_search: Option<i64>,
}

#[napi(object)]
pub struct RecordJs {
    pub chunk_id: String,
    pub doc_id: String,
    pub vector: Vec<f64>,
    pub metadata_json: String,
    pub chunk_text: Option<String>,
}

#[napi(object)]
pub struct UpsertResultJs {
    pub segment_ids: Vec<String>,
    pub buffered_count: i64,
}

#[napi(object)]
pub struct SearchOptsJs {
    pub collection_id: String,
    pub query_vector: Vec<f64>,
    pub top_k: i64,
    pub filters_json: Option<String>,
    pub manifest_version: Option<i64>,
    pub include_uncommitted: Option<bool>,
    /// Search mode: "vector" (default), "keyword", or "hybrid".
    pub mode: Option<String>,
    /// Query text for keyword/hybrid search.
    pub query_text: Option<String>,
    /// Weight for vector results in hybrid RRF fusion (default 1.0).
    pub vector_weight: Option<f64>,
    /// Weight for keyword results in hybrid RRF fusion (default 1.0).
    pub keyword_weight: Option<f64>,
    /// When true, include per-result scoring breakdown.
    pub explain: Option<bool>,
    /// Per-query ef_search override (range: 10-500). Overrides collection default.
    pub ef_search: Option<i64>,
}

#[napi(object)]
pub struct ExplainInfoJs {
    pub vector_score: Option<f64>,
    pub bm25_score: Option<f64>,
    pub rrf_score: Option<f64>,
    pub vector_rank: Option<i64>,
    pub bm25_rank: Option<i64>,
    pub chunk_preview: Option<String>,
    pub matched_terms: Vec<String>,
}

#[napi(object)]
pub struct SearchResultEngineJs {
    pub chunk_id: String,
    pub score: f64,
    pub committed: Option<bool>,
    pub explain: Option<ExplainInfoJs>,
}

#[napi(object)]
pub struct SearchResponseJs {
    pub results: Vec<SearchResultEngineJs>,
    pub manifest_version_used: i64,
}

#[napi(object)]
pub struct PublishOptsJs {
    pub segment_ids: Vec<String>,
    pub tombstone_ids: Vec<String>,
    pub embedding_model_id: String,
    pub pipeline_signature: String,
}

#[napi(object)]
pub struct ManifestJs {
    pub manifest_id: String,
    pub collection_id: String,
    pub version: i64,
    pub segment_ids: Vec<String>,
    pub tombstone_ids: Vec<String>,
    pub embedding_model_id: String,
    pub pipeline_signature: String,
    pub created_at: String,
    pub checksum: String,
}

#[napi(object)]
pub struct CompactResultJs {
    pub records_kept: i64,
    pub records_removed: i64,
    pub space_reclaimed_bytes: i64,
    pub manifest: ManifestJs,
}

/// The main AkiDB engine exposed to Node.js via NAPI.
/// Thread-safe via internal subsystem locks.
#[napi]
pub struct AkiDbEngine {
    inner: EngineInner,
}

#[napi]
impl AkiDbEngine {
    #[napi(constructor)]
    pub fn new(opts: EngineOptionsJs) -> Result<Self> {
        let engine = EngineInner::open(EngineOptions {
            storage_path: opts.storage_path.into(),
            disable_wal: opts.disable_wal.unwrap_or(false),
        })
        .map_err(napi::Error::from)?;

        Ok(Self {
            inner: engine,
        })
    }

    // ─── Collection (sync, read/write lock) ─────────────────────────────────

    #[napi]
    pub fn create_collection(&self, opts: CreateCollectionOptsJs) -> Result<CollectionJs> {
        let c = self.inner
            .create_collection(
                &opts.collection_id,
                opts.dimension,
                &opts.metric,
                &opts.embedding_model_id,
                opts.quantization.as_deref().unwrap_or("fp16"),
                opts.hnsw_m.unwrap_or(16),
                opts.hnsw_ef_construction.unwrap_or(200),
                opts.hnsw_ef_search.unwrap_or(100),
            )
            .map_err(napi::Error::from)?;
        Ok(collection_to_js(c))
    }

    #[napi]
    pub fn get_collection(&self, collection_id: String) -> Result<Option<CollectionJs>> {
        let c = self.inner.get_collection(&collection_id).map_err(napi::Error::from)?;
        Ok(c.map(collection_to_js))
    }

    #[napi]
    pub fn list_collections(&self) -> Result<Vec<CollectionJs>> {
        let collections = self.inner.list_collections().map_err(napi::Error::from)?;
        Ok(collections.into_iter().map(collection_to_js).collect())
    }

    #[napi]
    pub fn delete_collection(&self, collection_id: String) -> Result<()> {
        self.inner.delete_collection(&collection_id).map_err(napi::Error::from)
    }

    // ─── Write (sync, write lock) ───────────────────────────────────────────

    #[napi]
    pub fn upsert_batch(&self, collection_id: String, records: Vec<RecordJs>) -> Result<UpsertResultJs> {
        let records: Vec<NativeRecord> = records
            .into_iter()
            .map(record_js_to_native)
            .collect::<Result<Vec<_>>>()?;

        let result = self.inner
            .upsert_batch(&collection_id, &records)
            .map_err(napi::Error::from)?;

        Ok(UpsertResultJs {
            segment_ids: result.segment_ids,
            buffered_count: result.buffered_count as i64,
        })
    }

    #[napi]
    pub fn flush_writes(&self, collection_id: String) -> Result<Vec<String>> {
        self.inner.flush_writes(&collection_id).map_err(napi::Error::from)
    }

    // ─── Publish (sync, write lock) ─────────────────────────────────────────

    #[napi]
    pub fn publish(&self, collection_id: String, opts: PublishOptsJs) -> Result<ManifestJs> {
        let m = self.inner
            .publish(
                &collection_id,
                opts.segment_ids,
                opts.tombstone_ids,
                &opts.embedding_model_id,
                &opts.pipeline_signature,
            )
            .map_err(napi::Error::from)?;
        Ok(manifest_to_js(m))
    }

    #[napi]
    pub fn auto_publish(
        &self,
        collection_id: String,
        embedding_model_id: String,
        pipeline_signature: String,
    ) -> Result<ManifestJs> {
        let m = self.inner
            .auto_publish(&collection_id, &embedding_model_id, &pipeline_signature)
            .map_err(napi::Error::from)?;
        Ok(manifest_to_js(m))
    }

    // ─── Search (sync, read lock — cache uses internal Mutex) ───────────────

    #[napi]
    pub fn search(&self, opts: SearchOptsJs) -> Result<SearchResponseJs> {
        let filters = match opts.filters_json {
            Some(s) => Some(
                serde_json::from_str(&s)
                    .map_err(|e| Error::from_reason(format!("Invalid filters JSON: {e}")))?,
            ),
            None => None,
        };

        let query_vector: Vec<f32> = opts.query_vector.iter().map(|&f| f as f32).collect();
        let mode = crate::query::SearchMode::from_str(opts.mode.as_deref().unwrap_or("vector"));

        let response = self.inner
            .search(crate::query::SearchOptions {
                collection_id: opts.collection_id,
                query_vector,
                top_k: opts.top_k as usize,
                filters,
                manifest_version: opts.manifest_version,
                include_uncommitted: opts.include_uncommitted.unwrap_or(true),
                mode,
                query_text: opts.query_text,
                vector_weight: opts.vector_weight.unwrap_or(1.0),
                keyword_weight: opts.keyword_weight.unwrap_or(1.0),
                explain: opts.explain.unwrap_or(false),
                ef_search: opts.ef_search.map(|v| v as usize),
            })
            .map_err(napi::Error::from)?;

        Ok(SearchResponseJs {
            results: response
                .results
                .into_iter()
                .map(|r| SearchResultEngineJs {
                    chunk_id: r.chunk_id,
                    score: r.score,
                    committed: r.committed,
                    explain: r.explain.map(|e| ExplainInfoJs {
                        vector_score: e.vector_score,
                        bm25_score: e.bm25_score,
                        rrf_score: e.rrf_score,
                        vector_rank: e.vector_rank.map(|v| v as i64),
                        bm25_rank: e.bm25_rank.map(|v| v as i64),
                        chunk_preview: e.chunk_preview,
                        matched_terms: e.matched_terms,
                    }),
                })
                .collect(),
            manifest_version_used: response.manifest_version_used,
        })
    }

    // ─── Delete (sync, write lock) ──────────────────────────────────────────

    #[napi]
    pub fn delete_chunks(
        &self,
        collection_id: String,
        chunk_ids: Vec<String>,
        reason_code: String,
    ) -> Result<i64> {
        let count = self.inner
            .delete_chunks(&collection_id, &chunk_ids, &reason_code)
            .map_err(napi::Error::from)?;
        Ok(count as i64)
    }

    // ─── Compact (sync, write lock) ─────────────────────────────────────────

    #[napi]
    pub fn compact(&self, collection_id: String) -> Result<CompactResultJs> {
        let result = self.inner.compact(&collection_id).map_err(napi::Error::from)?;
        Ok(CompactResultJs {
            records_kept: result.records_kept as i64,
            records_removed: result.records_removed as i64,
            space_reclaimed_bytes: result.space_reclaimed_bytes,
            manifest: manifest_to_js(result.manifest),
        })
    }

    // ─── Rollback (sync, write lock) ────────────────────────────────────────

    #[napi]
    pub fn rollback(&self, collection_id: String, manifest_id: String) -> Result<ManifestJs> {
        let m = self.inner
            .rollback(&collection_id, &manifest_id)
            .map_err(napi::Error::from)?;
        Ok(manifest_to_js(m))
    }

    // ─── Introspection (sync, read lock) ────────────────────────────────────

    #[napi]
    pub fn get_storage_size_bytes(&self) -> Result<i64> {
        let size = self.inner.get_storage_size_bytes().map_err(napi::Error::from)?;
        Ok(size as i64)
    }

    #[napi]
    pub fn get_tombstone_count(&self, collection_id: String) -> Result<i64> {
        self.inner
            .get_tombstone_count(&collection_id)
            .map_err(napi::Error::from)
    }

    #[napi]
    pub fn get_segment_count(&self, collection_id: String) -> Result<i64> {
        self.inner
            .get_segment_count(&collection_id)
            .map_err(napi::Error::from)
    }

    // ─── Close ──────────────────────────────────────────────────────────────

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner.close().map_err(napi::Error::from)
    }
}

fn record_js_to_native(record: RecordJs) -> Result<NativeRecord> {
    let metadata = serde_json::from_str(&record.metadata_json)
        .map_err(|e| Error::from_reason(format!("Invalid metadata JSON for {}: {e}", record.chunk_id)))?;
    Ok(NativeRecord {
        chunk_id: record.chunk_id,
        doc_id: record.doc_id,
        vector: record.vector.into_iter().map(|value| value as f32).collect(),
        metadata,
        chunk_text: record.chunk_text,
    })
}

fn collection_to_js(c: crate::metadata::Collection) -> CollectionJs {
    CollectionJs {
        collection_id: c.collection_id,
        dimension: c.dimension,
        metric: c.metric,
        embedding_model_id: c.embedding_model_id,
        schema_version: c.schema_version,
        created_at: c.created_at,
        deleted_at: c.deleted_at,
        quantization: c.quantization,
        hnsw_m: c.hnsw_m,
        hnsw_ef_construction: c.hnsw_ef_construction,
        hnsw_ef_search: c.hnsw_ef_search,
    }
}

fn manifest_to_js(m: crate::metadata::Manifest) -> ManifestJs {
    ManifestJs {
        manifest_id: m.manifest_id,
        collection_id: m.collection_id,
        version: m.version,
        segment_ids: m.segment_ids,
        tombstone_ids: m.tombstone_ids,
        embedding_model_id: m.embedding_model_id,
        pipeline_signature: m.pipeline_signature,
        created_at: m.created_at,
        checksum: m.checksum,
    }
}
}
